-- TLS: configuration validation, and a real encrypted connection driven by
-- the reactor.
--
-- The configuration half matters because a TLS block that silently does not
-- apply is worse than no TLS at all — the operator believes the port is
-- encrypted. Every one of those cases is a hard error at load, and each is
-- pinned below.
--
-- The connection half matters because the interesting part of TLS on this
-- codebase is not the crypto, it is the event loop: a non-blocking TLS read
-- can need the socket to become WRITABLE (and vice versa), which no plaintext
-- socket ever does. So these run a genuine handshake and genuine traffic
-- through Reactor:read_exact / send_all, including a payload large enough to
-- span many TLS records and force partial writes — the exact shape that
-- breaks when want-states are mishandled.

local tls_m   = require("src.io.tls")
local Reactor = require("src.server.reactor")
local socket  = require("socket")
local os_utils = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_tls_test"
                                      or "/tmp/moonmq_tls_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function exists(path)
    local f = io.open(path, "rb")
    if f then f:close(); return true end
    return false
end

-- A throwaway self-signed certificate for 127.0.0.1/localhost. Generated
-- rather than committed: a private key in the repository is a key that ends up
-- trusted somewhere it should not be, and `openssl req` is present anywhere
-- luasec built.
local CERT = BASE_DIR .. "/cert.pem"
local KEY  = BASE_DIR .. "/key.pem"
local OTHER_CERT = BASE_DIR .. "/other-cert.pem"
local OTHER_KEY  = BASE_DIR .. "/other-key.pem"

local function generate(cert, key, cn)
    os.execute(string.format(
        "openssl req -x509 -newkey rsa:2048 -nodes -days 2 "
        .. "-subj '/CN=%s' -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' "
        .. "-keyout '%s' -out '%s' >/dev/null 2>&1", cn, key, cert))
    return exists(cert) and exists(key)
end

local have_certs = false
if tls_m.available and not os_utils.IS_WINDOWS then
    rmdir(BASE_DIR)
    os.execute(string.format("mkdir -p '%s'", BASE_DIR))
    have_certs = generate(CERT, KEY, "localhost")
                 and generate(OTHER_CERT, OTHER_KEY, "localhost")
end

describe("tls configuration", function()

    it("returns nothing when no block is given", function()
        assert.is_nil((tls_m.server_config(nil, "Server.Tls")))
        assert.is_nil((tls_m.server_config({ Enabled = false }, "Server.Tls")))
        -- Both halves of the return are nil: no config AND no error, which is
        -- how callers tell "not asked for" from "asked for and broken".
        local cfg, err = tls_m.server_config(nil, "Server.Tls")
        assert.is_nil(cfg)
        assert.is_nil(err)
    end)

    it("treats a block with no Enabled key as enabled", function()
        -- Writing out cert paths and getting plaintext because a flag was
        -- missing is the trap this avoids.
        if not have_certs then return end
        local cfg = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY }, "Server.Tls"))
        assert.are.equal(CERT, cfg.params.certificate)
        assert.are.equal("server", cfg.params.mode)
    end)

    it("requires a certificate and key for a listener", function()
        local cfg, err = tls_m.server_config({ Enabled = true }, "Server.Tls")
        assert.is_nil(cfg)
        assert.is_truthy(err:find("CertFile and KeyFile"))
        assert.is_truthy(err:find("Server.Tls"), "the error names the listener")
    end)

    it("rejects files it cannot read", function()
        local cfg, err = tls_m.server_config(
            { CertFile = BASE_DIR .. "/nope.pem", KeyFile = KEY }, "Server.Tls")
        assert.is_nil(cfg)
        assert.is_truthy(err:find("cannot read CertFile"))
    end)

    it("rejects an unknown Verify mode", function()
        if not have_certs then return end
        local cfg, err = tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY, Verify = "sometimes" }, "Server.Tls")
        assert.is_nil(cfg)
        assert.is_truthy(err:find("none, peer or required"))
    end)

    it("refuses to verify with nothing to verify against", function()
        if not have_certs then return end
        local cfg, err = tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY, Verify = "required" }, "Server.Tls")
        assert.is_nil(cfg)
        assert.is_truthy(err:find("CaFile"))
    end)

    it("maps Verify onto luasec's verify flags", function()
        if not have_certs then return end
        local none = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY }, "T"))
        assert.are.same({ "none" }, none.params.verify)

        local peer = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY, CaFile = CERT, Verify = "peer" }, "T"))
        assert.are.same({ "peer" }, peer.params.verify)

        local required = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY, CaFile = CERT,
              Verify = "required" }, "T"))
        assert.are.same({ "peer", "fail_if_no_peer_cert" }, required.params.verify)
    end)

    it("defaults a client to verifying and a server to not demanding", function()
        if not have_certs then return end
        local client = assert(tls_m.client_config({ CaFile = CERT }, "T"))
        assert.are.equal("peer", client.verify)

        local server = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY }, "T"))
        assert.are.equal("none", server.verify)
    end)

    it("accepts insecure as an explicit opt-out", function()
        local cfg = assert(tls_m.client_config({ Insecure = true }, "T"))
        assert.are.equal("none", cfg.verify)
        assert.are.same({ "none" }, cfg.params.verify)
    end)

    it("accepts lowercase keys, for callers configuring in Lua", function()
        if not have_certs then return end
        local cfg = assert(tls_m.client_config(
            { cafile = CERT, verify = "peer", server_name = "localhost" }, "T"))
        assert.are.equal(CERT, cfg.params.cafile)
        assert.are.equal("localhost", cfg.server_name)
    end)

    it("accepts `true` as shorthand, verifying against the system CA store", function()
        -- luasec has no default trust store, so without this a bare
        -- `tls = true` would either fail to start or — far worse — have to
        -- default to not verifying anything.
        local cfg, err = tls_m.client_config(true, "T")
        if not cfg and err and err:find("CaFile") then
            return   -- no system bundle on this host; the error is correct
        end
        assert.is_truthy(cfg, err)
        assert.are.equal("client", cfg.params.mode)
        assert.are.equal("peer", cfg.verify)
        assert.is_truthy(cfg.params.cafile, "should have found a system CA bundle")
    end)

    it("never falls back to the system store for a listener", function()
        -- Verify=required on a listener means mTLS against a private CA.
        -- Accepting anything signed by a public CA on the box instead would
        -- be a silent, severe widening.
        if not have_certs then return end
        local cfg, err = tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY, Verify = "required" }, "T")
        assert.is_nil(cfg)
        assert.is_truthy(err:find("CaFile"))
    end)

    it("excludes the broken protocol versions", function()
        if not have_certs then return end
        local cfg = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY }, "T"))
        local opts = {}
        for _, o in ipairs(cfg.params.options) do opts[o] = true end
        for _, banned in ipairs({ "no_sslv2", "no_sslv3", "no_tlsv1", "no_tlsv1_1" }) do
            assert.is_true(opts[banned], banned .. " must be disabled")
        end
        -- "any" rather than a pinned version, so TLS 1.3 is still reachable.
        assert.are.equal("any", cfg.params.protocol)
    end)

    it("validates the handshake timeout", function()
        if not have_certs then return end
        local cfg, err = tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY, HandshakeTimeout = 0 }, "T")
        assert.is_nil(cfg)
        assert.is_truthy(err:find("HandshakeTimeout"))
    end)

    it("describes a listener for the boot log", function()
        assert.are.equal("plaintext", tls_m.describe(nil))
        if not have_certs then return end
        local cfg = assert(tls_m.server_config({ CertFile = CERT, KeyFile = KEY }, "T"))
        assert.is_truthy(tls_m.describe(cfg):find("TLS"))
        assert.is_truthy(tls_m.describe(cfg):find("verify=none"))
    end)
end)

describe("tls over the reactor", function()

    if not tls_m.available or not have_certs then
        pending("luasec and openssl are needed for the TLS connection tests")
        return
    end

    -- Run one client exchange against a TLS echo listener, entirely inside the
    -- reactor. Returns whatever the client function returned.
    --
    -- Both halves go through the reactor's own I/O, so the want-state handling
    -- in read_exact/send_all is on the hot path of every case below.
    local function exchange(server_block, client_block, client_fn, port)
        local reactor = Reactor.new()
        local server_cfg = assert(tls_m.server_config(server_block, "test server"))
        local result, failure

        local _, lerr = reactor:listen("127.0.0.1", port, function(sock)
            -- Echo server: read a length prefix, then that many bytes back.
            local header = reactor:read_exact(sock, 4, socket.gettime() + 5)
            if header then
                local n = string.unpack(">I4", header)
                local body = reactor:read_exact(sock, n, socket.gettime() + 10)
                if body then
                    reactor:send_all(sock, header .. body, socket.gettime() + 10)
                end
            end
            pcall(function() sock:close() end)
        end, { tls = server_cfg })
        assert.is_nil(lerr)

        reactor:spawn(function()
            local ok, err = pcall(function()
                local raw = assert(socket.connect("127.0.0.1", port))
                local cfg = assert(tls_m.client_config(client_block, "test client"))
                local sock, herr = reactor:tls_handshake(
                    raw, cfg.params, socket.gettime() + 5)
                if not sock then
                    result = { handshake_error = tostring(herr) }
                    return
                end
                result = client_fn(reactor, sock)
                pcall(function() sock:close() end)
            end)
            if not ok then failure = err end
            reactor:stop()
        end)

        -- Safety net: never hang the suite if something goes wrong.
        reactor:spawn(function()
            reactor:sleep(20)
            reactor:stop()
        end)

        reactor:run()
        reactor:shutdown()

        if failure then error(failure) end
        return result
    end

    local function echo_once(payload)
        return function(reactor, sock)
            local ok, err = reactor:send_all(sock,
                string.pack(">I4", #payload) .. payload, socket.gettime() + 10)
            if not ok then return { error = tostring(err) } end
            local header = reactor:read_exact(sock, 4, socket.gettime() + 10)
            if not header then return { error = "no header" } end
            local n = string.unpack(">I4", header)
            local body = reactor:read_exact(sock, n, socket.gettime() + 10)
            return { echoed = body }
        end
    end

    it("completes a handshake and carries data", function()
        local out = exchange(
            { CertFile = CERT, KeyFile = KEY },
            { CaFile = CERT, Verify = "peer" },
            echo_once("hello over tls"), 19301)
        assert.is_nil(out.handshake_error)
        assert.are.equal("hello over tls", out.echoed)
    end)

    it("carries a payload spanning many TLS records", function()
        -- A TLS record holds at most 16 KiB, so this is ~64 records. It is the
        -- case that exercises partial writes and the want-state loops: a
        -- reactor that mishandled "wantwrite" would truncate or hang here
        -- rather than fail cleanly.
        local payload = string.rep("MoonMQ/", 150000)   -- ~1 MB
        local out = exchange(
            { CertFile = CERT, KeyFile = KEY },
            { CaFile = CERT, Verify = "peer" },
            echo_once(payload), 19302)
        assert.is_nil(out.handshake_error)
        assert.are.equal(#payload, #(out.echoed or ""))
        assert.are.equal(payload, out.echoed)
    end)

    it("refuses a server whose certificate is signed by another CA", function()
        -- The client trusts OTHER_CERT; the listener presents CERT.
        local out = exchange(
            { CertFile = CERT, KeyFile = KEY },
            { CaFile = OTHER_CERT, Verify = "peer" },
            echo_once("should not arrive"), 19303)
        assert.is_truthy(out.handshake_error,
            "an untrusted certificate must fail the handshake")
        assert.is_nil(out.echoed)
    end)

    it("connects to an untrusted server when verification is off", function()
        -- The same pairing as above, with Insecure — which is exactly why
        -- Insecure has to be spelled out rather than defaulted.
        local out = exchange(
            { CertFile = CERT, KeyFile = KEY },
            { Insecure = true },
            echo_once("dev mode"), 19304)
        assert.is_nil(out.handshake_error)
        assert.are.equal("dev mode", out.echoed)
    end)

    it("refuses a client with no certificate when Verify=required (mTLS)", function()
        local out = exchange(
            { CertFile = CERT, KeyFile = KEY, CaFile = CERT, Verify = "required" },
            { CaFile = CERT, Verify = "peer" },
            echo_once("no client cert"), 19305)

        -- The assertion is on the OUTCOME, not on which side notices first.
        -- Under TLS 1.3 the client finishes its handshake without waiting for
        -- the server's verdict, so the refusal surfaces as a dead connection
        -- on the next read rather than as a handshake error — either way, no
        -- data crosses, which is the property that matters.
        assert.is_nil(out.echoed,
            "a listener demanding client certs must not carry data without one")
    end)

    it("accepts a client presenting a trusted certificate (mTLS)", function()
        local out = exchange(
            { CertFile = CERT, KeyFile = KEY, CaFile = CERT, Verify = "required" },
            { CaFile = CERT, CertFile = CERT, KeyFile = KEY, Verify = "peer" },
            echo_once("mutual"), 19306)
        assert.is_nil(out.handshake_error)
        assert.are.equal("mutual", out.echoed)
    end)

    it("refuses a plaintext client on a TLS listener", function()
        -- Not a crash, and not a hang: the handshake fails and the connection
        -- is dropped, which is what an accidental `http://` on a TLS port
        -- should look like.
        local reactor = Reactor.new()
        local server_cfg = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY }, "test server"))
        local reached_handler = false

        assert(reactor:listen("127.0.0.1", 19307, function(sock)
            reached_handler = true
            pcall(function() sock:close() end)
        end, { tls = server_cfg }))

        local closed
        reactor:spawn(function()
            local raw = assert(socket.connect("127.0.0.1", 19307))
            raw:settimeout(3)
            raw:send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            -- The broker answers a non-TLS hello by dropping the connection.
            local _, err = raw:receive(1)
            closed = err
            raw:close()
            reactor:stop()
        end)
        reactor:spawn(function() reactor:sleep(10); reactor:stop() end)
        reactor:run()
        reactor:shutdown()

        assert.is_false(reached_handler,
            "a failed handshake must never reach the connection handler")
        assert.is_truthy(closed)
    end)

    it("closes the socket when a handshake fails", function()
        -- A failed handshake is the COMMON case on a public port — a plaintext
        -- client, a scanner, a peer that dies mid-negotiation. Leaking the
        -- descriptor on that path is a slow, self-inflicted fd exhaustion, so
        -- this counts them directly.
        local function open_fds()
            local p = io.popen("ls /proc/self/fd 2>/dev/null | wc -l")
            if not p then return nil end
            local n = tonumber(p:read("*a")) or 0
            p:close()
            return n
        end

        if not open_fds() then return end   -- no /proc; nothing to measure

        local reactor = Reactor.new()
        local server_cfg = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY }, "test server"))
        assert(reactor:listen("127.0.0.1", 19309, function(sock)
            pcall(function() sock:close() end)
        end, { tls = server_cfg }))

        local attempts, before = 25, nil
        reactor:spawn(function()
            for i = 1, attempts do
                local raw = socket.connect("127.0.0.1", 19309)
                if raw then
                    raw:settimeout(0.2)
                    raw:send("not a client hello\n")
                    raw:receive(1)
                    raw:close()
                end
                reactor:sleep(0.02)
                -- Measure after the first few, so one-off allocations (the
                -- listener, the SSL context) are not counted as growth.
                if i == 5 then before = open_fds() end
            end
            reactor:sleep(0.2)
            reactor:stop()
        end)
        reactor:spawn(function() reactor:sleep(15); reactor:stop() end)
        reactor:run()

        local after = open_fds()
        reactor:shutdown()

        assert.is_truthy(before)
        -- 20 more failed handshakes after the baseline. A leak would show up
        -- as ~20 extra descriptors; a couple of transient ones is noise.
        assert.is_true(after - before < 5,
            string.format("descriptors grew by %d across %d failed handshakes",
                after - before, attempts - 5))
    end)

    it("runs pre_tls before the handshake, so a refusal costs nothing", function()
        local reactor = Reactor.new()
        local server_cfg = assert(tls_m.server_config(
            { CertFile = CERT, KeyFile = KEY }, "test server"))
        local pre_called, handshaken = false, false

        assert(reactor:listen("127.0.0.1", 19308, function()
            handshaken = true
        end, {
            tls = server_cfg,
            pre_tls = function(_sock, _peer, ip)
                pre_called = ip
                return false   -- stand in for a banned IP
            end,
        }))

        reactor:spawn(function()
            local raw = assert(socket.connect("127.0.0.1", 19308))
            raw:settimeout(2)
            raw:receive(1)     -- expect an immediate close
            raw:close()
            reactor:sleep(0.1)
            reactor:stop()
        end)
        reactor:spawn(function() reactor:sleep(10); reactor:stop() end)
        reactor:run()
        reactor:shutdown()

        assert.are.equal("127.0.0.1", pre_called)
        assert.is_false(handshaken,
            "pre_tls must be able to refuse before any TLS work happens")
    end)
end)

describe("reactor want-state handling", function()

    it("parks on the direction TLS asks for, not the one the caller wanted", function()
        -- The whole TLS/event-loop interaction reduces to this: a read that
        -- reports "wantwrite" has to wait for WRITABILITY. Waiting for
        -- readability instead would hang until the deadline on every
        -- renegotiation.
        local reactor = Reactor.new()
        local sock = { name = "fake" }

        local co = coroutine.create(function()
            reactor:park(sock, "wantwrite", "read")
        end)
        coroutine.resume(co)
        assert.are.equal(co, reactor.write_waiters[sock])
        assert.is_nil(reactor.read_waiters[sock])

        local co2 = coroutine.create(function()
            reactor:park(sock, "wantread", "write")
        end)
        coroutine.resume(co2)
        assert.are.equal(co2, reactor.read_waiters[sock])
    end)

    it("falls back to the caller's direction for a plain timeout", function()
        local reactor = Reactor.new()
        local a, b = { name = "a" }, { name = "b" }

        local reader = coroutine.create(function() reactor:park(a, "timeout", "read") end)
        coroutine.resume(reader)
        assert.are.equal(reader, reactor.read_waiters[a])

        local writer = coroutine.create(function() reactor:park(b, "timeout", "write") end)
        coroutine.resume(writer)
        assert.are.equal(writer, reactor.write_waiters[b])
    end)

    it("reports a real error as not-would-block", function()
        assert.is_false(Reactor.would_block("closed"))
        assert.is_false(Reactor.would_block(nil))
        assert.is_true(Reactor.would_block("timeout"))
        assert.is_true(Reactor.would_block("wantread"))
        assert.is_true(Reactor.would_block("wantwrite"))

        local reactor = Reactor.new()
        local co = coroutine.create(function()
            assert.is_false(reactor:park({}, "closed", "read"))
        end)
        local ok = coroutine.resume(co)
        assert.is_true(ok)
    end)
end)
