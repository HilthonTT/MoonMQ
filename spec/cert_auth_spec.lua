local users_m  = require("src.server.users")
local auth     = require("src.server.auth")
local scram    = require("src.server.scram")
local tls_m    = require("src.io.tls")
local handlers = require("src.server.handlers")
local proto    = require("src.wire.protocol")
local uuid     = require("src.core.uuid")

local ITER = 1000
local function cred(password)
    return auth.hash_password(password, { iterations = ITER, format = auth.FORMAT_SCRAM })
end

-- Stands in for a luasec connection: just the methods the broker calls.
local function fake_tls_sock(o)
    o = o or {}
    return {
        info = function() return { protocol = o.protocol or "TLSv1.3" } end,
        exportkeyingmaterial = function(_, label, n)
            assert(label == "EXPORTER-Channel-Binding" and n == 32)
            return o.ekm
        end,
        getpeerverification = function() return o.verified ~= false end,
        getpeercertificate = function()
            if not o.cn and not o.san then return nil end
            return {
                subject = function()
                    return o.cn and { { name = "commonName", oid = "2.5.4.3", value = o.cn } } or {}
                end,
                extensions = function() return { ["2.5.29.17"] = o.san or {} } end,
            }
        end,
    }
end

local function load_store(users)
    return assert(users_m.load({ Users = users }))
end

describe("certificate names in the user store", function()
    it("loads a certificate-only user", function()
        local store = load_store({
            { Username = "orders", CertificateNames = { "orders.internal" },
              Superuser = true },
        })
        local user = store:get("orders")
        assert.is_nil(user.parsed)
        assert.are.same({ "orders.internal" }, user.cert_names)
        assert.is_truthy(store:describe():find("orders%[cert"))
    end)

    it("still requires some credential", function()
        local store, err = users_m.load({ Users = { { Username = "orders" } } })
        assert.is_nil(store)
        assert.is_truthy(err:find("CertificateNames"))
    end)

    it("rejects an empty or malformed list", function()
        for _, bad in ipairs({ {}, "orders.internal", { "" }, { 42 } }) do
            local store = users_m.load({ Users = {
                { Username = "orders", CertificateNames = bad } } })
            assert.is_nil(store)
        end
    end)

    it("refuses one certificate name claimed by two users", function()
        local store, err = users_m.load({ Users = {
            { Username = "a", CertificateNames = { "shared" } },
            { Username = "b", CertificateNames = { "shared" } },
        } })
        assert.is_nil(store)
        assert.is_truthy(err:find("shared"))
    end)

    it("maps a certificate's names to distinct users", function()
        local store = load_store({
            { Username = "a", CertificateNames = { "a.internal", "spiffe://prod/a" } },
            { Username = "b", CertificateNames = { "b.internal" } },
        })
        local found = store:for_certificate({ "a.internal", "spiffe://prod/a", "x" })
        assert.are.equal(1, #found)
        assert.are.equal("a", found[1].username)
        assert.are.equal(0, #store:for_certificate({ "nobody" }))
    end)
end)

describe("Auth:certificate_principal", function()
    local a

    before_each(function()
        a = auth.authenticator({ store = load_store({
            { Username = "orders", CertificateNames = { "orders.internal" } },
            { Username = "ops", PasswordHash = cred("pw"),
              CertificateNames = { "ops@example.com" }, Superuser = true },
        }) })
    end)

    it("resolves the one user a certificate maps to", function()
        local p = assert(a:certificate_principal({ "orders.internal" }))
        assert.are.equal("orders", p.username)
    end)

    it("honours an authzid the certificate is allowed to use", function()
        local p = assert(a:certificate_principal(
            { "orders.internal", "ops@example.com" }, "ops"))
        assert.are.equal("ops", p.username)
    end)

    it("refuses an authzid the certificate does not map to", function()
        local p, err = a:certificate_principal({ "orders.internal" }, "ops")
        assert.is_nil(p)
        assert.is_truthy(err:find("ops"))
    end)

    it("refuses an ambiguous certificate without an authzid", function()
        local p, err = a:certificate_principal({ "orders.internal", "ops@example.com" })
        assert.is_nil(p)
        assert.is_truthy(err:find("several"))
    end)

    it("refuses a certificate that maps to no one", function()
        assert.is_nil((a:certificate_principal({ "stranger" })))
    end)

    it("never lets a certificate-only user log in with a password", function()
        local ok = a:verify("orders", "", "10.0.0.9")
        assert.is_false(ok)
        local _, principal = a:scram_credential("orders")
        assert.is_nil(principal)
    end)
end)

describe("tls identity and exporter helpers", function()
    it("collects CN and SAN DNS, URI and email names", function()
        local names = assert(tls_m.peer_identity(fake_tls_sock({
            cn = "orders-svc",
            san = { dNSName = { "orders.internal" },
                    uniformResourceIdentifier = { "spiffe://prod/orders" },
                    rfc822Name = { "ops@example.com" } },
        })))
        assert.are.same({ "orders-svc", "orders.internal",
                          "spiffe://prod/orders", "ops@example.com" }, names)
    end)

    it("refuses a missing or unverified certificate", function()
        assert.is_nil((tls_m.peer_identity(fake_tls_sock({}))))
        assert.is_nil((tls_m.peer_identity(fake_tls_sock({ cn = "x", verified = false }))))
        assert.is_nil((tls_m.peer_identity({})))
    end)

    it("derives an exporter binding on TLS 1.3 only", function()
        local ekm = string.rep("k", 32)
        assert.are.equal(ekm, tls_m.exporter_binding(fake_tls_sock({ ekm = ekm })))
        assert.is_nil(tls_m.exporter_binding(
            fake_tls_sock({ ekm = ekm, protocol = "TLSv1.2" })))
        assert.is_nil(tls_m.exporter_binding({}))
    end)
end)

describe("SCRAM channel-binding negotiation by type", function()
    local EXP, END = string.rep("e", 32), string.rep("h", 32)
    local bindings = { [scram.CBIND_EXPORTER] = EXP, [scram.CBIND_TYPE] = END }

    local function first_with(cbind_type)
        return assert(scram.parse_client_first(
            (scram.client_first("alice", "cnonce", cbind_type))))
    end

    it("binds to the type the client asked for", function()
        assert.are.equal(EXP, scram.negotiate_cbind(
            first_with(scram.CBIND_EXPORTER), bindings, "preferred"))
        assert.are.equal(END, scram.negotiate_cbind(
            first_with(scram.CBIND_TYPE), bindings, "preferred"))
    end)

    it("refuses tls-exporter when the connection cannot provide it", function()
        local bound, err = scram.negotiate_cbind(first_with(scram.CBIND_EXPORTER),
            { [scram.CBIND_TYPE] = END }, "preferred")
        assert.is_nil(bound)
        assert.is_truthy(err:find("tls%-exporter"))
    end)

    it("still treats a bare string as the end-point hash", function()
        assert.are.equal(END, scram.negotiate_cbind(
            first_with(scram.CBIND_TYPE), END, "preferred"))
    end)
end)

describe("client and broker halves against each other", function()
    local Client = require("src.client")

    local function fake_transport(server, conn)
        local inbox = ""
        return {
            send = function(_self, data)
                local pos = 1
                while pos <= #data do
                    local len = string.unpack(">I4", data, pos)
                    local body = data:sub(pos + 4, pos + 3 + len)
                    pos = pos + 4 + len
                    local op, correl, payload = proto.parse_frame(body)
                    handlers.BY_OP[op](server, conn, correl, payload)
                end
                return #data
            end,
            receive = function(_self, n)
                if #inbox == 0 then return nil, "closed", "" end
                local take = inbox:sub(1, n)
                inbox = inbox:sub(#take + 1)
                return take
            end,
            close = function() end,
            settimeout = function() end,
            _deliver = function(frame) inbox = inbox .. frame end,
        }
    end

    local function setup(users, tls_cfg, sock)
        local server = {
            authenticator = auth.authenticator({ store = load_store(users) }),
            tls = tls_cfg,
        }
        local transport
        local conn = {
            id_short = "test", ip = "10.0.0.1", state = "greeted", sock = sock,
            send = function(_self, frame) transport._deliver(frame); return true end,
            close = function(self, reason, code, message)
                self.closed = { reason = reason, code = code, message = message }
                self.state = "closed"
            end,
            transition_to = function(self, state) self.state = state end,
        }
        transport = fake_transport(server, conn)
        local client = setmetatable({ sock = transport, closed = false, timeout = 1 }, Client)
        return client, conn, server
    end

    local VERIFYING = { verify = "required", channel_binding = "preferred" }

    it("logs in by client certificate with EXTERNAL", function()
        local client, conn = setup(
            { { Username = "orders", CertificateNames = { "orders.internal" },
                Acls = { { Resource = "topic", Name = "orders.*", Operations = { "read" } } } } },
            VERIFYING, fake_tls_sock({ cn = "orders.internal" }))
        assert.is_nil(client:_auth_external())
        assert.are.equal("authenticated", conn.state)
        assert.are.equal("orders", conn.principal.username)
        assert.is_true(conn.principal.acl:authorized("topic", "orders.eu", "read"))
    end)

    it("refuses EXTERNAL on a listener that does not verify client certificates", function()
        local client, conn, server = setup(
            { { Username = "orders", CertificateNames = { "orders.internal" } } },
            { verify = "none", channel_binding = "preferred" },
            fake_tls_sock({ cn = "orders.internal" }))
        local err = client:_auth_external()
        assert.is_truthy(err)
        assert.are_not.equal("authenticated", conn.state)
        assert.is_false(server.authenticator:is_banned("10.0.0.1"))
    end)

    it("refuses EXTERNAL for a certificate that maps to no user", function()
        local client, conn = setup(
            { { Username = "orders", CertificateNames = { "orders.internal" } } },
            VERIFYING, fake_tls_sock({ cn = "intruder" }))
        assert.is_truthy(client:_auth_external())
        assert.is_nil(conn.principal)
    end)

    it("binds SCRAM to the session with tls-exporter", function()
        local ekm = string.rep("x", 32)
        local client, conn = setup(
            { { Username = "orders", PasswordHash = cred("pw"), Superuser = true } },
            { verify = "none", channel_binding = "required",
              endpoint_hash = string.rep("h", 32) },
            fake_tls_sock({ ekm = ekm }))
        client.cbind, client.cbind_type = ekm, scram.CBIND_EXPORTER
        assert.is_nil(client:_auth_scram("orders", "pw"))
        assert.are.equal("authenticated", conn.state)
    end)

    it("fails SCRAM when the two ends derived different exporter values", function()
        local client, conn = setup(
            { { Username = "orders", PasswordHash = cred("pw"), Superuser = true } },
            { verify = "none", channel_binding = "preferred" },
            fake_tls_sock({ ekm = string.rep("s", 32) }))
        client.cbind, client.cbind_type = string.rep("c", 32), scram.CBIND_EXPORTER
        assert.is_truthy(client:_auth_scram("orders", "pw"))
        assert.are_not.equal("authenticated", conn.state)
    end)
end)
