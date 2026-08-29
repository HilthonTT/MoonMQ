-- SCRAM-SHA-256 (RFC 5802 / RFC 7677), both halves.
--
-- The anchor is the worked example from RFC 7677 §3: a full exchange with
-- fixed nonces, salt, and iteration count, for the password "pencil". If our
-- proof and server signature match those bytes, this implementation
-- interoperates with anything else that reads the same RFC — which is the only
-- meaningful definition of "correct" for a wire protocol. Round-tripping
-- against ourselves would pass just as happily with a subtly wrong auth
-- message.
--
-- The rest of the file is the adversarial half: replayed nonces, tampered
-- proofs, stripped channel binding, and the base64 strictness that stops two
-- different strings from decoding to the same nonce.

local scram    = require("src.server.scram")
local auth     = require("src.server.auth")
local users_m  = require("src.server.users")
local handlers = require("src.server.handlers")
local proto    = require("src.wire.protocol")
local uuid     = require("src.core.uuid")
local pbkdf2   = require("src.core.pbkdf2")
local b64      = require("src.core.base64")
local rng      = require("src.core.rng")

describe("base64", function()

    it("matches the RFC 4648 test vectors", function()
        local vectors = {
            { "",       ""         },
            { "f",      "Zg=="     },
            { "fo",     "Zm8="     },
            { "foo",    "Zm9v"     },
            { "foob",   "Zm9vYg==" },
            { "fooba",  "Zm9vYmE=" },
            { "foobar", "Zm9vYmFy" },
        }
        for _, v in ipairs(vectors) do
            assert.are.equal(v[2], b64.encode(v[1]))
            assert.are.equal(v[1], (b64.decode(v[2])))
        end
    end)

    it("round-trips arbitrary bytes", function()
        for len = 1, 40 do
            local raw = rng.bytes(len)
            assert.are.equal(raw, (b64.decode(b64.encode(raw))))
        end
    end)

    it("rejects malformed input rather than guessing at it", function()
        assert.is_nil((b64.decode("Zm9vYmF")))    -- length not a multiple of 4
        assert.is_nil((b64.decode("Zm9vYmF!")))   -- character outside alphabet
        assert.is_nil((b64.decode("Zm=vYmFy")))   -- padding in the middle
        assert.is_nil((b64.decode("Zh==")))       -- non-canonical trailing bits
        assert.is_nil((b64.decode("Zm9v YmFy")))  -- whitespace is not ignored
    end)
end)

describe("SCRAM-SHA-256 against the RFC 7677 vector", function()

    -- RFC 7677 §3.
    local PASSWORD     = "pencil"
    local CLIENT_NONCE = "rOprNGfwEbeRWgbNEkqO"
    local COMBINED     = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
    local SALT_B64     = "W22ZaJ0SNY7soEsUEjb6gQ=="
    local ITERATIONS   = 4096

    local CLIENT_FIRST = "n,,n=user,r=" .. CLIENT_NONCE
    local SERVER_FIRST = "r=" .. COMBINED .. ",s=" .. SALT_B64 .. ",i=" .. ITERATIONS
    local CLIENT_FINAL = "c=biws,r=" .. COMBINED
        .. ",p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    local SERVER_FINAL = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="

    local salt = assert(b64.decode(SALT_B64))

    it("produces the published client-final message", function()
        local salted = pbkdf2.pbkdf2_hmac_sha256(PASSWORD, salt, ITERATIONS, 32)
        local _, bare = scram.client_first("user", CLIENT_NONCE)
        assert.are.equal("n=user,r=" .. CLIENT_NONCE, bare)

        local final = scram.client_final(salted, bare, SERVER_FIRST, COMBINED)
        assert.are.equal(CLIENT_FINAL, final)
    end)

    it("accepts the published proof server-side and answers with the published signature", function()
        local salted = pbkdf2.pbkdf2_hmac_sha256(PASSWORD, salt, ITERATIONS, 32)
        local stored_key, server_key = scram.keys_from_salted(salted)

        local first = assert(scram.parse_client_first(CLIENT_FIRST))
        assert.are.equal("user", first.username)
        assert.are.equal(CLIENT_NONCE, first.nonce)

        local final = assert(scram.parse_client_final(CLIENT_FINAL))
        assert.is_true(scram.check_cbind(first.gs2, final.cbind))

        local auth_message = scram.auth_message(first.bare, SERVER_FIRST, final.without_proof)
        assert.is_true(scram.verify_proof(stored_key, auth_message, final.proof))
        assert.are.equal(SERVER_FINAL, scram.server_final(server_key, auth_message))
    end)

    it("lets the client verify the published server signature", function()
        local salted = pbkdf2.pbkdf2_hmac_sha256(PASSWORD, salt, ITERATIONS, 32)
        local _, bare = scram.client_first("user", CLIENT_NONCE)
        local _, expected = scram.client_final(salted, bare, SERVER_FIRST, COMBINED)
        assert.is_true(scram.verify_server_final(SERVER_FINAL, expected))
        assert.is_false((scram.verify_server_final(
            "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", expected)))
    end)
end)

describe("SCRAM message parsing", function()

    it("rejects a client-first that demands channel binding we cannot provide", function()
        local ok, err = scram.parse_client_first("p=tls-unique,,n=user,r=abc")
        assert.is_nil(ok)
        assert.is_truthy(err:find("channel binding"))
    end)

    it("rejects an authzid rather than silently ignoring it", function()
        -- Acting as another principal is what the ACL is for; quietly dropping
        -- the request to do so would be a confusing half-feature.
        local ok, err = scram.parse_client_first("n,a=admin,n=user,r=abc")
        assert.is_nil(ok)
        assert.is_truthy(err:find("authzid"))
    end)

    it("requires the username and nonce fields", function()
        assert.is_nil((scram.parse_client_first("n,,r=abc")))
        assert.is_nil((scram.parse_client_first("n,,n=user")))
        assert.is_nil((scram.parse_client_first("n,,n=user,r=")))
        assert.is_nil((scram.parse_client_first("")))
    end)

    it("round-trips escaped usernames", function()
        for _, name in ipairs({ "a=b", "a,b", "plain", "=2C", "a=b,c=d" }) do
            local escaped = scram.escape_username(name)
            assert.is_nil(escaped:find(","), "comma must be escaped")
            assert.are.equal(name, (scram.unescape_username(escaped)))
        end
    end)

    it("rejects an invalid escape instead of passing it through", function()
        assert.is_nil((scram.unescape_username("a=ZZb")))
        -- A trailing bare "=" is too short to look like an escape; a
        -- pattern-based implementation would leave it alone.
        assert.is_nil((scram.unescape_username("ab=")))
    end)

    it("takes the first value of a duplicated field", function()
        -- A second r= is an attempt to make the parser and the proof
        -- verification read different values.
        local first = assert(scram.parse_client_first("n,,n=user,r=aaa,r=bbb"))
        assert.are.equal("aaa", first.nonce)
    end)

    it("requires the server nonce to extend the client nonce", function()
        local ok, err = scram.parse_server_first("r=totallydifferent,s=YWJj,i=4096", "mynonce")
        assert.is_nil(ok)
        assert.is_truthy(err:find("extend"))

        -- Equal is not "extends": the server must contribute entropy.
        assert.is_nil((scram.parse_server_first("r=mynonce,s=YWJj,i=4096", "mynonce")))
        assert.is_truthy(scram.parse_server_first("r=mynonceXYZ,s=YWJj,i=4096", "mynonce"))
    end)

    it("rejects a nonsensical iteration count", function()
        assert.is_nil((scram.parse_server_first("r=abcX,s=YWJj,i=0", "abc")))
        assert.is_nil((scram.parse_server_first("r=abcX,s=YWJj,i=x", "abc")))
    end)

    it("surfaces a server error field", function()
        local ok, err = scram.parse_server_first("e=unknown-user", "abc")
        assert.is_nil(ok)
        assert.is_truthy(err:find("unknown%-user"))
    end)
end)

describe("SCRAM exchange", function()

    local ITER = 2048

    -- Drive both halves for a given stored credential format.
    local function exchange(credential, password, opts)
        opts = opts or {}
        local parsed = assert(auth.parse_credential(credential))
        local stored_key, server_key = auth.scram_keys(parsed)

        local client_nonce = scram.nonce(rng.bytes)
        local client_first, bare = scram.client_first("alice", client_nonce)

        local first = assert(scram.parse_client_first(client_first))
        local combined = first.nonce .. scram.nonce(rng.bytes)
        local server_first = scram.server_first(combined, parsed.salt, parsed.iterations)

        local sf = assert(scram.parse_server_first(server_first, client_nonce))
        local salted = pbkdf2.pbkdf2_hmac_sha256(password, sf.salt, sf.iterations, 32)
        local client_final, expected =
            scram.client_final(salted, bare, server_first, sf.nonce)

        local final = assert(scram.parse_client_final(client_final))
        assert.are.equal(combined, final.nonce, "client must echo the issued nonce")
        if opts.tamper_proof then
            final.proof = string.char(final.proof:byte(1) ~ 0xFF) .. final.proof:sub(2)
        end

        local auth_message = scram.auth_message(first.bare, server_first, final.without_proof)
        local ok = scram.verify_proof(stored_key, auth_message, final.proof)
        return ok, {
            server_final = scram.server_final(server_key, auth_message),
            expected     = expected,
        }
    end

    it("succeeds against a pbkdf2-format credential", function()
        local cred = auth.hash_password("s3cret", { iterations = ITER })
        local ok, out = exchange(cred, "s3cret")
        assert.is_true(ok)
        assert.is_true(scram.verify_server_final(out.server_final, out.expected))
    end)

    it("succeeds against a scram-format credential", function()
        local cred = auth.hash_password("s3cret",
            { iterations = ITER, format = auth.FORMAT_SCRAM })
        local ok, out = exchange(cred, "s3cret")
        assert.is_true(ok)
        assert.is_true(scram.verify_server_final(out.server_final, out.expected))
    end)

    it("produces identical keys from both stored formats of one password", function()
        -- Both formats must accept the same proofs, or migrating a credential
        -- would silently invalidate it.
        local pb = auth.hash_password("s3cret", { iterations = ITER })
        local parsed = assert(auth.parse_credential(pb))
        local sk1, vk1 = auth.scram_keys(parsed)

        -- Rebuild the scram form from the SAME salt and salted password.
        local sk2, vk2 = scram.keys_from_salted(parsed.hash)
        assert.are.equal(sk1, sk2)
        assert.are.equal(vk1, vk2)
    end)

    it("rejects a wrong password", function()
        local cred = auth.hash_password("s3cret", { iterations = ITER })
        assert.is_false((exchange(cred, "not-s3cret")))
    end)

    it("rejects a tampered proof", function()
        local cred = auth.hash_password("s3cret", { iterations = ITER })
        assert.is_false((exchange(cred, "s3cret", { tamper_proof = true })))
    end)

    it("rejects a client-final replayed against a fresh challenge", function()
        -- The proof covers the server-first message, nonce and all. Capturing
        -- a valid client-final off the wire and replaying it into a new
        -- connection therefore fails, because that connection issued a
        -- different nonce and computes a different auth message.
        local cred = auth.hash_password("s3cret", { iterations = ITER })
        local parsed = assert(auth.parse_credential(cred))
        local stored_key = auth.scram_keys(parsed)

        local client_nonce = scram.nonce(rng.bytes)
        local client_first, bare = scram.client_first("alice", client_nonce)
        local first = assert(scram.parse_client_first(client_first))

        local server_first_1 = scram.server_first(
            first.nonce .. scram.nonce(rng.bytes), parsed.salt, parsed.iterations)
        local sf1 = assert(scram.parse_server_first(server_first_1, client_nonce))
        local salted = pbkdf2.pbkdf2_hmac_sha256("s3cret", sf1.salt, sf1.iterations, 32)
        local captured = scram.client_final(salted, bare, server_first_1, sf1.nonce)
        local final = assert(scram.parse_client_final(captured))

        -- Sanity: it verifies on the connection it belongs to.
        assert.is_true(scram.verify_proof(stored_key,
            scram.auth_message(first.bare, server_first_1, final.without_proof),
            final.proof))

        -- Replayed into a second exchange with its own nonce: refused.
        local server_first_2 = scram.server_first(
            first.nonce .. scram.nonce(rng.bytes), parsed.salt, parsed.iterations)
        assert.are_not.equal(server_first_1, server_first_2)
        assert.is_false(scram.verify_proof(stored_key,
            scram.auth_message(first.bare, server_first_2, final.without_proof),
            final.proof))
    end)

    it("detects a stripped gs2 header via the c= field", function()
        local first = assert(scram.parse_client_first("n,,n=alice,r=abc"))
        assert.is_true(scram.check_cbind(first.gs2, "biws"))
        assert.is_false((scram.check_cbind(first.gs2, "eSws")))   -- base64("y,,")
        assert.is_false((scram.check_cbind(first.gs2, nil)))
    end)
end)

describe("SCRAM over the wire, through the handlers", function()

    -- The handler half is where the exchange meets connection state: the
    -- challenge must be answered on the same connection that asked for it, one
    -- attempt per challenge, and a failure has to leave the connection closed
    -- rather than half-authenticated.

    local ITER = 1000
    local PASSWORD = "orders-pw"

    local server

    local function fake_conn()
        return {
            id_short = "test",
            ip       = "10.0.0.1",
            state    = "greeted",
            sent     = {},
            closed   = nil,
            send = function(self, frame)
                self.sent[#self.sent + 1] = frame
                return true
            end,
            close = function(self, reason, code, message)
                self.closed = { reason = reason, code = code, message = message }
                self.state = "closed"
            end,
            transition_to = function(self, state) self.state = state end,
        }
    end

    local function unframe(frame) return proto.parse_frame(frame:sub(5)) end

    local function last(conn)
        local op, _, payload = unframe(conn.sent[#conn.sent])
        return op, payload
    end

    before_each(function()
        local store = assert(users_m.load({
            Users = {
                { Username = "orders",
                  PasswordHash = auth.hash_password(PASSWORD,
                      { iterations = ITER, format = auth.FORMAT_SCRAM }),
                  Acls = { { Resource = "topic", Name = "orders.*",
                             Operations = { "read", "write" } } } },
            },
        }))
        server = { authenticator = auth.authenticator({ store = store }) }
    end)

    -- Runs the client half against the handlers. Returns the connection.
    local function handshake(username, password, tamper)
        local conn = fake_conn()
        local client_nonce = scram.nonce(rng.bytes)
        local client_first, bare = scram.client_first(username, client_nonce)

        local _, _, p1 = unframe(proto.encode_auth_scram(
            uuid.bytes(), scram.MECHANISM, client_first))
        handlers.auth_scram(server, conn, uuid.bytes(), p1)
        if conn.closed then return conn end

        local op, payload = last(conn)
        assert.are.equal(proto.OP_AUTH_CHALLENGE, op)
        local challenge = assert(proto.decode_auth_challenge(payload))
        local sf = assert(scram.parse_server_first(challenge.message, client_nonce))

        local salted = pbkdf2.pbkdf2_hmac_sha256(password, sf.salt, sf.iterations, 32)
        local client_final, expected =
            scram.client_final(salted, bare, challenge.message, sf.nonce)
        if tamper then client_final = tamper(client_final, sf) end

        local _, _, p2 = unframe(proto.encode_auth_scram_final(uuid.bytes(), client_final))
        handlers.auth_scram_final(server, conn, uuid.bytes(), p2)
        conn.expected_signature = expected
        return conn
    end

    it("authenticates and attaches the principal", function()
        local conn = handshake("orders", PASSWORD)
        assert.is_nil(conn.closed)
        assert.are.equal("authenticated", conn.state)
        assert.are.equal("orders", conn.username)
        assert.are.equal("orders", conn.principal.username)
        assert.is_true(conn.principal.acl:authorized("topic", "orders.eu", "write"))
    end)

    it("returns a server signature the client can verify", function()
        -- Without this the client has proved itself to the broker but has no
        -- evidence the broker knew the credential — half of mutual auth.
        local conn = handshake("orders", PASSWORD)
        local op, payload = last(conn)
        assert.are.equal(proto.OP_AUTH_OK, op)
        local final = assert(proto.decode_auth_ok(payload))
        assert.is_true(scram.verify_server_final(final.message, conn.expected_signature))
    end)

    it("clears the exchange state so a proof cannot be retried", function()
        local conn = handshake("orders", PASSWORD)
        assert.is_nil(conn.scram)
    end)

    it("closes the connection on a wrong password", function()
        local conn = handshake("orders", "wrong")
        assert.is_truthy(conn.closed)
        assert.are.equal(proto.ERR_AUTH_FAILED, conn.closed.code)
        assert.is_nil(conn.principal)
    end)

    it("closes on an unknown user, with the same error and after a challenge", function()
        -- The challenge is answered normally for an unknown account (with a
        -- decoy salt), so the failure looks identical from outside.
        local conn = handshake("ghost", PASSWORD)
        assert.is_truthy(conn.closed)
        assert.are.equal(proto.ERR_AUTH_FAILED, conn.closed.code)
        assert.are.equal("invalid credentials", conn.closed.message)
        assert.is_true(#conn.sent >= 1, "an unknown user still gets a challenge")
    end)

    it("rejects a final message that echoes a different nonce", function()
        local conn = handshake("orders", PASSWORD, function(client_final)
            return (client_final:gsub("r=[^,]+", "r=not-the-issued-nonce", 1))
        end)
        assert.is_truthy(conn.closed)
        assert.is_truthy(conn.closed.message:find("nonce"))
    end)

    it("rejects a final message with a rewritten channel binding", function()
        local conn = handshake("orders", PASSWORD, function(client_final)
            return (client_final:gsub("^c=biws", "c=eSws", 1))
        end)
        assert.is_truthy(conn.closed)
        assert.is_truthy(conn.closed.message:find("channel%-binding"))
    end)

    it("refuses a final message with no preceding client-first", function()
        local conn = fake_conn()
        local _, _, payload = unframe(proto.encode_auth_scram_final(
            uuid.bytes(), "c=biws,r=x,p=YWJj"))
        handlers.auth_scram_final(server, conn, uuid.bytes(), payload)
        assert.is_truthy(conn.closed)
        assert.are.equal(proto.ERR_BAD_PROTOCOL, conn.closed.code)
    end)

    it("refuses an unsupported mechanism", function()
        local conn = fake_conn()
        local _, _, payload = unframe(proto.encode_auth_scram(
            uuid.bytes(), "SCRAM-SHA-1", "n,,n=orders,r=abc"))
        handlers.auth_scram(server, conn, uuid.bytes(), payload)
        assert.is_truthy(conn.closed)
        assert.is_truthy(conn.closed.message:find("SCRAM%-SHA%-256"))
    end)

    it("refuses a banned IP before doing any work", function()
        for _ = 1, 5 do server.authenticator:note_failure("10.0.0.1") end
        local conn = fake_conn()
        local _, _, payload = unframe(proto.encode_auth_scram(
            uuid.bytes(), scram.MECHANISM, "n,,n=orders,r=abc"))
        handlers.auth_scram(server, conn, uuid.bytes(), payload)
        assert.is_truthy(conn.closed)
        assert.is_truthy(conn.closed.message:find("banned"))
        assert.are.equal(0, #conn.sent)
    end)

    it("counts a failed proof toward the per-IP lockout", function()
        -- Five is the default max_failures; the ban must trip on the fifth, so
        -- a SCRAM brute-force is limited exactly like a password one.
        for _ = 1, 4 do handshake("orders", "wrong") end
        assert.is_false(server.authenticator:is_banned("10.0.0.1"))
        handshake("orders", "wrong")
        assert.is_true(server.authenticator:is_banned("10.0.0.1"))
    end)
end)

describe("client and broker halves against each other", function()

    -- The two halves are written from the same RFC but in different files; the
    -- only way to know they agree is to run one against the other. This drives
    -- the REAL client code (src/client/init.lua) into the REAL handlers over a
    -- fake duplex socket, so every layer between them — the frame encoders,
    -- the field caps, the opcode dispatch — is exercised too.
    --
    -- The exchange is strictly request/response, so no scheduler is needed:
    -- the fake socket dispatches on send and the reply is waiting by the time
    -- the client reads.

    local Client = require("src.client")

    local ITER = 1000

    local function fake_transport(server, conn)
        local inbox = ""      -- bytes waiting for the client to read
        return {
            send = function(_self, data)
                -- Frame the client's bytes and run them through dispatch.
                local pos = 1
                while pos <= #data do
                    local len = string.unpack(">I4", data, pos)
                    local body = data:sub(pos + 4, pos + 3 + len)
                    pos = pos + 4 + len
                    local op, correl, payload = proto.parse_frame(body)
                    local handler = handlers.BY_OP[op]
                    assert(handler, string.format("no handler for op 0x%02x", op))
                    handler(server, conn, correl, payload)
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

    local function run(username, password, credential)
        local store = assert(users_m.load({
            Users = { { Username = "orders", PasswordHash = credential,
                        Superuser = true } },
        }))
        local server = { authenticator = auth.authenticator({ store = store }) }

        local transport
        local conn = {
            id_short = "test", ip = "10.0.0.1", state = "greeted",
            send = function(_self, frame) transport._deliver(frame); return true end,
            close = function(self, reason, code, message)
                self.closed = { reason = reason, code = code, message = message }
                self.state = "closed"
            end,
            transition_to = function(self, state) self.state = state end,
        }
        transport = fake_transport(server, conn)

        local client = setmetatable({ sock = transport, closed = false, timeout = 1 },
            Client)
        local err = client:_auth_scram(username, password)
        return err, conn
    end

    it("completes a handshake against a pbkdf2-format credential", function()
        local err, conn = run("orders", "pw",
            auth.hash_password("pw", { iterations = ITER }))
        assert.is_nil(err)
        assert.are.equal("authenticated", conn.state)
        assert.are.equal("orders", conn.username)
    end)

    it("completes a handshake against a scram-format credential", function()
        local err, conn = run("orders", "pw",
            auth.hash_password("pw", { iterations = ITER, format = auth.FORMAT_SCRAM }))
        assert.is_nil(err)
        assert.are.equal("authenticated", conn.state)
    end)

    it("reports a wrong password as an error rather than authenticating", function()
        local err, conn = run("orders", "wrong",
            auth.hash_password("pw", { iterations = ITER }))
        assert.is_truthy(err)
        assert.are_not.equal("authenticated", conn.state)
    end)

    it("reports an unknown user the same way", function()
        local err, conn = run("ghost", "pw",
            auth.hash_password("pw", { iterations = ITER }))
        assert.is_truthy(err)
        assert.are_not.equal("authenticated", conn.state)
    end)
end)
