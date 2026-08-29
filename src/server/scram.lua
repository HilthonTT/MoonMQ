-- SCRAM-SHA-256 (RFC 5802 / RFC 7677), server and client halves.
--
-- Why this exists, given AUTH already works: the existing mechanism sends the
-- password itself to the broker, and MoonMQ speaks plaintext TCP (TLS is
-- still on the roadmap — docs/roadmap-security-consensus.md). Anyone on the
-- path reads the credential. SCRAM never transmits it: the client proves
-- possession against a per-user salt, and a passive observer learns nothing
-- replayable.
--
-- The second reason is the reactor. A password AUTH costs the broker a full
-- PBKDF2 derivation — 0.29 s natively at the recommended 600k iterations, and
-- minutes without luaossl — inline on the single event-loop thread, which is
-- why auth.lua carries a yield hook, an in-flight gate, and a success cache
-- just to survive it. **SCRAM moves that cost to the client.** The broker
-- stores the salted password (or better, only what is derived from it) and
-- answers with HMACs and one SHA-256: microseconds, no yielding, no gate.
-- An unauthenticated peer can no longer buy broker CPU by guessing.
--
-- Exchange (three frames, over OP_AUTH_SCRAM / OP_AUTH_CHALLENGE /
-- OP_AUTH_SCRAM_FINAL — see src/wire/protocol.lua):
--
--   client-first   n,,n=alice,r=<client nonce>
--   server-first   r=<client nonce><server nonce>,s=<salt b64>,i=<iterations>
--   client-final   c=biws,r=<combined nonce>,p=<client proof b64>
--   server-final   v=<server signature b64>
--
-- Channel binding is advertised as unsupported ("n,,"), which is honest:
-- there is no TLS channel to bind to yet. When TLS lands, the gs2 header is
-- where tls-server-end-point support would be added, and the client-final
-- `c=` field is what makes a downgrade detectable.

local sha2   = require("src.vendor.sha2")
local b64    = require("src.core.base64")
local ct     = require("src.core.ct")

local M = {}

M.MECHANISM = "SCRAM-SHA-256"

-- The gs2 header this implementation sends and accepts: no channel binding,
-- no authzid. base64("n,,") == "biws", the value that must come back in the
-- client-final `c=` field.
M.GS2_HEADER  = "n,,"
M.GS2_CBIND   = "biws"

local function hmac_bin(key, msg)
    return sha2.hex_to_bin(sha2.hmac(sha2.sha256, key, msg))
end
M.hmac_bin = hmac_bin

local function h_bin(msg)
    return sha2.hex_to_bin(sha2.sha256(msg))
end
M.h_bin = h_bin

local function xor_bin(a, b)
    if #a ~= #b then return nil end
    local out = {}
    for i = 1, #a do
        out[i] = string.char(a:byte(i) ~ b:byte(i))
    end
    return table.concat(out)
end
M.xor_bin = xor_bin

-- RFC 5802 §5.1: in the n= field a literal "=" is sent as "=3D" and "," as
-- "=2C", because both are field separators. Anything else after "=" is
-- malformed and must be rejected rather than passed through — otherwise
-- "a=2Cb" and a real comma become the same username.
function M.escape_username(name)
    return (name:gsub("=", "=3D"):gsub(",", "=2C"))
end

-- Scanned rather than gsub'd: gsub would leave a trailing bare "=" (too short
-- to match an escape pattern) untouched, quietly accepting a username the
-- grammar forbids.
function M.unescape_username(name)
    if not name:find("=", 1, true) then return name end

    local out, i, n = {}, 1, #name
    while i <= n do
        local c = name:sub(i, i)
        if c == "=" then
            local seq = name:sub(i + 1, i + 2)
            if seq == "3D" then
                out[#out + 1] = "="
            elseif seq == "2C" then
                out[#out + 1] = ","
            else
                return nil, "invalid escape in username"
            end
            i = i + 3
        else
            out[#out + 1] = c
            i = i + 1
        end
    end
    return table.concat(out)
end

-- Derive the two values a server needs from a salted password. StoredKey
-- verifies the client; ServerKey signs the response that proves to the client
-- it talked to a server that knew the credential (mutual authentication).
function M.keys_from_salted(salted_password)
    local client_key = hmac_bin(salted_password, "Client Key")
    return h_bin(client_key), hmac_bin(salted_password, "Server Key")
end

-- Nonces are `printable` per the grammar: any ASCII 0x21-0x7E except comma.
-- base64 output is a subset of that, so a base64'd random string is a legal
-- nonce with no further filtering.
function M.nonce(rng_bytes, n)
    return b64.encode(rng_bytes(n or 18))
end

local function parse_fields(str)
    local fields = {}
    for part in str:gmatch("[^,]+") do
        local k, v = part:match("^(%a)=(.*)$")
        if k then
            -- First occurrence wins: a duplicated field is an attempt to have
            -- the parser and the verifier disagree about which value is real.
            if fields[k] == nil then fields[k] = v end
        end
    end
    return fields
end

------------------------------------------------------------------------------
-- Server side
------------------------------------------------------------------------------

-- Parse "n,,n=user,r=nonce". Returns (parsed, nil) or (nil, err), where
-- parsed = { username, nonce, bare } and `bare` is the client-first-message
-- WITHOUT the gs2 header — the exact bytes that go into the auth message.
function M.parse_client_first(message)
    if type(message) ~= "string" or #message == 0 then
        return nil, "empty client-first message"
    end

    local cbind_flag = message:sub(1, 1)
    if cbind_flag == "p" then
        -- The client insists on channel binding; we have no channel to bind.
        return nil, "channel binding is not supported"
    end
    if cbind_flag ~= "n" and cbind_flag ~= "y" then
        return nil, "malformed gs2 header"
    end

    -- gs2-header = cbind-flag "," [ authzid ] ","
    local comma1 = message:find(",", 1, true)
    if not comma1 then return nil, "malformed gs2 header" end
    local comma2 = message:find(",", comma1 + 1, true)
    if not comma2 then return nil, "malformed gs2 header" end

    local authzid = message:sub(comma1 + 1, comma2 - 1)
    if authzid ~= "" then
        -- Acting as another principal is exactly the thing an ACL is for; a
        -- silently ignored authzid would be a confusing half-feature.
        return nil, "authzid is not supported"
    end

    local bare = message:sub(comma2 + 1)
    local fields = parse_fields(bare)
    if not fields.n then return nil, "missing username (n=)" end
    if not fields.r or fields.r == "" then return nil, "missing client nonce (r=)" end

    local username, uerr = M.unescape_username(fields.n)
    if not username then return nil, uerr end
    if username == "" then return nil, "empty username" end

    return {
        username = username,
        nonce    = fields.r,
        bare     = bare,
        gs2      = message:sub(1, comma2),
    }, nil
end

function M.server_first(combined_nonce, salt, iterations)
    return string.format("r=%s,s=%s,i=%d",
        combined_nonce, b64.encode(salt), iterations)
end

-- Parse "c=biws,r=nonce,p=proof". Returns { nonce, proof, cbind,
-- without_proof }.
function M.parse_client_final(message)
    if type(message) ~= "string" or #message == 0 then
        return nil, "empty client-final message"
    end

    local fields = parse_fields(message)
    if not fields.c then return nil, "missing channel binding (c=)" end
    if not fields.r then return nil, "missing nonce (r=)" end
    if not fields.p then return nil, "missing proof (p=)" end

    -- The auth message covers everything up to but excluding ",p=".
    local proof_at = message:find(",p=", 1, true)
    if not proof_at then return nil, "missing proof (p=)" end

    local proof, perr = b64.decode(fields.p)
    if not proof then return nil, perr end

    return {
        cbind         = fields.c,
        nonce         = fields.r,
        proof         = proof,
        without_proof = message:sub(1, proof_at - 1),
    }, nil
end

-- The client-final `c=` field must be base64 of the EXACT gs2 header the
-- client opened with. It is what makes a stripped channel-binding request
-- detectable: an attacker who rewrote "p=..." to "n,," in the first message
-- cannot fix up this field, because it is covered by the proof.
function M.check_cbind(client_first_gs2, c_field)
    return ct.equal(b64.encode(client_first_gs2), c_field or "")
end

function M.auth_message(client_first_bare, server_first, client_final_without_proof)
    return client_first_bare .. "," .. server_first .. "," .. client_final_without_proof
end

-- Verify a client proof against the stored key. Returns true when the client
-- demonstrably knows the password.
--
-- RecoveredClientKey = ClientProof XOR HMAC(StoredKey, AuthMessage); the
-- client knew the password iff H(RecoveredClientKey) == StoredKey. Note the
-- server never learns ClientKey unless the proof is valid, which is the
-- property that makes StoredKey safe(r) to have on disk.
function M.verify_proof(stored_key, auth_message, proof)
    if type(stored_key) ~= "string" or type(proof) ~= "string" then return false end
    local client_signature = hmac_bin(stored_key, auth_message)
    local recovered = xor_bin(proof, client_signature)
    if not recovered then return false end
    return ct.equal(h_bin(recovered), stored_key)
end

function M.server_final(server_key, auth_message)
    return "v=" .. b64.encode(hmac_bin(server_key, auth_message))
end

------------------------------------------------------------------------------
-- Client side (used by src/client/init.lua and by the specs, which drive both
-- halves against each other)
------------------------------------------------------------------------------

function M.client_first(username, nonce)
    local bare = string.format("n=%s,r=%s", M.escape_username(username), nonce)
    return M.GS2_HEADER .. bare, bare
end

function M.parse_server_first(message, client_nonce)
    if type(message) ~= "string" or #message == 0 then
        return nil, "empty server-first message"
    end

    local fields = parse_fields(message)
    if fields.e then return nil, "server error: " .. fields.e end
    if not fields.r then return nil, "missing nonce (r=)" end
    if not fields.s then return nil, "missing salt (s=)" end
    if not fields.i then return nil, "missing iteration count (i=)" end

    -- The server nonce MUST extend the client's. Without this check a
    -- man-in-the-middle could replay a recorded exchange by supplying its own
    -- nonce wholesale.
    if fields.r:sub(1, #client_nonce) ~= client_nonce or #fields.r <= #client_nonce then
        return nil, "server nonce does not extend the client nonce"
    end

    local salt, serr = b64.decode(fields.s)
    if not salt then return nil, serr end

    local iterations = tonumber(fields.i)
    if not iterations or iterations < 1 or iterations ~= math.floor(iterations) then
        return nil, "invalid iteration count"
    end

    return { nonce = fields.r, salt = salt, iterations = iterations }, nil
end

-- Build the client-final message and the server signature to expect back.
-- `salted_password` is PBKDF2(password, salt, iterations, 32) — the caller
-- supplies it so this module stays free of a KDF dependency (and so the
-- client can use the native one when luaossl is present).
function M.client_final(salted_password, client_first_bare, server_first, combined_nonce)
    local without_proof = string.format("c=%s,r=%s", M.GS2_CBIND, combined_nonce)
    local auth_message  = M.auth_message(client_first_bare, server_first, without_proof)

    local client_key       = hmac_bin(salted_password, "Client Key")
    local stored_key       = h_bin(client_key)
    local client_signature = hmac_bin(stored_key, auth_message)
    local proof            = xor_bin(client_key, client_signature)

    local server_key = hmac_bin(salted_password, "Server Key")
    local expected   = hmac_bin(server_key, auth_message)

    return without_proof .. ",p=" .. b64.encode(proof), expected, auth_message
end

-- Check the server-final message. A client that skips this has authenticated
-- the connection in one direction only — it has proved itself to the server
-- but has no evidence the server knew the credential at all.
function M.verify_server_final(message, expected_signature)
    if type(message) ~= "string" then return false, "empty server-final message" end

    local fields = parse_fields(message)
    if fields.e then return false, "server rejected: " .. fields.e end
    if not fields.v then return false, "missing server signature (v=)" end

    local got, derr = b64.decode(fields.v)
    if not got then return false, derr end
    if not ct.equal(got, expected_signature) then
        return false, "server signature mismatch"
    end
    return true, nil
end

return M
