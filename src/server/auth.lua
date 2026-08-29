-- Authentication with constant-time credential compare and per-IP
-- failure tracking.
--
-- Two stored credential formats, both salted PBKDF2-SHA256:
--
--   pbkdf2-sha256$<iterations>$<salt_hex>$<hash_hex>
--       The original. <hash> is PBKDF2(password, salt, iterations), which is
--       exactly SCRAM's SaltedPassword — so these credentials work with both
--       mechanisms unchanged. The catch: SaltedPassword is login-equivalent.
--       Someone who reads the config can authenticate as that user without
--       ever recovering the password.
--
--   scram-sha-256$<iterations>$<salt_hex>$<stored_key_hex>$<server_key_hex>
--       Preferred. Stores only what SCRAM verification needs (RFC 5802 §3):
--       StoredKey = H(HMAC(SaltedPassword, "Client Key")) and
--       ServerKey = HMAC(SaltedPassword, "Server Key"). Password AUTH still
--       verifies against it (derive, re-hash, compare StoredKey), and a
--       stolen config no longer yields a working SCRAM proof.
--
-- Both are produced by bin/moonmq-hash.lua; `--scram` selects the second.
--
-- Authentication itself is multi-user (src/server/users.lua): the
-- authenticator holds a STORE, and a successful verify returns the principal
-- — username, ACL, quota — that the rest of the request path authorizes
-- against.

local sha2     = require("src.vendor.sha2")
local socket   = require("socket")
local rng      = require("src.core.rng")
local ct       = require("src.core.ct")
local pbkdf2_m = require("src.core.pbkdf2")
local scram    = require("src.server.scram")
local acl_m    = require("src.server.acl")
local log      = require("src.log.logger").get("auth")

local M = {}

-- PBKDF2 lives in src/core/pbkdf2.lua (the SCRAM client half needs it too, and
-- should not have to require the server's user store to get at one function).
-- Re-exported here because bin/moonmq-hash.lua, the specs, and any operator
-- tooling all reach for it through this module.
local DEFAULT_PBKDF2_ITERATIONS = pbkdf2_m.DEFAULT_PBKDF2_ITERATIONS
local MAX_PBKDF2_ITERATIONS     = pbkdf2_m.MAX_PBKDF2_ITERATIONS
local pbkdf2_pure               = pbkdf2_m.pbkdf2_pure
local pbkdf2_hmac_sha256        = pbkdf2_m.pbkdf2_hmac_sha256

M.DEFAULT_PBKDF2_ITERATIONS = DEFAULT_PBKDF2_ITERATIONS
M.MAX_PBKDF2_ITERATIONS     = MAX_PBKDF2_ITERATIONS
M.pbkdf2_pure               = pbkdf2_pure
M.pbkdf2_hmac_sha256        = pbkdf2_hmac_sha256
M.kdf_backend               = pbkdf2_m.kdf_backend
M.estimate_verify_seconds   = pbkdf2_m.estimate_verify_seconds

local DEFAULT_SALT_BYTES = 16
local DEFAULT_HASH_BYTES = 32

-- Boot-time cost check thresholds. Below MIN_ITERATIONS a hash is cheap
-- everywhere and not worth timing (it also keeps the probe out of tests that
-- build throwaway authenticators). Above WARN_SECONDS per login, the operator
-- needs to know before the first client connects, not after.
local COST_CHECK_MIN_ITERATIONS = 1000
local COST_WARN_SECONDS         = 1.0

-- Constant-time string compare. The HMAC indirection that used to wrap
-- both inputs added no security: the key was a per-process random value
-- with no out-of-band sharing, so it didn't protect against any attacker
-- who couldn't already read process memory. We still want constant time
-- to avoid leaking field length / prefix matches via timing — but the
-- HMAC was pure overhead.
--
-- The `key` parameter is kept for API compatibility; it is ignored.
--
-- The comparison itself now lives in src/core/ct.lua, because SCRAM needs the
-- identical primitive for proof and signature checks and two copies of a
-- timing-sensitive compare is how one of them ends up weaker.
local function compare_secure(a, b, _key)
    return ct.equal(a, b)
end
M.compare_secure = compare_secure

M.FORMAT_PBKDF2 = "pbkdf2"
M.FORMAT_SCRAM  = "scram"

-- opts.format selects the stored shape:
--   "pbkdf2" (default) — pbkdf2-sha256$...$<salted_password>
--   "scram"            — scram-sha-256$...$<stored_key>$<server_key>
--
-- Prefer "scram" for new credentials: it verifies password AUTH just as well
-- and is not login-equivalent if the config leaks.
function M.hash_password(password, opts)
    opts = opts or {}
    local iterations = opts.iterations or DEFAULT_PBKDF2_ITERATIONS
    local salt_len   = opts.salt_bytes or DEFAULT_SALT_BYTES
    local hash_len   = opts.hash_bytes or DEFAULT_HASH_BYTES

    local salt = rng.bytes(salt_len)
    local hash = pbkdf2_hmac_sha256(password, salt, iterations, hash_len)

    if opts.format == M.FORMAT_SCRAM then
        -- SaltedPassword must be the full SHA-256 digest length for the SCRAM
        -- derivations to match what any RFC 5802 client computes.
        assert(hash_len == DEFAULT_HASH_BYTES,
            "scram credentials require a 32-byte derived key")
        local stored_key, server_key = scram.keys_from_salted(hash)
        return string.format("scram-sha-256$%d$%s$%s$%s", iterations,
            sha2.bin_to_hex(salt), sha2.bin_to_hex(stored_key),
            sha2.bin_to_hex(server_key))
    end

    return string.format("pbkdf2-sha256$%d$%s$%s",
        iterations, sha2.bin_to_hex(salt), sha2.bin_to_hex(hash))
end

local function check_iterations(iter_s)
    local iterations = tonumber(iter_s)
    if not iterations or iterations < 1 then
        return nil, "invalid iterations"
    end
    -- Sanity cap. Stored hashes are server-controlled so this isn't a
    -- DoS vector in practice, but a corrupted/typo'd config shouldn't
    -- be able to wedge auth in a 2^31-iter PBKDF2 loop.
    if iterations > MAX_PBKDF2_ITERATIONS then
        return nil, string.format("iterations exceeds maximum (%d)", MAX_PBKDF2_ITERATIONS)
    end
    return iterations
end

local function unhex(hex_str, what)
    local ok, bin = pcall(sha2.hex_to_bin, hex_str)
    if not ok then return nil, "invalid " .. what .. " hex" end
    return bin
end

local function parse_hash(stored)
    if type(stored) ~= "string" then return nil, "stored hash not a string" end

    local algo, iter_s, salt_hex, hash_hex =
        stored:match("^([%w%-]+)%$(%d+)%$(%x+)%$(%x+)$")
    if not algo then
        return nil, "malformed stored hash"
    end
    if algo ~= "pbkdf2-sha256" then
        return nil, "unsupported algorithm: " .. algo
    end

    local iterations, ierr = check_iterations(iter_s)
    if not iterations then return nil, ierr end

    local salt, serr = unhex(salt_hex, "salt")
    if not salt then return nil, serr end

    local hash, herr = unhex(hash_hex, "hash")
    if not hash then return nil, herr end

    return { iterations = iterations, salt = salt, hash = hash }, nil
end
M.parse_hash = parse_hash

local function parse_scram(stored)
    local iter_s, salt_hex, stored_hex, server_hex =
        stored:match("^scram%-sha%-256%$(%d+)%$(%x+)%$(%x+)%$(%x+)$")
    if not iter_s then return nil, "malformed scram credential" end

    local iterations, ierr = check_iterations(iter_s)
    if not iterations then return nil, ierr end

    local salt, serr = unhex(salt_hex, "salt")
    if not salt then return nil, serr end

    local stored_key, kerr = unhex(stored_hex, "stored key")
    if not stored_key then return nil, kerr end

    local server_key, verr = unhex(server_hex, "server key")
    if not server_key then return nil, verr end

    -- Both keys are SHA-256 outputs; a wrong length here means the credential
    -- was hand-edited, and would fail every login with an opaque error.
    if #stored_key ~= 32 or #server_key ~= 32 then
        return nil, "scram credential keys must be 32 bytes"
    end

    return {
        kind       = M.FORMAT_SCRAM,
        iterations = iterations,
        salt       = salt,
        stored_key = stored_key,
        server_key = server_key,
    }, nil
end

-- Parse either stored format. Returns a table carrying `kind` plus whatever
-- that format holds; callers use scram_keys/verify_password rather than
-- reaching into it.
function M.parse_credential(stored)
    if type(stored) ~= "string" then return nil, "credential not a string" end

    if stored:sub(1, 14) == "scram-sha-256$" then
        return parse_scram(stored)
    end

    local parsed, err = parse_hash(stored)
    if not parsed then return nil, err end
    parsed.kind = M.FORMAT_PBKDF2
    return parsed, nil
end

-- The (StoredKey, ServerKey) pair for a parsed credential — what the SCRAM
-- exchange needs and all it needs. For a pbkdf2 credential the stored hash IS
-- SaltedPassword, so the keys derive with two HMACs and no PBKDF2: SCRAM
-- authentication costs the broker microseconds regardless of the iteration
-- count, which is the whole point of moving the derivation to the client.
function M.scram_keys(parsed)
    if parsed.kind == M.FORMAT_SCRAM then
        return parsed.stored_key, parsed.server_key
    end
    return scram.keys_from_salted(parsed.hash)
end

-- Verify a plaintext password (the PLAIN/AUTH mechanism) against either
-- format. Both paths run one PBKDF2 derivation of the stored iteration count;
-- this is the expensive path SCRAM exists to avoid.
function M.verify_password(parsed, password, opts)
    local derived = pbkdf2_hmac_sha256(password, parsed.salt, parsed.iterations,
        parsed.kind == M.FORMAT_SCRAM and 32 or #parsed.hash, opts)

    if parsed.kind == M.FORMAT_SCRAM then
        local stored_key = scram.keys_from_salted(derived)
        return ct.equal(stored_key, parsed.stored_key)
    end
    return ct.equal(derived, parsed.hash)
end

local Auth = {}
Auth.__index = Auth

-- Tell the operator, at boot, what a login is going to cost. Getting this
-- wrong is silent otherwise: the credential carries its own iteration count,
-- so a 600k hash generated by `make hash` on a host with luaossl will still be
-- accepted by a broker without it — and then take minutes per AUTH, blocking
-- the reactor, with nothing in the log to explain why.
--
-- With many users the number that matters is the WORST one, since that is the
-- login that stalls the loop.
local function report_kdf_cost(store)
    local worst, worst_user = 0, nil
    for _, name in ipairs(store:names_sorted()) do
        local iterations = store:get(name).parsed.iterations
        if iterations > worst then worst, worst_user = iterations, name end
    end
    if worst < COST_CHECK_MIN_ITERATIONS then return worst end

    local backend  = M.kdf_backend()
    local estimate = M.estimate_verify_seconds(worst)
    if estimate >= COST_WARN_SECONDS then
        log:warn("PBKDF2 backend=%s iterations=%d (user %q): ~%.1fs of reactor "
            .. "time PER password login on this host. Install luaossl for a "
            .. "native derivation (~1000x faster), re-hash with fewer "
            .. "iterations (lua bin/moonmq-hash.lua <password> <iterations>), "
            .. "or move clients to SCRAM, which costs the broker nothing.",
            backend, worst, worst_user, estimate)
    else
        log:info("PBKDF2 backend=%s iterations=%d (~%.3fs per password login)",
            backend, worst, estimate)
    end
    return worst
end

-- opts.store: a user store (src/server/users.lua). Everything else tunes the
-- per-IP lockout and the reactor-stall mitigations, exactly as before.
function M.authenticator(opts)
    local store = assert(opts.store, "authenticator: store required")
    assert(store:count() > 0, "authenticator: store is empty")

    local worst_iterations = report_kdf_cost(store)

    return setmetatable({
        store          = store,
        max_failures   = opts.max_failures   or 5,
        failure_window = opts.failure_window or 60,
        ban_duration   = (opts.ban_duration   or 15) * 60,
        max_failure_entries = opts.max_failure_entries or 10000,
        failures       = {},
        failures_n     = 0,
        last_sweep     = socket.gettime(),

        -- Reactor-stall mitigations (see the note at the top of the file).
        max_inflight   = opts.max_inflight or 4,
        inflight       = 0,
        yield_fn       = nil,               -- installed by the Server
        -- Per-process random key for the success-digest cache. HMAC keying
        -- means the cache never stores anything derivable from the password
        -- by an attacker who can read a memory dump of just the cache slot.
        cred_key       = rng.bytes(32),
        cred_cache     = {},                -- username -> digest of last good
        -- Decoy credential for unknown usernames. Without it, "no such user"
        -- returns in microseconds while a real user costs a full PBKDF2
        -- derivation — a timing oracle that enumerates the user list. The
        -- decoy carries the store's worst iteration count so the wrong answer
        -- costs what the right one does.
        --
        -- Built from random bytes rather than by hashing a random password:
        -- the derivation is what we want to SPEND at verify time, not at boot
        -- (at 600k pure-Lua iterations, hashing one here would add minutes to
        -- startup). No password matches a random 32-byte target, so the decoy
        -- always fails, at exactly the right price.
        decoy_key      = rng.bytes(32),
        decoy          = {
            kind       = M.FORMAT_PBKDF2,
            iterations = math.max(worst_iterations, 1),
            salt       = rng.bytes(DEFAULT_SALT_BYTES),
            hash       = rng.bytes(DEFAULT_HASH_BYTES),
        },
    }, Auth)
end

-- Compatibility shim for the single-user configuration this module started
-- with (and for tests that build a throwaway authenticator). The user it
-- creates is a superuser, which is what a lone `Auth.Username` has always
-- meant in practice.
function M.static_authenticator(opts)
    assert(type(opts.username) == "string", "username required")

    local password_hash
    if opts.password_hash then
        local _, perr = M.parse_credential(opts.password_hash)
        assert(not perr, "invalid password_hash: " .. tostring(perr))
        password_hash = opts.password_hash
    elseif opts.password then
        log:warn("hashing plaintext password on startup. " ..
            "Replace Auth.Password with Auth.PasswordHash in config; " ..
            "generate the hash with `lua bin/moonmq-hash.lua <password>`.")
        password_hash = M.hash_password(opts.password)
    else
        error("static_authenticator: provide password_hash or password")
    end

    local parsed, cerr = M.parse_credential(password_hash)
    assert(parsed, cerr)
    local user = {
        username   = opts.username,
        credential = password_hash,
        parsed     = parsed,
        acl        = acl_m.ALLOW_ALL,
        quota      = opts.quota,
        superuser  = true,
    }

    -- A one-entry store, inline rather than via src/server/users.lua: that
    -- module builds stores from config and requires THIS one to parse
    -- credentials, so depending on it here would be a cycle.
    local names = { opts.username }
    local store = {
        get           = function(_, name) return name == opts.username and user or nil end,
        count         = function() return 1 end,
        names_sorted  = function() return names end,
        quota_specs   = function() return {} end,
        describe      = function() return opts.username .. "[" .. parsed.kind .. ",superuser]" end,
    }

    local a = M.authenticator({
        store          = store,
        max_failures   = opts.max_failures,
        failure_window = opts.failure_window,
        ban_duration   = opts.ban_duration,
        max_failure_entries = opts.max_failure_entries,
        max_inflight   = opts.max_inflight,
    })
    -- Historical field, still read by anything that introspects the
    -- authenticator (and by tests).
    a.username      = opts.username
    a.password_hash = password_hash
    return a
end

-- Install a cooperative-yield callback (the Server passes one that parks the
-- current coroutine on the reactor for a tick). With it installed, a PBKDF2
-- verification no longer freezes the event loop for its full duration.
function Auth:set_yield_fn(fn)
    self.yield_fn = fn
end
 
function Auth:_maybe_sweep(now)
    if now - self.last_sweep < 30 then return end
    self.last_sweep = now

    for ip, rec in pairs(self.failures) do
        local fresh  = rec.first_at     and (now - rec.first_at) < self.failure_window
        local banned = rec.banned_until and rec.banned_until > now
        if not fresh and not banned then
            self.failures[ip] = nil
            self.failures_n = self.failures_n - 1
        end
    end
end

-- Emergency eviction: caller exceeded max_failure_entries. Drop the
-- oldest non-banned entries until we're back under the cap. This bounds
-- memory under a rotating-IP attack at the cost of forgetting some
-- recent failures (those entries' attacker just got luckier).
function Auth:_evict_oldest(now)
    local victims = {}
    for ip, rec in pairs(self.failures) do
        local banned = rec.banned_until and rec.banned_until > now
        if not banned then
            victims[#victims+1] = { ip = ip, first_at = rec.first_at or 0 }
        end
    end
    table.sort(victims, function(a, b) return a.first_at < b.first_at end)

    local target = math.floor(self.max_failure_entries * 0.9)
    local removed = 0
    for i = 1, #victims do
        if self.failures_n <= target then break end
        self.failures[victims[i].ip] = nil
        self.failures_n = self.failures_n - 1
        removed = removed + 1
    end
    if removed > 0 then
        log:warn("failures table full, evicted %d oldest entries", removed)
    end
end

function Auth:is_banned(ip)
    if not ip then return false end
    local rec = self.failures[ip]
    if not rec or not rec.banned_until then return false end
    local now = socket.gettime()
    if rec.banned_until > now then
        return true, math.ceil(rec.banned_until - now)
    end
    return false
end

function Auth:_record_failure(ip, now)
    local rec = self.failures[ip]
    local existed = rec ~= nil
    if not rec or (now - (rec.first_at or 0)) > self.failure_window then
        rec = { count = 1, first_at = now }
    else
        rec.count = rec.count + 1
    end
    if rec.count >= self.max_failures then
        rec.banned_until = now + self.ban_duration
        log:warn("banning %s for %ds after %d failures",
            ip, self.ban_duration, rec.count)
    end
    self.failures[ip] = rec
    if not existed then
        self.failures_n = self.failures_n + 1
        if self.failures_n > self.max_failure_entries then
            self:_evict_oldest(now)
        end
    end
end

-- The authorization-relevant view of a user: no credential material, so it
-- can be attached to a connection, logged, and passed to handlers freely.
-- Memoized on the user record — it is read on every gated request.
local function principal_of(user)
    if not user.principal then
        user.principal = {
            username  = user.username,
            acl       = user.acl,
            quota     = user.quota,
            superuser = user.superuser,
        }
    end
    return user.principal
end
M.principal_of = principal_of

-- Keyed digest of a credential pair for the success cache. \0 separates the
-- fields so ("ab","c") can't collide with ("a","bc").
function Auth:_cred_digest(user, pass)
    return sha2.hmac(sha2.sha256, self.cred_key, (user or "") .. "\0" .. (pass or ""))
end

-- Clear an IP's failure record after a success.
function Auth:note_success(ip)
    ip = ip or "?"
    if self.failures[ip] ~= nil then
        self.failures[ip] = nil
        self.failures_n = self.failures_n - 1
    end
end

-- Record a failed attempt against an IP (public: the SCRAM handler in
-- src/server/handlers.lua drives its own exchange and reports the outcome).
function Auth:note_failure(ip)
    self:_record_failure(ip or "?", socket.gettime())
end

function Auth:principal(username)
    local user = self.store:get(username)
    return user and principal_of(user) or nil
end

-- Returns (ok, err, principal). The principal is what the request path
-- authorizes against; nil on failure.
function Auth:verify(user, pass, ip)
    local now = socket.gettime()
    self:_maybe_sweep(now)

    ip = ip or "?"

    local banned, remaining = self:is_banned(ip)
    if banned then
        return false, string.format("ip banned for %d more seconds", remaining)
    end

    local record = self.store:get(user)

    -- Fast path: exactly the credentials that last verified successfully for
    -- this user. Skips the expensive PBKDF2 for legitimate reconnects; a wrong
    -- guess never matches (the digest is HMAC-keyed and success-only).
    -- Revealing "these are the good credentials" via the faster path is moot —
    -- a successful AUTH reveals that anyway.
    local digest = self:_cred_digest(user, pass)
    if record and self.cred_cache[user]
       and compare_secure(digest, self.cred_cache[user]) then
        self:note_success(ip)
        return true, nil, principal_of(record)
    end

    -- Concurrency gate on the expensive derivation. Rejected attempts do NOT
    -- count toward the IP ban — the gate trips under an attacker's parallel
    -- flood, and a legitimate user caught in it should retry, not get banned.
    if self.inflight >= self.max_inflight then
        return false, "auth busy, retry"
    end

    -- An unknown username is verified against the decoy credential so it costs
    -- the same derivation a real one does. Without this, response time answers
    -- "does this account exist?" for free, and a user list is the first half of
    -- a credential-stuffing run.
    local parsed = record and record.parsed or self.decoy

    self.inflight = self.inflight + 1
    -- pcall guards the inflight counter — a yield_fn that throws at shutdown
    -- must not leak a slot.
    local ok, pass_ok = pcall(M.verify_password, parsed, pass or "",
        { yield_fn = self.yield_fn })
    self.inflight = self.inflight - 1
    if not ok then
        return false, "auth aborted"
    end

    if record and pass_ok then
        self.cred_cache[user] = digest
        self:note_success(ip)
        return true, nil, principal_of(record)
    end

    self:_record_failure(ip, now)
    return false, "invalid credentials"
end

------------------------------------------------------------------------------
-- SCRAM support
--
-- The handler owns the exchange (it spans three frames and lives on the
-- connection); the authenticator owns the credential lookup and the lockout
-- bookkeeping, so both mechanisms share one ban table.
------------------------------------------------------------------------------

-- What the server needs to answer a client-first message. For an unknown user
-- this returns a DECOY whose salt and iteration count are derived from the
-- username — stable across attempts, indistinguishable from a real account,
-- and guaranteed to fail at proof verification. A server that answered "no
-- such user" here, or that produced a fresh random salt each time, would leak
-- the user list to anyone who can open a socket.
--
-- Returns (credential, principal_or_nil) where credential =
-- { salt, iterations, stored_key, server_key }.
function Auth:scram_credential(username)
    local record = self.store:get(username)
    if record then
        local stored_key, server_key = M.scram_keys(record.parsed)
        return {
            salt       = record.parsed.salt,
            iterations = record.parsed.iterations,
            stored_key = stored_key,
            server_key = server_key,
        }, principal_of(record)
    end

    local salt_hex = sha2.hmac(sha2.sha256, self.decoy_key, "salt\0" .. (username or ""))
    local key_hex  = sha2.hmac(sha2.sha256, self.decoy_key, "key\0" .. (username or ""))
    return {
        salt       = sha2.hex_to_bin(salt_hex):sub(1, DEFAULT_SALT_BYTES),
        iterations = self.decoy.iterations,
        stored_key = sha2.hex_to_bin(key_hex),
        server_key = sha2.hex_to_bin(key_hex),
    }, nil
end

return M