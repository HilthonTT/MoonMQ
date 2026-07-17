-- Authentication with constant-time credential compare and per-IP
-- failure tracking.
--
-- Stored hash format: pbkdf2-sha256$<iterations>$<salt_hex>$<hash_hex>

local sha2   = require("src.vendor.sha2")
local socket = require("socket")
local rng    = require("src.core.rng")
local log    = require("src.log.logger").get("auth")

local M = {}

-- NIST SP 800-132 / OWASP 2024 guidance for PBKDF2-HMAC-SHA256.
-- 10000 (the previous default) is brute-forceable on commodity GPUs in
-- 2026. New hashes use 600k; existing stored hashes encode their own
-- iteration count so this only affects hash_password() callers.
local DEFAULT_PBKDF2_ITERATIONS = 600000
local DEFAULT_SALT_BYTES        = 16
local DEFAULT_HASH_BYTES        = 32

-- Exported so the hash CLI (bin/moonmq-hash.lua) and any other caller pick
-- up the same default instead of hard-coding a copy that silently drifts.
-- NOTE: verification cost scales linearly with this count, and PBKDF2 here
-- is pure-Lua. Three mitigations keep an unauthenticated attacker from
-- wedging the single reactor thread with it (each AUTH used to stall the
-- whole event loop for the full 600k iterations):
--   * verification yields back to the reactor every YIELD_EVERY iterations
--     when the Server installs a yield_fn (see Auth:set_yield_fn), so other
--     connections keep making progress during a derivation;
--   * a keyed digest of the last successful credentials short-circuits
--     re-verification, so legitimate reconnects don't pay PBKDF2 at all;
--   * at most max_inflight derivations run concurrently — beyond that AUTH
--     fails fast with "busy" (without counting toward the IP ban, so an
--     attacker can't use the gate to get victims banned).
M.DEFAULT_PBKDF2_ITERATIONS = DEFAULT_PBKDF2_ITERATIONS

-- How many PBKDF2 iterations run between yields when a yield_fn is
-- installed. ~8k iterations is a few ms of pure-Lua HMAC — long enough to
-- keep the derivation efficient, short enough that produce/fetch latency
-- stays flat while someone is authenticating.
local YIELD_EVERY = 8192

-- Upper bound on the iteration count parse_hash will accept. A corrupted or
-- typo'd config shouldn't be able to wedge auth in a 2^31-iter PBKDF2 loop.
-- Exported so the hash CLI (bin/moonmq-hash.lua) rejects the same ceiling and
-- can't emit a hash the broker then refuses to load at boot.
local MAX_PBKDF2_ITERATIONS = 1000000
M.MAX_PBKDF2_ITERATIONS = MAX_PBKDF2_ITERATIONS

local function xor_strings(a, b)
    assert(#a == #b, "xor_strings: length mismatch")
    local out = {}
    for i = 1, #a do
        out[i] = string.char(a:byte(i) ~ b:byte(i))
    end
    return table.concat(out)
end

-- opts (optional): { yield_fn = function() ... end } — called every
-- YIELD_EVERY iterations so a cooperative scheduler can interleave other
-- work. Callers without a scheduler (the hash CLI, tests) just omit it.
local function pbkdf2_hmac_sha256(password, salt, iterations, dklen, opts)
    assert(type(dklen) == "number" and dklen > 0, "dklen must be positive")
    local yield_fn = opts and opts.yield_fn
    local hLen = 32
    local blocks = math.ceil(dklen / hLen)
    local output = {}
    for block_i = 1, blocks do
        local int_be = string.pack(">I4", block_i)
        local u_hex  = sha2.hmac(sha2.sha256, password, salt .. int_be)
        local u_bin  = sha2.hex_to_bin(u_hex)
        local f      = u_bin

        for i = 2, iterations do
            u_hex = sha2.hmac(sha2.sha256, password, u_bin)
            u_bin = sha2.hex_to_bin(u_hex)
            f     = xor_strings(f, u_bin)
            if yield_fn and i % YIELD_EVERY == 0 then yield_fn() end
        end

        output[block_i] = f
    end

    return table.concat(output):sub(1, dklen)
end
M.pbkdf2_hmac_sha256 = pbkdf2_hmac_sha256

-- Constant-time string compare. The HMAC indirection that used to wrap
-- both inputs added no security: the key was a per-process random value
-- with no out-of-band sharing, so it didn't protect against any attacker
-- who couldn't already read process memory. We still want constant time
-- to avoid leaking field length / prefix matches via timing — but the
-- HMAC was pure overhead.
--
-- The `key` parameter is kept for API compatibility; it is ignored.
local function compare_secure(a, b, _key)
    if type(a) ~= "string" or type(b) ~= "string" then return false end

    -- Compare in constant time over max(|a|, |b|) so the length itself
    -- is also constant-time. Mismatched lengths always return false.
    local la, lb = #a, #b
    local n = la > lb and la or lb
    if n == 0 then return la == lb end

    local diff = la ~ lb  -- nonzero if lengths differ
    for i = 1, n do
        local ai = (i <= la) and a:byte(i) or 0
        local bi = (i <= lb) and b:byte(i) or 0
        diff = diff | (ai ~ bi)
    end
    return diff == 0
end
M.compare_secure = compare_secure

function M.hash_password(password, opts)
    opts = opts or {}
    local iterations = opts.iterations or DEFAULT_PBKDF2_ITERATIONS
    local salt_len   = opts.salt_bytes or DEFAULT_SALT_BYTES
    local hash_len   = opts.hash_bytes or DEFAULT_HASH_BYTES
 
    local salt = rng.bytes(salt_len)
    local hash = pbkdf2_hmac_sha256(password, salt, iterations, hash_len)
    return string.format("pbkdf2-sha256$%d$%s$%s",
        iterations, sha2.bin_to_hex(salt), sha2.bin_to_hex(hash))
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

    local ok_s, salt = pcall(sha2.hex_to_bin, salt_hex)
    if not ok_s then return nil, "invalid salt hex" end
 
    local ok_h, hash = pcall(sha2.hex_to_bin, hash_hex)
    if not ok_h then return nil, "invalid hash hex" end
 
    return { iterations = iterations, salt = salt, hash = hash }, nil
end
M.parse_hash = parse_hash

local Auth = {}
Auth.__index = Auth

function M.static_authenticator(opts)
    assert(type(opts.username) == "string", "username required")
 
    local password_hash
    if opts.password_hash then
        local _, perr = parse_hash(opts.password_hash)
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

    return setmetatable({
        username       = opts.username,
        password_hash  = password_hash,
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
        cred_cache     = nil,               -- digest of last good (user, pass)
    }, Auth)
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

function Auth:_verify_password(password, stored)
    local parsed, perr = parse_hash(stored)
    if not parsed then return false, perr end
    local computed = pbkdf2_hmac_sha256(
        password, parsed.salt, parsed.iterations, #parsed.hash,
        { yield_fn = self.yield_fn })
    return compare_secure(computed, parsed.hash)
end

-- Keyed digest of a credential pair for the success cache. \0 separates the
-- fields so ("ab","c") can't collide with ("a","bc").
function Auth:_cred_digest(user, pass)
    return sha2.hmac(sha2.sha256, self.cred_key, (user or "") .. "\0" .. (pass or ""))
end

function Auth:verify(user, pass, ip)
    local now = socket.gettime()
    self:_maybe_sweep(now)

    ip = ip or "?"

    local banned, remaining = self:is_banned(ip)
    if banned then
        return false, string.format("ip banned for %d more seconds", remaining)
    end

    -- Fast path: exactly the credentials that last verified successfully.
    -- Skips the expensive PBKDF2 for legitimate reconnects; a wrong guess
    -- never matches (the digest is HMAC-keyed and success-only). Revealing
    -- "these are the good credentials" via the faster path is moot — a
    -- successful AUTH reveals that anyway.
    local digest = self:_cred_digest(user, pass)
    if self.cred_cache and compare_secure(digest, self.cred_cache) then
        if self.failures[ip] ~= nil then
            self.failures[ip] = nil
            self.failures_n = self.failures_n - 1
        end
        return true, nil
    end

    -- Concurrency gate on the expensive derivation. Rejected attempts do NOT
    -- count toward the IP ban — the gate trips under an attacker's parallel
    -- flood, and a legitimate user caught in it should retry, not get banned.
    if self.inflight >= self.max_inflight then
        return false, "auth busy, retry"
    end

    self.inflight = self.inflight + 1
    -- Run BOTH compares regardless of intermediate results so timing
    -- doesn't reveal which field is wrong. pcall guards the inflight
    -- counter — a yield_fn that throws at shutdown must not leak a slot.
    local ok, user_ok, pass_ok = pcall(function()
        return compare_secure(user or "", self.username),
               self:_verify_password(pass or "", self.password_hash)
    end)
    self.inflight = self.inflight - 1
    if not ok then
        return false, "auth aborted"
    end

    if user_ok and pass_ok then
        self.cred_cache = digest
        if self.failures[ip] ~= nil then
            self.failures[ip] = nil
            self.failures_n = self.failures_n - 1
        end
        return true, nil
    end

    self:_record_failure(ip, now)
    return false, "invalid credentials"
end

return M