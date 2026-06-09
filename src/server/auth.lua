-- Authentication with constant-time credential compare and per-IP
-- failure tracking.
--
-- Stored hash format: pbkdf2-sha256$<iterations>$<salt_hex>$<hash_hex>

local sha2   = require("sha2")
local socket = require("socket")
local rng    = require("src.server.rng")
local log    = require("src.log.logger").get("auth")

local M = {}

local DEFAULT_PBKDF2_ITERATIONS = 10000
local DEFAULT_SALT_BYTES        = 16
local DEFAULT_HASH_BYTES        = 32

local function xor_strings(a, b)
    assert(#a == #b, "xor_strings: length mismatch")
    local out = {}
    for i = 1, #a do
        out[i] = string.char(a:byte(i) ~ b:byte(i))
    end
    return table.concat(out)
end

local function pbkdf2_hmac_sha256(password, salt, iterations, dklen)
    assert(type(dklen) == "number" and dklen > 0, "dklen must be positive")
    local hLen = 32
    local blocks = math.ceil(dklen / hLen)
    local output = {}
    for block_i = 1, blocks do
        local int_be = string.pack(">I4", block_i)
        local u_hex  = sha2.hmac(sha2.sha256, password, salt .. int_be)
        local u_bin  = sha2.hex_to_bin(u_hex)
        local f      = u_bin

        for _ = 2, iterations do
            u_hex = sha2.hmac(sha2.sha256, password, u_bin)
            u_bin = sha2.hex_to_bin(u_hex)
            f     = xor_strings(f, u_bin)
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
    if iterations > 1000000 then
        return nil, "iterations exceeds maximum (1000000)"
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
    }, Auth)
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
        password, parsed.salt, parsed.iterations, #parsed.hash)
    return compare_secure(computed, parsed.hash)
end
 
function Auth:verify(user, pass, ip)
    local now = socket.gettime()
    self:_maybe_sweep(now)

    ip = ip or "?"

    local banned, remaining = self:is_banned(ip)
    if banned then
        return false, string.format("ip banned for %d more seconds", remaining)
    end

    -- Run BOTH compares regardless of intermediate results so timing
    -- doesn't reveal which field is wrong.
    local user_ok = compare_secure(user or "", self.username)
    local pass_ok = self:_verify_password(pass or "", self.password_hash)

    if user_ok and pass_ok then
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