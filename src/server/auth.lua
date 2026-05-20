-- Authentication with constant-time credential compare and per-IP
-- failure tracking.
--
-- Stored hash format: pbkdf2-sha256$<iterations>$<salt_hex>$<hash_hex>

local sha2   = require("sha2")
local socket = require("socket")
local rng    = require("src.server.rng")
 
local M = {}
 
local DEFAULT_PBKDF2_ITERATIONS = 10000
local DEFAULT_SALT_BYTES        = 16
local DEFAULT_HASH_BYTES        = 32

local DEFAULT_PBKDF2_ITERATIONS = 10000
local DEFAULT_SALT_BYTES        = 16
local DEFAULT_HASH_BYTES        = 32

local function xor_strings(a, b)
    local out = {}
    for i = 1, #a do
        out[i] = string.char(a:byte(i) ~ b:byte(i))
    end
    return table.concat(out)
end

local function pbkdf2_hmac_sha256(password, salt, iterations, dklen)
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

local function compare_secure(a, b, key)
    if type(a) ~= "string" or type(b) ~= "string" then return false end

    local ha = sha2.hmac(sha2.sha256, key, a)
    local hb = sha2.hmac(sha2.sha256, key, b)
    if #ha ~= #hb then return false end

    local diff = 0
    for i = 1, #ha do
        diff = diff | (ha:byte(i) ~ hb:byte(i))
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
        io.stderr:write(
            "[auth] WARNING: hashing plaintext password on startup. " ..
            "Replace Auth.Password with Auth.PasswordHash in config; " ..
            "generate the hash with `lua bin/moonmq-hash.lua <password>`.\n")
        password_hash = M.hash_password(opts.password)
    else
        error("static_authenticator: provide password_hash or password")
    end
 
    return setmetatable({
        username       = opts.username,
        password_hash  = password_hash,
        hmac_key       = rng.bytes(32),
        max_failures   = opts.max_failures   or 5,
        failure_window = opts.failure_window or 60,
        ban_duration   = (opts.ban_duration   or 15) * 60,
        failures       = {},
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
        end
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
    if not rec or (now - (rec.first_at or 0)) > self.failure_window then
        rec = { count = 1, first_at = now }
    else
        rec.count = rec.count + 1
    end
    if rec.count >= self.max_failures then
        rec.banned_until = now + self.ban_duration
        io.stderr:write(string.format(
            "[auth] banning %s for %ds after %d failures\n",
            ip, self.ban_duration, rec.count))
    end
    self.failures[ip] = rec
end
 
function Auth:_verify_password(password, stored)
    local parsed, perr = parse_hash(stored)
    if not parsed then return false, perr end
    local computed = pbkdf2_hmac_sha256(
        password, parsed.salt, parsed.iterations, #parsed.hash)
    return compare_secure(computed, parsed.hash, self.hmac_key)
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
    local user_ok = compare_secure(user or "", self.username, self.hmac_key)
    local pass_ok = self:_verify_password(pass or "", self.password_hash)
 
    if user_ok and pass_ok then
        self.failures[ip] = nil
        return true, nil
    end
 
    self:_record_failure(ip, now)
    return false, "invalid credentials"
end

return M