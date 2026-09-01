local sha2     = require("src.vendor.sha2")
local socket   = require("socket")
local rng      = require("src.core.rng")
local ct       = require("src.core.ct")
local pbkdf2_m = require("src.core.pbkdf2")
local scram    = require("src.server.scram")
local acl_m    = require("src.server.acl")
local log      = require("src.log.logger").get("auth")

local M = {}

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

local COST_CHECK_MIN_ITERATIONS = 1000
local COST_WARN_SECONDS         = 1.0

local function compare_secure(a, b, _key)
    return ct.equal(a, b)
end
M.compare_secure = compare_secure

M.FORMAT_PBKDF2 = "pbkdf2"
M.FORMAT_SCRAM  = "scram"

function M.hash_password(password, opts)
    opts = opts or {}
    local iterations = opts.iterations or DEFAULT_PBKDF2_ITERATIONS
    local salt_len   = opts.salt_bytes or DEFAULT_SALT_BYTES
    local hash_len   = opts.hash_bytes or DEFAULT_HASH_BYTES

    local salt = rng.bytes(salt_len)
    local hash = pbkdf2_hmac_sha256(password, salt, iterations, hash_len)

    if opts.format == M.FORMAT_SCRAM then
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

function M.scram_keys(parsed)
    if parsed.kind == M.FORMAT_SCRAM then
        return parsed.stored_key, parsed.server_key
    end
    return scram.keys_from_salted(parsed.hash)
end

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

        max_inflight   = opts.max_inflight or 4,
        inflight       = 0,
        yield_fn       = nil,
        cred_key       = rng.bytes(32),
        cred_cache     = {},
        decoy_key      = rng.bytes(32),
        decoy          = {
            kind       = M.FORMAT_PBKDF2,
            iterations = math.max(worst_iterations, 1),
            salt       = rng.bytes(DEFAULT_SALT_BYTES),
            hash       = rng.bytes(DEFAULT_HASH_BYTES),
        },
    }, Auth)
end

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
    a.username      = opts.username
    a.password_hash = password_hash
    return a
end

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

function Auth:_cred_digest(user, pass)
    return sha2.hmac(sha2.sha256, self.cred_key, (user or "") .. "\0" .. (pass or ""))
end

function Auth:note_success(ip)
    ip = ip or "?"
    if self.failures[ip] ~= nil then
        self.failures[ip] = nil
        self.failures_n = self.failures_n - 1
    end
end

function Auth:note_failure(ip)
    self:_record_failure(ip or "?", socket.gettime())
end

function Auth:principal(username)
    local user = self.store:get(username)
    return user and principal_of(user) or nil
end

function Auth:verify(user, pass, ip)
    local now = socket.gettime()
    self:_maybe_sweep(now)

    ip = ip or "?"

    local banned, remaining = self:is_banned(ip)
    if banned then
        return false, string.format("ip banned for %d more seconds", remaining)
    end

    local record = self.store:get(user)

    local digest = self:_cred_digest(user, pass)
    if record and self.cred_cache[user]
       and compare_secure(digest, self.cred_cache[user]) then
        self:note_success(ip)
        return true, nil, principal_of(record)
    end

    if self.inflight >= self.max_inflight then
        return false, "auth busy, retry"
    end

    local parsed = record and record.parsed or self.decoy

    self.inflight = self.inflight + 1
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