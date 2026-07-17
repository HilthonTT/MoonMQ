-- Cryptographically-secure random bytes. Used for HMAC keys, password
-- salts, and any other value that must be unpredictable to a remote
-- attacker.
--
-- Strategy: prefer the OS CSPRNG (/dev/urandom on Unix, BCryptGenRandom
-- on Windows via FFI). Fall back to math.random ONLY with a loud
-- warning.

local os_utils = require("src.core.os")
local log      = require("src.log.logger").get("rng")

local M = {}

-- Keep /dev/urandom open across calls. Every UUID, salt, and connection
-- ID allocation hits this path; opening/closing the file each time turns
-- a cheap read into 3 syscalls. The handle is process-lifetime; if the
-- read ever fails we drop the cached handle and retry once on next call.
local urandom_handle = nil
local function urandom_unix(n)
    if not urandom_handle then
        local f, err = io.open("/dev/urandom", "rb")
        if not f then return nil, err end
        f:setvbuf("no")  -- don't buffer randomness through stdio
        urandom_handle = f
    end
    local bytes = urandom_handle:read(n)
    if not bytes or #bytes < n then
        -- Drop the cached handle; the next call will reopen.
        pcall(function() urandom_handle:close() end)
        urandom_handle = nil
        return nil, "short read from /dev/urandom"
    end
    return bytes
end

local urandom_windows
do
    local has_ffi, ffi = pcall(require, "ffi")
    if has_ffi then
        ffi.cdef[[
            long BCryptGenRandom(void* hAlgorithm,
                                 unsigned char* pbBuffer,
                                 unsigned long cbBuffer,
                                 unsigned long dwFlags);
        ]]
        local ok, bcrypt = pcall(ffi.load, "bcrypt")
        if ok then
            local USE_SYSTEM_RNG = 2
            urandom_windows = function(n)
                local buf = ffi.new("unsigned char[?]", n)
                local status = bcrypt.BCryptGenRandom(nil, buf, n, USE_SYSTEM_RNG)
                if status ~= 0 then
                    return nil, string.format("BCryptGenRandom: 0x%08X", status)
                end
                return ffi.string(buf, n)
            end
        end
    end
end

local fallback_warned = false
local function urandom_fallback(n)
    if not fallback_warned then
        log:warn("no OS CSPRNG available, falling back to math.random. " ..
            "This is NOT cryptographically secure. " ..
            "Use Lua on Linux/macOS, or LuaJIT on Windows with bcrypt.dll.")
        fallback_warned = true
 
        local socket_ok, socket = pcall(require, "socket")
        local seed_a = os.time()
        local seed_b = socket_ok
            and math.floor(socket.gettime() * 1e9)
            or  math.floor(os.clock() * 1e6)
 
        if not pcall(math.randomseed, seed_a, seed_b) then
            pcall(math.randomseed, seed_a)
        end
    end
 
    local bytes = {}
    for i = 1, n do
        bytes[i] = string.char(math.random(0, 255))
    end
    return table.concat(bytes)
end

function M.bytes(n)
    assert(type(n) == "number" and n > 0, "n must be a positive integer")
 
    -- Explicit branch, not `and/or`: on Windows without FFI, urandom_windows
    -- is nil and the `or` would fall through to urandom_unix — a guaranteed
    -- /dev/urandom miss that spams a "CSPRNG failed" warning on every call.
    local picked
    if os_utils.IS_WINDOWS then
        picked = urandom_windows
    else
        picked = urandom_unix
    end
    if picked then
        local b, err = picked(n)
        if b then return b end
        log:warn("OS CSPRNG failed (%s); falling back", err)
    end
    return urandom_fallback(n)
end
 
return M