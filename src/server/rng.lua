-- Cryptographically-secure random bytes. Used for HMAC keys, password
-- salts, and any other value that must be unpredictable to a remote
-- attacker.
--
-- Strategy: prefer the OS CSPRNG (/dev/urandom on Unix, BCryptGenRandom
-- on Windows via FFI). Fall back to math.random ONLY with a loud
-- warning.

local IS_WINDOWS = package.config:sub(1, 1) == "\\"
 
local M = {}

local function urandom_unix(n)
    local f, err = io.open("/dev/urandom", "rb")
    if not f then return nil, err end
    local bytes = f:read(n)
    f:close()
    if not bytes or #bytes < n then
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
        io.stderr:write(
            "[rng] WARNING: no OS CSPRNG available, falling back to math.random. " ..
            "This is NOT cryptographically secure. " ..
            "Use Lua on Linux/macOS, or LuaJIT on Windows with bcrypt.dll.\n")
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
 
    local picked = IS_WINDOWS and urandom_windows or urandom_unix
    if picked then
        local b, err = picked(n)
        if b then return b end
        io.stderr:write(string.format("[rng] OS CSPRNG failed (%s); falling back\n", err))
    end
    return urandom_fallback(n)
end
 
return M