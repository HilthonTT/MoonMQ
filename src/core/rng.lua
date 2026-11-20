local os_utils = require("src.core.os")
local log      = require("src.log.logger").get("rng")

local M = {}

local urandom_handle = nil
local function urandom_unix(n)
    if not urandom_handle then
        local f, err = io.open("/dev/urandom", "rb")
        if not f then return nil, err end
        f:setvbuf("no")
        urandom_handle = f
    end
    local bytes = urandom_handle:read(n)
    if not bytes or #bytes < n then
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

local openssl_rand
do
    local ok, rand = pcall(require, "openssl.rand")
    if ok and type(rand) == "table" and type(rand.bytes) == "function" then
        openssl_rand = function(n)
            local ready_ok, ready = pcall(function()
                return rand.ready == nil or rand.ready()
            end)
            if ready_ok and not ready then
                return nil, "openssl.rand not seeded"
            end
            local got, bytes = pcall(rand.bytes, n)
            if not got then return nil, tostring(bytes) end
            if type(bytes) ~= "string" or #bytes ~= n then
                return nil, "short read from openssl.rand"
            end
            return bytes
        end
    end
end

M._sources = {}
if os_utils.IS_WINDOWS then
    if urandom_windows then
        M._sources[#M._sources + 1] = { name = "BCryptGenRandom", fn = urandom_windows }
    end
else
    M._sources[#M._sources + 1] = { name = "/dev/urandom", fn = urandom_unix }
end
if openssl_rand then
    M._sources[#M._sources + 1] = { name = "openssl.rand", fn = openssl_rand }
end

function M.bytes(n)
    assert(type(n) == "number" and n > 0, "n must be a positive integer")

    local errors = {}
    for i = 1, #M._sources do
        local source = M._sources[i]
        local b, err = source.fn(n)
        if b then return b end
        errors[#errors + 1] = string.format("%s: %s", source.name, tostring(err))
        log:warn("CSPRNG source %s failed (%s)", source.name, tostring(err))
    end
    if #errors == 0 then
        errors[1] = "no source available"
    end
    error("no cryptographically secure random source available ("
        .. table.concat(errors, "; ")
        .. "); install luaossl or run where /dev/urandom is readable", 2)
end

return M
