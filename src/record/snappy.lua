-- LuaJIT FFI binding to libsnappy. Used in place of the `snappy` luarocks
-- rock, which is not published for Lua 5.1. API mirrors what the rock
-- would have exposed but uses (value, err) return convention.

local ffi = require("ffi")

ffi.cdef[[
typedef enum {
    SNAPPY_OK = 0,
    SNAPPY_INVALID_INPUT = 1,
    SNAPPY_BUFFER_TOO_SMALL = 2
} snappy_status;

size_t snappy_max_compressed_length(size_t source_length);

snappy_status snappy_compress(const char* input,
                              size_t input_length,
                              char* compressed,
                              size_t* compressed_length);

snappy_status snappy_uncompressed_length(const char* compressed,
                                          size_t compressed_length,
                                          size_t* result);

snappy_status snappy_uncompress(const char* compressed,
                                size_t compressed_length,
                                char* uncompressed,
                                size_t* uncompressed_length);
]]

-- ffi.load("snappy") resolves to libsnappy.so on Linux/WSL and
-- libsnappy.dylib on macOS. The unversioned symlink is provided by the
-- -dev package; if that's missing, libsnappy.so.1 is the runtime name.
local lib = ffi.load("snappy")

local SNAPPY_OK              = 0
local SNAPPY_INVALID_INPUT   = 1
local SNAPPY_BUFFER_TOO_SMALL = 2

-- Upper bound on a single decompressed frame. snappy_uncompressed_length only
-- parses the varint length prefix at the front of the frame — it does NOT
-- validate the body — so a tiny (~5-byte) malformed input can declare a ~4 GiB
-- output and force that allocation before snappy_uncompress ever runs (a
-- memory-amplification bomb). 256 MiB is far above any real message while still
-- refusing an obviously bogus declared length. Callers that legitimately need
-- larger frames can raise this.
local MAX_UNCOMPRESSED_BYTES = 256 * 1024 * 1024

local function status_name(code)
    code = tonumber(code)
    if     code == SNAPPY_OK              then return "OK"
    elseif code == SNAPPY_INVALID_INPUT   then return "INVALID_INPUT"
    elseif code == SNAPPY_BUFFER_TOO_SMALL then return "BUFFER_TOO_SMALL"
    else   return string.format("UNKNOWN(%d)", code)
    end
end

local function compress(data)
    assert(type(data) == "string", "data must be a string")

    local in_len  = #data
    local max_len = tonumber(lib.snappy_max_compressed_length(in_len))
    local out     = ffi.new("char[?]", max_len)
    local out_len = ffi.new("size_t[1]", max_len)

    local status = lib.snappy_compress(data, in_len, out, out_len)
    if status ~= SNAPPY_OK then
        return nil, string.format("snappy_compress: %s", status_name(status))
    end

    return ffi.string(out, out_len[0]), nil
end

local function uncompress(data)
    assert(type(data) == "string", "data must be a string")

    local in_len  = #data
    local out_len = ffi.new("size_t[1]")

    -- Snappy stores the uncompressed length in the frame header — read it
    -- first so we can size the output buffer exactly. No iterative grow.
    local status = lib.snappy_uncompressed_length(data, in_len, out_len)
    if status ~= SNAPPY_OK then
        return nil, string.format("snappy_uncompressed_length: %s", status_name(status))
    end

    -- Reject an attacker-declared length before allocating for it. Without this
    -- a malformed frame declaring a huge length triggers a giant allocation
    -- (bomb), and ffi.new itself RAISES on allocation failure rather than
    -- returning nil — so the allocation + uncompress is pcall-wrapped to turn
    -- any such failure into a clean (nil, err) instead of an uncaught crash.
    local declared = tonumber(out_len[0])
    if declared > MAX_UNCOMPRESSED_BYTES then
        return nil, string.format(
            "snappy_uncompress: declared length %d exceeds max %d",
            declared, MAX_UNCOMPRESSED_BYTES)
    end

    local ok, result, err = pcall(function()
        local out = ffi.new("char[?]", out_len[0])
        local st  = lib.snappy_uncompress(data, in_len, out, out_len)
        if st ~= SNAPPY_OK then
            return nil, string.format("snappy_uncompress: %s", status_name(st))
        end
        return ffi.string(out, out_len[0]), nil
    end)
    if not ok then
        -- pcall caught a raised error (e.g. ffi.new allocation failure); `result`
        -- holds the error object here.
        return nil, string.format("snappy_uncompress: %s", tostring(result))
    end
    if result == nil then
        return nil, err
    end
    return result, nil
end

return {
    compress   = compress,
    uncompress = uncompress,
}
