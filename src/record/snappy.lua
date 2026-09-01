local M = {}

local ok, ffi = pcall(require, "ffi")
local lib
if ok and ffi then
    pcall(function()
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
        lib = ffi.load("snappy")
    end)
end

M.available = (lib ~= nil)

local SNAPPY_OK              = 0

local MAX_UNCOMPRESSED_BYTES = 256 * 1024 * 1024

local function status_name(code)
    code = tonumber(code)
    if     code == 0 then return "OK"
    elseif code == 1 then return "INVALID_INPUT"
    elseif code == 2 then return "BUFFER_TOO_SMALL"
    else   return string.format("UNKNOWN(%d)", code)
    end
end

function M.compress(data)
    assert(type(data) == "string", "data must be a string")
    if not lib then return nil, "snappy unavailable (no ffi/libsnappy)" end

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

function M.uncompress(data)
    assert(type(data) == "string", "data must be a string")
    if not lib then return nil, "snappy unavailable (no ffi/libsnappy)" end

    local in_len  = #data
    local out_len = ffi.new("size_t[1]")

    local status = lib.snappy_uncompressed_length(data, in_len, out_len)
    if status ~= SNAPPY_OK then
        return nil, string.format("snappy_uncompressed_length: %s", status_name(status))
    end

    local declared = tonumber(out_len[0])
    if declared > MAX_UNCOMPRESSED_BYTES then
        return nil, string.format(
            "snappy_uncompress: declared length %d exceeds max %d",
            declared, MAX_UNCOMPRESSED_BYTES)
    end

    local pok, result, err = pcall(function()
        local out = ffi.new("char[?]", out_len[0])
        local st  = lib.snappy_uncompress(data, in_len, out, out_len)
        if st ~= SNAPPY_OK then
            return nil, string.format("snappy_uncompress: %s", status_name(st))
        end
        return ffi.string(out, out_len[0]), nil
    end)
    if not pok then
        return nil, string.format("snappy_uncompress: %s", tostring(result))
    end
    if result == nil then
        return nil, err
    end
    return result, nil
end

return M
