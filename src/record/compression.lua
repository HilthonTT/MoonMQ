-- Compression codecs for stored record values. Both codecs are OPTIONAL and
-- loaded lazily so a broker missing lua-zlib and/or libsnappy still boots and
-- only rejects produce requests that ask for the unavailable codec.
--
--   * gzip   — via lua-zlib (pure C rock).
--   * snappy — via src/record/snappy.lua (LuaJIT FFI + libsnappy).
--
-- Codec ids are the same integers stored in the record's `attrs` byte (see
-- src/record/message.lua): 0 none, 1 gzip, 2 snappy. This module is the single
-- place that turns a codec id + bytes into compressed/decompressed bytes; the
-- broker produce/deliver paths call M.compress / M.decompress and never touch
-- the underlying libraries directly.

local msg_m = require("src.record.message")

local CompressionType = {
    CompressionNone   = msg_m.CODEC_NONE,
    CompressionGzip   = msg_m.CODEC_GZIP,
    CompressionSnappy = msg_m.CODEC_SNAPPY,
}

-- Lazy zlib handle. nil = not yet probed, false = probed & unavailable,
-- table = the loaded module. Deferred so `require`ing this module never fails
-- on a host without lua-zlib.
local zlib_state = nil
local function get_zlib()
    if zlib_state == nil then
        local ok, z = pcall(require, "zlib")
        zlib_state = (ok and z) or false
    end
    return zlib_state or nil
end

local snappy = require("src.record.snappy")  -- load-safe; self-reports availability

-- available reports whether `codec_id` can be used on this host right now.
-- CompressionNone is always available.
local function available(codec_id)
    if codec_id == msg_m.CODEC_NONE   then return true end
    if codec_id == msg_m.CODEC_GZIP   then return get_zlib() ~= nil end
    if codec_id == msg_m.CODEC_SNAPPY then return snappy.available end
    return false
end

-- codec_id maps a human name ("none"/"gzip"/"snappy") to its integer id, or
-- returns (nil, err) for an unknown name. Case-insensitive.
local NAME_TO_ID = {
    none   = msg_m.CODEC_NONE,
    gzip   = msg_m.CODEC_GZIP,
    snappy = msg_m.CODEC_SNAPPY,
}
local function codec_id(name)
    local id = NAME_TO_ID[tostring(name):lower()]
    if id == nil then
        return nil, string.format("unknown compression codec %q", tostring(name))
    end
    return id
end

local function codec_name(id)
    if id == msg_m.CODEC_NONE   then return "none" end
    if id == msg_m.CODEC_GZIP   then return "gzip" end
    if id == msg_m.CODEC_SNAPPY then return "snappy" end
    return string.format("unknown(%s)", tostring(id))
end

local function gzip_compress(data)
    local zlib = get_zlib()
    if not zlib then return nil, "gzip unavailable (lua-zlib not installed)" end
    -- window_size = 31 selects the gzip wrapper (15 base + 16 gzip flag).
    -- pcall'd because lua-zlib raises on bad input rather than returning err.
    local stream = zlib.deflate(zlib.BEST_COMPRESSION, 31)
    local ok, out = pcall(stream, data, "finish")
    if not ok then
        return nil, string.format("failed to compress with gzip: %s", out)
    end
    return out, nil
end

local function gzip_decompress(data)
    local zlib = get_zlib()
    if not zlib then return nil, "gzip unavailable (lua-zlib not installed)" end
    -- windowBits = 31 = gzip-only inflate, matching our compress side.
    local stream = zlib.inflate(31)
    local ok, out = pcall(stream, data, "finish")
    if not ok then
        return nil, string.format("failed to decompress with gzip: %s", out)
    end
    return out, nil
end

-- compress returns (bytes, nil) or (nil, err). CompressionNone is a pass-through
-- so callers can hand any codec id through uniformly.
local function compress(codec_id_, data)
    assert(type(data) == "string", "data must be a string")
    if codec_id_ == msg_m.CODEC_NONE then
        return data, nil
    elseif codec_id_ == msg_m.CODEC_GZIP then
        return gzip_compress(data)
    elseif codec_id_ == msg_m.CODEC_SNAPPY then
        return snappy.compress(data)
    end
    return nil, string.format("unknown compression codec id %s", tostring(codec_id_))
end

-- decompress is the inverse of compress. (bytes, nil) or (nil, err).
local function decompress(codec_id_, data)
    assert(type(data) == "string", "data must be a string")
    if codec_id_ == msg_m.CODEC_NONE then
        return data, nil
    elseif codec_id_ == msg_m.CODEC_GZIP then
        return gzip_decompress(data)
    elseif codec_id_ == msg_m.CODEC_SNAPPY then
        return snappy.uncompress(data)
    end
    return nil, string.format("unknown compression codec id %s", tostring(codec_id_))
end

return {
    CompressionType = CompressionType,
    available   = available,
    codec_id    = codec_id,
    codec_name  = codec_name,
    compress    = compress,
    decompress  = decompress,
}
