local msg_m = require("src.record.message")

local CompressionType = {
    CompressionNone   = msg_m.CODEC_NONE,
    CompressionGzip   = msg_m.CODEC_GZIP,
    CompressionSnappy = msg_m.CODEC_SNAPPY,
}

local zlib_state = nil
local function get_zlib()
    if zlib_state == nil then
        local ok, z = pcall(require, "zlib")
        zlib_state = (ok and z) or false
    end
    return zlib_state or nil
end

local snappy = require("src.record.snappy")

local function available(codec_id)
    if codec_id == msg_m.CODEC_NONE   then return true end
    if codec_id == msg_m.CODEC_GZIP   then return get_zlib() ~= nil end
    if codec_id == msg_m.CODEC_SNAPPY then return snappy.available end
    return false
end

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
    local stream = zlib.inflate(31)
    local ok, out = pcall(stream, data, "finish")
    if not ok then
        return nil, string.format("failed to decompress with gzip: %s", out)
    end
    return out, nil
end

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
