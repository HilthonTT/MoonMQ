local msg_m = require("src.message")
local snappy = require("src.snappy")
local zlib = require("zlib")

-- CompressionType defines the compression algorithm used
local CompressionType = {
    -- CompressionNone means no compression is used
    CompressionNone     = 0,
    -- CompressionGzip uses gzip compression
    CompressionGzip     = 1,
    -- CompressionSnappy uses snappy compression
    CompressionSnappy   = 2,
}

local function compress_gzip(data)
    -- window_size = 31 selects the gzip wrapper (15 base + 16 gzip flag).
    -- The default of 15 produces raw zlib output, which gunzip can't read.
    -- pcall'd because lua-zlib raises on bad input rather than returning err.
    local stream = zlib.deflate(zlib.BEST_COMPRESSION, 31)
    local ok, compressed = pcall(stream, data, "finish")
    if not ok then
        return nil, ("failed to compress with gzip: %s"):format(compressed)
    end
    return compressed, nil
end

local function compress_snappy(data)
    local compressed, err = snappy.compress(data)
    if not compressed then
        return nil, ("failed to compress with snappy: %s"):format(err)
    end
    return compressed, nil
end

local function decompress_gzip(data)
    -- windowBits = 31 = gzip-only inflate, matching our compress side. Use
    -- 47 (= 15 + 32) if you want zlib-or-gzip auto-detection.
    local stream = zlib.inflate(31)
    local ok, decompressed = pcall(stream, data, "finish")
    if not ok then
        return nil, ("failed to decompress with gzip: %s"):format(decompressed)
    end
    return decompressed, nil
end

local function decompress_snappy(data)
    local decompressed, err = snappy.uncompress(data)
    if not decompressed then
        return nil, ("failed to decompress with snappy: %s"):format(err)
    end
    return decompressed, nil
end

local CompressedMessage = {}
CompressedMessage.__index = CompressedMessage

function CompressedMessage.new(msg, compression_type)
    assert(getmetatable(msg) == msg_m.Message, "msg must be a Message instance")
    assert(type(compression_type) == "number", "compression_type must be a number")
    
    local value, err
    if compression_type == CompressionType.CompressionNone then
        value = msg.value
    elseif compression_type == CompressionType.CompressionGzip then
        value, err = compress_gzip(msg.value)
    elseif compression_type == CompressionType.CompressionSnappy then
        value, err = compress_snappy(msg.value)
    else
        return nil, ("unknown compression type: %d"):format(compression_type)
    end

    if err then
        return nil, err
    end

    return setmetatable({
        key              = msg.key,
        timestamp        = msg.timestamp,
        value            = value,
        compression_type = compression_type,
    }, CompressedMessage), nil
end

-- to_message returns a plain Message reconstructed from this compressed
-- record. The returned Message's value is still the compressed bytes —
-- decompress separately if you need the original payload.
function CompressedMessage:to_message()
    return msg_m.Message.new(self.key, self.value, self.timestamp)
end

-- decompress reconstructs the original Message from a CompressedMessage.
-- Inverse of CompressedMessage.new. Returns (Message, nil) on success or
-- (nil, err) on failure.
local function decompress(comp_msg)
    assert(getmetatable(comp_msg) == CompressedMessage,
           "comp_msg must be a CompressedMessage instance")

    local value, err
    local ct = comp_msg.compression_type

    if ct == CompressionType.NONE then
        value = comp_msg.value
    elseif ct == CompressionType.GZIP then
        value, err = decompress_gzip(comp_msg.value)
    elseif ct == CompressionType.SNAPPY then
        value, err = decompress_snappy(comp_msg.value)
    else
        return nil, ("unknown compression type: %d"):format(ct)
    end

    if err then
        return nil, err
    end

    return msg_m.Message.new(comp_msg.key, value, comp_msg.timestamp), nil
end

-- Method form for ergonomics: `comp_msg:decompress()` is identical to
-- the free `decompress(comp_msg)` above.
function CompressedMessage:decompress()
    return decompress(self)
end

return {
    CompressionType = CompressionType,
    CompressedMessage = CompressedMessage,
    decompress = decompress,
}
