local compression = require("src.record.compression")
local message     = require("src.record.message")
local proto       = require("src.wire.protocol")
local brk_m       = require("src.broker")
local consumer_m  = require("src.broker.consumer")
local os_utils    = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_compress_test"
                                      or "/tmp/moonmq_compress_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function unframe(frame)
    return proto.parse_frame(frame:sub(5))
end

local function stored_message(codec, key, value)
    local bytes = assert(compression.compress(codec, value))
    return message.Message.new(key, bytes, 1, codec & message.ATTR_CODEC_MASK)
end

describe("compression module", function()
    it("none is a pass-through and always available", function()
        assert.is_true(compression.available(message.CODEC_NONE))
        local out = assert(compression.compress(message.CODEC_NONE, "hello"))
        assert.are.equal("hello", out)
        assert.are.equal("hello", assert(compression.decompress(message.CODEC_NONE, out)))
    end)

    it("round-trips gzip when available", function()
        if not compression.available(message.CODEC_GZIP) then
            pending("lua-zlib not installed")
            return
        end
        local data = string.rep("the quick brown fox ", 100)
        local c = assert(compression.compress(message.CODEC_GZIP, data))
        assert.is_true(#c < #data, "gzip should shrink a repetitive payload")
        assert.are.equal(data, assert(compression.decompress(message.CODEC_GZIP, c)))
    end)

    it("reports snappy availability honestly and errors when absent", function()
        if compression.available(message.CODEC_SNAPPY) then
            local data = string.rep("abc", 50)
            local c = assert(compression.compress(message.CODEC_SNAPPY, data))
            assert.are.equal(data, assert(compression.decompress(message.CODEC_SNAPPY, c)))
        else
            local c, err = compression.compress(message.CODEC_SNAPPY, "x")
            assert.is_nil(c)
            assert.is_string(err)
        end
    end)

    it("maps codec names to ids", function()
        assert.are.equal(message.CODEC_NONE,   assert(compression.codec_id("none")))
        assert.are.equal(message.CODEC_GZIP,   assert(compression.codec_id("GZIP")))
        assert.are.equal(message.CODEC_SNAPPY, assert(compression.codec_id("snappy")))
        assert.is_nil(compression.codec_id("lz4"))
    end)
end)

describe("compression wire format", function()
    it("PRODUCE carries the codec byte", function()
        local frame = proto.encode_produce(("\0"):rep(16), "orders", "k", "v",
            message.CODEC_GZIP)
        local _, _, payload = unframe(frame)
        local p = assert(proto.decode_produce(payload))
        assert.are.equal(message.CODEC_GZIP, p.codec)
        assert.are.equal("orders", p.topic)
        assert.are.equal("k", p.key)
        assert.are.equal("v", p.value)
    end)

    it("PRODUCE defaults to codec none", function()
        local frame = proto.encode_produce(("\0"):rep(16), "orders", "k", "v")
        local _, _, payload = unframe(frame)
        local p = assert(proto.decode_produce(payload))
        assert.are.equal(message.CODEC_NONE, p.codec)
    end)

    it("PRODUCE_IDEMPOTENT carries epoch + codec", function()
        local frame = proto.encode_produce_idempotent(("\0"):rep(16),
            42, 7, "orders", "k", "v", 3, message.CODEC_SNAPPY)
        local _, _, payload = unframe(frame)
        local p = assert(proto.decode_produce_idempotent(payload))
        assert.are.equal(42, p.pid)
        assert.are.equal(7, p.seq)
        assert.are.equal(3, p.epoch)
        assert.are.equal(message.CODEC_SNAPPY, p.codec)
        assert.are.equal("v", p.value)
    end)
end)

describe("compression end-to-end (store + read)", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("stores a compressed value and a consumer reads back plaintext", function()
        if not compression.available(message.CODEC_GZIP) then
            pending("lua-zlib not installed")
            return
        end
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("logs", 1))
        local topic = assert(broker:get_topic("logs"))
        local part  = topic.partitions[1]

        local plaintext = string.rep("compressible-log-line ", 200)
        local msg = stored_message(message.CODEC_GZIP, "", plaintext)
        assert(#msg.value < #plaintext, "value should be stored compressed")
        assert(part:write_message(msg))

        local c = consumer_m.Consumer.new(broker, "g")
        assert(c:subscribe("logs"))
        local recs = assert(c:poll())
        assert.are.equal(1, #recs)
        assert.are.equal(plaintext, recs[1].value,
            "consumer must receive decompressed plaintext")
    end)

    it("uncompressed records are unaffected", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("logs", 1))
        local part = assert(broker:get_topic("logs")).partitions[1]
        assert(part:write_message(message.Message.new("k", "plain", 1)))

        local c = consumer_m.Consumer.new(broker, "g")
        assert(c:subscribe("logs"))
        local recs = assert(c:poll())
        assert.are.equal(1, #recs)
        assert.are.equal("plain", recs[1].value)
    end)
end)
