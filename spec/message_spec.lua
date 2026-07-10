local message_m = require("src.record.message")
local Message       = message_m.Message
local MessageHeader = message_m.MessageHeader
local serialize     = message_m.serialize_message

describe("Message.new", function()
    it("creates a message with the given fields", function()
        local m = Message.new("k", "v", 100)
        assert.are.equal("k", m.key)
        assert.are.equal("v", m.value)
        assert.are.equal(100, m.timestamp)
    end)

    it("rejects non-string key", function()
        assert.has_error(function() Message.new(1, "v", 0) end)
    end)

    it("rejects non-string value", function()
        assert.has_error(function() Message.new("k", 2, 0) end)
    end)

    it("rejects non-number timestamp", function()
        assert.has_error(function() Message.new("k", "v", "now") end)
    end)

    it("accepts empty key and value", function()
        local m = Message.new("", "", 0)
        assert.are.equal("", m.key)
        assert.are.equal("", m.value)
    end)
end)

describe("MessageHeader.new", function()
    it("creates a header with the given fields", function()
        local h = MessageHeader.new(4, 42)
        assert.are.equal(4, h.key_size)
        assert.are.equal(42, h.timestamp)
    end)

    it("rejects non-number key_size", function()
        assert.has_error(function() MessageHeader.new("4", 0) end)
    end)

    it("rejects non-number timestamp", function()
        assert.has_error(function() MessageHeader.new(0, "0") end)
    end)
end)

describe("serialize_message", function()
    it("rejects a non-Message argument", function()
        assert.has_error(function() serialize({key = "k", value = "v", timestamp = 0}) end)
    end)

    it("emits the v2 layout: len(8)|hdr(13)|hdrCRC(4)|payload|payCRC(4)", function()
        local m = Message.new("k1", "abcd", 0x01020304)
        local bytes, err = serialize(m)
        assert.is_nil(err)
        assert.is_string(bytes)

        -- payload size = #key + #value = 2 + 4 = 6
        -- total (excluding length prefix) = 13 + 4 + 6 + 4 = 27
        assert.are.equal(8 + 27, #bytes)

        local total = string.unpack(">I8", bytes, 1)
        assert.are.equal(27, total)

        -- header = attrs(1) | k_size(4) | ts(8), starting at byte 9.
        local attrs = string.unpack(">B", bytes, 9)
        assert.are.equal(0, attrs)

        local key_size = string.unpack(">I4", bytes, 10)
        assert.are.equal(2, key_size)

        local ts = string.unpack(">I8", bytes, 14)
        assert.are.equal(0x01020304, ts)
    end)

    it("round-trips header and payload CRCs that validate", function()
        local crc32 = require("src.core.crc32")
        local HDR = message_m.HEADER_LEN
        local m = Message.new("AA", "BBBB", 7)
        local bytes = serialize(m)

        local header  = bytes:sub(9, 9 + HDR - 1)
        local hdr_crc = string.unpack(">I4", bytes, 9 + HDR)
        assert.are.equal(crc32(header), hdr_crc)

        local payload_start = 9 + HDR + 4
        local payload_len   = #m.key + #m.value
        local payload = bytes:sub(payload_start, payload_start + payload_len - 1)
        local pay_crc = string.unpack(">I4", bytes, payload_start + payload_len)

        assert.are.equal("AABBBB", payload)
        assert.are.equal(crc32(payload), pay_crc)
    end)

    it("stores and round-trips the attrs byte via decode_body", function()
        local attrs = message_m.CODEC_SNAPPY | message_m.ATTR_CONTROL
        local m = Message.new("k", "v", 5, attrs)
        local bytes = serialize(m)
        -- Strip the 8-byte length prefix and decode the body.
        local decoded, derr = message_m.decode_body(bytes:sub(9))
        assert.is_nil(derr)
        assert.are.equal("k", decoded.key)
        assert.are.equal("v", decoded.value)
        assert.are.equal(attrs, decoded.attrs)
        assert.are.equal(message_m.CODEC_SNAPPY, decoded:codec())
        assert.is_true(decoded:is_control())
    end)

    it("decode_body rejects a corrupt CRC", function()
        local m = Message.new("k", "value", 5)
        local bytes = serialize(m)
        local body = bytes:sub(9)
        -- Flip a byte in the payload region (last payload byte before its CRC).
        local i = #body - 4
        local corrupted = body:sub(1, i - 1)
            .. string.char((body:byte(i) ~ 0xFF) & 0xFF)
            .. body:sub(i + 1)
        local decoded, derr = message_m.decode_body(corrupted)
        assert.is_nil(decoded)
        assert.is_string(derr)
    end)

    it("deserialize_record round-trips through a file handle", function()
        local m = Message.new("key", "payload", 9, message_m.CODEC_GZIP)
        local bytes = serialize(m)
        local path = os.tmpname()
        local wf = assert(io.open(path, "wb")); wf:write(bytes); wf:close()
        local rf = assert(io.open(path, "rb"))
        local got, framed, err = message_m.deserialize_record(rf)
        rf:close(); os.remove(path)
        assert.is_nil(err)
        assert.are.equal(#bytes, framed)
        assert.are.equal("key", got.key)
        assert.are.equal("payload", got.value)
        assert.are.equal(message_m.CODEC_GZIP, got:codec())
    end)
end)

