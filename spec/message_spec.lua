-- spec/message_spec.lua
local message_m = require("src.message")
local Message       = message_m.Message
local MessageHeader = message_m.MessageHeader
local serialize     = message_m.serialize_message
local Pool          = message_m.Pool

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

    it("emits the documented layout: len(8)|hdr(12)|hdrCRC(4)|payload|payCRC(4)", function()
        local m = Message.new("k1", "abcd", 0x01020304)
        local bytes, err = serialize(m)
        assert.is_nil(err)
        assert.is_string(bytes)

        -- payload size = #key + #value = 2 + 4 = 6
        -- total (excluding length prefix) = 12 + 4 + 6 + 4 = 26
        assert.are.equal(8 + 26, #bytes)

        local total = string.unpack(">I8", bytes, 1)
        assert.are.equal(26, total)

        local key_size = string.unpack(">I4", bytes, 9)
        assert.are.equal(2, key_size)

        local ts = string.unpack(">I8", bytes, 13)
        assert.are.equal(0x01020304, ts)
    end)

    it("round-trips header and payload CRCs that validate", function()
        local crc32 = require("src.crc32")
        local m = Message.new("AA", "BBBB", 7)
        local bytes = serialize(m)

        local header  = bytes:sub(9, 9 + 12 - 1)
        local hdr_crc = string.unpack(">I4", bytes, 9 + 12)
        assert.are.equal(crc32(header), hdr_crc)

        local payload_start = 9 + 12 + 4
        local payload_len   = #m.key + #m.value
        local payload = bytes:sub(payload_start, payload_start + payload_len - 1)
        local pay_crc = string.unpack(">I4", bytes, payload_start + payload_len)

        assert.are.equal("AABBBB", payload)
        assert.are.equal(crc32(payload), pay_crc)
    end)
end)

describe("Pool", function()
    it("uses the factory when empty", function()
        local n = 0
        local p = Pool.new(function() n = n + 1; return {id = n} end)
        local a = p:get()
        local b = p:get()
        assert.are.equal(1, a.id)
        assert.are.equal(2, b.id)
    end)

    it("recycles items via put/get", function()
        local p = Pool.new(function() return {} end)
        local a = p:get()
        p:put(a)
        local b = p:get()
        assert.are.equal(a, b)
    end)

    it("calls reset() before retaining an item", function()
        local reset_calls = 0
        local p = Pool.new(
            function() return {x = 99} end,
            function(o) reset_calls = reset_calls + 1; o.x = 0 end
        )
        local item = p:get()
        p:put(item)
        assert.are.equal(1, reset_calls)
        assert.are.equal(0, item.x)
    end)

    it("drops items beyond max", function()
        local p = Pool.new(function() return {} end, nil, 2)
        p:put({}); p:put({}); p:put({})
        assert.are.equal(2, p.count)
    end)
end)
