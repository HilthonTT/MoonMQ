-- ┌────────┬─────────────┬────────────┬───────────────┬─────────────┐
-- │ Length │ Header(12B) │ HeaderCRC  │ Payload(var)  │ PayloadCRC  │
-- │ (8B)   │ k_size(4)   │ (4B)       │ key||value    │ (4B)        │
-- │        │ ts(8)       │            │               │             │
-- └────────┴─────────────┴────────────┴───────────────┴─────────────┘
--
-- Length covers everything after the prefix: header(12) + header_crc(4) +
-- payload + payload_crc(4). Both CRCs are IEEE 802.3 CRC-32 over the
-- preceding bytes, big-endian.

local crc32 = require("src.crc32")

local Message = {}
Message.__index = Message

function Message.new(key, value, timestamp)
    assert(type(key) == "string", "key must be a string")
    assert(type(value) == "string", "value must be a string")
    assert(type(timestamp) == "number", "timestamp must be a number")

    return setmetatable({
        key = key,
        value = value,
        timestamp = timestamp,
    }, Message)
end

local MessageHeader = {}
MessageHeader.__index = MessageHeader

function MessageHeader.new(key_size, timestamp)
    assert(type(key_size) == "number", "key_size must be a number")
    assert(type(timestamp) == "number", "timestamp must be a number")

    return setmetatable({
        key_size = key_size,
        timestamp = timestamp,
    }, MessageHeader)
end

--- Serializes a Message into its CRC-protected binary wire format.
--- Layout: len(8) | header(12) | header_crc(4) | key||value | payload_crc(4)
--- Returns the full byte string, or nil and an error.
local function serialize_message(msg)
    assert(getmetatable(msg) == Message, "msg must be a Message instance")

    local header  = string.pack(">I4I8", #msg.key, msg.timestamp)
    local payload = msg.key .. msg.value

    local header_crc  = crc32(header)
    local payload_crc = crc32(payload)

    -- total_size excludes the 8-byte length prefix itself.
    local total_size = #header + 4 + #payload + 4

    return string.pack(">I8", total_size)
        .. header
        .. string.pack(">I4", header_crc)
        .. payload
        .. string.pack(">I4", payload_crc), nil
end

return {
    Message = Message,
    MessageHeader = MessageHeader,
    serialize_message = serialize_message,
}
