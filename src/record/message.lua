-- ┌────────┬─────────────┬────────────┬───────────────┬─────────────┐
-- │ Length │ Header(12B) │ HeaderCRC  │ Payload(var)  │ PayloadCRC  │
-- │ (8B)   │ k_size(4)   │ (4B)       │ key||value    │ (4B)        │
-- │        │ ts(8)       │            │               │             │
-- └────────┴─────────────┴────────────┴───────────────┴─────────────┘
--
-- Length covers everything after the prefix: header(12) + header_crc(4) +
-- payload + payload_crc(4). Both CRCs are IEEE 802.3 CRC-32 over the
-- preceding bytes, big-endian.

local crc32 = require("src.core.crc32")

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

    -- The wire format stores the timestamp as an unsigned 64-bit integer.
    -- Message.new only checks `type == "number"`, so a fractional or negative
    -- value gets this far; reject it with the module's (nil, err) contract
    -- rather than letting string.pack raise an opaque "unsigned overflow" /
    -- "no integer representation" error.
    local ts = msg.timestamp
    if ts < 0 or ts % 1 ~= 0 then
        return nil, string.format(
            "timestamp must be a non-negative integer, got %s", tostring(ts))
    end

    local header  = string.pack(">I4I8", #msg.key, ts)
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

local HEADER_LEN = 12

--- Reads exactly one record from `file` at its current position, validating
--- both CRCs, and returns (Message, framed_size, nil) where framed_size is the
--- total on-disk byte length consumed (8-byte length prefix + body). On a
--- short read (EOF / torn tail) or a corrupt record it returns
--- (nil, nil, err). Mirrors the decode logic in Partition:read_message /
--- SegmentedPartition:read_message so all readers stay in lock-step.
local function deserialize_record(file)
    local size_bytes = file:read(8)
    if not size_bytes or #size_bytes < 8 then
        return nil, nil, "failed to read message size: unexpected EOF"
    end
    local total_size = string.unpack(">I8", size_bytes)

    -- Bound total_size BEFORE allocating the read. The length prefix is not
    -- covered by either CRC, so a torn write or at-rest/on-the-wire corruption
    -- can turn it into any u64. Reading it blindly lets a single flipped byte
    -- request a multi-gigabyte string (OOM/crash); when the high bit is set the
    -- value unpacks to a negative Lua integer that io.read casts to a huge
    -- size_t. The lower-bound check (moved up from below) also rejects the
    -- negative case since any negative is < HEADER_LEN + 4 + 4.
    if total_size < HEADER_LEN + 4 + 4 then
        return nil, nil, "corrupt header: total_size too small"
    end
    local cur  = file:seek()          -- position just past the 8-byte prefix
    local endp = file:seek("end")
    if cur then file:seek("set", cur) end
    if not cur or not endp or total_size > endp - cur then
        return nil, nil, "corrupt length prefix: exceeds remaining file bytes"
    end

    local body = file:read(total_size)
    if not body or #body < total_size then
        return nil, nil, "failed to read message body: unexpected EOF"
    end

    local header_bytes       = body:sub(1, HEADER_LEN)
    local stored_header_crc  = string.unpack(">I4", body, HEADER_LEN + 1)
    local payload_start      = HEADER_LEN + 4 + 1
    local payload_end        = #body - 4
    local payload            = body:sub(payload_start, payload_end)
    local stored_payload_crc = string.unpack(">I4", body, payload_end + 1)

    if crc32(header_bytes) ~= stored_header_crc then
        return nil, nil, "header checksum mismatch"
    end
    if crc32(payload) ~= stored_payload_crc then
        return nil, nil, "payload checksum mismatch"
    end

    local key_size, timestamp = string.unpack(">I4I8", header_bytes)
    if key_size < 0 or key_size > #payload then
        return nil, nil, "corrupt header: key_size out of range"
    end

    local key   = payload:sub(1, key_size)
    local value = payload:sub(key_size + 1)

    return Message.new(key, value, timestamp), 8 + total_size, nil
end

return {
    Message = Message,
    MessageHeader = MessageHeader,
    serialize_message = serialize_message,
    deserialize_record = deserialize_record,
}
