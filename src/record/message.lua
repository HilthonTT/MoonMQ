-- Record format v2. All numbers big-endian.
--
-- ┌────────┬────────────────────────┬────────────┬───────────────┬─────────────┐
-- │ Length │ Header(13B)            │ HeaderCRC  │ Payload(var)  │ PayloadCRC  │
-- │ (8B)   │ attrs(1) k_size(4)     │ (4B)       │ key||value'   │ (4B)        │
-- │        │ ts(8)                  │            │               │             │
-- └────────┴────────────────────────┴────────────┴───────────────┴─────────────┘
--
-- Length covers everything after the prefix: header(13) + header_crc(4) +
-- payload + payload_crc(4). Both CRCs are IEEE 802.3 CRC-32 over the
-- preceding bytes, big-endian.
--
-- `attrs` is a bitfield (see ATTR_* below):
--   bits 0-1  compression codec  (0 none, 1 gzip, 2 snappy)
--   bit  2    control-record flag (transaction COMMIT/ABORT marker)
--   bit  3    transactional-data flag: the header is EXTENDED by
--             pid(8) + epoch(2) identifying the producer session that wrote
--             the record (needed to filter aborted records under
--             read_committed — see docs/transactions.md)
--   bits 4-7  reserved (must be 0)
--
-- IMPORTANT: `value'` is the value bytes AS STORED — already compressed when
-- the codec bits are set. The KEY is always stored plaintext so commitlog
-- key-compaction still matches keys, and `k_size` is the plaintext key length.
-- This module is a pure codec: it round-trips `attrs` and the stored value
-- bytes verbatim. Compression/decompression is the caller's job (see
-- src/record/compression.lua and the broker produce/deliver paths).
--
-- This file is the SINGLE source of the on-disk framing. Other readers
-- (SegmentedPartition:read_message, segment_verify, commitlog Segment) frame
-- the length prefix themselves for bounds-checking, then decode the body via
-- M.decode_body so the format lives in exactly one place.

local crc32 = require("src.core.crc32")

local Message = {}
Message.__index = Message

-- Compression codec ids, mirrored from src/record/compression.lua's
-- CompressionType so the two never drift. Duplicated (rather than required)
-- to avoid a require cycle: compression.lua requires this module.
local CODEC_NONE   = 0
local CODEC_GZIP   = 1
local CODEC_SNAPPY = 2

-- attrs bitfield masks.
local ATTR_CODEC_MASK = 0x03   -- bits 0-1
local ATTR_CONTROL    = 0x04   -- bit 2
local ATTR_TXN        = 0x08   -- bit 3

-- attrs (optional, default 0) carries the compression codec + control/txn
-- flags. Callers that don't care (the vast majority) omit it and get 0.
-- pid/epoch identify the producer session and are REQUIRED (enforced at
-- serialize time) when attrs has ATTR_TXN set; ignored otherwise.
function Message.new(key, value, timestamp, attrs, pid, epoch)
    assert(type(key) == "string", "key must be a string")
    assert(type(value) == "string", "value must be a string")
    assert(type(timestamp) == "number", "timestamp must be a number")
    if attrs ~= nil then
        assert(type(attrs) == "number", "attrs must be a number")
    end

    return setmetatable({
        key = key,
        value = value,
        timestamp = timestamp,
        attrs = attrs or 0,
        pid = pid,
        epoch = epoch,
    }, Message)
end

-- Accessors for the attrs bitfield so callers don't hardcode masks.
function Message:codec()
    return self.attrs & ATTR_CODEC_MASK
end

function Message:is_control()
    return (self.attrs & ATTR_CONTROL) ~= 0
end

function Message:is_txn()
    return (self.attrs & ATTR_TXN) ~= 0
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

-- Header: attrs(1) | k_size(4) | ts(8) = 13 bytes. When ATTR_TXN is set the
-- header is extended by pid(8) | epoch(2) = 23 bytes; the header CRC covers
-- whichever form was written. attrs is the first byte either way, so a
-- decoder can pick the length before slicing.
local HEADER_LEN = 13
local TXN_HEADER_LEN = HEADER_LEN + 8 + 2
-- Smallest legal body (everything after the 8-byte length prefix):
-- header(13) + header_crc(4) + payload_crc(4). An empty key+value is allowed.
local MIN_BODY = HEADER_LEN + 4 + 4

--- Serializes a Message into its CRC-protected binary wire format.
--- Layout: len(8) | header(13) | header_crc(4) | key||value | payload_crc(4)
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

    local attrs = msg.attrs or 0
    if attrs < 0 or attrs > 0xFF or attrs % 1 ~= 0 then
        return nil, string.format("attrs must be a byte (0..255), got %s",
            tostring(attrs))
    end

    local header
    if (attrs & ATTR_TXN) ~= 0 then
        if type(msg.pid) ~= "number" or type(msg.epoch) ~= "number" then
            return nil, "transactional record requires pid and epoch"
        end
        header = string.pack(">BI4I8I8I2", attrs, #msg.key, ts, msg.pid, msg.epoch)
    else
        header = string.pack(">BI4I8", attrs, #msg.key, ts)
    end
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

--- Decodes one record BODY (everything after the 8-byte length prefix),
--- validating both CRCs. This is the single authoritative decoder; the
--- file-based deserialize_record and the offset-based readers in the storage
--- backends all funnel through it. Returns (Message, nil) or (nil, err).
local function decode_body(body)
    if type(body) ~= "string" or #body < MIN_BODY then
        return nil, "corrupt record: body shorter than minimum"
    end

    -- attrs is always the first byte; it decides the header length (the
    -- transactional extension carries pid+epoch). We trust it only to SIZE
    -- the header slice — the header CRC then validates the whole header,
    -- including the attrs byte we peeked at.
    local peek_attrs = body:byte(1)
    local hlen = ((peek_attrs & ATTR_TXN) ~= 0) and TXN_HEADER_LEN or HEADER_LEN
    if #body < hlen + 4 + 4 then
        return nil, "corrupt record: body shorter than its header form"
    end

    local header_bytes       = body:sub(1, hlen)
    local stored_header_crc  = string.unpack(">I4", body, hlen + 1)
    local payload_start      = hlen + 4 + 1
    local payload_end        = #body - 4
    local payload            = body:sub(payload_start, payload_end)
    local stored_payload_crc = string.unpack(">I4", body, payload_end + 1)

    if crc32(header_bytes) ~= stored_header_crc then
        return nil, "header checksum mismatch"
    end
    if crc32(payload) ~= stored_payload_crc then
        return nil, "payload checksum mismatch"
    end

    local attrs, key_size, timestamp = string.unpack(">BI4I8", header_bytes)
    local pid, epoch
    if (attrs & ATTR_TXN) ~= 0 then
        pid, epoch = string.unpack(">I8I2", header_bytes, HEADER_LEN + 1)
    end
    if key_size < 0 or key_size > #payload then
        return nil, "corrupt header: key_size out of range"
    end

    local key   = payload:sub(1, key_size)
    local value = payload:sub(key_size + 1)

    return Message.new(key, value, timestamp, attrs, pid, epoch), nil
end

--- Reads exactly one record from `file` at its current position, validating
--- both CRCs, and returns (Message, framed_size, nil) where framed_size is the
--- total on-disk byte length consumed (8-byte length prefix + body). On a
--- short read (EOF / torn tail) or a corrupt record it returns
--- (nil, nil, err). Bounds the declared length against the bytes actually
--- remaining before allocating, so a corrupt length prefix can't drive a huge
--- read.
local function deserialize_record(file)
    local size_bytes = file:read(8)
    if not size_bytes or #size_bytes < 8 then
        return nil, nil, "failed to read message size: unexpected EOF"
    end
    local total_size = string.unpack(">I8", size_bytes)

    -- Bound total_size BEFORE allocating the read. The length prefix is not
    -- covered by either CRC, so a torn write or at-rest/on-the-wire corruption
    -- can turn it into any u64. The lower-bound check also rejects the negative
    -- case (high bit set → negative Lua integer) since any negative is < MIN_BODY.
    if total_size < MIN_BODY then
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

    local msg, derr = decode_body(body)
    if not msg then
        return nil, nil, derr
    end

    return msg, 8 + total_size, nil
end

return {
    Message = Message,
    MessageHeader = MessageHeader,
    serialize_message = serialize_message,
    decode_body = decode_body,
    deserialize_record = deserialize_record,
    HEADER_LEN = HEADER_LEN,
    TXN_HEADER_LEN = TXN_HEADER_LEN,
    MIN_BODY = MIN_BODY,
    -- Codec ids re-exported so storage/broker code can reference them without
    -- pulling in compression.lua (which has optional native deps).
    CODEC_NONE   = CODEC_NONE,
    CODEC_GZIP   = CODEC_GZIP,
    CODEC_SNAPPY = CODEC_SNAPPY,
    ATTR_CODEC_MASK = ATTR_CODEC_MASK,
    ATTR_CONTROL    = ATTR_CONTROL,
    ATTR_TXN        = ATTR_TXN,
}
