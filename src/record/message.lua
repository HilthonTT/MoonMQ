local crc32     = require("src.core.crc32")
local headers_m = require("src.record.headers")

local Message = {}
Message.__index = Message

local CODEC_NONE   = 0
local CODEC_GZIP   = 1
local CODEC_SNAPPY = 2

local ATTR_CODEC_MASK = 0x03
local ATTR_CONTROL    = 0x04
local ATTR_TXN        = 0x08
local ATTR_HEADERS    = 0x10

function Message.new(key, value, timestamp, attrs, pid, epoch, headers)
    assert(type(key) == "string", "key must be a string")
    assert(type(value) == "string", "value must be a string")
    assert(type(timestamp) == "number", "timestamp must be a number")
    if attrs ~= nil then
        assert(type(attrs) == "number", "attrs must be a number")
    end
    if headers ~= nil then
        assert(type(headers) == "table", "headers must be a list")
    end

    return setmetatable({
        key = key,
        value = value,
        timestamp = timestamp,
        attrs = attrs or 0,
        pid = pid,
        epoch = epoch,
        headers = headers,
    }, Message)
end

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

local HEADER_LEN = 13
local TXN_HEADER_LEN = HEADER_LEN + 8 + 2
local HEADERS_FIELD_LEN = 4
local MIN_BODY = HEADER_LEN + 4 + 4

local function header_len(attrs)
    local n = ((attrs & ATTR_TXN) ~= 0) and TXN_HEADER_LEN or HEADER_LEN
    if (attrs & ATTR_HEADERS) ~= 0 then n = n + HEADERS_FIELD_LEN end
    return n
end

local function serialize_message(msg)
    assert(getmetatable(msg) == Message, "msg must be a Message instance")

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

    local hblob = ""
    attrs = attrs & (0xFF ~ ATTR_HEADERS)
    if msg.headers and #msg.headers > 0 then
        local list, herr = headers_m.normalize(msg.headers)
        if not list then return nil, herr end
        hblob = headers_m.encode(list)
        attrs = attrs | ATTR_HEADERS
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
    if (attrs & ATTR_HEADERS) ~= 0 then
        header = header .. string.pack(">I4", #hblob)
    end
    local payload = hblob .. msg.key .. msg.value

    local header_crc  = crc32(header)
    local payload_crc = crc32(payload)

    local total_size = #header + 4 + #payload + 4

    return string.pack(">I8", total_size)
        .. header
        .. string.pack(">I4", header_crc)
        .. payload
        .. string.pack(">I4", payload_crc), nil
end

local function decode_body(body)
    if type(body) ~= "string" or #body < MIN_BODY then
        return nil, "corrupt record: body shorter than minimum"
    end

    local hlen = header_len(body:byte(1))
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
    local pos = HEADER_LEN + 1
    if (attrs & ATTR_TXN) ~= 0 then
        pid, epoch, pos = string.unpack(">I8I2", header_bytes, pos)
    end

    local headers
    local kstart = 1
    if (attrs & ATTR_HEADERS) ~= 0 then
        local hblob_len = string.unpack(">I4", header_bytes, pos)
        if hblob_len > #payload then
            return nil, "corrupt header: headers_len out of range"
        end
        local list, next_pos = headers_m.decode(payload:sub(1, hblob_len), 1)
        if not list then
            return nil, "corrupt record headers: " .. tostring(next_pos)
        end
        if next_pos ~= hblob_len + 1 then
            return nil, "corrupt record headers: trailing bytes"
        end
        headers = list
        kstart = hblob_len + 1
    end

    if key_size < 0 or key_size > #payload - kstart + 1 then
        return nil, "corrupt header: key_size out of range"
    end

    local key   = payload:sub(kstart, kstart + key_size - 1)
    local value = payload:sub(kstart + key_size)

    return Message.new(key, value, timestamp, attrs, pid, epoch, headers), nil
end

local function deserialize_record(file)
    local size_bytes = file:read(8)
    if not size_bytes or #size_bytes < 8 then
        return nil, nil, "failed to read message size: unexpected EOF"
    end
    local total_size = string.unpack(">I8", size_bytes)

    if total_size < MIN_BODY then
        return nil, nil, "corrupt header: total_size too small"
    end
    local cur  = file:seek()
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
    CODEC_NONE   = CODEC_NONE,
    CODEC_GZIP   = CODEC_GZIP,
    CODEC_SNAPPY = CODEC_SNAPPY,
    ATTR_CODEC_MASK = ATTR_CODEC_MASK,
    ATTR_CONTROL    = ATTR_CONTROL,
    ATTR_TXN        = ATTR_TXN,
    ATTR_HEADERS    = ATTR_HEADERS,
}
