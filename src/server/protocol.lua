-- Binary wire protocol between MoonMQ clients and the broker daemon.
-- All numbers big-endian. Strings are length-prefixed (u32) UTF-8.
--
-- ┌──────────────┬────────┬────────────────┬──────────────┐
-- │ FrameLen(4B) │ Op(1B) │ CorrelID(4B)   │ Payload(var) │
-- └──────────────┴────────┴────────────────┴──────────────┘
--
-- FrameLen covers everything after itself (op + correl_id + payload).
-- No application CRC: TCP handles integrity at the transport layer, and
-- adding our own checksum just costs CPU on the hot path. On-disk records
-- still CRC themselves because the disk layer can corrupt independently.

local M = {}
M.__index = M

M.PROTOCOL_VERSION = 1

-- Opcodes. Client requests 0x01-0x7F, server replies 0x80-0xFE.
-- 0xFF reserved for future framing extensions.
M.OP_HELLO        = 0x01
M.OP_AUTH         = 0x02
M.OP_PRODUCE      = 0x03
M.OP_FETCH        = 0x04
M.OP_SUBSCRIBE    = 0x05
M.OP_COMMIT       = 0x06
M.OP_CREATE_TOPIC = 0x07
M.OP_LIST_TOPICS  = 0x08
M.OP_PING         = 0x09
M.OP_GOODBYE      = 0x0A

M.OP_WELCOME      = 0x80
M.OP_AUTH_OK      = 0x81
M.OP_PRODUCE_ACK  = 0x82
M.OP_RECORD       = 0x83
M.OP_TOPIC_LIST   = 0x84
M.OP_PONG         = 0x85
M.OP_OK           = 0x86
M.OP_ERROR        = 0xFE

-- Error codes. Keep numeric so non-Lua clients can switch on them.
M.ERR_BAD_FRAME       = 1
M.ERR_UNKNOWN_OP      = 2
M.ERR_NOT_AUTHED      = 3
M.ERR_AUTH_FAILED     = 4
M.ERR_TOPIC_MISSING   = 5
M.ERR_RATE_LIMITED    = 6
M.ERR_INTERNAL        = 7
M.ERR_BAD_PROTOCOL    = 8
M.ERR_FRAME_TOO_LARGE = 9

local function encode_string(s)
    assert(type(s) == "string", "s must be a string")
    return string.pack(">I4", #s) .. s
end
M.encode_string = encode_string

local function encode_frame(op, correl_id, payload)
    local body = string.pack(">BI4", op, correl_id) .. payload
    return string.pack(">I4", #body) .. body
end
M.encode_frame = encode_frame

function M.encode_hello(correl_id)
    local payload = string.pack(">I4", M.PROTOCOL_VERSION)
    return encode_frame(M.OP_HELLO, correl_id, payload)
end

function M.encode_welcome(correl_id, server_version)
    local payload = string.pack(">I4", server_version)
    return encode_frame(M.OP_WELCOME, correl_id, payload)
end

function M.encode_auth(correl_id, username, password)
    local payload = encode_string(username) .. encode_string(password)
    return encode_frame(M.OP_AUTH, correl_id, payload)
end

function M.encode_auth_ok(correl_id)
    return encode_frame(M.OP_AUTH_OK, correl_id, "")
end

function M.encode_produce(correl_id, topic, key, value)
    local payload = encode_string(topic) .. encode_string(key) .. encode_string(value)
    return encode_frame(M.OP_PRODUCE, correl_id, payload)
end

function M.encode_produce_ack(correl_id, partition, offset)
    local payload = string.pack(">i4I8", partition, offset)
    return encode_frame(M.OP_PRODUCE_ACK, correl_id, payload)
end

function M.encode_fetch(correl_id, max_records)
    local payload = string.pack(">I4", max_records)
    return encode_frame(M.OP_FETCH, correl_id, payload)
end

function M.encode_record(correl_id, topic, partition, offset, timestamp, key, value)
    local payload = encode_string(topic)
        .. string.pack(">I4I8I8", partition, offset, timestamp)
        .. encode_string(key)
        .. encode_string(value)

    return encode_frame(M.OP_RECORD, correl_id, payload)
end

function M.encode_subscribe(correl_id, topic, group_id)
    local payload = encode_string(topic) .. encode_string(group_id)
    return encode_frame(M.OP_SUBSCRIBE, correl_id, payload)
end

function M.encode_error(correl_id, code, message)
    local payload = string.pack(">I2", code) .. encode_string(message)
    return encode_frame(M.OP_ERROR, correl_id, payload)
end

function M.encode_ping(correl_id) return encode_frame(M.OP_PING, correl_id, "") end
function M.encode_pong(correl_id) return encode_frame(M.OP_PONG, correl_id, "") end
function M.encode_ok(correl_id)   return encode_frame(M.OP_OK,   correl_id, "") end

local function decode_string(buf, pos)
    if #buf - pos + 1 < 4 then
        return nil, nil, "truncated string length"
    end
    local len = string.unpack(">I4", buf, pos)
    pos = pos + 4

    if len > #buf - pos + 1 then
        return nil, nil, "truncated string body"
    end
    return buf:sub(pos, pos + len - 1), pos + len, nil
end
M.decode_string = decode_string

function M.decode_hello(payload)
    if #payload < 4 then return nil, "short hello" end
    return { version = string.unpack(">I4", payload, 1) }, nil
end

function M.decode_auth(payload)
    local user, p, err = decode_string(payload, 1)
    if not user then return nil, err end
    local pass, _, perr = decode_string(payload, p)
    if not pass then return nil, perr end
    return { username = user, password = pass }, nil
end

function M.decode_produce(payload)
    local topic, p, err = decode_string(payload, 1)
    if not topic then return nil, err end
    local key, p2, kerr = decode_string(payload, p)
    if not key then return nil, kerr end
    local value, _, verr = decode_string(payload, p2)
    if not value then return nil, verr end
    return { topic = topic, key = key, value = value }, nil
end

function M.decode_subscribe(payload)
    local topic, p, err = decode_string(payload, 1)
    if not topic then return nil, err end
    local group, _, gerr = decode_string(payload, p)
    if not group then return nil, gerr end
    return { topic = topic, group_id = group }, nil
end

function M.decode_fetch(payload)
    if #payload < 4 then return nil, "short fetch" end
    return { max_records = string.unpack(">I4", payload, 1) }, nil
end

function M.decode_create_topic(payload)
    local name, p, err = decode_string(payload, 1)
    if not name then return nil, err end
    if #payload - p + 1 < 4 then return nil, "short create_topic" end
    return { name = name, num_partitions = string.unpack(">I4", payload, p) }, nil
end

return M