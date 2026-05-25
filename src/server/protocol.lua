-- Binary wire protocol between MoonMQ clients and the broker daemon.
-- All numbers big-endian. Strings are length-prefixed (u32) UTF-8.
--
-- ┌──────────────┬────────┬────────────────┬──────────────┐
-- │ FrameLen(4B) │ Op(1B) │ CorrelID(16B)  │ Payload(var) │
-- └──────────────┴────────┴────────────────┴──────────────┘
--
-- CorrelID is a 16-byte UUID (see src/server/uuid.lua), matching the
-- connection identifiers used throughout the server.
--
-- FrameLen covers everything after itself (op + correl_id + payload).
-- No application CRC: TCP handles integrity at the transport layer, and
-- adding our own checksum just costs CPU on the hot path. On-disk records
-- still CRC themselves because the disk layer can corrupt independently.

local M = {}
M.__index = M

M.PROTOCOL_VERSION = 1
M.SERVER_NAME = "MoonMQ"
M.SERVER_VERSION = "v0.01"

-- Opcodes. Client requests 0x01-0x7F, server replies 0x80-0xFE.
-- 0xFF reserved for future framing extensions.
-- Client to server
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

M.OP_IDENTIFY_CLIENT = 0x0D

-- Bidirectional
M.OP_HEARTBEAT_REQ   = 0x0B
M.OP_HEARTBEAT_RESP  = 0x0C

-- Server to clients
M.OP_WELCOME       = 0x80
M.OP_AUTH_OK       = 0x81
M.OP_PRODUCE_ACK   = 0x82
M.OP_RECORD        = 0x83
M.OP_TOPIC_LIST    = 0x84
M.OP_PONG          = 0x85
M.OP_OK            = 0x86
M.OP_IDENTIFY_ACK  = 0x87
M.OP_ERROR         = 0xFE

-- Correlation IDs are 16-byte UUIDs.
local CORREL_ID_LEN = 16
M.CORREL_ID_LEN = CORREL_ID_LEN

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
    assert(type(correl_id) == "string" and #correl_id == CORREL_ID_LEN,
        "correl_id must be a 16-byte string")
    local body = string.pack(">B", op) .. correl_id .. payload
    return string.pack(">I4", #body) .. body
end
M.encode_frame = encode_frame

-- Splits a frame body (everything after the u32 length prefix) into its
-- opcode, correlation ID, and payload. Returns (op, correl, payload, nil)
-- or (nil, nil, nil, err).
local function parse_frame(body)
    if type(body) ~= "string" or #body < 1 + CORREL_ID_LEN then
        return nil, nil, nil, "frame shorter than header"
    end
    local op = string.unpack(">B", body, 1)
    local correl = body:sub(2, 1 + CORREL_ID_LEN)
    local payload = body:sub(2 + CORREL_ID_LEN)
    return op, correl, payload, nil
end
M.parse_frame = parse_frame

function M.encode_hello(correl_id)
    local payload = string.pack(">I4", M.PROTOCOL_VERSION)
    return encode_frame(M.OP_HELLO, correl_id, payload)
end

function M.encode_welcome(correl_id, proto_version)
    local payload = string.pack(">I4", proto_version)
    return encode_frame(M.OP_WELCOME, correl_id, payload)
end

function M.encode_identify_client(correl_id, name, version)
    local payload = encode_string(name) .. encode_string(version)
    return encode_frame(M.OP_IDENTIFY_CLIENT, correl_id, payload)
end

function M.encode_identify_ack(correl_id, server_name, server_version)
    local payload = encode_string(server_name) .. encode_string(server_version)
    return encode_frame(M.OP_IDENTIFY_ACK, correl_id, payload)
end

function M.encode_heartbeat_req(correl_id)
    return encode_frame(M.OP_HEARTBEAT_REQ, correl_id, "")
end

function M.encode_heartbeat_resp(correl_id)
    return encode_frame(M.OP_HEARTBEAT_RESP, correl_id, "")
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
    local payload = string.pack(">I4I8", partition, offset)
    return encode_frame(M.OP_PRODUCE_ACK, correl_id, payload)
end

function M.encode_fetch(correl_id, topic, group_id, max_records)
    local payload = encode_string(topic) .. encode_string(group_id)
        .. string.pack(">I4", max_records)
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

function M.encode_topic_list(correl_id, names)
    local parts = { string.pack(">I4", #names) }
    for i = 1, #names do
        parts[#parts + 1] = encode_string(names[i])
    end
    return encode_frame(M.OP_TOPIC_LIST, correl_id, table.concat(parts))
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
    -- string.unpack returns the next position; use it directly rather
    -- than adding sizeof by hand. Keeps this in sync if the prefix
    -- format ever changes.
    local len, next_pos = string.unpack(">I4", buf, pos)

    if len > #buf - next_pos + 1 then
        return nil, nil, "truncated string body"
    end
    return buf:sub(next_pos, next_pos + len - 1), next_pos + len, nil
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
    local topic, p, err = decode_string(payload, 1)
    if not topic then return nil, err end
    local group, p2, gerr = decode_string(payload, p)
    if not group then return nil, gerr end
    if #payload - p2 + 1 < 4 then return nil, "short fetch" end
    return {
        topic       = topic,
        group_id    = group,
        max_records = string.unpack(">I4", payload, p2),
    }, nil
end

function M.decode_identify_client(payload)
    local name, p, err = decode_string(payload, 1)
    if not name then return nil, err end
    local version, _, verr = decode_string(payload, p)
    if not version then return nil, verr end
    return { name = name, version = version }, nil
end

function M.decode_create_topic(payload)
    local name, p, err = decode_string(payload, 1)
    if not name then return nil, err end
    if #payload - p + 1 < 4 then return nil, "short create_topic" end
    return { name = name, num_partitions = string.unpack(">I4", payload, p) }, nil
end

function M.decode_commit(payload)
    local topic, p, err = decode_string(payload, 1)
    if not topic then return nil, err or "truncated commit topic" end

    if #payload - p + 1 < 12 then
        return nil, "short commit"
    end

    local partition, offset = string.unpack(">I4I8", payload, p)
    return { topic = topic, partition = partition, offset = offset }, nil
end

function M.decode_topic_list(payload)
    if #payload < 4 then return nil, "short topic list" end
    local count = string.unpack(">I4", payload, 1)
    local pos = 5
    local names = {}
    for i = 1, count do
        local n, np, derr = decode_string(payload, pos)
        if not n then return nil, derr end
        names[i] = n
        pos = np or pos
    end
    return names, nil
end

function M.decode_error(payload)
    if #payload < 2 then return nil, "short error" end
    local code = string.unpack(">I2", payload, 1)
    local msg, _, merr = decode_string(payload, 3)
    if not msg then return nil, merr end
    return { code = code, message = msg }, nil
end

return M