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

M.OP_IDENTIFY_CLIENT  = 0x0D
-- Idempotent producer (Kafka-style). INIT_PRODUCER_ID requests a u64
-- producer ID from the broker; subsequent PRODUCE_IDEMPOTENT frames
-- carry that PID plus a per-(PID, topic, partition) monotonic sequence
-- number, letting the broker dedup retries. PIDs are session-scoped
-- (live in broker memory only, expire when the connection closes).
M.OP_INIT_PRODUCER_ID = 0x0E
M.OP_PRODUCE_IDEMPOTENT = 0x0F

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
M.OP_PRODUCER_ID   = 0x88
M.OP_ERROR         = 0xFE

-- Correlation IDs are 16-byte UUIDs.
local CORREL_ID_LEN = 16
M.CORREL_ID_LEN = CORREL_ID_LEN

-- Error codes. Keep numeric so non-Lua clients can switch on them.
M.ERR_BAD_FRAME            = 1
M.ERR_UNKNOWN_OP           = 2
M.ERR_NOT_AUTHED           = 3
M.ERR_AUTH_FAILED          = 4
M.ERR_TOPIC_MISSING        = 5
M.ERR_RATE_LIMITED         = 6
M.ERR_INTERNAL             = 7
M.ERR_BAD_PROTOCOL         = 8
M.ERR_FRAME_TOO_LARGE      = 9
-- Idempotent producer error codes.
-- DUPLICATE_SEQUENCE: seq <= last_seq seen for (PID, topic, partition).
--   This is the normal retry-after-success case; the broker returns the
--   *original* offset alongside a successful ack (not this error). The
--   error fires only when sequences appear truly out of order (gap).
M.ERR_NO_PRODUCER_ID       = 10
M.ERR_OUT_OF_ORDER_SEQUENCE = 11

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

-- Idempotent producer wire format.
-- INIT_PRODUCER_ID request: empty payload (reserved for future
-- transaction timeout / transactional_id fields).
-- Response (OP_PRODUCER_ID): u64 PID.
-- PRODUCE_IDEMPOTENT request: u64 pid | u32 seq | string topic | string key | string value
-- Response: identical to OP_PRODUCE_ACK (partition + offset). Duplicate
-- retries return the ORIGINAL ack — see _handle_produce_idempotent.
function M.encode_init_producer_id(correl_id)
    return encode_frame(M.OP_INIT_PRODUCER_ID, correl_id, "")
end

function M.encode_producer_id(correl_id, pid)
    local payload = string.pack(">I8", pid)
    return encode_frame(M.OP_PRODUCER_ID, correl_id, payload)
end

function M.decode_producer_id(payload)
    if #payload < 8 then return nil, "short producer_id" end
    return { pid = string.unpack(">I8", payload, 1) }, nil
end

function M.encode_produce_idempotent(correl_id, pid, seq, topic, key, value)
    local payload = string.pack(">I8I4", pid, seq)
        .. encode_string(topic)
        .. encode_string(key)
        .. encode_string(value)
    return encode_frame(M.OP_PRODUCE_IDEMPOTENT, correl_id, payload)
end

-- decode_produce_idempotent lives lower in the file, after decode_string
-- and the per-field max-length constants are declared. Lua forward
-- references to `local` bindings don't work, hence the placement.

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

function M.encode_commit(correl_id, topic, partition, offset)
    local payload = encode_string(topic) .. string.pack(">I4I8", partition, offset)
    return encode_frame(M.OP_COMMIT, correl_id, payload)
end

function M.encode_create_topic(correl_id, name, num_partitions)
    local payload = encode_string(name) .. string.pack(">I4", num_partitions)
    return encode_frame(M.OP_CREATE_TOPIC, correl_id, payload)
end

function M.encode_list_topics(correl_id)
    return encode_frame(M.OP_LIST_TOPICS, correl_id, "")
end

function M.encode_goodbye(correl_id)
    return encode_frame(M.OP_GOODBYE, correl_id, "")
end

function M.encode_ping(correl_id) return encode_frame(M.OP_PING, correl_id, "") end
function M.encode_pong(correl_id) return encode_frame(M.OP_PONG, correl_id, "") end
function M.encode_ok(correl_id)   return encode_frame(M.OP_OK,   correl_id, "") end

-- Default cap on any single length-prefixed string. Individual decoders
-- can pass a tighter `max_len` when they know the field's domain (e.g.
-- topic names cap at 249 from util.validate_topic_name). The default is
-- still applied at the boundary so a malformed wire frame can't
-- allocate an attacker-chosen number of bytes per field.
local MAX_STRING_LEN = 64 * 1024

local function decode_string(buf, pos, max_len)
    if #buf - pos + 1 < 4 then
        return nil, nil, "truncated string length"
    end
    -- string.unpack returns the next position; use it directly rather
    -- than adding sizeof by hand. Keeps this in sync if the prefix
    -- format ever changes.
    local len, next_pos = string.unpack(">I4", buf, pos)

    local cap = max_len or MAX_STRING_LEN
    if len > cap then
        return nil, nil, string.format(
            "string field too long (%d > %d)", len, cap)
    end
    if len > #buf - next_pos + 1 then
        return nil, nil, "truncated string body"
    end
    return buf:sub(next_pos, next_pos + len - 1), next_pos + len, nil
end
M.decode_string = decode_string
M.MAX_STRING_LEN = MAX_STRING_LEN

-- Per-field caps. Topic name (249) matches util.validate_topic_name.
-- Group id (256) is generous for human-readable group names. Identify
-- name/version are already capped at the dispatch layer; we add caps
-- here too so any future caller gets the same defense without having
-- to remember to add a second check.
local MAX_TOPIC_NAME    = 249
local MAX_GROUP_ID      = 256
local MAX_CLIENT_NAME   = 128
local MAX_CLIENT_VERSION = 64
M.MAX_TOPIC_NAME = MAX_TOPIC_NAME
M.MAX_GROUP_ID   = MAX_GROUP_ID

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
    local topic, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not topic then return nil, err end
    local key, p2, kerr = decode_string(payload, p)
    if not key then return nil, kerr end
    local value, _, verr = decode_string(payload, p2)
    if not value then return nil, verr end
    return { topic = topic, key = key, value = value }, nil
end

function M.decode_subscribe(payload)
    local topic, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not topic then return nil, err end
    local group, _, gerr = decode_string(payload, p, MAX_GROUP_ID)
    if not group then return nil, gerr end
    return { topic = topic, group_id = group }, nil
end

function M.decode_fetch(payload)
    local topic, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not topic then return nil, err end
    local group, p2, gerr = decode_string(payload, p, MAX_GROUP_ID)
    if not group then return nil, gerr end
    if #payload - p2 + 1 < 4 then return nil, "short fetch" end
    return {
        topic       = topic,
        group_id    = group,
        max_records = string.unpack(">I4", payload, p2),
    }, nil
end

function M.decode_identify_client(payload)
    local name, p, err = decode_string(payload, 1, MAX_CLIENT_NAME)
    if not name then return nil, err end
    local version, _, verr = decode_string(payload, p, MAX_CLIENT_VERSION)
    if not version then return nil, verr end
    return { name = name, version = version }, nil
end

function M.decode_create_topic(payload)
    local name, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not name then return nil, err end
    if #payload - p + 1 < 4 then return nil, "short create_topic" end
    return { name = name, num_partitions = string.unpack(">I4", payload, p) }, nil
end

function M.decode_commit(payload)
    local topic, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
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

-- Idempotent produce decode. Lives here because it depends on the
-- decode_string helper and the MAX_TOPIC_NAME constant declared above.
function M.decode_produce_idempotent(payload)
    if #payload < 12 then return nil, "short produce_idempotent header" end
    local pid, seq, p = string.unpack(">I8I4", payload, 1)
    local topic, p2, terr = decode_string(payload, p, MAX_TOPIC_NAME)
    if not topic then return nil, terr end
    local key, p3, kerr = decode_string(payload, p2)
    if not key then return nil, kerr end
    local value, _, verr = decode_string(payload, p3)
    if not value then return nil, verr end
    return {
        pid       = pid,
        seq       = seq,
        topic     = topic,
        key       = key,
        value     = value,
    }, nil
end

return M