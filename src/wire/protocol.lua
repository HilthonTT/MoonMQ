-- Binary wire protocol between MoonMQ clients and the broker daemon.
-- All numbers big-endian. Strings are length-prefixed (u32) UTF-8.
--
-- ┌──────────────┬────────┬────────────────┬──────────────┐
-- │ FrameLen(4B) │ Op(1B) │ CorrelID(16B)  │ Payload(var) │
-- └──────────────┴────────┴────────────────┴──────────────┘
--
-- CorrelID is a 16-byte UUID (see src/core/uuid.lua), matching the
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

-- Consumer-group coordination (Kafka-style). JOIN_GROUP registers this
-- connection as a member subscribing to one or more topics and gets back
-- the partition assignment the broker's ConsumerGroup computed for it.
-- GROUP_HEARTBEAT renews the member's lease (distinct from the connection
-- liveness HEARTBEAT_REQ/RESP); LEAVE_GROUP departs voluntarily. A member
-- is also dropped automatically when its connection closes.
M.OP_JOIN_GROUP       = 0x10
M.OP_LEAVE_GROUP      = 0x11
M.OP_GROUP_HEARTBEAT  = 0x12

-- Transactions (Kafka-style, atomic multi-partition — see docs/transactions.md).
-- BEGIN_TXN starts a transaction on a connection that opened a durable producer
-- (a producer_name / transactional_id); transactional PRODUCE_IDEMPOTENT frames
-- then implicitly enrol their partitions, TXN_OFFSET_COMMIT buffers offsets into
-- the txn, and END_TXN commits or aborts atomically. All three are acked with
-- OP_OK.
M.OP_BEGIN_TXN        = 0x13
M.OP_END_TXN          = 0x14
M.OP_TXN_OFFSET_COMMIT = 0x15

-- Dead-letter queue (see docs/dlq.md). NACK reports that the consumer failed
-- to process a delivered record. The broker counts attempts per
-- (group, topic, partition, offset): below the configured maximum it rewinds
-- the group's committed offset so the record is redelivered; at the maximum
-- it moves the record to the topic's dead-letter topic (<topic>.dlq) and
-- advances the group past it. Acked with NACK_ACK either way.
M.OP_NACK             = 0x16

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
M.OP_GROUP_ASSIGNMENT = 0x89
M.OP_NACK_ACK      = 0x8A
M.OP_ERROR         = 0xFE

-- Correlation IDs are 16-byte UUIDs.
local CORREL_ID_LEN = 16
M.CORREL_ID_LEN = CORREL_ID_LEN

-- Consumer isolation levels, carried as an optional trailing byte on
-- FETCH/SUBSCRIBE (absent = READ_UNCOMMITTED, so old clients are unaffected).
-- READ_COMMITTED consumers stop at the Last Stable Offset and never see
-- records of aborted transactions — see docs/transactions.md.
M.ISOLATION_READ_UNCOMMITTED = 0
M.ISOLATION_READ_COMMITTED   = 1

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
-- Consumer-group error codes.
-- GROUP_MEMBER_UNKNOWN: heartbeat/leave naming a member the coordinator
--   has no record of — either it was reaped for inactivity (client should
--   re-JOIN_GROUP) or it never joined on this connection.
-- GROUP_CONFLICT: a connection tried to join a second, different group;
--   one connection maps to at most one group membership.
M.ERR_GROUP_MEMBER_UNKNOWN = 12
M.ERR_GROUP_CONFLICT       = 13
-- Transactional / durable-producer error codes (see docs/transactions.md).
-- PRODUCER_FENCED: a produce/transaction op arrived with an epoch older than
--   the current one for this producer id — a newer session (same
--   transactional_id / producer_name) has taken over and fenced this one.
M.ERR_PRODUCER_FENCED      = 50
M.ERR_INVALID_TXN_STATE    = 51
M.ERR_TRANSACTION_TIMED_OUT = 52
M.ERR_OFFSETS_OUT_OF_RANGE_FOR_TXN = 53

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

-- PRODUCE payload: u8 codec | str topic | str key | str value.
-- `codec` is the compression codec the broker should store the value under
-- (0 none, 1 gzip, 2 snappy — see src/record/message.lua). Defaults to 0 so
-- callers that don't compress are unaffected.
function M.encode_produce(correl_id, topic, key, value, codec)
    local payload = string.pack(">B", codec or 0)
        .. encode_string(topic) .. encode_string(key) .. encode_string(value)
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
-- INIT_PRODUCER_ID request now carries an optional producer_name (a stable
-- identity — Kafka's transactional_id). Empty name = today's ephemeral
-- session-scoped PID; a non-empty name gets a durable PID + monotonic epoch
-- back so idempotence survives reconnects/restarts and old sessions are fenced.
function M.encode_init_producer_id(correl_id, producer_name)
    return encode_frame(M.OP_INIT_PRODUCER_ID, correl_id,
        encode_string(producer_name or ""))
end

function M.decode_init_producer_id(payload)
    -- Back-compat: an empty payload (old clients) means no producer name.
    -- Uses M.decode_string (exported) because the local `decode_string` isn't in
    -- scope this high in the file — same forward-reference reason
    -- decode_produce_idempotent is defined near the bottom.
    if #payload == 0 then return { producer_name = "" }, nil end
    local name, _, err = M.decode_string(payload, 1, M.MAX_PRODUCER_NAME)
    if not name then return nil, err end
    return { producer_name = name }, nil
end

-- PRODUCER_ID reply: u64 pid | u16 epoch.
function M.encode_producer_id(correl_id, pid, epoch)
    local payload = string.pack(">I8I2", pid, epoch or 0)
    return encode_frame(M.OP_PRODUCER_ID, correl_id, payload)
end

function M.decode_producer_id(payload)
    if #payload < 10 then return nil, "short producer_id" end
    local pid, epoch = string.unpack(">I8I2", payload, 1)
    return { pid = pid, epoch = epoch }, nil
end

-- PRODUCE_IDEMPOTENT payload:
--   u64 pid | u32 seq | u16 epoch | u8 codec | str topic | str key | str value
-- epoch fences zombie producers (durable/transactional producers, see
-- src/storage/producer_state.lua); 0 for a plain session-scoped idempotent
-- producer. codec is the compression codec (as in PRODUCE).
function M.encode_produce_idempotent(correl_id, pid, seq, topic, key, value, epoch, codec)
    local payload = string.pack(">I8I4I2B", pid, seq, epoch or 0, codec or 0)
        .. encode_string(topic)
        .. encode_string(key)
        .. encode_string(value)
    return encode_frame(M.OP_PRODUCE_IDEMPOTENT, correl_id, payload)
end

-- decode_produce_idempotent lives lower in the file, after decode_string
-- and the per-field max-length constants are declared. Lua forward
-- references to `local` bindings don't work, hence the placement.

-- isolation (optional) is an ISOLATION_* byte; omitted/0 = read_uncommitted.
function M.encode_fetch(correl_id, topic, group_id, max_records, isolation)
    local payload = encode_string(topic) .. encode_string(group_id)
        .. string.pack(">I4", max_records)
        .. string.pack(">B", isolation or M.ISOLATION_READ_UNCOMMITTED)
    return encode_frame(M.OP_FETCH, correl_id, payload)
end

function M.encode_record(correl_id, topic, partition, offset, timestamp, key, value)
    local payload = encode_string(topic)
        .. string.pack(">I4I8I8", partition, offset, timestamp)
        .. encode_string(key)
        .. encode_string(value)

    return encode_frame(M.OP_RECORD, correl_id, payload)
end

-- isolation (optional) is an ISOLATION_* byte; omitted/0 = read_uncommitted.
function M.encode_subscribe(correl_id, topic, group_id, isolation)
    local payload = encode_string(topic) .. encode_string(group_id)
        .. string.pack(">B", isolation or M.ISOLATION_READ_UNCOMMITTED)
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

-- NACK: str topic | u32 partition | u64 offset | str reason. `offset` is the
-- record's own offset as delivered in OP_RECORD (not the post-read cursor).
function M.encode_nack(correl_id, topic, partition, offset, reason)
    local payload = encode_string(topic)
        .. string.pack(">I4I8", partition, offset)
        .. encode_string(reason or "")
    return encode_frame(M.OP_NACK, correl_id, payload)
end

-- NACK_ACK reply: u8 dead_lettered | u16 attempts | str dlq_topic (empty
-- unless dead_lettered).
function M.encode_nack_ack(correl_id, dead_lettered, attempts, dlq_topic)
    local payload = string.pack(">BI2", dead_lettered and 1 or 0, attempts)
        .. encode_string(dlq_topic or "")
    return encode_frame(M.OP_NACK_ACK, correl_id, payload)
end

-- BEGIN_TXN carries no payload: the connection already holds the durable
-- producer identity (pid/epoch/name) it transacts under.
function M.encode_begin_txn(correl_id)
    return encode_frame(M.OP_BEGIN_TXN, correl_id, "")
end

-- END_TXN: u8 commit (1 = commit, 0 = abort).
function M.encode_end_txn(correl_id, commit)
    return encode_frame(M.OP_END_TXN, correl_id,
        string.pack(">B", commit and 1 or 0))
end

function M.decode_end_txn(payload)
    if #payload < 1 then return nil, "short end_txn" end
    return { commit = string.unpack(">B", payload, 1) ~= 0 }, nil
end

-- TXN_OFFSET_COMMIT: str group | u32 count | (str topic | u32 partition | u64 offset)*
function M.encode_txn_offset_commit(correl_id, group, offsets)
    local parts = { encode_string(group), string.pack(">I4", #offsets) }
    for i = 1, #offsets do
        local o = offsets[i]
        parts[#parts + 1] = encode_string(o.topic)
        parts[#parts + 1] = string.pack(">I4I8", o.partition, o.offset)
    end
    return encode_frame(M.OP_TXN_OFFSET_COMMIT, correl_id, table.concat(parts))
end

function M.encode_create_topic(correl_id, name, num_partitions)
    local payload = encode_string(name) .. string.pack(">I4", num_partitions)
    return encode_frame(M.OP_CREATE_TOPIC, correl_id, payload)
end

function M.encode_list_topics(correl_id)
    return encode_frame(M.OP_LIST_TOPICS, correl_id, "")
end

-- Consumer-group wire formats.
--
-- JOIN_GROUP request:
--   string group_id | string member_id | u32 topic_count | (string topic)*
-- An empty member_id asks the broker to assign one (first join); the
-- assigned id comes back in the GROUP_ASSIGNMENT reply.
function M.encode_join_group(correl_id, group_id, member_id, topics)
    local parts = {
        encode_string(group_id),
        encode_string(member_id),
        string.pack(">I4", #topics),
    }
    for i = 1, #topics do
        parts[#parts + 1] = encode_string(topics[i])
    end
    return encode_frame(M.OP_JOIN_GROUP, correl_id, table.concat(parts))
end

-- GROUP_ASSIGNMENT reply:
--   string member_id | u32 topic_count
--     | (string topic | u32 part_count | (u32 partition_id)*)*
-- `assignment` is { [topic] = { partition_id, ... }, ... }. Topics are
-- emitted in sorted order so the wire bytes are deterministic.
function M.encode_group_assignment(correl_id, member_id, assignment)
    local topics = {}
    for topic in pairs(assignment) do topics[#topics + 1] = topic end
    table.sort(topics)

    local parts = { encode_string(member_id), string.pack(">I4", #topics) }
    for _, topic in ipairs(topics) do
        local ids = assignment[topic]
        parts[#parts + 1] = encode_string(topic)
        parts[#parts + 1] = string.pack(">I4", #ids)
        for i = 1, #ids do
            parts[#parts + 1] = string.pack(">I4", ids[i])
        end
    end
    return encode_frame(M.OP_GROUP_ASSIGNMENT, correl_id, table.concat(parts))
end

-- LEAVE_GROUP / GROUP_HEARTBEAT share a body: string group_id | string member_id.
function M.encode_leave_group(correl_id, group_id, member_id)
    return encode_frame(M.OP_LEAVE_GROUP, correl_id,
        encode_string(group_id) .. encode_string(member_id))
end

function M.encode_group_heartbeat(correl_id, group_id, member_id)
    return encode_frame(M.OP_GROUP_HEARTBEAT, correl_id,
        encode_string(group_id) .. encode_string(member_id))
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
local MAX_MEMBER_ID     = 256
local MAX_CLIENT_NAME   = 128
local MAX_CLIENT_VERSION = 64
-- Credential caps. The password is the PBKDF2/HMAC *key*, and HMAC re-derives
-- its block key from the full key on every one of the (600k) iterations, so
-- verify cost is O(iterations * password_length). Left at the 64 KiB default
-- string cap, a single AUTH frame with a 64 KiB password stalls the entire
-- single-threaded reactor for seconds. Cap both fields tightly — no legitimate
-- username/password approaches 1 KiB.
local MAX_USERNAME      = 256
local MAX_PASSWORD      = 1024
-- Cap topics-per-join so a single JOIN_GROUP frame can't ask us to
-- decode an attacker-chosen number of length-prefixed strings. 256 is
-- far above any realistic subscription set.
local MAX_GROUP_TOPICS  = 256
-- Producer name (a.k.a. transactional_id): a stable, human-assigned identity.
-- 256 is generous; it's the key into __producer_state.
local MAX_PRODUCER_NAME = 256
-- NACK failure reason: free-form client text that ends up stored inside the
-- dead-letter envelope, so cap it well below the generic string bound.
local MAX_NACK_REASON   = 1024
M.MAX_TOPIC_NAME = MAX_TOPIC_NAME
M.MAX_GROUP_ID   = MAX_GROUP_ID
M.MAX_MEMBER_ID  = MAX_MEMBER_ID
M.MAX_GROUP_TOPICS = MAX_GROUP_TOPICS
M.MAX_PRODUCER_NAME = MAX_PRODUCER_NAME
M.MAX_NACK_REASON   = MAX_NACK_REASON

function M.decode_hello(payload)
    if #payload < 4 then return nil, "short hello" end
    return { version = string.unpack(">I4", payload, 1) }, nil
end

function M.decode_auth(payload)
    local user, p, err = decode_string(payload, 1, MAX_USERNAME)
    if not user then return nil, err end
    local pass, _, perr = decode_string(payload, p, MAX_PASSWORD)
    if not pass then return nil, perr end
    return { username = user, password = pass }, nil
end

function M.decode_produce(payload)
    if #payload < 1 then return nil, "short produce header" end
    local codec = string.unpack(">B", payload, 1)
    local topic, p, err = decode_string(payload, 2, MAX_TOPIC_NAME)
    if not topic then return nil, err end
    local key, p2, kerr = decode_string(payload, p)
    if not key then return nil, kerr end
    local value, _, verr = decode_string(payload, p2)
    if not value then return nil, verr end
    return { codec = codec, topic = topic, key = key, value = value }, nil
end

function M.decode_subscribe(payload)
    local topic, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not topic then return nil, err end
    local group, p2, gerr = decode_string(payload, p, MAX_GROUP_ID)
    if not group then return nil, gerr end
    -- Optional trailing isolation byte (older clients don't send it).
    local isolation = M.ISOLATION_READ_UNCOMMITTED
    if p2 and #payload - p2 + 1 >= 1 then
        isolation = string.unpack(">B", payload, p2)
    end
    return { topic = topic, group_id = group, isolation = isolation }, nil
end

function M.decode_fetch(payload)
    local topic, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not topic then return nil, err end
    local group, p2, gerr = decode_string(payload, p, MAX_GROUP_ID)
    if not group then return nil, gerr end
    if #payload - p2 + 1 < 4 then return nil, "short fetch" end
    local max_records, p3 = string.unpack(">I4", payload, p2)
    -- Optional trailing isolation byte (older clients don't send it).
    local isolation = M.ISOLATION_READ_UNCOMMITTED
    if #payload - p3 + 1 >= 1 then
        isolation = string.unpack(">B", payload, p3)
    end
    return {
        topic       = topic,
        group_id    = group,
        max_records = max_records,
        isolation   = isolation,
    }, nil
end

function M.decode_join_group(payload)
    local group, p, gerr = decode_string(payload, 1, MAX_GROUP_ID)
    if not group then return nil, gerr end
    local member, p2, merr = decode_string(payload, p, MAX_MEMBER_ID)
    if not member then return nil, merr end
    if #payload - p2 + 1 < 4 then return nil, "short join_group topic count" end

    local count, pos = string.unpack(">I4", payload, p2)
    if count > MAX_GROUP_TOPICS then
        return nil, string.format("too many topics (%d > %d)", count, MAX_GROUP_TOPICS)
    end

    local topics = {}
    for i = 1, count do
        local t, np, terr = decode_string(payload, pos, MAX_TOPIC_NAME)
        if not t then return nil, terr end
        topics[i] = t
        pos = np
    end
    return { group_id = group, member_id = member, topics = topics }, nil
end

-- Inverse of encode_group_assignment. Returns
-- { member_id, assignment = { [topic] = { partition_id, ... } } }.
function M.decode_group_assignment(payload)
    local member, p, merr = decode_string(payload, 1, MAX_MEMBER_ID)
    if not member then return nil, merr end
    if #payload - p + 1 < 4 then return nil, "short assignment topic count" end

    local tcount, pos = string.unpack(">I4", payload, p)
    local assignment = {}
    for _ = 1, tcount do
        local topic, np, terr = decode_string(payload, pos, MAX_TOPIC_NAME)
        if not topic then return nil, terr end
        pos = np

        if #payload - pos + 1 < 4 then return nil, "short assignment partition count" end
        local pcount, pp = string.unpack(">I4", payload, pos)
        pos = pp

        local ids = {}
        for i = 1, pcount do
            if #payload - pos + 1 < 4 then return nil, "short assignment partition id" end
            local id, npp = string.unpack(">I4", payload, pos)
            ids[i] = id
            pos = npp
        end
        assignment[topic] = ids
    end
    return { member_id = member, assignment = assignment }, nil
end

-- LEAVE_GROUP and GROUP_HEARTBEAT carry the same body.
local function decode_group_member_ref(payload)
    local group, p, gerr = decode_string(payload, 1, MAX_GROUP_ID)
    if not group then return nil, gerr end
    local member, _, merr = decode_string(payload, p, MAX_MEMBER_ID)
    if not member then return nil, merr end
    return { group_id = group, member_id = member }, nil
end
M.decode_leave_group     = decode_group_member_ref
M.decode_group_heartbeat = decode_group_member_ref

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

function M.decode_nack(payload)
    local topic, p, err = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not topic then return nil, err or "truncated nack topic" end
    if #payload - p + 1 < 12 then
        return nil, "short nack"
    end
    local partition, offset, p2 = string.unpack(">I4I8", payload, p)
    local reason, _, rerr = decode_string(payload, p2, MAX_NACK_REASON)
    if not reason then return nil, rerr end
    return { topic = topic, partition = partition, offset = offset,
             reason = reason }, nil
end

function M.decode_nack_ack(payload)
    if #payload < 3 then return nil, "short nack_ack" end
    local dead, attempts, p = string.unpack(">BI2", payload, 1)
    local dlq_topic, _, err = decode_string(payload, p, MAX_TOPIC_NAME)
    if not dlq_topic then return nil, err end
    return {
        dead_lettered = dead ~= 0,
        attempts      = attempts,
        dlq_topic     = dlq_topic ~= "" and dlq_topic or nil,
    }, nil
end

-- Cap offsets-per-txn-commit so one frame can't ask us to decode an
-- attacker-chosen number of triples. 1024 is far above any realistic batch.
local MAX_TXN_OFFSETS = 1024

function M.decode_txn_offset_commit(payload)
    local group, p, gerr = decode_string(payload, 1, MAX_GROUP_ID)
    if not group then return nil, gerr end
    if #payload - p + 1 < 4 then return nil, "short txn_offset_commit count" end
    local count, pos = string.unpack(">I4", payload, p)
    if count > MAX_TXN_OFFSETS then
        return nil, string.format("too many offsets (%d > %d)", count, MAX_TXN_OFFSETS)
    end
    local offsets = {}
    for _ = 1, count do
        local topic, np, terr = decode_string(payload, pos, MAX_TOPIC_NAME)
        if not topic then return nil, terr end
        if #payload - np + 1 < 12 then return nil, "short txn_offset entry" end
        local partition, offset = string.unpack(">I4I8", payload, np)
        offsets[#offsets + 1] =
            { topic = topic, partition = partition, offset = offset }
        pos = np + 12
    end
    return { group = group, offsets = offsets }, nil
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
    -- header = pid(8) + seq(4) + epoch(2) + codec(1) = 15 bytes.
    if #payload < 15 then return nil, "short produce_idempotent header" end
    local pid, seq, epoch, codec, p = string.unpack(">I8I4I2B", payload, 1)
    local topic, p2, terr = decode_string(payload, p, MAX_TOPIC_NAME)
    if not topic then return nil, terr end
    local key, p3, kerr = decode_string(payload, p2)
    if not key then return nil, kerr end
    local value, _, verr = decode_string(payload, p3)
    if not value then return nil, verr end
    return {
        pid       = pid,
        seq       = seq,
        epoch     = epoch,
        codec     = codec,
        topic     = topic,
        key       = key,
        value     = value,
    }, nil
end

return M