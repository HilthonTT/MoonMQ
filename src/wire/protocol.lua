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
-- 0x09 is retired (was PING; connection liveness is HEARTBEAT_REQ/RESP below).
-- Do not reuse — old clients may still emit it and must fail as UNKNOWN_OP.
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

-- Batching. One PRODUCE_BATCH frame carries N records for a topic and is
-- answered by one PRODUCE_BATCH_ACK carrying N (partition, offset) pairs —
-- amortising framing, dispatch, and (crucially) the acks=1 fsync across the
-- whole batch instead of paying all three per record. FETCH asks for a batched
-- reply via its flags byte and gets one RECORD_BATCH instead of N RECORD
-- frames. See docs/batching.md.
M.OP_PRODUCE_BATCH    = 0x17

-- Offset introspection. LIST_OFFSETS asks for the readable offset range of
-- every partition of one topic and is answered by a single OFFSETS frame.
-- Nothing before it could express "where does this log start and end", which
-- made two ordinary things impossible: seeking to the head or tail of a
-- partition, and computing consumer lag (latest - committed) client-side.
-- Both bounds are already tracked in memory by every storage backend, so the
-- broker answers without touching disk.
M.OP_LIST_OFFSETS     = 0x18

-- Topic administration beyond CREATE/LIST. Until these, a topic's config was
-- write-once at creation: topic_config.lua persisted retention, cleanup policy
-- and segment sizing to a sidecar, and the cleaners acted on it, but no client
-- could read it back, change it, or remove a topic at all — you edited the
-- sidecar by hand and restarted the broker.
--
-- DELETE_TOPIC is refused for internal topics (the `__` prefix) because
-- __consumer_offsets and __producer_state are broker state, not user data.
M.OP_DELETE_TOPIC       = 0x19
M.OP_DESCRIBE_TOPIC     = 0x1A
M.OP_ALTER_TOPIC_CONFIG = 0x1B

-- Consumer-group introspection. The coordinator already holds members,
-- assignments and (via OffsetManager) committed offsets; none of it was
-- reachable, which is why lag had to be reconstructed out-of-band by
-- scripts/lag_monitor.py. DESCRIBE_GROUP answers all three from the
-- coordinator's own state, so lag becomes a broker-side fact.
--
-- DELETE_GROUP only succeeds on a group with no live members — the same rule
-- Kafka applies, and for the same reason: deleting under an active member
-- leaves it holding an assignment nothing will ever rebalance.
M.OP_LIST_GROUPS      = 0x1C
M.OP_DESCRIBE_GROUP   = 0x1D
M.OP_DELETE_GROUP     = 0x1E

-- Bidirectional
M.OP_HEARTBEAT_REQ   = 0x0B
M.OP_HEARTBEAT_RESP  = 0x0C

-- Server to clients
M.OP_WELCOME       = 0x80
M.OP_AUTH_OK       = 0x81
M.OP_PRODUCE_ACK   = 0x82
M.OP_RECORD        = 0x83
M.OP_TOPIC_LIST    = 0x84
-- 0x85 is retired (was PONG). See the note on 0x09.
M.OP_OK            = 0x86
M.OP_IDENTIFY_ACK  = 0x87
M.OP_PRODUCER_ID   = 0x88
M.OP_GROUP_ASSIGNMENT = 0x89
M.OP_NACK_ACK      = 0x8A
M.OP_PRODUCE_BATCH_ACK = 0x8B
M.OP_RECORD_BATCH  = 0x8C
M.OP_OFFSETS       = 0x8D
M.OP_TOPIC_DESCRIPTION = 0x8E
M.OP_GROUP_LIST        = 0x8F
M.OP_GROUP_DESCRIPTION = 0x90
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

-- Optional trailing flags byte on FETCH, after the isolation byte. A broker
-- that predates batching stops decoding at isolation and answers with the
-- legacy one-RECORD-frame-per-record stream, so setting the bit is safe
-- against any broker version.
M.FETCH_FLAG_BATCHED = 0x01

-- PRODUCE_BATCH flags byte. IDEMPOTENT means the frame carries the
-- (pid, base_seq, epoch) triple and the batch participates in producer-state
-- dedup; without it the batch is a plain multi-record append.
M.BATCH_FLAG_IDEMPOTENT = 0x01

-- Per-partition flags on an OFFSETS entry. `earliest` and `latest` are always
-- exact; the other two bounds are not always knowable, so each carries a bit
-- saying whether the number next to it means anything. When a bit is clear the
-- encoder has substituted `latest`, which keeps a flags-ignoring client honest
-- (it reads a real offset, just a conservative one) without a sentinel value.
--
--   HWM_EXACT — the high watermark is a real min-LEO-across-in-sync-followers
--               reading. Clear when replication is configured but no follower
--               is currently in sync, so there is no meaningful minimum.
--   LSO_KNOWN — the Last Stable Offset is a real read ceiling. Set whenever a
--               transaction coordinator answered, INCLUDING the ordinary case
--               of no transaction in flight, where the stable point simply is
--               the log end. Clear only on a broker built without one.
--   LOCAL     — this broker serves the partition. Clear means the cluster has
--               moved it elsewhere and the bounds are from a stale local copy;
--               ask the owning broker for authoritative numbers.
M.OFFSETS_FLAG_HWM_EXACT = 0x01
M.OFFSETS_FLAG_LSO_KNOWN = 0x02
M.OFFSETS_FLAG_LOCAL     = 0x04

-- LIST_OFFSETS query modes, carried as an optional trailing byte exactly the
-- way FETCH grew isolation and flags. Absent = BOUNDS, so a client that
-- predates this sends the old one-string body and a broker that predates it
-- stops decoding after the topic and answers with bounds only.
--
--   BOUNDS    — the earliest/latest/hwm/lso reply that LIST_OFFSETS has always
--               given. No extra fields follow the mode byte.
--   TIMESTAMP — a u64 millisecond timestamp follows. The reply carries the
--               ordinary bounds AND a trailing per-partition section giving
--               the earliest offset whose record timestamp is >= the query,
--               which is Kafka's offsetForTimes semantics.
M.LIST_OFFSETS_MODE_BOUNDS    = 0
M.LIST_OFFSETS_MODE_TIMESTAMP = 1

-- Set on a FOR_TIMES entry when the partition actually had a record at or
-- after the requested timestamp. Clear means "no such record" — the partition
-- is empty, or every record predates the query — and the offset field is then
-- the partition's `latest`, i.e. where such a record would eventually land.
-- Same substitute-don't-sentinel rule as the OFFSETS_FLAG_* fields above.
M.FOR_TIMES_FLAG_FOUND = 0x01

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
-- BATCH_TOO_LARGE: more records in one PRODUCE_BATCH than the broker will
-- accept. Split and resend — the batch was NOT partially applied.
M.ERR_BATCH_TOO_LARGE      = 14
-- Topic/group administration error codes.
-- GROUP_MISSING: DESCRIBE_GROUP / DELETE_GROUP naming a group this
--   coordinator has no record of.
-- GROUP_NOT_EMPTY: DELETE_GROUP on a group that still has live members.
-- INVALID_CONFIG: ALTER_TOPIC_CONFIG with an unknown key, an unparseable
--   value, or a key that cannot be changed after creation (`backend`).
-- TOPIC_FORBIDDEN: an admin op aimed at an internal topic (`__` prefix).
M.ERR_GROUP_MISSING        = 15
M.ERR_GROUP_NOT_EMPTY      = 16
M.ERR_INVALID_CONFIG       = 17
M.ERR_TOPIC_FORBIDDEN      = 18
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

-- PRODUCE_BATCH payload:
--   u8 flags | u8 codec | str topic
--     | [u64 pid | u32 base_seq | u16 epoch]   (only when flags has IDEMPOTENT)
--     | u32 count | (str key | str value)*
--
-- All records go to one topic; the broker still partitions each record
-- individually (same key-hash / sticky partitioner as PRODUCE), so a batch may
-- span partitions. Under IDEMPOTENT the i-th record carries sequence
-- base_seq + i, which keeps the existing per-(pid, topic) monotonic contract.
--
-- `records` is a list of { key = , value = } (key defaults to "").
function M.encode_produce_batch(correl_id, topic, records, opts)
    assert(type(records) == "table", "records must be a list")
    opts = opts or {}
    local idempotent = opts.pid ~= nil

    local parts = {
        string.pack(">BB",
            idempotent and M.BATCH_FLAG_IDEMPOTENT or 0,
            opts.codec or 0),
        encode_string(topic),
    }
    if idempotent then
        parts[#parts + 1] = string.pack(">I8I4I2",
            opts.pid, opts.base_seq or 0, opts.epoch or 0)
    end
    parts[#parts + 1] = string.pack(">I4", #records)
    for i = 1, #records do
        local r = records[i]
        parts[#parts + 1] = encode_string(r.key or "")
        parts[#parts + 1] = encode_string(r.value or "")
    end
    return encode_frame(M.OP_PRODUCE_BATCH, correl_id, table.concat(parts))
end

-- PRODUCE_BATCH_ACK payload:
--   u32 count | (u32 partition | u64 offset)* | u16 err_code | str err_message
--
-- `count` is how many records were actually appended, always a PREFIX of the
-- request: records past it were not written. err_code 0 means the whole batch
-- landed. A non-zero code with count > 0 is a partial append — the client may
-- resend the tail (under IDEMPOTENT the sequence space lines up so the resend
-- is treated as fresh, not as a duplicate).
function M.encode_produce_batch_ack(correl_id, acks, err_code, err_message)
    local parts = { string.pack(">I4", #acks) }
    for i = 1, #acks do
        parts[#parts + 1] = string.pack(">I4I8", acks[i].partition, acks[i].offset)
    end
    parts[#parts + 1] = string.pack(">I2", err_code or 0)
    parts[#parts + 1] = encode_string(err_message or "")
    return encode_frame(M.OP_PRODUCE_BATCH_ACK, correl_id, table.concat(parts))
end

-- RECORD_BATCH payload:
--   u32 count | (str topic | u32 partition | u64 offset | u64 timestamp
--                | str key | str value)*
-- Same per-record fields as OP_RECORD, minus the per-record frame header.
function M.encode_record_batch(correl_id, records)
    local parts = { string.pack(">I4", #records) }
    for i = 1, #records do
        local r = records[i]
        parts[#parts + 1] = encode_string(r.topic)
        parts[#parts + 1] = string.pack(">I4I8I8",
            r.partition, r.offset, r.timestamp or 0)
        parts[#parts + 1] = encode_string(r.key or "")
        parts[#parts + 1] = encode_string(r.value or "")
    end
    return encode_frame(M.OP_RECORD_BATCH, correl_id, table.concat(parts))
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
-- flags (optional) trails isolation; set FETCH_FLAG_BATCHED to ask for one
-- RECORD_BATCH reply instead of a RECORD frame per record.
function M.encode_fetch(correl_id, topic, group_id, max_records, isolation, flags)
    local payload = encode_string(topic) .. encode_string(group_id)
        .. string.pack(">I4", max_records)
        .. string.pack(">B", isolation or M.ISOLATION_READ_UNCOMMITTED)
        .. string.pack(">B", flags or 0)
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

-- LIST_OFFSETS request:
--   str topic [| u8 mode | u64 timestamp_ms]
--
-- `timestamp_ms` nil asks for bounds only and emits the bare one-string body
-- older brokers expect. Passing one appends the TIMESTAMP mode byte and the
-- query, and the reply gains its FOR_TIMES section — the extension shape this
-- function's comment reserved when LIST_OFFSETS first shipped.
--
-- Unlike the bounds (in-memory counters), a timestamp query walks the log: the
-- segmented backend binary-searches the sparse .timeindex to a nearby file
-- position and linear-scans one index interval from there, and the commitlog
-- backend, which keeps no time index, scans from its oldest retained offset.
-- Cheap enough for a seek, not something to put in a poll loop.
function M.encode_list_offsets(correl_id, topic, timestamp_ms)
    local payload = encode_string(topic)
    if timestamp_ms ~= nil then
        assert(math.type(timestamp_ms) == "integer" and timestamp_ms >= 0,
            "timestamp_ms must be a non-negative integer")
        payload = payload ..
            string.pack(">BI8", M.LIST_OFFSETS_MODE_TIMESTAMP, timestamp_ms)
    end
    return encode_frame(M.OP_LIST_OFFSETS, correl_id, payload)
end

-- OFFSETS reply:
--   u32 count
--     | (u32 partition | u64 earliest | u64 latest
--        | u64 high_watermark | u64 lso | u8 flags)*
--
-- Entries are emitted in partition order. `earliest` is the oldest offset
-- still readable after retention, `latest` the offset the next append will
-- get, so an empty partition reports earliest == latest.
--
-- Callers pass nil for high_watermark / lso when the value isn't available and
-- this substitutes `latest` with the corresponding flag cleared — see the
-- OFFSETS_FLAG_* comments for why a substituted value beats a sentinel.
-- `for_times` (optional) appends a trailing section answering a TIMESTAMP
-- query:
--
--   u32 count | (u32 partition | u64 offset | u8 flags)*
--
-- It is appended only when the request asked for it. That placement is what
-- makes the extension safe: decode_offsets reads exactly `count` fixed-width
-- entries and returns, so a client built before this existed stops at the end
-- of the bounds section and never sees the extra bytes.
function M.encode_offsets(correl_id, entries, for_times)
    assert(type(entries) == "table", "entries must be a list")

    local parts = { string.pack(">I4", #entries) }
    for i = 1, #entries do
        local e = entries[i]
        local flags = 0
        local hwm, lso = e.high_watermark, e.lso
        if hwm then flags = flags | M.OFFSETS_FLAG_HWM_EXACT else hwm = e.latest end
        if lso then flags = flags | M.OFFSETS_FLAG_LSO_KNOWN else lso = e.latest end
        if e.local_leader then flags = flags | M.OFFSETS_FLAG_LOCAL end
        parts[#parts + 1] = string.pack(">I4I8I8I8I8B",
            e.partition, e.earliest, e.latest, hwm, lso, flags)
    end

    if for_times then
        parts[#parts + 1] = string.pack(">I4", #for_times)
        for i = 1, #for_times do
            local t = for_times[i]
            parts[#parts + 1] = string.pack(">I4I8B", t.partition, t.offset,
                t.found and M.FOR_TIMES_FLAG_FOUND or 0)
        end
    end

    return encode_frame(M.OP_OFFSETS, correl_id, table.concat(parts))
end

-- Topic administration.
--
-- DELETE_TOPIC / DESCRIBE_TOPIC request: str name. DELETE is acked with OP_OK.
function M.encode_delete_topic(correl_id, name)
    return encode_frame(M.OP_DELETE_TOPIC, correl_id, encode_string(name))
end

function M.encode_describe_topic(correl_id, name)
    return encode_frame(M.OP_DESCRIBE_TOPIC, correl_id, encode_string(name))
end

-- TOPIC_DESCRIPTION reply:
--   str name | u32 partition_count | u32 config_count | (str key | str value)*
--
-- Config values are strings even for the numeric keys. The sidecar mixes
-- numbers (retention, max_segment_size) with strings (backend,
-- cleanup_policy), and a client that just prints them or round-trips them into
-- ALTER_TOPIC_CONFIG should not have to carry a per-key type table to do it.
-- Only keys actually set on the topic are emitted — an absent key means the
-- partition default is in force, which is not the same as a set value that
-- happens to equal the default.
function M.encode_topic_description(correl_id, name, num_partitions, config)
    local keys = {}
    for k in pairs(config) do keys[#keys + 1] = k end
    table.sort(keys)

    local parts = {
        encode_string(name),
        string.pack(">I4", num_partitions),
        string.pack(">I4", #keys),
    }
    for _, k in ipairs(keys) do
        parts[#parts + 1] = encode_string(k)
        parts[#parts + 1] = encode_string(tostring(config[k]))
    end
    return encode_frame(M.OP_TOPIC_DESCRIPTION, correl_id, table.concat(parts))
end

-- ALTER_TOPIC_CONFIG request:
--   str name | u32 count | (str key | str value)*
-- Acked with OP_OK. Values travel as strings and the broker parses them
-- against the key's declared type, so the wire format does not need to change
-- when a new config key is added.
function M.encode_alter_topic_config(correl_id, name, config)
    local keys = {}
    for k in pairs(config) do keys[#keys + 1] = k end
    table.sort(keys)

    local parts = { encode_string(name), string.pack(">I4", #keys) }
    for _, k in ipairs(keys) do
        parts[#parts + 1] = encode_string(k)
        parts[#parts + 1] = encode_string(tostring(config[k]))
    end
    return encode_frame(M.OP_ALTER_TOPIC_CONFIG, correl_id, table.concat(parts))
end

-- Consumer-group administration.
--
-- LIST_GROUPS request: empty body.
function M.encode_list_groups(correl_id)
    return encode_frame(M.OP_LIST_GROUPS, correl_id, "")
end

-- GROUP_LIST reply:
--   u32 count | (str group_id | str state | u32 member_count)*
-- Groups are emitted in sorted id order so the bytes are deterministic.
function M.encode_group_list(correl_id, groups)
    local parts = { string.pack(">I4", #groups) }
    for i = 1, #groups do
        local g = groups[i]
        parts[#parts + 1] = encode_string(g.group_id)
        parts[#parts + 1] = encode_string(g.state)
        parts[#parts + 1] = string.pack(">I4", g.member_count)
    end
    return encode_frame(M.OP_GROUP_LIST, correl_id, table.concat(parts))
end

function M.encode_describe_group(correl_id, group_id)
    return encode_frame(M.OP_DESCRIBE_GROUP, correl_id, encode_string(group_id))
end

-- GROUP_DESCRIPTION reply:
--   str group_id | str state
--   | u32 member_count
--       | (str member_id | u32 topic_count
--            | (str topic | u32 part_count | (u32 partition_id)*)*)*
--   | u32 offset_count | (str topic | u32 partition | u64 committed)*
--
-- The offsets section is the group's DURABLE committed positions from
-- __consumer_offsets, not the members' in-flight cursors, and it is
-- independent of membership: a group with no live members still has offsets,
-- which is exactly the state you want to inspect before deleting it. Pairing
-- these with LIST_OFFSETS `latest` (or `lso` under read_committed) is what
-- makes lag computable without the broker having to define it.
function M.encode_group_description(correl_id, desc)
    local parts = { encode_string(desc.group_id), encode_string(desc.state) }

    local members = desc.members or {}
    parts[#parts + 1] = string.pack(">I4", #members)
    for i = 1, #members do
        local m = members[i]
        parts[#parts + 1] = encode_string(m.member_id)

        local topics = {}
        for t in pairs(m.assignment or {}) do topics[#topics + 1] = t end
        table.sort(topics)

        parts[#parts + 1] = string.pack(">I4", #topics)
        for _, topic in ipairs(topics) do
            local ids = m.assignment[topic]
            parts[#parts + 1] = encode_string(topic)
            parts[#parts + 1] = string.pack(">I4", #ids)
            for j = 1, #ids do
                parts[#parts + 1] = string.pack(">I4", ids[j])
            end
        end
    end

    local offsets = desc.offsets or {}
    parts[#parts + 1] = string.pack(">I4", #offsets)
    for i = 1, #offsets do
        local o = offsets[i]
        parts[#parts + 1] = encode_string(o.topic)
        parts[#parts + 1] = string.pack(">I4I8", o.partition, o.offset)
    end

    return encode_frame(M.OP_GROUP_DESCRIPTION, correl_id, table.concat(parts))
end

function M.encode_delete_group(correl_id, group_id)
    return encode_frame(M.OP_DELETE_GROUP, correl_id, encode_string(group_id))
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

function M.encode_ok(correl_id) return encode_frame(M.OP_OK, correl_id, "") end

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
-- Upper bound on partitions in one OFFSETS reply, mirroring the num_partitions
-- ceiling CREATE_TOPIC enforces. Caps how much a single count field can make a
-- client allocate. Declared up here with the other field caps because the
-- group-description decoder needs it too, and that one sits above the OFFSETS
-- decoders -- a `local` referenced before its declaration would silently read
-- a nil global instead.
local MAX_PARTITIONS = 1024
M.MAX_PARTITIONS = MAX_PARTITIONS
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
    -- Optional trailing isolation byte (older clients don't send it), then an
    -- optional flags byte after it (clients older than batching send neither).
    local isolation = M.ISOLATION_READ_UNCOMMITTED
    local flags     = 0
    if #payload - p3 + 1 >= 1 then
        local iso, p4 = string.unpack(">B", payload, p3)
        isolation = iso
        if #payload - p4 + 1 >= 1 then
            flags = string.unpack(">B", payload, p4)
        end
    end
    return {
        topic       = topic,
        group_id    = group,
        max_records = max_records,
        isolation   = isolation,
        flags       = flags,
        batched     = (flags & M.FETCH_FLAG_BATCHED) ~= 0,
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

-- DELETE_TOPIC / DESCRIBE_TOPIC / DELETE_GROUP / DESCRIBE_GROUP all carry a
-- single length-prefixed name, so they share a decoder factory rather than
-- four near-identical functions.
local function single_name_decoder(field, max_len)
    return function(payload)
        local name, _, err = decode_string(payload, 1, max_len)
        if not name then return nil, err end
        return { [field] = name }, nil
    end
end

M.decode_delete_topic   = single_name_decoder("name", MAX_TOPIC_NAME)
M.decode_describe_topic = single_name_decoder("name", MAX_TOPIC_NAME)
M.decode_describe_group = single_name_decoder("group_id", MAX_GROUP_ID)
M.decode_delete_group   = single_name_decoder("group_id", MAX_GROUP_ID)

-- Cap on config entries in one ALTER_TOPIC_CONFIG frame. The recognised key
-- set is six entries (topic_config.lua NUMBER_KEYS + STRING_KEYS); 64 leaves
-- room to grow while still bounding how many length-prefixed pairs a single
-- frame can make us decode.
local MAX_CONFIG_ENTRIES = 64
M.MAX_CONFIG_ENTRIES = MAX_CONFIG_ENTRIES
-- Config keys and values are short identifiers and numbers. Capping them well
-- under the 64 KiB default keeps a malformed frame from allocating on our
-- behalf, the same reason the credential fields are capped.
local MAX_CONFIG_KEY   = 64
local MAX_CONFIG_VALUE = 256

function M.decode_alter_topic_config(payload)
    local name, p, nerr = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not name then return nil, nerr end
    if #payload - p + 1 < 4 then return nil, "short alter_topic_config count" end

    local count, pos = string.unpack(">I4", payload, p)
    if count > MAX_CONFIG_ENTRIES then
        return nil, string.format(
            "too many config entries (%d > %d)", count, MAX_CONFIG_ENTRIES)
    end

    local config = {}
    for _ = 1, count do
        local key, np, kerr = decode_string(payload, pos, MAX_CONFIG_KEY)
        if not key then return nil, kerr end
        local val, np2, verr = decode_string(payload, np, MAX_CONFIG_VALUE)
        if not val then return nil, verr end
        config[key] = val
        pos = np2
    end

    return { name = name, config = config }, nil
end

-- Inverse of encode_topic_description.
function M.decode_topic_description(payload)
    local name, p, nerr = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not name then return nil, nerr end
    if #payload - p + 1 < 8 then return nil, "short topic description" end

    local num_partitions, p2 = string.unpack(">I4", payload, p)
    local count, pos = string.unpack(">I4", payload, p2)
    if count > MAX_CONFIG_ENTRIES then
        return nil, string.format(
            "too many config entries (%d > %d)", count, MAX_CONFIG_ENTRIES)
    end

    local config = {}
    for _ = 1, count do
        local key, np, kerr = decode_string(payload, pos, MAX_CONFIG_KEY)
        if not key then return nil, kerr end
        local val, np2, verr = decode_string(payload, np, MAX_CONFIG_VALUE)
        if not val then return nil, verr end
        config[key] = val
        pos = np2
    end

    return { name = name, num_partitions = num_partitions, config = config }, nil
end

-- Cap on groups in one GROUP_LIST reply, mirroring the max_groups ceiling the
-- coordinator enforces on live groups.
local MAX_GROUPS_LISTED = 4096
M.MAX_GROUPS_LISTED = MAX_GROUPS_LISTED

function M.decode_group_list(payload)
    if #payload < 4 then return nil, "short group list" end
    local count, pos = string.unpack(">I4", payload, 1)
    if count > MAX_GROUPS_LISTED then
        return nil, string.format(
            "too many groups (%d > %d)", count, MAX_GROUPS_LISTED)
    end

    local groups = {}
    for i = 1, count do
        local gid, np, gerr = decode_string(payload, pos, MAX_GROUP_ID)
        if not gid then return nil, gerr end
        local state, np2, serr = decode_string(payload, np, MAX_GROUP_ID)
        if not state then return nil, serr end
        if #payload - np2 + 1 < 4 then return nil, "short group list entry" end
        local members, np3 = string.unpack(">I4", payload, np2)
        groups[i] = { group_id = gid, state = state, member_count = members }
        pos = np3
    end
    return groups, nil
end

-- Inverse of encode_group_description.
function M.decode_group_description(payload)
    local gid, p, gerr = decode_string(payload, 1, MAX_GROUP_ID)
    if not gid then return nil, gerr end
    local state, pos, serr = decode_string(payload, p, MAX_GROUP_ID)
    if not state then return nil, serr end

    if #payload - pos + 1 < 4 then return nil, "short group description" end
    local mcount, mp = string.unpack(">I4", payload, pos)
    if mcount > MAX_GROUP_TOPICS then
        return nil, string.format("too many members (%d)", mcount)
    end
    pos = mp

    local members = {}
    for i = 1, mcount do
        local mid, np, merr = decode_string(payload, pos, MAX_MEMBER_ID)
        if not mid then return nil, merr end
        pos = np

        if #payload - pos + 1 < 4 then return nil, "short member topic count" end
        local tcount, tp = string.unpack(">I4", payload, pos)
        if tcount > MAX_GROUP_TOPICS then
            return nil, string.format("too many topics (%d)", tcount)
        end
        pos = tp

        local assignment = {}
        for _ = 1, tcount do
            local topic, tnp, terr = decode_string(payload, pos, MAX_TOPIC_NAME)
            if not topic then return nil, terr end
            pos = tnp

            if #payload - pos + 1 < 4 then return nil, "short assignment count" end
            local pcount, pp = string.unpack(">I4", payload, pos)
            if pcount > MAX_PARTITIONS then
                return nil, string.format("too many partitions (%d)", pcount)
            end
            pos = pp

            local ids = {}
            for j = 1, pcount do
                if #payload - pos + 1 < 4 then return nil, "short partition id" end
                local id, npp = string.unpack(">I4", payload, pos)
                ids[j] = id
                pos = npp
            end
            assignment[topic] = ids
        end
        members[i] = { member_id = mid, assignment = assignment }
    end

    if #payload - pos + 1 < 4 then return nil, "short offset count" end
    local ocount, op = string.unpack(">I4", payload, pos)
    if ocount > MAX_PARTITIONS then
        return nil, string.format("too many offsets (%d)", ocount)
    end
    pos = op

    local offsets = {}
    for i = 1, ocount do
        local topic, np, terr = decode_string(payload, pos, MAX_TOPIC_NAME)
        if not topic then return nil, terr end
        if #payload - np + 1 < 12 then return nil, "short offset entry" end
        local partition, offset, npp = string.unpack(">I4I8", payload, np)
        offsets[i] = { topic = topic, partition = partition, offset = offset }
        pos = npp
    end

    return {
        group_id = gid,
        state    = state,
        members  = members,
        offsets  = offsets,
    }, nil
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

-- Cap records-per-produce-batch so one frame can't ask us to decode an
-- attacker-chosen number of key/value pairs. The real bound on a batch is
-- MaxFrameSize (the framer rejects the frame before we ever get here); this
-- is the belt-and-braces count bound, checked before any allocation.
--
-- MAX_IDEMPOTENT_BATCH is tighter because an idempotent batch's per-record
-- acks are persisted in the producer-state memo so an exact retry can replay
-- them (see src/storage/producer_state.lua) — 1024 records is a ~12 KB memo
-- record, which is the most we're willing to write per batch.
local MAX_BATCH_RECORDS     = 10000
local MAX_IDEMPOTENT_BATCH  = 1024
M.MAX_BATCH_RECORDS    = MAX_BATCH_RECORDS
M.MAX_IDEMPOTENT_BATCH = MAX_IDEMPOTENT_BATCH

function M.decode_produce_batch(payload)
    if #payload < 2 then return nil, "short produce_batch header" end
    local flags, codec, p = string.unpack(">BB", payload, 1)
    local topic, p2, terr = decode_string(payload, p, MAX_TOPIC_NAME)
    if not topic then return nil, terr end

    local idempotent = (flags & M.BATCH_FLAG_IDEMPOTENT) ~= 0
    local pid, base_seq, epoch
    if idempotent then
        if #payload - p2 + 1 < 14 then return nil, "short produce_batch producer header" end
        pid, base_seq, epoch, p2 = string.unpack(">I8I4I2", payload, p2)
    end

    if #payload - p2 + 1 < 4 then return nil, "short produce_batch count" end
    local count, pos = string.unpack(">I4", payload, p2)
    if count == 0 then return nil, "produce_batch with no records" end
    if count > MAX_BATCH_RECORDS then
        return nil, string.format("too many records (%d > %d)", count, MAX_BATCH_RECORDS)
    end
    -- Cheapest possible record is two empty length prefixes; reject a count
    -- that the remaining bytes cannot possibly satisfy before we start
    -- allocating a table sized for it.
    if count * 8 > #payload - pos + 1 then
        return nil, string.format("produce_batch count %d exceeds payload", count)
    end

    local records = {}
    for i = 1, count do
        local key, np, kerr = decode_string(payload, pos)
        if not key then return nil, kerr end
        local value, np2, verr = decode_string(payload, np)
        if not value then return nil, verr end
        records[i] = { key = key, value = value }
        pos = np2
    end

    return {
        flags      = flags,
        idempotent = idempotent,
        codec      = codec,
        topic      = topic,
        pid        = pid,
        base_seq   = base_seq,
        epoch      = epoch,
        records    = records,
    }, nil
end

function M.decode_produce_batch_ack(payload)
    if #payload < 4 then return nil, "short produce_batch_ack" end
    local count, pos = string.unpack(">I4", payload, 1)
    if count > MAX_BATCH_RECORDS then
        return nil, string.format("too many acks (%d > %d)", count, MAX_BATCH_RECORDS)
    end
    if count * 12 > #payload - pos + 1 then
        return nil, "produce_batch_ack count exceeds payload"
    end
    local acks = {}
    for i = 1, count do
        local partition, offset, np = string.unpack(">I4I8", payload, pos)
        acks[i] = { partition = partition, offset = offset }
        pos = np
    end
    if #payload - pos + 1 < 2 then return nil, "short produce_batch_ack status" end
    local code, cp = string.unpack(">I2", payload, pos)
    local message, _, merr = decode_string(payload, cp)
    if not message then return nil, merr end
    return { acks = acks, code = code, message = message }, nil
end

function M.decode_record_batch(payload)
    if #payload < 4 then return nil, "short record_batch" end
    local count, pos = string.unpack(">I4", payload, 1)
    if count > MAX_BATCH_RECORDS then
        return nil, string.format("too many records (%d > %d)", count, MAX_BATCH_RECORDS)
    end
    local records = {}
    for i = 1, count do
        local topic, p, terr = decode_string(payload, pos, MAX_TOPIC_NAME)
        if not topic then return nil, terr end
        if #payload - p + 1 < 20 then return nil, "short record_batch entry" end
        local partition, offset, timestamp, p2 = string.unpack(">I4I8I8", payload, p)
        local key, p3, kerr = decode_string(payload, p2)
        if not key then return nil, kerr end
        local value, p4, verr = decode_string(payload, p3)
        if not value then return nil, verr end
        records[i] = {
            topic     = topic,
            partition = partition,
            offset    = offset,
            timestamp = timestamp,
            key       = key,
            value     = value,
        }
        pos = p4
    end
    return records, nil
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

function M.decode_list_offsets(payload)
    local topic, p, terr = decode_string(payload, 1, MAX_TOPIC_NAME)
    if not topic then return nil, terr end

    -- Optional trailing mode byte; clients older than offset-for-timestamp
    -- send nothing after the topic.
    local mode = M.LIST_OFFSETS_MODE_BOUNDS
    local timestamp
    if #payload - p + 1 >= 1 then
        local m, p2 = string.unpack(">B", payload, p)
        mode = m
        if mode == M.LIST_OFFSETS_MODE_TIMESTAMP then
            -- A TIMESTAMP mode byte with no timestamp behind it is a malformed
            -- frame, not an old client: an old client never sends the byte at
            -- all. Reject rather than silently degrading to a bounds query,
            -- which would answer a question nobody asked.
            if #payload - p2 + 1 < 8 then
                return nil, "list_offsets: timestamp mode without a timestamp"
            end
            timestamp = string.unpack(">I8", payload, p2)
        elseif mode ~= M.LIST_OFFSETS_MODE_BOUNDS then
            return nil, string.format("list_offsets: unknown mode %d", mode)
        end
    end

    return { topic = topic, mode = mode, timestamp = timestamp }, nil
end

-- Returns the entry list described on encode_offsets, partition order
-- preserved. The flag bits come back as booleans: `hwm_exact` and `lso_known`
-- say whether those two fields are real readings or the `latest` stand-in,
-- and `leader` says the answering broker serves the partition.
function M.decode_offsets(payload)
    if #payload < 4 then return nil, "short offsets" end
    local count, pos = string.unpack(">I4", payload, 1)
    if count > MAX_PARTITIONS then
        return nil, string.format(
            "too many partitions (%d > %d)", count, MAX_PARTITIONS)
    end

    -- partition(4) + earliest(8) + latest(8) + hwm(8) + lso(8) + flags(1)
    local ENTRY_SIZE = 37
    local entries = {}
    for i = 1, count do
        if #payload - pos + 1 < ENTRY_SIZE then return nil, "short offsets entry" end
        local partition, earliest, latest, hwm, lso, flags, np =
            string.unpack(">I4I8I8I8I8B", payload, pos)
        entries[i] = {
            partition      = partition,
            earliest       = earliest,
            latest         = latest,
            high_watermark = hwm,
            lso            = lso,
            hwm_exact      = (flags & M.OFFSETS_FLAG_HWM_EXACT) ~= 0,
            lso_known      = (flags & M.OFFSETS_FLAG_LSO_KNOWN) ~= 0,
            leader         = (flags & M.OFFSETS_FLAG_LOCAL) ~= 0,
        }
        pos = np
    end

    -- Optional FOR_TIMES section (see encode_offsets). Absent on a bounds
    -- reply and on any broker built before offset-for-timestamp, so its
    -- absence is normal, not an error. Attached as a named field on the
    -- entries list: `#entries` and ipairs() still see only the bounds
    -- entries, so every existing caller is unaffected.
    if #payload - pos + 1 >= 4 then
        local tcount, tpos = string.unpack(">I4", payload, pos)
        if tcount > MAX_PARTITIONS then
            return nil, string.format(
                "too many for_times entries (%d > %d)", tcount, MAX_PARTITIONS)
        end
        -- partition(4) + offset(8) + flags(1)
        local TIME_ENTRY_SIZE = 13
        local times = {}
        for i = 1, tcount do
            if #payload - tpos + 1 < TIME_ENTRY_SIZE then
                return nil, "short for_times entry"
            end
            local partition, offset, flags, np = string.unpack(">I4I8B", payload, tpos)
            times[i] = {
                partition = partition,
                offset    = offset,
                found     = (flags & M.FOR_TIMES_FLAG_FOUND) ~= 0,
            }
            tpos = np
        end
        entries.for_times = times
    end

    return entries, nil
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