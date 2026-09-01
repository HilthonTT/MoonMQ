local M = {}
M.__index = M

M.PROTOCOL_VERSION = 1
M.SERVER_NAME = "MoonMQ"
M.SERVER_VERSION = "v0.01"

M.OP_HELLO        = 0x01
M.OP_AUTH         = 0x02
M.OP_PRODUCE      = 0x03
M.OP_FETCH        = 0x04
M.OP_SUBSCRIBE    = 0x05
M.OP_COMMIT       = 0x06
M.OP_CREATE_TOPIC = 0x07
M.OP_LIST_TOPICS  = 0x08
M.OP_GOODBYE      = 0x0A

M.OP_IDENTIFY_CLIENT  = 0x0D
M.OP_INIT_PRODUCER_ID = 0x0E
M.OP_PRODUCE_IDEMPOTENT = 0x0F

M.OP_JOIN_GROUP       = 0x10
M.OP_LEAVE_GROUP      = 0x11
M.OP_GROUP_HEARTBEAT  = 0x12

M.OP_BEGIN_TXN        = 0x13
M.OP_END_TXN          = 0x14
M.OP_TXN_OFFSET_COMMIT = 0x15

M.OP_NACK             = 0x16

M.OP_PRODUCE_BATCH    = 0x17

M.OP_LIST_OFFSETS     = 0x18

M.OP_DELETE_TOPIC       = 0x19
M.OP_DESCRIBE_TOPIC     = 0x1A
M.OP_ALTER_TOPIC_CONFIG = 0x1B

M.OP_LIST_GROUPS      = 0x1C
M.OP_DESCRIBE_GROUP   = 0x1D
M.OP_DELETE_GROUP     = 0x1E

M.OP_AUTH_SCRAM       = 0x1F
M.OP_AUTH_SCRAM_FINAL = 0x20

M.OP_HEARTBEAT_REQ   = 0x0B
M.OP_HEARTBEAT_RESP  = 0x0C

M.OP_WELCOME       = 0x80
M.OP_AUTH_OK       = 0x81
M.OP_PRODUCE_ACK   = 0x82
M.OP_RECORD        = 0x83
M.OP_TOPIC_LIST    = 0x84
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
M.OP_AUTH_CHALLENGE    = 0x91
M.OP_ERROR         = 0xFE

local CORREL_ID_LEN = 16
M.CORREL_ID_LEN = CORREL_ID_LEN

M.ISOLATION_READ_UNCOMMITTED = 0
M.ISOLATION_READ_COMMITTED   = 1

M.FETCH_FLAG_BATCHED = 0x01

M.BATCH_FLAG_IDEMPOTENT = 0x01

M.OFFSETS_FLAG_HWM_EXACT = 0x01
M.OFFSETS_FLAG_LSO_KNOWN = 0x02
M.OFFSETS_FLAG_LOCAL     = 0x04

M.LIST_OFFSETS_MODE_BOUNDS    = 0
M.LIST_OFFSETS_MODE_TIMESTAMP = 1

M.FOR_TIMES_FLAG_FOUND = 0x01

M.ERR_BAD_FRAME            = 1
M.ERR_UNKNOWN_OP           = 2
M.ERR_NOT_AUTHED           = 3
M.ERR_AUTH_FAILED          = 4
M.ERR_TOPIC_MISSING        = 5
M.ERR_RATE_LIMITED         = 6
M.ERR_INTERNAL             = 7
M.ERR_BAD_PROTOCOL         = 8
M.ERR_FRAME_TOO_LARGE      = 9
M.ERR_NO_PRODUCER_ID       = 10
M.ERR_OUT_OF_ORDER_SEQUENCE = 11
M.ERR_GROUP_MEMBER_UNKNOWN = 12
M.ERR_GROUP_CONFLICT       = 13
M.ERR_BATCH_TOO_LARGE      = 14
M.ERR_GROUP_MISSING        = 15
M.ERR_GROUP_NOT_EMPTY      = 16
M.ERR_INVALID_CONFIG       = 17
M.ERR_TOPIC_FORBIDDEN      = 18
M.ERR_NOT_AUTHORIZED       = 19
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

function M.encode_auth_ok(correl_id, extra)
    return encode_frame(M.OP_AUTH_OK, correl_id,
        extra and extra ~= "" and encode_string(extra) or "")
end

function M.encode_auth_scram(correl_id, mechanism, client_first)
    local payload = encode_string(mechanism) .. encode_string(client_first)
    return encode_frame(M.OP_AUTH_SCRAM, correl_id, payload)
end

function M.encode_auth_challenge(correl_id, server_first)
    return encode_frame(M.OP_AUTH_CHALLENGE, correl_id, encode_string(server_first))
end

function M.encode_auth_scram_final(correl_id, client_final)
    return encode_frame(M.OP_AUTH_SCRAM_FINAL, correl_id, encode_string(client_final))
end

function M.encode_produce(correl_id, topic, key, value, codec)
    local payload = string.pack(">B", codec or 0)
        .. encode_string(topic) .. encode_string(key) .. encode_string(value)
    return encode_frame(M.OP_PRODUCE, correl_id, payload)
end

function M.encode_produce_ack(correl_id, partition, offset)
    local payload = string.pack(">I4I8", partition, offset)
    return encode_frame(M.OP_PRODUCE_ACK, correl_id, payload)
end

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

function M.encode_produce_batch_ack(correl_id, acks, err_code, err_message)
    local parts = { string.pack(">I4", #acks) }
    for i = 1, #acks do
        parts[#parts + 1] = string.pack(">I4I8", acks[i].partition, acks[i].offset)
    end
    parts[#parts + 1] = string.pack(">I2", err_code or 0)
    parts[#parts + 1] = encode_string(err_message or "")
    return encode_frame(M.OP_PRODUCE_BATCH_ACK, correl_id, table.concat(parts))
end

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

function M.encode_init_producer_id(correl_id, producer_name)
    return encode_frame(M.OP_INIT_PRODUCER_ID, correl_id,
        encode_string(producer_name or ""))
end

function M.decode_init_producer_id(payload)
    if #payload == 0 then return { producer_name = "" }, nil end
    local name, _, err = M.decode_string(payload, 1, M.MAX_PRODUCER_NAME)
    if not name then return nil, err end
    return { producer_name = name }, nil
end

function M.encode_producer_id(correl_id, pid, epoch)
    local payload = string.pack(">I8I2", pid, epoch or 0)
    return encode_frame(M.OP_PRODUCER_ID, correl_id, payload)
end

function M.decode_producer_id(payload)
    if #payload < 10 then return nil, "short producer_id" end
    local pid, epoch = string.unpack(">I8I2", payload, 1)
    return { pid = pid, epoch = epoch }, nil
end

function M.encode_produce_idempotent(correl_id, pid, seq, topic, key, value, epoch, codec)
    local payload = string.pack(">I8I4I2B", pid, seq, epoch or 0, codec or 0)
        .. encode_string(topic)
        .. encode_string(key)
        .. encode_string(value)
    return encode_frame(M.OP_PRODUCE_IDEMPOTENT, correl_id, payload)
end


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

function M.encode_nack(correl_id, topic, partition, offset, reason)
    local payload = encode_string(topic)
        .. string.pack(">I4I8", partition, offset)
        .. encode_string(reason or "")
    return encode_frame(M.OP_NACK, correl_id, payload)
end

function M.encode_nack_ack(correl_id, dead_lettered, attempts, dlq_topic)
    local payload = string.pack(">BI2", dead_lettered and 1 or 0, attempts)
        .. encode_string(dlq_topic or "")
    return encode_frame(M.OP_NACK_ACK, correl_id, payload)
end

function M.encode_begin_txn(correl_id)
    return encode_frame(M.OP_BEGIN_TXN, correl_id, "")
end

function M.encode_end_txn(correl_id, commit)
    return encode_frame(M.OP_END_TXN, correl_id,
        string.pack(">B", commit and 1 or 0))
end

function M.decode_end_txn(payload)
    if #payload < 1 then return nil, "short end_txn" end
    return { commit = string.unpack(">B", payload, 1) ~= 0 }, nil
end

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

function M.encode_delete_topic(correl_id, name)
    return encode_frame(M.OP_DELETE_TOPIC, correl_id, encode_string(name))
end

function M.encode_describe_topic(correl_id, name)
    return encode_frame(M.OP_DESCRIBE_TOPIC, correl_id, encode_string(name))
end

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

function M.encode_list_groups(correl_id)
    return encode_frame(M.OP_LIST_GROUPS, correl_id, "")
end

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

local MAX_STRING_LEN = 64 * 1024

local function decode_string(buf, pos, max_len)
    if #buf - pos + 1 < 4 then
        return nil, nil, "truncated string length"
    end
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

local MAX_TOPIC_NAME    = 249
local MAX_GROUP_ID      = 256
local MAX_MEMBER_ID     = 256
local MAX_CLIENT_NAME   = 128
local MAX_CLIENT_VERSION = 64
local MAX_USERNAME      = 256
local MAX_PASSWORD      = 1024
local MAX_PARTITIONS = 1024
M.MAX_PARTITIONS = MAX_PARTITIONS
local MAX_GROUP_TOPICS  = 256
local MAX_PRODUCER_NAME = 256
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

local MAX_SASL_MESSAGE = 4096
local MAX_MECHANISM    = 64
M.MAX_SASL_MESSAGE = MAX_SASL_MESSAGE

function M.decode_auth_scram(payload)
    local mechanism, p, err = decode_string(payload, 1, MAX_MECHANISM)
    if not mechanism then return nil, err end
    local message, _, merr = decode_string(payload, p, MAX_SASL_MESSAGE)
    if not message then return nil, merr end
    return { mechanism = mechanism, message = message }, nil
end

function M.decode_auth_challenge(payload)
    local message, _, err = decode_string(payload, 1, MAX_SASL_MESSAGE)
    if not message then return nil, err end
    return { message = message }, nil
end

function M.decode_auth_scram_final(payload)
    local message, _, err = decode_string(payload, 1, MAX_SASL_MESSAGE)
    if not message then return nil, err end
    return { message = message }, nil
end

function M.decode_auth_ok(payload)
    if not payload or #payload == 0 then return { message = "" }, nil end
    local message, _, err = decode_string(payload, 1, MAX_SASL_MESSAGE)
    if not message then return nil, err end
    return { message = message }, nil
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

local MAX_CONFIG_ENTRIES = 64
M.MAX_CONFIG_ENTRIES = MAX_CONFIG_ENTRIES
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

    local mode = M.LIST_OFFSETS_MODE_BOUNDS
    local timestamp
    if #payload - p + 1 >= 1 then
        local m, p2 = string.unpack(">B", payload, p)
        mode = m
        if mode == M.LIST_OFFSETS_MODE_TIMESTAMP then
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

function M.decode_offsets(payload)
    if #payload < 4 then return nil, "short offsets" end
    local count, pos = string.unpack(">I4", payload, 1)
    if count > MAX_PARTITIONS then
        return nil, string.format(
            "too many partitions (%d > %d)", count, MAX_PARTITIONS)
    end

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

    if #payload - pos + 1 >= 4 then
        local tcount, tpos = string.unpack(">I4", payload, pos)
        if tcount > MAX_PARTITIONS then
            return nil, string.format(
                "too many for_times entries (%d > %d)", tcount, MAX_PARTITIONS)
        end
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

function M.decode_produce_idempotent(payload)
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