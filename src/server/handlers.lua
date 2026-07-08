-- Application-level command handlers, one per opcode. Extracted from
-- server.lua so the Server stays focused on the listener, capacity
-- accounting, and lifecycle; everything protocol-command-shaped lives here.
--
-- Every handler has the same signature:
--
--     handler(server, conn, correl, payload)
--
-- and is looked up through the BY_OP table by Server:dispatch. Handlers reach
-- broker state through `server` (producer, broker, reactor, coordinator) and
-- per-connection state through `conn`.

local proto      = require("src.wire.protocol")
local Connection = require("src.server.connection")
local uuid       = require("src.core.uuid")
local consumer_m = require("src.broker.consumer")
local msg_m      = require("src.record.message")
local metrics    = require("src.metrics")
local log        = require("src.log.logger").get("server")
local push_log   = require("src.log.logger").get("push")

local M = {}

function M.hello(server, conn, correl, payload)
    local h, err = proto.decode_hello(payload)
    if not h then
        conn:close(Connection.REASON_BAD_FRAME, proto.ERR_BAD_FRAME, err)
        return
    end
    if h.version ~= proto.PROTOCOL_VERSION then
        conn:close(Connection.REASON_BAD_PROTOCOL, proto.ERR_BAD_PROTOCOL,
            string.format("expected v%d got v%d", proto.PROTOCOL_VERSION, h.version))
        return
    end
    conn:transition_to(Connection.STATE_GREETED)
    conn:send(proto.encode_welcome(correl, proto.PROTOCOL_VERSION))
end

function M.identify_client(server, conn, correl, payload)
    local i, err = proto.decode_identify_client(payload)
    if not i then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if #i.name > 128 or #i.version > 64 then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            "name (max 128) or version (max 64) too long"))
        return
    end
    conn.client_name    = i.name
    conn.client_version = i.version
    log:info("conn=%s identified client=%s/%s",
        conn.id_short, conn.client_name, conn.client_version)
    conn:send(proto.encode_identify_ack(correl,
        proto.SERVER_NAME, proto.SERVER_VERSION))
end

function M.auth(server, conn, correl, payload)
    local a, err = proto.decode_auth(payload)
    if not a then
        conn:close(Connection.REASON_BAD_FRAME, proto.ERR_BAD_FRAME, err)
        return
    end

    if not server.authenticator then
        log:warn("no authenticator configured, allowing")
        conn.username = a.username
        conn:transition_to(Connection.STATE_AUTHENTICATED)
        conn:send(proto.encode_auth_ok(correl))
        return
    end

    local ok, auth_err = server.authenticator:verify(a.username, a.password, conn.ip)
    if not ok then
        -- Don't log the supplied username — it's attacker-controlled.
        conn:close(Connection.REASON_AUTH_FAILED, proto.ERR_AUTH_FAILED,
            auth_err or "auth failed")
        return
    end

    conn.username = a.username
    conn:transition_to(Connection.STATE_AUTHENTICATED)
    conn:send(proto.encode_auth_ok(correl))
end

function M.produce(server, conn, correl, payload)
    if conn.rate_limiter and not conn.rate_limiter:take(1) then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            "produce rate exceeded"))
        return
    end

    local p, err = proto.decode_produce(payload)
    if not p then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    local msg = msg_m.Message.new(p.key, p.value, 0)
    local part_id, offset, werr = server.producer:produce(p.topic, msg)
    if werr then
        local code = werr:find("does not exist", 1, true)
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn:send(proto.encode_error(correl, code, werr))
        return
    end

    metrics.inc("moonmq_produce_records_total", 1, { topic = p.topic })

    conn:send(proto.encode_produce_ack(correl, part_id, offset))
end

-- INIT_PRODUCER_ID: assign a u64 producer ID to this connection. Repeated
-- calls on the same connection are tolerated but reset all per-PID seq
-- state (a client that asks for a fresh PID has lost track of its
-- sequences). PIDs are not durable.
function M.init_producer_id(server, conn, correl, _payload)
    local pid = server.next_pid
    server.next_pid = server.next_pid + 1
    conn.pid = pid
    conn.seq_state = {}
    log:info("conn=%s assigned producer_id=%d", conn.id_short, pid)
    conn:send(proto.encode_producer_id(correl, pid))
end

-- PRODUCE_IDEMPOTENT: like PRODUCE, but checks (PID, topic, seq) for
-- monotonicity. Dedup contract per (PID, topic):
--   * seq == last_seq + 1  → append, return offset, update memo.
--   * seq == last_seq      → idempotent retry. DON'T append; return the
--     original offset+partition. Without this the retry would duplicate.
--   * seq <  last_seq      → ERR_OUT_OF_ORDER_SEQUENCE (stale).
--   * seq >  last_seq + 1  → ERR_OUT_OF_ORDER_SEQUENCE (gap).
-- First record's seq must be 0 (last_seq starts at -1 implicitly).
--
-- Why per-(PID, topic) and not per-(PID, topic, partition)? Two reasons:
-- (1) it lets the client track ONE seq counter per topic without
-- needing to know the broker's partition count or hash function, and
-- (2) TCP guarantees per-connection in-order delivery, so a single
-- monotonic counter across partitions is consistent. The full
-- Kafka-style per-partition seq becomes necessary only when producers
-- batch across partitions in parallel, which we don't.
function M.produce_idempotent(server, conn, correl, payload)
    if conn.rate_limiter and not conn.rate_limiter:take(1) then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            "produce rate exceeded"))
        return
    end

    if not conn.pid then
        conn:send(proto.encode_error(correl, proto.ERR_NO_PRODUCER_ID,
            "INIT_PRODUCER_ID required before PRODUCE_IDEMPOTENT"))
        return
    end

    local p, err = proto.decode_produce_idempotent(payload)
    if not p then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    if p.pid ~= conn.pid then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            string.format("pid mismatch: frame=%d conn=%d",
                p.pid, conn.pid)))
        return
    end

    local slot = conn.seq_state[p.topic]
    local last_seq = slot and slot.last_seq or -1

    if p.seq == last_seq and slot then
        -- Idempotent retry: replay the original ack.
        conn:send(proto.encode_produce_ack(correl,
            slot.last_partition, slot.last_offset))
        return
    elseif p.seq < last_seq or p.seq > last_seq + 1 then
        conn:send(proto.encode_error(correl, proto.ERR_OUT_OF_ORDER_SEQUENCE,
            string.format("expected seq %d, got %d (pid=%d %s)",
                last_seq + 1, p.seq, conn.pid, p.topic)))
        return
    end

    -- seq == last_seq + 1: fresh record. Route through the normal
    -- producer so partitioning + group-commit fsync behave identically
    -- to the non-idempotent path. The (partition, offset) we get back
    -- is what we memo so a retry can replay the same ack.
    local msg = msg_m.Message.new(p.key, p.value, 0)
    local partition_id, offset, werr = server.producer:produce(p.topic, msg)
    if werr then
        local code = werr:find("does not exist", 1, true)
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn:send(proto.encode_error(correl, code, werr))
        return
    end

    conn.seq_state[p.topic] = {
        last_seq       = p.seq,
        last_offset    = offset,
        last_partition = partition_id,
    }
    metrics.inc("moonmq_produce_records_total", 1, { topic = p.topic })
    metrics.inc("moonmq_idempotent_produce_total", 1, { topic = p.topic })

    conn:send(proto.encode_produce_ack(correl, partition_id, offset))
end

local function ensure_consumer(conn, broker, group_id)
    if conn.consumer then
        if conn.consumer.group_id ~= group_id then
            return nil, string.format("group_id mismatch (already in group %s)",
                conn.consumer.group_id)
        end
        return conn.consumer
    end
    conn.consumer = consumer_m.Consumer.new(broker, group_id)
    return conn.consumer
end

-- Push-mode delivery loop, one coroutine per subscribed connection. Spawned
-- by M.subscribe; runs until the connection closes or delivery fails.
local function subscriber_loop(server, conn)
    while conn.state ~= Connection.STATE_CLOSED do
        -- Re-scope to the current assignment each pass so a rebalance (another
        -- member joining/leaving) takes effect on the next poll.
        server.coordinator:apply_assignment(conn)
        local records, err = conn.consumer:poll()
        if err then
            push_log:error("conn=%s poll: %s", conn.id_short, err)
            return
        end
        if records and #records > 0 then
            for i = 1, #records do
                if conn.state == Connection.STATE_CLOSED then return end
                local r = records[i]
                local frame = proto.encode_record(uuid.ZERO,
                    r.topic, r.partition, r.offset, r.timestamp, r.key, r.value)
                if not conn:send(frame) then return end
                metrics.inc("moonmq_fetch_records_total", 1, { topic = r.topic })
                -- Commit only after the record is accepted by the send layer.
                -- poll() already advanced the in-memory cursor to the next
                -- offset for this partition; persist that. If conn:send had
                -- failed we'd have returned above without committing, so the
                -- record is redelivered rather than silently lost — at-least-
                -- once on the push path. (send() enqueues; a peer that dies
                -- after enqueue but before transmit still redelivers on
                -- reconnect since the offset wasn't committed until here.)
                local adv = conn.consumer.offsets[r.topic]
                    and conn.consumer.offsets[r.topic][r.partition]
                if adv then
                    local cok, cerr =
                        conn.consumer:commit_offset(r.topic, r.partition, adv)
                    if not cok then
                        push_log:error("conn=%s commit: %s", conn.id_short, cerr)
                        return
                    end
                end
            end
        else
            server.reactor:sleep(server.push_interval)
        end
    end
end

function M.subscribe(server, conn, correl, payload)
    local s, err = proto.decode_subscribe(payload)
    if not s then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if conn.mode == "pull" then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "connection already in pull mode (used FETCH)"))
        return
    end
    local consumer, cerr = ensure_consumer(conn, server.broker, s.group_id)
    if not consumer then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL, cerr))
        return
    end
    local sok, serr = consumer:subscribe(s.topic)
    if not sok then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            serr or "subscribe failed"))
        return
    end
    conn.subscriptions[s.topic] = true
    conn.mode = "push"
    -- Push mode commits AFTER a record is handed to the send layer (see
    -- subscriber_loop), not inside poll(). Turn off poll()'s auto-commit so a
    -- record isn't marked consumed before we've even tried to deliver it.
    consumer.auto_commit = false
    conn:send(proto.encode_ok(correl))

    if not conn.subscriber_co then
        conn.subscriber_co = server.reactor:spawn(function()
            subscriber_loop(server, conn)
        end)
    end
end

function M.fetch(server, conn, correl, payload)
    local f, err = proto.decode_fetch(payload)
    if not f then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if conn.mode == "push" then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "connection already in push mode (used SUBSCRIBE)"))
        return
    end
    local consumer, cerr = ensure_consumer(conn, server.broker, f.group_id)
    if not consumer then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL, cerr))
        return
    end
    if not conn.subscriptions[f.topic] then
        local sok, sberr = consumer:subscribe(f.topic)
        if not sok then
            conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
                sberr or "subscribe failed"))
            return
        end
        conn.subscriptions[f.topic] = true
    end
    conn.mode = "pull"

    -- Restrict to this member's assigned partitions (no-op unless joined).
    server.coordinator:apply_assignment(conn)

    local records, perr = consumer:poll()
    if perr then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, perr))
        return
    end
    if records then
        local limit = math.min(#records, f.max_records or #records)
        for i = 1, limit do
            local r = records[i]
            local frame = proto.encode_record(correl,
                r.topic, r.partition, r.offset, r.timestamp, r.key, r.value)
            if not conn:send(frame) then return end
        end
        if limit > 0 then
            metrics.inc("moonmq_fetch_records_total", limit, { topic = f.topic })
        end
    end
    conn:send(proto.encode_ok(correl))
end

function M.commit(server, conn, correl, payload)
    local c, err = proto.decode_commit(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if not conn.consumer then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "commit requires prior subscribe/fetch"))
        return
    end
    -- Bounds-check the partition against the topic before forwarding to
    -- the consumer. Without this, a client could commit to any u32
    -- partition id — currently the consumer stub silently accepts it,
    -- but that's a future foot-gun once offset persistence lands.
    local topic, terr = server.broker:get_topic(c.topic)
    if not topic then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            terr or "topic missing"))
        return
    end
    if c.partition < 1 or c.partition > #topic.partitions then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            string.format("partition %d out of range (1..%d)",
                c.partition, #topic.partitions)))
        return
    end
    -- Fence commits from a connection whose group membership has lapsed. A
    -- member the coordinator evicted (heartbeat timeout) or that left still
    -- holds a live socket; without this it could keep committing and clobber
    -- the offset the partition's new owner is advancing. Non-group commits
    -- (conn.group_id unset) are unaffected.
    if conn.group_id then
        if not server.coordinator:member_alive(conn.group_id, conn.member_id) then
            conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
                "group membership lapsed; rejoin before committing"))
            return
        end
    end
    local ok, cerr = conn.consumer:commit_offset(c.topic, c.partition, c.offset)
    if not ok then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL,
            cerr or "commit failed"))
        return
    end
    conn:send(proto.encode_ok(correl))
end

function M.create_topic(server, conn, correl, payload)
    local c, err = proto.decode_create_topic(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if c.num_partitions < 1 or c.num_partitions > 1024 then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            "num_partitions out of range (1..1024)"))
        return
    end
    -- Topic-count cap. Without this a client (authenticated or not,
    -- depending on the auth config) can CREATE_TOPIC in a loop and
    -- exhaust the broker's in-memory topic table + descriptors. We
    -- check the current count against max_topics before allocating
    -- anything on disk.
    local current = #server.broker:list_topics()
    if current >= server.max_topics then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            string.format("topic limit reached (%d)", server.max_topics)))
        return
    end
    local _, terr = server.broker:create_topic(c.name, c.num_partitions)
    if terr then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, terr))
        return
    end
    metrics.set("moonmq_topic_count", current + 1)
    conn:send(proto.encode_ok(correl))
end

function M.list_topics(server, conn, correl, _payload)
    -- Bound response size. Sorting by name first so the truncation is
    -- deterministic across calls (rather than depending on table-pair
    -- iteration order). For the common case the bound is irrelevant —
    -- it only kicks in if max_topics has been raised past max_list_topics.
    local all = server.broker:list_topics()
    table.sort(all)
    if #all > server.max_list_topics then
        local truncated = {}
        for i = 1, server.max_list_topics do truncated[i] = all[i] end
        all = truncated
    end
    conn:send(proto.encode_topic_list(correl, all))
end

-- JOIN_GROUP: register this connection as a member of a group subscribing
-- to one or more topics, and reply with the partitions assigned to it.
-- An empty member_id means "first join, assign me one" — we use the
-- connection's short id so it's stable for this connection's lifetime.
function M.join_group(server, conn, correl, payload)
    local j, err = proto.decode_join_group(payload)
    if not j then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if #j.topics == 0 then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            "join_group requires at least one topic"))
        return
    end
    -- One group membership per connection: a connection already bound to a
    -- different group must LEAVE_GROUP (or reconnect) before joining another.
    if conn.group_id and conn.group_id ~= j.group_id then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_CONFLICT,
            string.format("connection already in group %s", conn.group_id)))
        return
    end

    local member_id = j.member_id
    if member_id == "" then
        member_id = conn.member_id or conn.id_short
    end

    local group, gerr = server.coordinator:get_or_create(j.group_id)
    if not group then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED, gerr))
        return
    end
    local assignment, jerr = group:join(member_id, j.topics)
    if not assignment then
        -- join() fails when a subscribed topic doesn't exist, or the group
        -- has been closed. Map the missing-topic case to a precise code.
        local code = (jerr and jerr:find("get topic", 1, true))
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn:send(proto.encode_error(correl, code, jerr or "join failed"))
        return
    end

    conn.group_id  = j.group_id
    conn.member_id = member_id
    -- If a Consumer already exists on this connection (a prior FETCH/SUBSCRIBE
    -- created one before the client joined), scope it to the new assignment now
    -- rather than waiting for the next poll.
    server.coordinator:apply_assignment(conn)
    log:info("conn=%s joined group=%s member=%s state=%s",
        conn.id_short, j.group_id, member_id, group:state())
    conn:send(proto.encode_group_assignment(correl, member_id, assignment))
end

-- LEAVE_GROUP: voluntary departure. Survivors rebalance; an emptied group
-- collapses back to its empty state (handled inside ConsumerGroup:leave).
function M.leave_group(server, conn, correl, payload)
    local l, err = proto.decode_leave_group(payload)
    if not l then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    local group = server.coordinator:get(l.group_id)
    if not group or conn.group_id ~= l.group_id then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
            "not a member of this group"))
        return
    end

    group:leave(conn.member_id)
    log:info("conn=%s left group=%s member=%s state=%s",
        conn.id_short, l.group_id, conn.member_id, group:state())
    conn.group_id  = nil
    conn.member_id = nil
    conn:send(proto.encode_ok(correl))
end

-- GROUP_HEARTBEAT: renew the member's lease so the reaper doesn't evict it.
-- A heartbeat for a member the coordinator has already reaped (or never
-- saw) returns GROUP_MEMBER_UNKNOWN, signalling the client to re-JOIN_GROUP.
function M.group_heartbeat(server, conn, correl, payload)
    local h, err = proto.decode_group_heartbeat(payload)
    if not h then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    local group = server.coordinator:get(h.group_id)
    if not group or conn.group_id ~= h.group_id then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
            "not a member of this group"))
        return
    end

    local ok, herr = group:heartbeat(conn.member_id)
    if not ok then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
            herr or "unknown member"))
        return
    end
    conn:send(proto.encode_ok(correl))
end

function M.goodbye(_server, conn, _correl, _payload)
    conn:close(Connection.REASON_CLIENT_GOODBYE)
end

-- Opcode → handler. Server:dispatch resolves through this table; an opcode
-- absent here is answered with ERR_UNKNOWN_OP.
M.BY_OP = {
    [proto.OP_HELLO]              = M.hello,
    [proto.OP_AUTH]               = M.auth,
    [proto.OP_IDENTIFY_CLIENT]    = M.identify_client,
    [proto.OP_PRODUCE]            = M.produce,
    [proto.OP_INIT_PRODUCER_ID]   = M.init_producer_id,
    [proto.OP_PRODUCE_IDEMPOTENT] = M.produce_idempotent,
    [proto.OP_SUBSCRIBE]          = M.subscribe,
    [proto.OP_FETCH]              = M.fetch,
    [proto.OP_COMMIT]             = M.commit,
    [proto.OP_CREATE_TOPIC]       = M.create_topic,
    [proto.OP_LIST_TOPICS]        = M.list_topics,
    [proto.OP_JOIN_GROUP]         = M.join_group,
    [proto.OP_LEAVE_GROUP]        = M.leave_group,
    [proto.OP_GROUP_HEARTBEAT]    = M.group_heartbeat,
    [proto.OP_GOODBYE]            = M.goodbye,
}

return M
