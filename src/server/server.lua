-- TCP front-end for the broker. Owns:
--   - the TCP listener
--   - capacity accounting (max_connections, max_connections_per_ip)
--   - ban enforcement at accept time
--   - opcode dispatch on authenticated connections
--
-- Per-connection lifecycle (reader/sender/heartbeat coroutines, send
-- queue, state machine, close-reason logging) is delegated to
-- src/server/connection.lua so this file stays focused on the
-- application-level command handlers.
--
-- Patterns lifted from KurrentDB's TCP transport:
--   - 16-byte UUID connection IDs (logged on accept & on close)
--   - 16-byte UUID correlation IDs in every frame
--   - Heartbeat liveness probes with miss-count drop
--   - IDENTIFY_CLIENT / IDENTIFY_ACK handshake for app name+version
--   - Typed close reasons in every close log line
--   - Handshake watchdog: AUTH must complete within a deadline
--   - Layered framing (framer.lua) decoupled from opcode dispatch
--   - Pending-send cap reinterpreted as a per-write deadline
--   - Pre-auth opcode whitelist (only HELLO/AUTH/IDENTIFY/HEARTBEAT allowed)

local socket      = require("socket")
local Reactor     = require("src.server.reactor")
local proto       = require("src.server.protocol")
local Connection  = require("src.server.connection")
local uuid        = require("src.server.uuid")
local brk_m       = require("src.storage.broker")
local prd_m       = require("src.client.producer")
local consumer_m  = require("src.client.consumer")
local groups_m    = require("src.client.groups")
local msg_m       = require("src.record.message")
local metrics     = require("src.server.metrics")
local MetricsHttp = require("src.server.metrics_http")
local version_m   = require("src.core.version")
local log         = require("src.log.logger").get("server")
local push_log    = require("src.log.logger").get("push")

-- Tightened from a hypothetical 16 MiB to 1 MiB. A 1 MiB cap is still
-- generous for a message queue and bounds worst-case memory under
-- slowloris-style partial-frame attacks.
local DEFAULT_MAX_FRAME           = 1 * 1024 * 1024
-- Max bytes queued for transmission per connection. Past this, the
-- connection is closed with pending_send_too_large.
local DEFAULT_MAX_PENDING_BYTES   = 16 * 1024 * 1024
-- Time budget for a single socket write. Lower → more aggressive
-- slow-consumer detection at the cost of dropping legitimately slow
-- networks.
local DEFAULT_SEND_DEADLINE       = 30
-- Tightened from 10 -> 5s. Legitimate clients complete HELLO + AUTH in
-- well under a second on a working network; 5s is generous enough to
-- absorb a TCP slow start but short enough that a banned/attacker IP
-- doesn't tie up a connection slot for long. See run_handshake_watchdog.
local DEFAULT_HANDSHAKE_DEADLINE  = 5
-- Per-frame deadline for the reader. Splits into pre-auth (short, to
-- bound slowloris-style partial-frame attacks at the front door) and
-- steady-state (after AUTHENTICATED — longer, since legitimate
-- consumers may sit between FETCHes).
local DEFAULT_PRE_AUTH_READ_DEADLINE = 5
local DEFAULT_IDLE_DEADLINE          = 60
local DEFAULT_HEARTBEAT_INTERVAL  = 30
local DEFAULT_HEARTBEAT_MISS      = 3
local DEFAULT_PUSH_INTERVAL       = 0.05
-- Cap on the number of topics the broker will accept. Without this a
-- client (authenticated or not, depending on config) could call
-- CREATE_TOPIC in a loop and exhaust both in-memory state and the
-- LIST_TOPICS response budget.
local DEFAULT_MAX_TOPICS = 1024
-- Bound on the wire size of a LIST_TOPICS response. Independently of
-- max_topics, the response includes 4 bytes per topic plus the names,
-- and we cap topics' names at 249 bytes, so worst-case
-- 1024 * (4 + 249) ≈ 256 KiB. Truncate well before that.
local DEFAULT_MAX_LIST_TOPICS = 1024
-- Group-commit defaults. Linger is the max time a single acks=1 producer
-- waits for siblings to join the batch; max_waiters caps batch size so
-- a busy partition doesn't accumulate unbounded RAM. 2 ms is short
-- enough to be invisible end-to-end yet long enough to collect a useful
-- batch under any meaningful load — Kafka's default linger is 0 (per-
-- request fsync) but with the segregated IO threads we don't yet have.
local DEFAULT_GROUP_COMMIT_LINGER_S    = 0.002
local DEFAULT_GROUP_COMMIT_MAX_WAITERS = 64
-- How often the coordinator ticks ConsumerGroup:check_heartbeats to evict
-- members that stopped sending GROUP_HEARTBEAT. The group's own session
-- deadline is 30s (src/client/groups.lua); polling every 10s detects a
-- dead member within ~10s of the deadline, which is plenty responsive for
-- a rebalance without spinning the reactor.
local DEFAULT_GROUP_REAPER_INTERVAL_S  = 10
-- Ceiling on distinct live consumer groups (see max_groups).
local DEFAULT_MAX_GROUPS = 1024
-- How often the broker pumps partition cleaners. tick_cleaner is a cheap
-- no-op until a partition's cleaner is actually due, so a tight-ish interval
-- just keeps retention responsive without meaningful cost.
local DEFAULT_CLEANER_TICK_INTERVAL_S = 5

local Server = {}
Server.__index = Server

function Server.new(opts)
    opts = opts or {}
    assert(opts.data_dir, "opts.data_dir required")

    local broker, berr = brk_m.Broker.new(opts.data_dir, {
        default_backend = opts.default_backend,
    })
    if not broker then return nil, berr end

    -- Seed the topic_count gauge from what we just loaded so the gauge
    -- is meaningful immediately after restart, before any CREATE_TOPIC
    -- runs to bump it.
    metrics.set("moonmq_topic_count", #broker:list_topics())

    -- Register HELP/TYPE for the metric families and expose a build-info gauge
    -- (Prometheus convention: a constant 1 carrying version/commit as labels,
    -- so dashboards can join runtime series against the build that produced
    -- them). Describing families gives scrapers proper # HELP/# TYPE headers.
    local vinfo = version_m.GetVersionInfo()
    metrics.describe("moonmq_build_info", "gauge", "Build metadata; value is always 1.")
    metrics.set("moonmq_build_info", 1,
        { version = vinfo.version, commit = vinfo.git_commit })
    metrics.describe("moonmq_topic_count", "gauge", "Number of user topics.")
    metrics.describe("moonmq_connections_open", "gauge", "Currently open connections.")
    metrics.describe("moonmq_produce_records_total", "counter", "Records produced.")
    metrics.describe("moonmq_fetch_records_total", "counter", "Records delivered to consumers.")

    local producer = prd_m.Producer.new(broker, prd_m.AckMode.AckLeader)

    return setmetatable({
        broker   = broker,
        producer = producer,
        reactor  = Reactor.new(),
        host     = opts.host or "0.0.0.0",
        port     = opts.port or 9092,

        -- Idempotent producer state. next_pid is a monotonic counter
        -- assigned by INIT_PRODUCER_ID; the value lives only for this
        -- broker process (no persistence — see docs/transactions.md
        -- for the full-coordinator design that would make PIDs durable).
        -- Starts at 1 so 0 can be reserved as "unassigned" in client code.
        next_pid = 1,

        max_frame                = opts.max_frame                or DEFAULT_MAX_FRAME,
        max_pending_bytes        = opts.max_pending_bytes        or DEFAULT_MAX_PENDING_BYTES,
        send_deadline            = opts.send_deadline            or DEFAULT_SEND_DEADLINE,
        handshake_deadline       = opts.handshake_deadline       or DEFAULT_HANDSHAKE_DEADLINE,
        pre_auth_read_deadline   = opts.pre_auth_read_deadline   or DEFAULT_PRE_AUTH_READ_DEADLINE,
        idle_deadline            = opts.idle_deadline            or DEFAULT_IDLE_DEADLINE,
        heartbeat_interval       = opts.heartbeat_interval       or DEFAULT_HEARTBEAT_INTERVAL,
        heartbeat_miss_threshold = opts.heartbeat_miss_threshold or DEFAULT_HEARTBEAT_MISS,
        push_interval            = opts.push_interval            or DEFAULT_PUSH_INTERVAL,
        max_topics               = opts.max_topics               or DEFAULT_MAX_TOPICS,
        max_list_topics          = opts.max_list_topics          or DEFAULT_MAX_LIST_TOPICS,

        max_connections        = opts.max_connections        or 1024,
        max_connections_per_ip = opts.max_connections_per_ip or 32,
        connections            = 0,
        conn_by_ip             = {},
        connections_by_id      = {},

        -- Metrics endpoints
        metrics_host = opts.metrics_host or "127.0.0.1",
        metrics_port = opts.metrics_port,  -- nil = off

        authenticator        = opts.authenticator,
        rate_limiter_factory = opts.rate_limiter_factory,

        group_commit_linger_s    = opts.group_commit_linger_s
                                   or DEFAULT_GROUP_COMMIT_LINGER_S,
        group_commit_max_waiters = opts.group_commit_max_waiters
                                   or DEFAULT_GROUP_COMMIT_MAX_WAITERS,

        -- Consumer-group coordinators, keyed by group_id. Shared across
        -- connections (each connection is one member), created lazily on
        -- the first JOIN_GROUP for a group. The reaper loop in :start()
        -- ages out members that stop heartbeating.
        groups                = {},
        group_reaper_interval = opts.group_reaper_interval
                                or DEFAULT_GROUP_REAPER_INTERVAL_S,
        -- Cap on distinct consumer groups, mirroring max_topics. Without it an
        -- authenticated client could JOIN_GROUP with unbounded distinct group
        -- ids and grow self.groups without limit. The reaper also drops emptied
        -- groups so this is a ceiling on *live* groups, not a lifetime total.
        max_groups            = opts.max_groups or DEFAULT_MAX_GROUPS,
        -- How often to pump partition retention/compaction cleaners. The
        -- SegmentedPartition cleaner only advances when tick_cleaner is called,
        -- so without this loop time-based retention never runs and disk grows
        -- unbounded on the default backend.
        cleaner_tick_interval = opts.cleaner_tick_interval
                                or DEFAULT_CLEANER_TICK_INTERVAL_S,
        running               = false,
    }, Server)
end

function Server:_register_conn(ip)
    if self.connections >= self.max_connections then
        return false, "server at capacity"
    end
    local n = self.conn_by_ip[ip] or 0
    if n >= self.max_connections_per_ip then
        return false, "too many connections from this address"
    end
    self.connections = self.connections + 1
    self.conn_by_ip[ip] = n + 1
    return true
end

function Server:_unregister_conn(conn)
    -- close() is idempotent; bail if already unregistered.
    if self.connections_by_id[conn.id] == nil then return end
    self.connections_by_id[conn.id] = nil

    -- A member's connection dropping is an implicit LEAVE_GROUP — depart
    -- now and rebalance survivors rather than waiting out the heartbeat
    -- deadline. Guarded by connections_by_id above so it runs exactly once.
    if conn.group_id then
        local group = self.groups[conn.group_id]
        if group then group:leave(conn.member_id) end
        conn.group_id = nil
        conn.member_id = nil
    end

    if self.connections > 0 then
        self.connections = self.connections - 1
    end
    local n = (self.conn_by_ip[conn.ip] or 1) - 1
    if n <= 0 then
        self.conn_by_ip[conn.ip] = nil
    else
        self.conn_by_ip[conn.ip] = n
    end
end

function Server:_handle(sock, peer, ip)
    -- Banned IPs are slammed shut BEFORE registering the connection, so
    -- a banned attacker can't tie up max_connections_per_ip slots for
    -- the duration of the handshake deadline.
    if self.authenticator and self.authenticator.is_banned then
        local banned, remaining = self.authenticator:is_banned(ip)
        if banned then
            local f = proto.encode_error(uuid.ZERO, proto.ERR_AUTH_FAILED,
                string.format("banned for %ds", remaining))
            pcall(function() sock:send(f); sock:close() end)
            return
        end
    end

    local reg_ok, reason = self:_register_conn(ip)
    if not reg_ok then
        local f = proto.encode_error(uuid.ZERO, proto.ERR_RATE_LIMITED, reason)
        pcall(function() sock:send(f); sock:close() end)
        return
    end

    local conn = Connection.new(self, sock, peer, ip)
    self.connections_by_id[conn.id] = conn

    metrics.inc("moonmq_connections_accepted_total")
    metrics.set("moonmq_connections_open", self.connections)

    -- conn:start() runs the reader inline; an uncaught error here would
    -- otherwise leak the capacity slot (the connection is registered but
    -- _unregister_conn never runs). close() is idempotent and unregisters.
    local start_ok, start_err = pcall(conn.start, conn)
    if not start_ok then
        log:error("conn=%s start failed: %s",
            conn.id_short, tostring(start_err))
        conn:close(Connection.REASON_READ_ERROR)
    end
end

function Server:dispatch(conn, op, correl, payload)
    local stop = metrics.timer(
        "moonmq_dispatch_duration_seconds", 
        { op = string.format("0x%02x", op) })

    if     op == proto.OP_HELLO              then self:_handle_hello(conn, correl, payload)
    elseif op == proto.OP_AUTH               then self:_handle_auth(conn, correl, payload)
    elseif op == proto.OP_IDENTIFY_CLIENT    then self:_handle_identify_client(conn, correl, payload)
    elseif op == proto.OP_PRODUCE            then self:_handle_produce(conn, correl, payload)
    elseif op == proto.OP_INIT_PRODUCER_ID   then self:_handle_init_producer_id(conn, correl, payload)
    elseif op == proto.OP_PRODUCE_IDEMPOTENT then self:_handle_produce_idempotent(conn, correl, payload)
    elseif op == proto.OP_SUBSCRIBE          then self:_handle_subscribe(conn, correl, payload)
    elseif op == proto.OP_FETCH              then self:_handle_fetch(conn, correl, payload)
    elseif op == proto.OP_COMMIT             then self:_handle_commit(conn, correl, payload)
    elseif op == proto.OP_CREATE_TOPIC       then self:_handle_create_topic(conn, correl, payload)
    elseif op == proto.OP_LIST_TOPICS        then self:_handle_list_topics(conn, correl, payload)
    elseif op == proto.OP_JOIN_GROUP         then self:_handle_join_group(conn, correl, payload)
    elseif op == proto.OP_LEAVE_GROUP        then self:_handle_leave_group(conn, correl, payload)
    elseif op == proto.OP_GROUP_HEARTBEAT    then self:_handle_group_heartbeat(conn, correl, payload)
    elseif op == proto.OP_GOODBYE            then conn:close(Connection.REASON_CLIENT_GOODBYE)
    else
        conn:send(proto.encode_error(correl, proto.ERR_UNKNOWN_OP,
            string.format("op 0x%02X", op)))
    end

    stop()
end

function Server:_handle_hello(conn, correl, payload)
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

function Server:_handle_identify_client(conn, correl, payload)
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

function Server:_handle_auth(conn, correl, payload)
    local a, err = proto.decode_auth(payload)
    if not a then
        conn:close(Connection.REASON_BAD_FRAME, proto.ERR_BAD_FRAME, err)
        return
    end

    if not self.authenticator then
        log:warn("no authenticator configured, allowing")
        conn.username = a.username
        conn:transition_to(Connection.STATE_AUTHENTICATED)
        conn:send(proto.encode_auth_ok(correl))
        return
    end

    local ok, auth_err = self.authenticator:verify(a.username, a.password, conn.ip)
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

function Server:_handle_produce(conn, correl, payload)
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
    local part_id, offset, werr = self.producer:produce(p.topic, msg)
    if werr then
        local code = werr:find("does not exist", 1, true)
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn:send(proto.encode_error(correl, code, werr))
        return
    end

    metrics.inc("moonmq_produce_records_total", 1, { topic = p.topic })

    conn:send(proto.encode_produce_ack(correl, part_id, offset))
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

-- INIT_PRODUCER_ID: assign a u64 producer ID to this connection. Repeated
-- calls on the same connection are tolerated but reset all per-PID seq
-- state (a client that asks for a fresh PID has lost track of its
-- sequences). PIDs are not durable.
function Server:_handle_init_producer_id(conn, correl, _payload)
    local pid = self.next_pid
    self.next_pid = self.next_pid + 1
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
function Server:_handle_produce_idempotent(conn, correl, payload)
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
    local partition_id, offset, werr = self.producer:produce(p.topic, msg)
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

function Server:_handle_subscribe(conn, correl, payload)
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
    local consumer, cerr = ensure_consumer(conn, self.broker, s.group_id)
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
    -- _subscriber_loop), not inside poll(). Turn off poll()'s auto-commit so a
    -- record isn't marked consumed before we've even tried to deliver it.
    consumer.auto_commit = false
    conn:send(proto.encode_ok(correl))

    if not conn.subscriber_co then
        conn.subscriber_co = self.reactor:spawn(function()
            self:_subscriber_loop(conn)
        end)
    end
end

function Server:_subscriber_loop(conn)
    while conn.state ~= Connection.STATE_CLOSED do
        -- Re-scope to the current assignment each pass so a rebalance (another
        -- member joining/leaving) takes effect on the next poll.
        self:_apply_group_assignment(conn)
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
            self.reactor:sleep(self.push_interval)
        end
    end
end

function Server:_handle_fetch(conn, correl, payload)
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
    local consumer, cerr = ensure_consumer(conn, self.broker, f.group_id)
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
    self:_apply_group_assignment(conn)

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

function Server:_handle_commit(conn, correl, payload)
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
    local topic, terr = self.broker:get_topic(c.topic)
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
        local group = self.groups[conn.group_id]
        if not group or not group.members[conn.member_id] then
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

function Server:_handle_create_topic(conn, correl, payload)
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
    local current = #self.broker:list_topics()
    if current >= self.max_topics then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            string.format("topic limit reached (%d)", self.max_topics)))
        return
    end
    local _, terr = self.broker:create_topic(c.name, c.num_partitions)
    if terr then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, terr))
        return
    end
    metrics.set("moonmq_topic_count", current + 1)
    conn:send(proto.encode_ok(correl))
end

function Server:_handle_list_topics(conn, correl, _payload)
    -- Bound response size. Sorting by name first so the truncation is
    -- deterministic across calls (rather than depending on table-pair
    -- iteration order). For the common case the bound is irrelevant —
    -- it only kicks in if max_topics has been raised past max_list_topics.
    local all = self.broker:list_topics()
    table.sort(all)
    if #all > self.max_list_topics then
        local truncated = {}
        for i = 1, self.max_list_topics do truncated[i] = all[i] end
        all = truncated
    end
    conn:send(proto.encode_topic_list(correl, all))
end

-- Lazily fetch (or create) the coordinator for a group. Shared across all
-- connections whose members belong to the group. Returns (group, nil) or
-- (nil, err) when creating a new group would exceed max_groups.
function Server:_get_or_create_group(group_id)
    local group = self.groups[group_id]
    if not group then
        -- Count live groups; the reaper drops emptied ones so this is a
        -- ceiling on concurrent groups, not a lifetime total.
        local n = 0
        for _ in pairs(self.groups) do n = n + 1 end
        if n >= self.max_groups then
            return nil, string.format("group limit reached (%d)", self.max_groups)
        end
        group = groups_m.ConsumerGroup.new(self.broker, group_id)
        self.groups[group_id] = group
    end
    return group
end

-- Sync the connection's Consumer to the partitions the coordinator has
-- assigned this member, so poll()/FETCH only touch owned partitions. This is
-- what makes a consumer group actually divide work — the coordinator computed
-- assignments before, but the read path ignored them and every member read
-- every partition. Called before each poll and right after JOIN_GROUP.
--
-- No-op unless the connection has joined a group AND has a Consumer. A member
-- the coordinator has since evicted (present group, absent member) is pinned to
-- "own nothing" so a zombie can't keep draining partitions until it notices its
-- heartbeat was rejected.
function Server:_apply_group_assignment(conn)
    if not (conn.group_id and conn.member_id and conn.consumer) then return end
    local group = self.groups[conn.group_id]
    if not group then return end
    local member = group.members[conn.member_id]
    if member and member.partitions then
        conn.consumer:set_assignment(member.partitions)
    else
        conn.consumer:set_assignment({})  -- evicted: own nothing
    end
end

-- JOIN_GROUP: register this connection as a member of a group subscribing
-- to one or more topics, and reply with the partitions assigned to it.
-- An empty member_id means "first join, assign me one" — we use the
-- connection's short id so it's stable for this connection's lifetime.
function Server:_handle_join_group(conn, correl, payload)
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

    local group, gerr = self:_get_or_create_group(j.group_id)
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
    self:_apply_group_assignment(conn)
    log:info("conn=%s joined group=%s member=%s state=%s",
        conn.id_short, j.group_id, member_id, group:state())
    conn:send(proto.encode_group_assignment(correl, member_id, assignment))
end

-- LEAVE_GROUP: voluntary departure. Survivors rebalance; an emptied group
-- collapses back to its empty state (handled inside ConsumerGroup:leave).
function Server:_handle_leave_group(conn, correl, payload)
    local l, err = proto.decode_leave_group(payload)
    if not l then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    local group = self.groups[l.group_id]
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
function Server:_handle_group_heartbeat(conn, correl, payload)
    local h, err = proto.decode_group_heartbeat(payload)
    if not h then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    local group = self.groups[h.group_id]
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

-- Periodically age out group members that stopped heartbeating. Runs for
-- the lifetime of the reactor; ConsumerGroup:check_heartbeats evicts stale
-- members and rebalances survivors (or collapses an emptied group).
function Server:_run_group_reaper()
    while self.running do
        self.reactor:sleep(self.group_reaper_interval)
        if not self.running then return end
        for gid, group in pairs(self.groups) do
            group:check_heartbeats()
            -- Drop coordinators with no members so an idle broker doesn't
            -- accumulate empty groups forever (and so max_groups counts only
            -- live groups). Safe to delete the current key during pairs().
            -- A stale connection still holding this group_id will get
            -- GROUP_MEMBER_UNKNOWN on its next heartbeat/commit and rejoin.
            if next(group.members) == nil then
                self.groups[gid] = nil
            end
        end
    end
end

-- Periodically pump every partition's retention/compaction cleaner. Without
-- this the SegmentedPartition cleaner (a manually-driven coroutine) never
-- advances, so time-based retention silently never runs and disk grows
-- unbounded on the default backend.
function Server:_run_cleaner_tick()
    while self.running do
        self.reactor:sleep(self.cleaner_tick_interval)
        if not self.running then return end
        local ok, ran = pcall(self.broker.tick_cleaners, self.broker)
        if not ok then
            log:error("cleaner tick: %s", tostring(ran))
        end
    end
end

-- Install handlers for SIGINT, SIGTERM, and SIGTSTP (Ctrl+Z) so the
-- reactor stops on the next tick. The handler must do as little as
-- possible — just flip the flag. Real teardown happens after run()
-- returns. luaposix is optional: if it's missing we just skip signals
-- and the user can still SIGKILL.
function Server:_install_signal_handlers()
    local ok, signal = pcall(require, "posix.signal")
    if not ok then
        log:warn("luaposix missing, no signal handling")
        return
    end

    local function on_signal(signo)
        log:info("got signal %d, shutting down", signo)
        self.reactor:stop()
    end

    signal.signal(signal.SIGINT,  on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    -- Ctrl+Z normally suspends; trap it for a clean exit per request.
    signal.signal(signal.SIGTSTP, on_signal)
end

function Server:start()
    local _, err = self.reactor:listen(self.host, self.port,
        function(sock, peer, ip) self:_handle(sock, peer, ip) end)
    if err then return nil, err end

    -- Wire group commit. The factory closure is invoked on every
    -- existing partition right now, and on every partition created by
    -- future CREATE_TOPIC ops. Without this, request_sync falls back to
    -- per-call fsync (same behaviour as before #8).
    local linger      = self.group_commit_linger_s
    local max_waiters = self.group_commit_max_waiters
    local reactor     = self.reactor
    self.broker:attach_committer_factory(function(p)
        p:attach_committer(reactor, {
            linger_s    = linger,
            max_waiters = max_waiters,
        })
    end)
    log:info("group commit: linger=%.3fs max_waiters=%d",
        linger, max_waiters)

    self:_install_signal_handlers()

    -- Start the consumer-group reaper. running gates its loop so it exits
    -- cleanly when the reactor stops (set false after :run() returns).
    self.running = true
    self.reactor:spawn(function() self:_run_group_reaper() end)
    self.reactor:spawn(function() self:_run_cleaner_tick() end)

    log:info("listening on %s:%d (proto v%d, %s/%s)",
        self.host, self.port, proto.PROTOCOL_VERSION,
        proto.SERVER_NAME, proto.SERVER_VERSION)

    if self.metrics_port then
        local mh = MetricsHttp.new({
            reactor = self.reactor,
            host    = self.metrics_host,
            port    = self.metrics_port,
            server  = self,  -- powers /stats snapshot
        })
        mh:start()
    end

    self.reactor:run()
    self.running = false

    -- Reactor returned (signal or :stop()). Drain any pending group-
    -- commit waiters before closing sockets so producers parked on
    -- request_sync don't sit on an undelivered ACK forever. The data
    -- they wrote is durable either way (their bytes hit the page cache
    -- before they parked, and partition close fsyncs again), but
    -- draining gives clients a chance to receive the success ACK if
    -- they're still connected.
    self.broker:detach_committers()

    log:info("reactor stopped, closing sockets")
    self.reactor:shutdown()

    return true
end

function Server:stop()
    for _, conn in pairs(self.connections_by_id) do
        conn:close(Connection.REASON_SERVER_SHUTDOWN,
            proto.ERR_INTERNAL, "server shutting down")
    end
    self.reactor:stop()
end

-- snapshot returns a Lua table summarising broker state for the JSON
-- /stats endpoint. Deliberately bounded — full topic enumeration is
-- truncated past `max_list_topics` (the same cap the wire endpoint uses)
-- and partitions surface counts rather than per-partition byte arrays.
-- A separate `top_topics_by_bytes` list gives operators visibility into
-- where data lives without exploding the response under high topic counts.
function Server:snapshot()
    local topics = self.broker:list_topics()
    table.sort(topics)
    local truncated = false
    if #topics > self.max_list_topics then
        local keep = {}
        for i = 1, self.max_list_topics do keep[i] = topics[i] end
        topics = keep
        truncated = true
    end

    local topic_summaries = {}
    local with_bytes = {}
    for _, name in ipairs(topics) do
        local t = self.broker.topic_manager.topics[name]
        if t then
            local parts = t.partitions or {}
            local total_bytes = 0
            local total_segments = 0
            for _, p in ipairs(parts) do
                total_bytes = total_bytes + (p.offset or 0)
                total_segments = total_segments + (p.segments and #p.segments or 0)
            end
            topic_summaries[#topic_summaries + 1] = {
                name           = name,
                partitions     = #parts,
                bytes_on_disk  = total_bytes,
                segment_count  = total_segments,
            }
            with_bytes[#with_bytes + 1] = topic_summaries[#topic_summaries]
        end
    end

    table.sort(with_bytes, function(a, b)
        return a.bytes_on_disk > b.bytes_on_disk
    end)
    local top_n = {}
    for i = 1, math.min(10, #with_bytes) do top_n[i] = with_bytes[i] end

    return {
        server = {
            name           = proto.SERVER_NAME,
            version        = proto.SERVER_VERSION,
            protocol      = proto.PROTOCOL_VERSION,
            host                   = self.host,
            port                   = self.port,
        },
        connections = {
            open                  = self.connections,
            max                   = self.max_connections,
            max_per_ip            = self.max_connections_per_ip,
        },
        topics = {
            count                 = #self.broker:list_topics(),
            max                            = self.max_topics,
            listed                = #topic_summaries,
            listed_truncated      = truncated,
            top_by_bytes            = top_n,
        },
    }
end

return Server