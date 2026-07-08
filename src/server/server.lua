-- TCP front-end for the broker. Owns:
--   - the TCP listener
--   - capacity accounting (max_connections, max_connections_per_ip)
--   - ban enforcement at accept time
--   - opcode dispatch on authenticated connections
--   - background loops (group reaper, partition cleaner tick)
--
-- The pieces it delegates:
--   - per-connection lifecycle (reader/sender/heartbeat coroutines, send
--     queue, state machine, close-reason logging) → src/server/connection.lua
--   - application-level command handlers, one per opcode → src/server/handlers.lua
--   - consumer-group registry/assignment/reaping → src/server/group_coordinator.lua
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

local Reactor          = require("src.server.reactor")
local proto            = require("src.wire.protocol")
local Connection       = require("src.server.connection")
local handlers         = require("src.server.handlers")
local GroupCoordinator = require("src.server.group_coordinator")
local uuid             = require("src.core.uuid")
local brk_m            = require("src.broker")
local prd_m            = require("src.broker.producer")
local metrics          = require("src.metrics")
local MetricsHttp      = require("src.server.metrics_http")
local version_m        = require("src.core.version")
local log              = require("src.log.logger").get("server")

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
-- deadline is 30s (src/broker/groups.lua); polling every 10s detects a
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
        broker      = broker,
        producer    = producer,
        reactor     = Reactor.new(),
        coordinator = GroupCoordinator.new(broker, {
            max_groups = opts.max_groups or DEFAULT_MAX_GROUPS,
        }),
        host        = opts.host or "0.0.0.0",
        port        = opts.port or 9092,

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

        group_reaper_interval = opts.group_reaper_interval
                                or DEFAULT_GROUP_REAPER_INTERVAL_S,
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

    -- Implicit LEAVE_GROUP on socket drop. Guarded by connections_by_id
    -- above so it runs exactly once.
    self.coordinator:handle_disconnect(conn)

    if self.connections > 0 then
        self.connections = self.connections - 1
    end
    local n = (self.conn_by_ip[conn.ip] or 1) - 1
    if n <= 0 then
        self.conn_by_ip[conn.ip] = nil
    else
        self.conn_by_ip[conn.ip] = n
    end

    -- Re-emit the live gauge on close. It was only ever set on accept, so
    -- without this it reports cumulative accepts and never reflects closes —
    -- climbing monotonically and reading as "at capacity" when it isn't.
    metrics.set("moonmq_connections_open", self.connections)
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

    -- Connection.new must be inside a pcall too: _register_conn already claimed
    -- a capacity slot (and a per-IP slot), and if construction throws before the
    -- connection is tracked in connections_by_id, _unregister_conn's guard makes
    -- it a no-op — permanently leaking those slots. On failure, release them.
    local made_ok, conn = pcall(Connection.new, self, sock, peer, ip)
    if not made_ok then
        log:error("Connection.new failed for %s: %s", ip, tostring(conn))
        if self.connections > 0 then self.connections = self.connections - 1 end
        local n = (self.conn_by_ip[ip] or 1) - 1
        self.conn_by_ip[ip] = n > 0 and n or nil
        pcall(function() sock:close() end)
        return
    end
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

    local handler = handlers.BY_OP[op]
    if handler then
        handler(self, conn, correl, payload)
    else
        conn:send(proto.encode_error(correl, proto.ERR_UNKNOWN_OP,
            string.format("op 0x%02X", op)))
    end

    stop()
end

-- Periodically age out group members that stopped heartbeating. Runs for
-- the lifetime of the reactor; each pass evicts stale members, rebalances
-- survivors, and drops emptied groups (see GroupCoordinator:reap).
function Server:_run_group_reaper()
    while self.running do
        self.reactor:sleep(self.group_reaper_interval)
        if not self.running then return end
        self.coordinator:reap()
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
            name     = proto.SERVER_NAME,
            version  = proto.SERVER_VERSION,
            protocol = proto.PROTOCOL_VERSION,
            host     = self.host,
            port     = self.port,
        },
        connections = {
            open       = self.connections,
            max        = self.max_connections,
            max_per_ip = self.max_connections_per_ip,
        },
        topics = {
            count            = #self.broker:list_topics(),
            max              = self.max_topics,
            listed           = #topic_summaries,
            listed_truncated = truncated,
            top_by_bytes     = top_n,
        },
    }
end

return Server
