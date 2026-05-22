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

local Reactor    = require("src.server.reactor")
local proto      = require("src.server.protocol")
local Connection = require("src.server.connection")
local uuid       = require("src.server.uuid")
local brk_m      = require("src.broker")
local prd_m      = require("src.producer")
local consumer_m = require("src.consumer")
local msg_m      = require("src.message")

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
local DEFAULT_HANDSHAKE_DEADLINE  = 10
local DEFAULT_HEARTBEAT_INTERVAL  = 30
local DEFAULT_HEARTBEAT_MISS      = 3
local DEFAULT_PUSH_INTERVAL       = 0.05

local Server = {}
Server.__index = Server

function Server.new(opts)
    opts = opts or {}
    assert(opts.data_dir, "opts.data_dir required")

    local broker, berr = brk_m.Broker.new(opts.data_dir)
    if not broker then return nil, berr end

    local producer = prd_m.Producer.new(broker, prd_m.AckMode.AckLeader)

    return setmetatable({
        broker   = broker,
        producer = producer,
        reactor  = Reactor.new(),
        host     = opts.host or "0.0.0.0",
        port     = opts.port or 9092,

        max_frame                = opts.max_frame                or DEFAULT_MAX_FRAME,
        max_pending_bytes        = opts.max_pending_bytes        or DEFAULT_MAX_PENDING_BYTES,
        send_deadline            = opts.send_deadline            or DEFAULT_SEND_DEADLINE,
        handshake_deadline       = opts.handshake_deadline       or DEFAULT_HANDSHAKE_DEADLINE,
        heartbeat_interval       = opts.heartbeat_interval       or DEFAULT_HEARTBEAT_INTERVAL,
        heartbeat_miss_threshold = opts.heartbeat_miss_threshold or DEFAULT_HEARTBEAT_MISS,
        push_interval            = opts.push_interval            or DEFAULT_PUSH_INTERVAL,

        max_connections        = opts.max_connections        or 1024,
        max_connections_per_ip = opts.max_connections_per_ip or 32,
        connections            = 0,
        conn_by_ip             = {},
        connections_by_id      = {},

        authenticator        = opts.authenticator,
        rate_limiter_factory = opts.rate_limiter_factory,
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

    local ok, reason = self:_register_conn(ip)
    if not ok then
        local f = proto.encode_error(uuid.ZERO, proto.ERR_RATE_LIMITED, reason)
        pcall(function() sock:send(f); sock:close() end)
        return
    end

    local conn = Connection.new(self, sock, peer, ip)
    self.connections_by_id[conn.id] = conn

    -- conn:start() runs the reader inline; an uncaught error here would
    -- otherwise leak the capacity slot (the connection is registered but
    -- _unregister_conn never runs). close() is idempotent and unregisters.
    local ok, err = pcall(conn.start, conn)
    if not ok then
        io.stderr:write(string.format("[server] conn=%s start failed: %s\n",
            conn.id_short, tostring(err)))
        conn:close(Connection.REASON_READ_ERROR)
    end
end

function Server:dispatch(conn, op, correl, payload)
    if     op == proto.OP_HELLO           then self:_handle_hello(conn, correl, payload)
    elseif op == proto.OP_AUTH            then self:_handle_auth(conn, correl, payload)
    elseif op == proto.OP_IDENTIFY_CLIENT then self:_handle_identify_client(conn, correl, payload)
    elseif op == proto.OP_PRODUCE         then self:_handle_produce(conn, correl, payload)
    elseif op == proto.OP_SUBSCRIBE       then self:_handle_subscribe(conn, correl, payload)
    elseif op == proto.OP_FETCH           then self:_handle_fetch(conn, correl, payload)
    elseif op == proto.OP_COMMIT          then self:_handle_commit(conn, correl, payload)
    elseif op == proto.OP_CREATE_TOPIC    then self:_handle_create_topic(conn, correl, payload)
    elseif op == proto.OP_LIST_TOPICS     then self:_handle_list_topics(conn, correl, payload)
    elseif op == proto.OP_GOODBYE         then conn:close(Connection.REASON_CLIENT_GOODBYE)
    else
        conn:send(proto.encode_error(correl, proto.ERR_UNKNOWN_OP,
            string.format("op 0x%02X", op)))
    end
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
    io.stderr:write(string.format("[server] conn=%s identified client=%s/%s\n",
        conn.id_short, conn.client_name, conn.client_version))
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
        io.stderr:write("[server] WARN: no authenticator configured, allowing\n")
        conn.authed   = true
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

    conn.authed   = true
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
    conn:send(proto.encode_ok(correl))

    if not conn.subscriber_co then
        conn.subscriber_co = self.reactor:spawn(function()
            self:_subscriber_loop(conn)
        end)
    end
end

function Server:_subscriber_loop(conn)
    while conn.state ~= Connection.STATE_CLOSED do
        local records, err = conn.consumer:poll()
        if err then
            io.stderr:write(string.format("[push] conn=%s poll: %s\n",
                conn.id_short, err))
            return
        end
        if records and #records > 0 then
            for i = 1, #records do
                if conn.state == Connection.STATE_CLOSED then return end
                local r = records[i]
                local frame = proto.encode_record(uuid.ZERO,
                    r.topic, r.partition, r.offset, r.timestamp, r.key, r.value)
                if not conn:send(frame) then return end
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
    local _, terr = self.broker:create_topic(c.name, c.num_partitions)
    if terr then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, terr))
        return
    end
    conn:send(proto.encode_ok(correl))
end

function Server:_handle_list_topics(conn, correl, _payload)
    conn:send(proto.encode_topic_list(correl, self.broker:list_topics()))
end

-- Install handlers for SIGINT, SIGTERM, and SIGTSTP (Ctrl+Z) so the
-- reactor stops on the next tick. The handler must do as little as
-- possible — just flip the flag. Real teardown happens after run()
-- returns. luaposix is optional: if it's missing we just skip signals
-- and the user can still SIGKILL.
function Server:_install_signal_handlers()
    local ok, signal = pcall(require, "posix.signal")
    if not ok then
        io.stderr:write("[server] luaposix missing, no signal handling\n")
        return
    end

    local function on_signal(signo)
        io.stderr:write(string.format(
            "[server] got signal %d, shutting down\n", signo))
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

    self:_install_signal_handlers()

    io.stderr:write(string.format(
        "[server] listening on %s:%d (proto v%d, %s/%s)\n",
        self.host, self.port, proto.PROTOCOL_VERSION,
        proto.SERVER_NAME, proto.SERVER_VERSION))
    self.reactor:run()

    -- Reactor returned (signal or :stop()). Close everything we own.
    io.stderr:write("[server] reactor stopped, closing sockets\n")
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

return Server