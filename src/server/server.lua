-- TCP front-end for the broker. Frame loop only; per-opcode handling
-- and auth are intentionally split out so we can iterate on them
-- independently

local socket = require("socket")
local Reactor = require("src.server.reactor")
local proto = require("src.server.protocol")
local brk_m = require("src.broker")
local msg_m = require("src.message")
local prd_m      = require("src.producer")
local consumer_m = require("src.consumer")

local DEFAULT_MAX_FRAME       = 16 * 1024 * 1024  -- 16 MiB
local DEFAULT_HANDSHAKE_DEADLINE = 30             -- seconds
local DEFAULT_IDLE_DEADLINE   = 300               -- seconds (5 min)
local DEFAULT_PUSH_INTERVAL = 0.05

local Server = {}
Server.__index = Server

function Server.new(opts)
    opts = opts or {}
    assert(opts.data_dir, "opts.data_dir required")

    local broker, berr = brk_m.Broker.new(opts.data_dir)
    if not broker then return nil, berr end

    local producer = prd_m.Producer.new(broker, prd_m.AckMode.AckLeader)

    return setmetatable({
        broker        = broker,
        producer      = producer,
        reactor       = Reactor.new(),
        host          = opts.host or "0.0.0.0",
        port          = opts.port or 9092,
        max_frame     = opts.max_frame or DEFAULT_MAX_FRAME,
        idle_deadline = opts.idle_deadline or DEFAULT_IDLE_DEADLINE,
        handshake_deadline = opts.handshake_deadline or DEFAULT_HANDSHAKE_DEADLINE,
        push_interval = opts.push_interval or DEFAULT_PUSH_INTERVAL,
 
        max_connections        = opts.max_connections or 1024,
        max_connections_per_ip = opts.max_connections_per_ip or 32,
        connections            = 0,
        conn_by_ip             = {},
 
        authenticator        = opts.authenticator,
        rate_limiter_factory = opts.rate_limiter_factory,
    }, Server)
end

-- Read one frame with a deadline. Returns (op, correl, payload, err).
local function read_frame(reactor, sock, max_frame, deadline)
    assert(getmetatable(reactor) == Reactor, "reactor must be a Reactor instance")

    local len_bytes, err = reactor:read_exact(sock, 4, deadline)
    if not len_bytes then return nil, nil, nil, err end

    local frame_len = string.unpack(">I4", len_bytes)
    if frame_len < 5 then
        return nil, nil, nil, "frame too small"
    end

    if frame_len > max_frame then
        return nil, nil, nil, "frame exceeds max"
    end

    local body, berr = reactor:read_exact(sock, frame_len, deadline)
    if not body then return nil, nil, nil, berr end

    local op, correl = string.unpack(">BI4", body)
    return op, correl, body:sub(6), nil
end

local function peer_ip(peer)
    return (peer:match("^(.+):%d+$")) or peer
end

function Server:_register_conn(ip)
    if self.connections >= self.max_connections then
        return false, "server at capacity"
    end
    local n = (self.conn_by_ip[ip] or 0)
    if n >= self.max_connections_per_ip then
        return false, "too many connections from this address"
    end
    self.connections = self.connections + 1
    self.conn_by_ip[ip] = n + 1
    return true
end

function Server:_unregister_conn(ip)
    if self.connections > 0 then
        self.connections = self.connections - 1
    end
    local n = (self.conn_by_ip[ip] or 1) - 1
    if n <= 0 then
        self.conn_by_ip[ip] = nil
    else
        self.conn_by_ip[ip] = n
    end
end

-- One coroutine per connection. Reads frames, dispatches, writes replies.
function Server:_handle(sock, peer)
    local ip = peer_ip(peer)

    local ok, reason = self:_register_conn(ip)
    if not ok then
        -- Send a single ERROR frame so well-behaved clients learn why.
        -- Best-effort: ignore send errors.
        local f = proto.encode_error(0, proto.ERR_RATE_LIMITED, reason)
        pcall(function() sock:send(f) end)
        sock:close()
        return
    end

    local conn = {
        sock          = sock,
        peer          = peer,
        ip            = ip,
        authed        = false,
        username      = nil,
        rate_limiter  = self.rate_limiter_factory and self.rate_limiter_factory() or nil,
        subscriptions = {},   -- topic_name -> true
    }

    local function send_frame(frame)
        return self.reactor:send_all(sock, frame)
    end
    conn.send = send_frame

    local handshake_deadline = socket.gettime() + self.handshake_deadline

    while true do
        local deadline
        if conn.authed then
            deadline = socket.gettime() + self.idle_deadline
        else
            deadline = handshake_deadline
        end

        local op, correl, payload, err = read_frame(
            self.reactor, sock, self.max_frame, deadline)

        if err then
            break  -- "closed", "deadline exceeded", "frame too large", etc.
        end

        local hok, herr = self:_dispatch(conn, op, correl, payload)
        if not hok then
            -- Handler signaled fatal protocol error; drop the connection.
            io.stderr:write(string.format(
                "[server] %s dropping: %s\n", peer, herr or "?"))
            break
        end
    end

    sock:close()
    self:_unregister_conn(ip)
end

-- Dispatch one frame. Returns (true, nil) to keep the connection alive,
-- or (false, reason) to terminate it. Per-op errors that the client can
-- recover from are sent as ERROR frames and return (true, nil).
function Server:_dispatch(conn, op, correl, payload)
    if not conn.authed and op ~= proto.OP_HELLO and op ~= proto.OP_AUTH then
        conn.send(proto.encode_error(correl, proto.ERR_NOT_AUTHED, "authenticate first"))
        return true
    end
 
    if op == proto.OP_HELLO         then return self:_handle_hello(conn, correl, payload)
    elseif op == proto.OP_AUTH         then return self:_handle_auth(conn, correl, payload)
    elseif op == proto.OP_PING         then conn.send(proto.encode_pong(correl)); return true
    elseif op == proto.OP_PRODUCE      then return self:_handle_produce(conn, correl, payload)
    elseif op == proto.OP_SUBSCRIBE    then return self:_handle_subscribe(conn, correl, payload)
    elseif op == proto.OP_FETCH        then return self:_handle_fetch(conn, correl, payload)
    elseif op == proto.OP_COMMIT       then return self:_handle_commit(conn, correl, payload)
    elseif op == proto.OP_CREATE_TOPIC then return self:_handle_create_topic(conn, correl, payload)
    elseif op == proto.OP_LIST_TOPICS  then return self:_handle_list_topics(conn, correl, payload)
    elseif op == proto.OP_GOODBYE      then return false, "client goodbye"
    else
        conn.send(proto.encode_error(correl, proto.ERR_UNKNOWN_OP,
            string.format("op 0x%02X", op)))
        return true
    end
end

function Server:_handle_hello(conn, correl, payload)
    local h, herr = proto.decode_hello(payload)
    if not h then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, herr))
        return true
    end
    if h.version ~= proto.PROTOCOL_VERSION then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            string.format("expected v%d got v%d",
                proto.PROTOCOL_VERSION, h.version)))
        return false, "protocol version"
    end
    conn.send(proto.encode_welcome(correl, proto.PROTOCOL_VERSION))
    return true
end
 
function Server:_handle_auth(conn, correl, payload)
    local a, aerr = proto.decode_auth(payload)
    if not a then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, aerr))
        return true
    end
 
    if not self.authenticator then
        io.stderr:write("[server] WARN: no authenticator, allowing\n")
        conn.authed   = true
        conn.username = a.username
        conn.send(proto.encode_auth_ok(correl))
        return true
    end
 
    local ok, auth_err = self.authenticator:verify(a.username, a.password, conn.ip)
    if not ok then
        -- Don't echo the supplied username in logs — it could be
        -- attacker-controlled and pollute logs / trigger log injection
        -- on downstream log processors.
        conn.send(proto.encode_error(correl, proto.ERR_AUTH_FAILED,
            auth_err or "auth failed"))
        return false, "auth failed"
    end
 
    conn.authed   = true
    conn.username = a.username
    conn.send(proto.encode_auth_ok(correl))
    return true
end


function Server:_handle_produce(conn, correl, payload)
    if conn.rate_limiter and not conn.rate_limiter:take(1) then
        conn.send(proto.encode_error(correl, proto.ERR_RATE_LIMITED, "produce rate exceeded"))
        return true
    end
 
    local p, perr = proto.decode_produce(payload)
    if not p then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, perr))
        return true
    end
 
    local msg = msg_m.Message.new(p.key, p.value, 0)
    local part_id, offset, werr = self.producer:produce(p.topic, msg)
    if werr then
        local code = werr:find("does not exist", 1, true)
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn.send(proto.encode_error(correl, code, werr))
        return true
    end
 
    conn.send(proto.encode_produce_ack(correl, part_id, offset))
    return true
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
    local s, serr = proto.decode_subscribe(payload)
    if not s then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, serr))
        return true
    end
    if conn.mode == "pull" then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "connection already in pull mode (used FETCH)"))
        return true
    end
    local consumer, cerr = ensure_consumer(conn, self.broker, s.group_id)
    if not consumer then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL, cerr))
        return true
    end
    local ok, err = consumer:subscribe(s.topic)
    if not ok then
        conn.send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING, err or "subscribe failed"))
        return true
    end
    conn.subscriptions[s.topic] = true
    conn.mode = "push"
    conn.send(proto.encode_ok(correl))
 
    if not conn.subscriber_co then
        conn.subscriber_co = self.reactor:spawn(function()
            self:_subscriber_loop(conn)
        end)
    end
    return true
end
 
function Server:_subscriber_loop(conn)
    while not conn.closed do
        local records, err = conn.consumer:poll()
        if err then
            io.stderr:write(string.format("[push] %s poll: %s\n", conn.peer, err))
            return
        end
        if records and #records > 0 then
            for i = 1, #records do
                if conn.closed then return end
                local r = records[i]
                local frame = proto.encode_record(0,
                    r.topic, r.partition, r.offset, r.timestamp, r.key, r.value)
                if not conn.send(frame) then return end
            end
        else
            self.reactor:sleep(self.push_interval)
        end
    end
end
 
function Server:_handle_fetch(conn, correl, payload)
    local f, ferr = proto.decode_fetch(payload)
    if not f then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, ferr))
        return true
    end
    if conn.mode == "push" then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "connection already in push mode (used SUBSCRIBE)"))
        return true
    end
    local consumer, cerr = ensure_consumer(conn, self.broker, f.group_id)
    if not consumer then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL, cerr))
        return true
    end
    if not conn.subscriptions[f.topic] then
        local sok, sber = consumer:subscribe(f.topic)
        if not sok then
            conn.send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
                sber or "subscribe failed"))
            return true
        end
        conn.subscriptions[f.topic] = true
    end
    conn.mode = "pull"
 
    local records, perr = consumer:poll()
    if perr then
        conn.send(proto.encode_error(correl, proto.ERR_INTERNAL, perr))
        return true
    end
    if records then
        local limit = math.min(#records, f.max_records or #records)
        for i = 1, limit do
            local r = records[i]
            local frame = proto.encode_record(correl,
                r.topic, r.partition, r.offset, r.timestamp, r.key, r.value)
            if not conn.send(frame) then return false, "send failed" end
        end
    end
    conn.send(proto.encode_ok(correl))
    return true
end
 
function Server:_handle_commit(conn, correl, payload)
    local c, cerr = proto.decode_commit(payload)
    if not c then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, cerr))
        return true
    end
    if not conn.consumer then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "commit requires prior subscribe/fetch"))
        return true
    end
    local ok, err = conn.consumer:commit_offset(c.topic, c.partition, c.offset)
    if not ok then
        conn.send(proto.encode_error(correl, proto.ERR_INTERNAL, err or "commit failed"))
        return true
    end
    conn.send(proto.encode_ok(correl))
    return true
end
 
function Server:_handle_create_topic(conn, correl, payload)
    local c, cerr = proto.decode_create_topic(payload)
    if not c then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, cerr))
        return true
    end
    if c.num_partitions < 1 or c.num_partitions > 1024 then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            "num_partitions out of range (1..1024)"))
        return true
    end
    local _, err = self.broker:create_topic(c.name, c.num_partitions)
    if err then
        conn.send(proto.encode_error(correl, proto.ERR_INTERNAL, err))
        return true
    end
    conn.send(proto.encode_ok(correl))
    return true
end
 
function Server:_handle_list_topics(conn, correl, _payload)
    conn.send(proto.encode_topic_list(correl, self.broker:list_topics()))
    return true
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
    local _, err = self.reactor:listen(self.host, self.port, function(sock, peer)
        self:_handle(sock, peer)
    end)
    if err then return nil, err end

    self:_install_signal_handlers()

    io.stderr:write(string.format("[server] listening on %s:%d\n",
        self.host, self.port))
    self.reactor:run()

    -- Reactor returned (signal or :stop()). Close everything we own.
    io.stderr:write("[server] reactor stopped, closing sockets\n")
    self.reactor:shutdown()
    return true
end

function Server:stop()
    self.reactor:stop()
end

return Server