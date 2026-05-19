-- TCP front-end for the broker. Frame loop only; per-opcode handling
-- and auth are intentionally split out so we can iterate on them
-- independently

local socket = require("socket")
local Reactor = require("src.server.reactor")
local proto = require("src.server.protocol")
local brk_m = require("src.broker")
local msg_m = require("src.message")

local DEFAULT_MAX_FRAME       = 16 * 1024 * 1024  -- 16 MiB
local DEFAULT_HANDSHAKE_DEADLINE = 30             -- seconds
local DEFAULT_IDLE_DEADLINE   = 300               -- seconds (5 min)

local Server = {}
Server.__index = Server

function Server.new(opts)
    opts = opts or {}
    assert(opts.data_dir, "opts.data_dir required")

    local broker, berr = brk_m.Broker.new(opts.data_dir)
    if not broker then return nil, berr end

    return setmetatable({
        broker        = broker,
        reactor       = Reactor.new(),
        host          = opts.host or "0.0.0.0",
        port          = opts.port or 9092,
        max_frame     = opts.max_frame or DEFAULT_MAX_FRAME,
        idle_deadline = opts.idle_deadline or DEFAULT_IDLE_DEADLINE,
        handshake_deadline = opts.handshake_deadline or DEFAULT_HANDSHAKE_DEADLINE,

        -- DDoS knobs
        max_connections        = opts.max_connections or 1024,
        max_connections_per_ip = opts.max_connections_per_ip or 32,
        connections            = 0,
        conn_by_ip             = {},  -- ip -> count

        -- Pluggables (fill in next)
        authenticator = opts.authenticator,  -- function(user, pass) -> ok, err
        rate_limiter_factory = opts.rate_limiter_factory, -- function() -> bucket
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
    self.connections = self.connections - 1
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
    -- HELLO and AUTH are the only ops allowed before auth completes.
    if not conn.authed and op ~= proto.OP_HELLO and op ~= proto.OP_AUTH then
        conn.send(proto.encode_error(correl, proto.ERR_NOT_AUTHED,
            "authenticate first"))
        return true, nil
    end

    if op == proto.OP_HELLO then
        local h, herr = proto.decode_hello(payload)
        if not h then
            conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, herr))
            return true
        end
        if h.version ~= proto.PROTOCOL_VERSION then
            conn.send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
                "version mismatch"))
            return false, "protocol version"
        end
        conn.send(proto.encode_welcome(correl, proto.PROTOCOL_VERSION))
        return true

    elseif op == proto.OP_AUTH then
        local a, aerr = proto.decode_auth(payload)
        if not a then
            conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, aerr))
            return true
        end
        if not self.authenticator then
            -- No authenticator configured = open mode. Useful for dev,
            -- but log loudly so it doesn't ship to prod silently.
            io.stderr:write("[server] WARN: no authenticator configured, allowing\n")
            conn.authed = true
            conn.username = a.username
            conn.send(proto.encode_auth_ok(correl))
            return true
        end
        local ok, auth_err = self.authenticator(a.username, a.password, conn.ip)
        if not ok then
            conn.send(proto.encode_error(correl, proto.ERR_AUTH_FAILED,
                auth_err or "auth failed"))
            -- Don't disconnect immediately; let the auth module decide
            -- (it may want to apply per-IP backoff before we kill the
            -- connection). For now: drop after one failure.
            return false, "auth failed"
        end
        conn.authed = true
        conn.username = a.username
        conn.send(proto.encode_auth_ok(correl))
        return true

    elseif op == proto.OP_PING then
        conn.send(proto.encode_pong(correl))
        return true

    elseif op == proto.OP_PRODUCE then
        if conn.rate_limiter and not conn.rate_limiter:take(1) then
            conn.send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
                "too many produces"))
            return true
        end
        return self:_handle_produce(conn, correl, payload)

    elseif op == proto.OP_GOODBYE then
        return false, "client goodbye"

    -- TODO: FETCH, SUBSCRIBE, COMMIT, CREATE_TOPIC, LIST_TOPICS
    else
        conn.send(proto.encode_error(correl, proto.ERR_UNKNOWN_OP,
            string.format("op 0x%02X", op)))
        return true
    end
end

function Server:_handle_produce(conn, correl, payload)
    local p, perr = proto.decode_produce(payload)
    if not p then
        conn.send(proto.encode_error(correl, proto.ERR_BAD_FRAME, perr))
        return true
    end

    local topic, terr = self.broker:get_topic(p.topic)
    if not topic then
        conn.send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING, terr))
        return true
    end

    -- Reusing the existing Producer would be cleaner; inlining here so
    -- this file stays focused on the network plumbing. Swap for Producer
    -- once you wire it through.
    local msg = msg_m.Message.new(p.key, p.value, 0)
    local crc32 = require("src.crc32")

    local part_id = (crc32(p.key) % #topic.partitions) + 1
    local partition = topic.partitions[part_id]
    local offset, werr = partition:write_message(msg)
    if werr then
        conn.send(proto.encode_error(correl, proto.ERR_INTERNAL, werr))
        return true
    end

    conn.send(proto.encode_produce_ack(correl, part_id, offset))
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