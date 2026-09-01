local socket          = require("socket")
local uuid     = require("src.core.uuid")
local framer   = require("src.server.framer")
local proto    = require("src.wire.protocol")
local metrics  = require("src.metrics")
local log      = require("src.log.logger").get("server")

local Connection = {}
Connection.__index = Connection

Connection.STATE_NEW = "new"
Connection.STATE_GREETED = "greeted"
Connection.STATE_AUTHENTICATED = "authenticated"
Connection.STATE_CLOSED = "closed"

Connection.REASON_CLIENT_GOODBYE = "client_goodbye"
Connection.REASON_PEER_CLOSED = "peer_closed"
Connection.REASON_READ_ERROR = "read_error"
Connection.REASON_WRITE_DEADLINE = "write_deadline_exceeded"
Connection.REASON_READ_DEADLINE_EXCEEDED = "read_deadline_exceeded"
Connection.REASON_HANDSHAKE_TIMEOUT = "handshake_timeout"
Connection.REASON_HEARTBEAT_TIMEOUT = "heartbeat_timeout"
Connection.REASON_AUTH_FAILED = "auth_failed"
Connection.REASON_BAD_FRAME = "bad_frame"
Connection.REASON_FRAME_TOO_LARGE = "frame_too_large"
Connection.REASON_PENDING_SEND_TOO_LARGE = "pending_send_too_large"
Connection.REASON_BAD_STATE = "bad_state_for_op"
Connection.REASON_BAD_PROTOCOL = "bad_protocol"
Connection.REASON_SERVER_SHUTDOWN = "server_shutdown"

function Connection.new(server, sock, peer, ip)
    assert(type(peer) == "string", "peer must be a string")
    assert(type(ip) == "string", "ip must be a string")

    local id = uuid.bytes()
    local connection = setmetatable({
        server = server,
        sock = sock,
        peer = peer,
        ip = ip,
        id = id,
        id_short = uuid.short(id),
        state = Connection.STATE_NEW,
        close_reason = nil,

        send_queue = {},
        send_head = 1,
        send_tail = 0,
        pending_bytes = 0,
        sender_co = nil,
        sender_suspended = nil,

        reader_co = nil,
        heartbeat_co = nil,
        watchdog_co = nil,
        subscriber_co = nil,

        username = nil,
        principal = nil,
        scram = nil,
        auth_in_progress = false,
        client_name = nil,
        client_version = nil,
        consumer = nil,
        subscriptions = {},
        mode = nil,
        group_id = nil,
        member_id = nil,
        rate_limiter = server.rate_limiter_factory
                          and server.rate_limiter_factory() or nil,
        pid = nil,
        seq_state = {},

        started_at = socket.gettime(),
        last_activity_at = socket.gettime(),
        bytes_received = 0,
        bytes_sent = 0,
        frames_received = 0,
        frames_sent = 0,
    }, Connection)


    return connection
end

function Connection:transition_to(new_state)
    assert(type(new_state) == "string", "new_state must be a string")

    if self.state == Connection.STATE_CLOSED then return end
    self.state = new_state
    if new_state == Connection.STATE_AUTHENTICATED and not self.heartbeat_co then
        self.heartbeat_co = self.server.reactor:spawn(function()
            self:run_heartbeat()
        end)
    end
end

function Connection:can_handle(op)
    assert(type(op) == "number", "op must be a number")

    if self.state == Connection.STATE_CLOSED then return false end

    if op == proto.OP_HEARTBEAT_REQ or op == proto.OP_HEARTBEAT_RESP or op == proto.OP_GOODBYE then
        return true
    end

    if self.state == Connection.STATE_NEW then
        return op == proto.OP_HELLO
    elseif self.state == Connection.STATE_GREETED then
        return op == proto.OP_IDENTIFY_CLIENT
            or op == proto.OP_AUTH
            or op == proto.OP_AUTH_SCRAM
            or op == proto.OP_AUTH_SCRAM_FINAL
    elseif self.state == Connection.STATE_AUTHENTICATED then
        return op ~= proto.OP_HELLO
            and op ~= proto.OP_AUTH
            and op ~= proto.OP_AUTH_SCRAM
            and op ~= proto.OP_AUTH_SCRAM_FINAL
    end

    return false
end

function Connection:send(frame)
    if self.state == Connection.STATE_CLOSED then return false end

    if self.pending_bytes + #frame > self.server.max_pending_bytes then
        self:close(Connection.REASON_PENDING_SEND_TOO_LARGE)
        return false
    end

    self.send_tail = self.send_tail + 1
    self.send_queue[self.send_tail] = frame
    self.pending_bytes = self.pending_bytes + #frame

    if self.sender_suspended then
        local co = self.sender_suspended
        self.sender_suspended = nil
        self.server.reactor:schedule(co)
    end

    metrics.set("moonmq_pending_bytes", self.pending_bytes, { conn = self.id_short })

    return true
end

function Connection:_log_close()
    local duration = socket.gettime() - self.started_at
    local ident = ""
    if self.client_name then
        ident = string.format(" client=%s/%s",
            self.client_name, self.client_version or "?")
    end
    if self.username then
        ident = ident .. " user=" .. self.username
    end
    log:info("closed conn=%s peer=%s reason=%s recv=%dB/%df sent=%dB/%df dur=%.1fs%s",
        self.id_short, self.peer, self.close_reason or "unknown",
        self.bytes_received, self.frames_received,
        self.bytes_sent, self.frames_sent,
        duration, ident)

    metrics.inc("moonmq_connections_closed_total", 1, { reason = self.close_reason })
    metrics.observe("moonmq_connection_duration_seconds",
    socket.gettime() - self.started_at)
end

function Connection:close(reason, err_code, err_msg)
    if self.state == Connection.STATE_CLOSED then return end

    self.state = Connection.STATE_CLOSED
    self.close_reason = reason

    if err_code then
        local ok, frame = pcall(proto.encode_error,
            uuid.ZERO, err_code, err_msg or reason)
        if ok then
            local deadline = socket.gettime() + 0.25
            pcall(function()
                self.server.reactor:send_all(self.sock, frame, deadline)
            end)
        end
    end

    self:_log_close()

    pcall(function() self.sock:close() end)
    self.server:_unregister_conn(self)

    metrics.delete("moonmq_pending_bytes", { conn = self.id_short })

    if self.sender_suspended then
        local co = self.sender_suspended
        self.sender_suspended = nil
        self.server.reactor:schedule(co)
    end
end

function Connection:run_sender()
    self.sender_co = coroutine.running()
    local send_timeout = self.server.send_deadline

    while self.state ~= Connection.STATE_CLOSED do
        while self.send_head <= self.send_tail do
            if self.state == Connection.STATE_CLOSED then return end

            local frame = self.send_queue[self.send_head]
            self.send_queue[self.send_head] = nil
            self.send_head = self.send_head + 1

            local deadline = send_timeout and (socket.gettime() + send_timeout) or nil
            local stop_timer = metrics.timer("moonmq_send_duration_seconds")
            local ok, err = self.server.reactor:send_all(self.sock, frame, deadline)
            stop_timer()
            if ok then
                self.pending_bytes = self.pending_bytes - #frame
                self.bytes_sent = self.bytes_sent + #frame
                self.frames_sent = self.frames_sent + 1

                metrics.inc("moonmq_frames_sent_total")
                metrics.inc("moonmq_bytes_sent_total", #frame)
            else
                if err == "write deadline exceeded" then
                    self:close(Connection.REASON_WRITE_DEADLINE)
                else
                    self:close(Connection.REASON_PEER_CLOSED)
                end
                return
            end
        end

        if self.state == Connection.STATE_CLOSED then return end

        self.sender_suspended = coroutine.running()
        if self.send_head <= self.send_tail or self.state == Connection.STATE_CLOSED then
            self.sender_suspended = nil
        else
            coroutine.yield()
        end
    end
end

function Connection:run_reader()
    self.reader_co = coroutine.running()
    while self.state ~= Connection.STATE_CLOSED do
        local deadline
        if self.state == Connection.STATE_AUTHENTICATED then
            deadline = socket.gettime() + self.server.idle_deadline
        else
            deadline = socket.gettime() + self.server.pre_auth_read_deadline
        end

        local body, ferr = framer.read_frame(
            self.server.reactor, self.sock, self.server.max_frame, deadline)

        if not body then
            if ferr and ferr:find("exceeds max", 1, true) then
                self:close(Connection.REASON_FRAME_TOO_LARGE,
                    proto.ERR_FRAME_TOO_LARGE, ferr)
            elseif ferr == "closed" then
                self:close(Connection.REASON_PEER_CLOSED)
            elseif ferr and ferr:find("deadline", 1, true) then
                self:close(Connection.REASON_READ_DEADLINE_EXCEEDED,
                    proto.ERR_INTERNAL, ferr)
            else
                self:close(Connection.REASON_READ_ERROR)
            end
            return
        end

        local total = 4 + #body
        self.bytes_received = self.bytes_received + total
        self.frames_received = self.frames_received + 1
        self.last_activity_at = socket.gettime()

        local op, correl, payload, perr = proto.parse_frame(body)
        if not op then
            self:close(Connection.REASON_BAD_FRAME, proto.ERR_BAD_FRAME, perr)
            return
        end

        metrics.inc("moonmq_frames_received_total", 1, { op = string.format("0x%02x", op) })

        if not self:can_handle(op) then
            self:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
                string.format("op 0x%02X not allowed in state %s",
                    op, self.state)))
        elseif op == proto.OP_HEARTBEAT_REQ then
            self:send(proto.encode_heartbeat_resp(correl))
        elseif op == proto.OP_HEARTBEAT_RESP then
        else
            self.server:dispatch(self, op, correl, payload)
        end
    end
end

function Connection:run_heartbeat()
    local interval  = self.server.heartbeat_interval
    local threshold = self.server.heartbeat_miss_threshold
    local timeout   = interval * threshold

    while self.state ~= Connection.STATE_CLOSED do
        self.server.reactor:sleep(interval)
        if self.state == Connection.STATE_CLOSED then return end

        local idle = socket.gettime() - self.last_activity_at

        if idle > timeout then
            self:close(Connection.REASON_HEARTBEAT_TIMEOUT,
                proto.ERR_INTERNAL,
                string.format("no activity for %.0fs (threshold %.0fs)",
                    idle, timeout))
            return
        end

        if idle >= interval / 2 then
            self:send(proto.encode_heartbeat_req(uuid.bytes()))
        end
    end
end

local AUTH_GRACE_TICK = 0.25

function Connection:run_handshake_watchdog()
    self.server.reactor:sleep(self.server.handshake_deadline)

    while self.auth_in_progress and self.state ~= Connection.STATE_CLOSED do
        self.server.reactor:sleep(AUTH_GRACE_TICK)
    end

    if self.state ~= Connection.STATE_CLOSED
       and self.state ~= Connection.STATE_AUTHENTICATED then
        self:close(Connection.REASON_HANDSHAKE_TIMEOUT,
            proto.ERR_BAD_PROTOCOL,
            string.format("handshake not completed within %gs",
                self.server.handshake_deadline))
    end
end

function Connection:start()
    self.server.reactor:spawn(function() self:run_sender() end)
    self.server.reactor:spawn(function() self:run_handshake_watchdog() end)

    log:info("accepted conn=%s peer=%s", self.id_short, self.peer)

    self:run_reader()
end

return Connection