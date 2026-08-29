-- Per-connection state, lifecycle, and I/O queues.
--
-- A Connection owns:
--   - A TCP socket and its UUID identifier
--   - An explicit state machine (new → greeted → authenticated → closed)
--   - A bounded outbound send queue with pending-bytes accounting
--   - Three coroutines:
--       reader    — frames bytes off the socket, validates state, dispatches
--       sender    — drains the send queue (with a per-write deadline)
--       heartbeat — periodic liveness probes (starts on authenticate)
--   - A handshake watchdog that closes the conn if AUTH hasn't completed
--     within handshake_deadline seconds
--   - Telemetry counters (bytes/frames in & out, started_at) for logging
--     on close
--
-- The server.lua dispatcher treats Connection as opaque: it calls
-- conn:send(frame) and conn:close(reason). The state machine validates
-- which ops are legal in each state.
--
-- Closing is idempotent and safe from any coroutine. The first close()
-- call writes a best-effort final error frame (bypassing the queue, since
-- the queue is about to be discarded), marks state=closed, closes the
-- socket (which unblocks any pending reads on the reader coroutine), and
-- wakes the sender if it's parked. The heartbeat and watchdog observe
-- state==closed on their next wakeup and exit.

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

        -- Outbound queue (FIFO via head/tail). pending_bytes is the
        -- sum of #frame for queued frames + the in-flight frame.
        send_queue = {},
        send_head = 1,
        send_tail = 0,
        pending_bytes = 0,
        sender_co = nil,
        sender_suspended = nil,

        -- Coroutines
        reader_co = nil,
        heartbeat_co = nil,
        watchdog_co = nil,
        subscriber_co = nil,

        -- Application state (populated as handshake progresses)
        username = nil,
        -- The authenticated principal: { username, acl, quota, superuser }.
        -- Set by AUTH / AUTH_SCRAM and never changed afterwards; every
        -- authorization decision on this connection reads it. nil means the
        -- broker has no authenticator configured at all (OPEN mode), which is
        -- the only case where handlers skip the ACL check.
        principal = nil,
        -- In-flight SCRAM exchange (username, nonce, salt, keys, the
        -- client-first/server-first bytes the proof is computed over).
        -- Discarded the moment the exchange ends, either way.
        scram = nil,
        -- Set by the AUTH handler for the duration of the credential
        -- verification. The handshake watchdog waits it out rather than
        -- counting the broker's own PBKDF2 derivation against the peer.
        auth_in_progress = false,
        client_name = nil,
        client_version = nil,
        consumer = nil,
        subscriptions = {},
        mode = nil,  -- "push" (SUBSCRIBE) | "pull" (FETCH)
        -- Consumer-group membership. Set by JOIN_GROUP; one connection maps
        -- to at most one (group_id, member_id). Cleared on LEAVE_GROUP, and
        -- the server departs the group automatically when the conn closes
        -- (see Server:_unregister_conn).
        group_id = nil,
        member_id = nil,
        rate_limiter = server.rate_limiter_factory
                          and server.rate_limiter_factory() or nil,
        -- Idempotent producer state. pid is set once via INIT_PRODUCER_ID
        -- and lives for the connection's lifetime — there's no PID
        -- persistence so reconnecting requires a fresh INIT_PRODUCER_ID.
        -- seq_state maps topic_name → { last_seq, last_offset, last_partition }
        -- letting the broker dedup retries of the same (PID, topic, seq)
        -- tuple. Per-topic (not per-partition) because TCP's in-order
        -- delivery makes a single monotonic counter per topic both
        -- consistent AND lets the client get away with not knowing the
        -- broker's partition count or hash function.
        pid = nil,
        seq_state = {},

        -- Telemetry
        started_at = socket.gettime(),
        last_activity_at = socket.gettime(),
        bytes_received = 0,
        bytes_sent = 0,
        frames_received = 0,
        frames_sent = 0,
    }, Connection)

    -- NB: moonmq_connections_accepted_total / moonmq_connections_open are
    -- bumped by Server:_handle right after Connection.new returns, where the
    -- registered connection count is authoritative. Incrementing here too
    -- double-counted every accept, so it's deliberately left to the server.

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

-- Which opcodes are legal in the current state? Heartbeats and goodbye
-- are always legal so we can probe/disconnect even mid-handshake.
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
        -- HELLO is one-shot; re-issuing it post-handshake is malformed. AUTH is
        -- likewise one-shot: re-authenticating on an already-authenticated
        -- connection would re-run the expensive inline PBKDF2 (a reactor-stall
        -- amplifier) and let a session silently swap its username — and with
        -- it the ACL every later request is checked against — mid-stream. The
        -- SCRAM frames are barred for the same reason.
        return op ~= proto.OP_HELLO
            and op ~= proto.OP_AUTH
            and op ~= proto.OP_AUTH_SCRAM
            and op ~= proto.OP_AUTH_SCRAM_FINAL
    end

    return false
end

-- Enqueue a frame for transmission. Non-blocking. Returns true if
-- accepted, false if dropped because the queue is full (which also
-- triggers close — a backed-up queue means a slow or dead consumer).
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

-- Idempotent. Safe to call from any coroutine. If err_code is set, a
-- best-effort final error frame is written synchronously (bypassing the
-- queue — the queue is about to be discarded).
function Connection:close(reason, err_code, err_msg)
    if self.state == Connection.STATE_CLOSED then return end

    -- Transition first so re-entrant close() calls (from inside the
    -- sender/reader/heartbeat coroutines that may run as a side effect
    -- of the courtesy send below) early-out at the guard above.
    self.state = Connection.STATE_CLOSED
    self.close_reason = reason

    if err_code then
        local ok, frame = pcall(proto.encode_error,
            uuid.ZERO, err_code, err_msg or reason)
        if ok then
            -- Best-effort courtesy frame. Route through the reactor's
            -- non-blocking send_all so a wedged peer can't stall the
            -- event loop. Cap at 250ms — error frames are tiny, and
            -- this is a teardown path; we'd rather drop the frame than
            -- delay the whole reactor.
            local deadline = socket.gettime() + 0.25
            pcall(function()
                self.server.reactor:send_all(self.sock, frame, deadline)
            end)
        end
    end

    self:_log_close()

    pcall(function() self.sock:close() end)
    self.server:_unregister_conn(self)

    -- Drop this connection's per-conn gauge series so it doesn't linger in the
    -- registry (and every scrape) for the life of the process.
    metrics.delete("moonmq_pending_bytes", { conn = self.id_short })

    -- Wake sender so it observes state==closed and exits.
    if self.sender_suspended then
        local co = self.sender_suspended
        self.sender_suspended = nil
        self.server.reactor:schedule(co)
    end
    -- Heartbeat and watchdog wake on their own timers; reader unblocks
    -- when sock:close() returns errors from receive().
end

function Connection:run_sender()
    self.sender_co = coroutine.running()
    local send_timeout = self.server.send_deadline

    while self.state ~= Connection.STATE_CLOSED do
        -- Drain everything currently queued.
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

        -- Park until enqueue or close wakes us. The double-check after
        -- arming sender_suspended closes a race where send() runs (and
        -- already saw sender_suspended==nil) between our last queue
        -- check and the yield: without this, we'd sleep until the NEXT
        -- enqueue while a frame sits unsent.
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
        -- Per-frame read deadline. Without this a slowloris peer can
        -- send 4 bytes of length prefix and then dribble payload bytes
        -- for the full heartbeat interval (default 90s) per connection,
        -- multiplied by max_connections_per_ip. The deadline tightens
        -- before AUTH (pre-auth peers should complete the handshake
        -- fast; this also caps the cost of the handshake watchdog
        -- gathering enough rope to fire) and relaxes once authenticated.
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

         -- Frame size for telemetry: length prefix (4) + body length
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
            -- Soft reject: send error, stay open. This lets a confused
            -- client recover. Repeated misbehavior will trip the
            -- pending-bytes cap or heartbeat timeout eventually.
            self:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
                string.format("op 0x%02X not allowed in state %s",
                    op, self.state)))
        elseif op == proto.OP_HEARTBEAT_REQ then
            -- Client probing us — immediate response, no app dispatch.
            self:send(proto.encode_heartbeat_resp(correl))
        elseif op == proto.OP_HEARTBEAT_RESP then
            -- Response to our probe. last_activity_at was already
            -- updated above; nothing more to do.
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

        -- Send a probe if we're past half the interval — the response
        -- updates last_activity_at and prevents the timeout above.
        if idle >= interval / 2 then
            self:send(proto.encode_heartbeat_req(uuid.bytes()))
        end
    end
end

-- How often the watchdog re-checks while a credential verification is running.
local AUTH_GRACE_TICK = 0.25

function Connection:run_handshake_watchdog()
    self.server.reactor:sleep(self.server.handshake_deadline)

    -- A verification the BROKER is still computing is not a stalled peer. The
    -- PBKDF2 here is pure-Lua and cooperatively yields mid-derivation (see
    -- src/server/auth.lua), so any reasonably strong stored hash outlasts
    -- handshake_deadline — the broker's own recommended 600k iterations takes
    -- minutes. Counting that against the deadline slammed the socket shut on
    -- clients that had already sent CORRECT credentials, every single time
    -- (reason=handshake_timeout). Wait the derivation out instead: it is
    -- bounded by the stored hash's iteration count (capped at
    -- auth.MAX_PBKDF2_ITERATIONS), and Auth's max_inflight gate bounds how
    -- many can be running at once, so this can't be stretched by a peer.
    while self.auth_in_progress and self.state ~= Connection.STATE_CLOSED do
        self.server.reactor:sleep(AUTH_GRACE_TICK)
    end

    if self.state ~= Connection.STATE_CLOSED
       and self.state ~= Connection.STATE_AUTHENTICATED then
        -- %g, not %d: handshake_deadline comes from JSON config and may be
        -- fractional. %d on a non-integer raises ("number has no integer
        -- representation"), and since the format is evaluated as an argument
        -- to close(), the raise killed the watchdog coroutine BEFORE it could
        -- close anything — so a fractional deadline silently disabled
        -- slowloris eviction entirely.
        self:close(Connection.REASON_HANDSHAKE_TIMEOUT,
            proto.ERR_BAD_PROTOCOL,
            string.format("handshake not completed within %gs",
                self.server.handshake_deadline))
    end
end

-- Start the sender and watchdog as background coroutines, then run the
-- reader inline in the accept coroutine. When the reader returns the
-- connection is closed; this function then returns and the accept
-- coroutine exits.
function Connection:start()
    self.server.reactor:spawn(function() self:run_sender() end)
    self.server.reactor:spawn(function() self:run_handshake_watchdog() end)

    log:info("accepted conn=%s peer=%s", self.id_short, self.peer)

    self:run_reader()
end

return Connection