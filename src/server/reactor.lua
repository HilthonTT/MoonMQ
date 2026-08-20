-- socket.select-based event loop with coroutine handlers.
--
-- Each connection runs in a coroutine that uses blocking-style API
-- (read_exact, send_all, sleep). The reactor handles the actual
-- multiplexing. Single-threaded — no locks needed, but no parallelism
-- either, so beware blocking syscalls (disk I/O, os.execute) on hot
-- paths.

local socket   = require("socket")
local os_utils = require("src.core.os")
local metrics  = require("src.metrics")
local log      = require("src.log.logger").get("reactor")

local Reactor = {}
Reactor.__index = Reactor

-- select(2) can only watch descriptors numbered below FD_SETSIZE (1024 on
-- Linux), and luasocket does not degrade gracefully at that boundary: it
-- raises "descriptor too large for set size" from socket.select. That error
-- comes out of run()'s own stack frame, past every per-coroutine
-- safe_resume, so before this guard existed a broker that reached its
-- configured MaxConnections (1024, the shipped default) died outright —
-- reproduced end-to-end at exactly 1024 accepted connections.
--
-- LuaSocket 3.x publishes the compiled-in limit as socket._SETSIZE.
Reactor.SELECT_LIMIT = socket._SETSIZE or 1024

-- opts (optional):
--   fd_limit — highest watchable descriptor + 1. Defaults to the select
--              limit; the specs lower it to exercise the rejection path.
function Reactor.new(opts)
    return setmetatable({
        running        = false,
        read_waiters   = {},   -- sock -> coroutine
        write_waiters  = {},   -- sock -> coroutine
        timer_waiters  = {},   -- coroutine -> wake_at_timestamp
        ready          = {},   -- coroutines to resume on next tick
        listeners      = {},   -- sock -> accept_handler(client_sock, peer)

        fd_limit       = (opts and opts.fd_limit) or Reactor.SELECT_LIMIT,
        -- Count of accepts refused for being unwatchable. Surfaced by the
        -- server as a metric; a nonzero value means the fd budget is too
        -- tight for the configured connection cap.
        fd_rejected    = 0,
    }, Reactor)
end

-- True when `sock` can go into a select set. getfd() is the descriptor number
-- on POSIX; on Windows it is a SOCKET handle, which is not an index into the
-- fd set, so the number carries no meaning there and every socket passes (the
-- server's connection cap is what bounds Windows).
--
-- Sockets that report no descriptor (-1, e.g. an unconnected TCP object) pass
-- too: there is nothing to compare, and select will reject them on its own
-- terms if they are genuinely unusable.
function Reactor:can_watch(sock)
    if os_utils.IS_WINDOWS then return true end
    if type(sock) ~= "table" and type(sock) ~= "userdata" then return true end
    local getfd = sock.getfd
    if type(getfd) ~= "function" then return true end

    local ok, fd = pcall(getfd, sock)
    if not ok or type(fd) ~= "number" or fd < 0 then return true end
    return fd < self.fd_limit
end

function Reactor:spawn(fn, ...)
    local co = coroutine.create(fn)
    local args = { ... }
    self.ready[#self.ready + 1] = { co = co, args = args }
    return co
end

-- Re-queue an already-created coroutine for resumption on the next tick.
-- Used to wake coroutines parked with a bare coroutine.yield() — e.g. the
-- per-connection sender waiting for the send queue to fill.
function Reactor:schedule(co)
    assert(type(co) == "thread", "schedule expects a coroutine")
    self.ready[#self.ready + 1] = { co = co }
end

local function safe_resume(co, ...)
    local ok, err = coroutine.resume(co, ...)
    if not ok then
        log:error("coroutine error: %s", tostring(err))
    end
    return ok
end

function Reactor:wait_readable(sock)
    local co = assert(coroutine.running(), "wait_readable needs a coroutine")
    self.read_waiters[sock] = co
    coroutine.yield()
end

function Reactor:wait_writable(sock)
    local co = assert(coroutine.running(), "wait_writable needs a coroutine")
    self.write_waiters[sock] = co
    coroutine.yield()
end

function Reactor:sleep(seconds)
    local co = assert(coroutine.running(), "sleep needs a coroutine")
    self.timer_waiters[co] = socket.gettime() + seconds
    coroutine.yield()
end

function Reactor:read_exact(sock, n, deadline)
    assert(type(n) == "number", "n must be a number")
    assert(deadline == nil or type(deadline) == "number",
        "deadline must be a number or nil")

    sock:settimeout(0)
    local buf = {}
    local got = 0

    while got < n do
        local data, err, partial = sock:receive(n - got)
        if data then
            buf[#buf + 1] = data
            got = got + #data
        else
            if partial and #partial > 0 then
                buf[#buf + 1] = partial
                got = got + #partial
            end
            
            if err == "timeout" then
                if got >= n then break end
                if deadline and socket.gettime() > deadline then
                    return nil, "deadline exceeded"
                end
                self:wait_readable(sock)
            else
                return nil, err -- "closed", network error, etc.
            end
        end
    end
    return table.concat(buf)
end

function Reactor:send_all(sock, data, deadline)
    assert(deadline == nil or type(deadline) == "number",
        "deadline must be a number or nil")
    sock:settimeout(0)
    local sent = 0
    local total = #data
    while sent < total do
        local ok, err, last = sock:send(data, sent + 1, total)
        if ok then
            sent = ok
        else
            sent = last or sent
            if err == "timeout" then
                if deadline and socket.gettime() > deadline then
                    return nil, "write deadline exceeded"
                end
                self:wait_writable(sock)
            else
                return nil, err
            end
        end
    end
    return true
end

function Reactor:accept(listener)
    listener:settimeout(0)
    while true do
        local client, err = listener:accept()
        if client then return client end
        if err == "timeout" then
            self:wait_readable(listener)
        else
            return nil, err
        end
    end
end

function Reactor:listen(host, port, on_accept)
    local listener, err = socket.bind(host, port)
    if not listener then return nil, err end

    self.listeners[listener] = on_accept

    self:spawn(function()
        while self.running do
            local client, aerr = self:accept(listener)
            if not client then
                if not self.running then break end
                log:error("accept: %s", aerr)
                self:sleep(0.1)
            elseif not self:can_watch(client) then
                -- Above the select ceiling: this socket could never be
                -- multiplexed, and handing it to on_accept would park a
                -- coroutine on a descriptor that takes the whole event loop
                -- down on the next tick. Drop it here instead. The peer sees
                -- an immediate close, which is what any capacity rejection
                -- looks like from the other end.
                self.fd_rejected = self.fd_rejected + 1
                metrics.inc("moonmq_connections_refused_fd_total", 1)
                if self.fd_rejected == 1 or self.fd_rejected % 100 == 0 then
                    log:error("refusing connection: descriptor at or above the "
                        .. "select limit (%d); %d refused so far. Lower "
                        .. "MaxConnections or raise the fd budget.",
                        self.fd_limit, self.fd_rejected)
                end
                pcall(function() client:close() end)
            else
                local ip, peer_port = client:getpeername()
                ip = ip or "?"
                local peer = string.format("%s:%s", ip, peer_port or "?")
                self:spawn(function() on_accept(client, peer, ip) end)
            end
        end
        self.listeners[listener] = nil
        listener:close()
    end)

    return listener
end

function Reactor:run()
    self.running = true
    while self.running do
        -- 1. Drain ready queue first (spawns, woken-up coros, etc.).
        --    Iterate a snapshot — handlers may push more onto the queue.
        local ready = self.ready
        self.ready = {}
        for i = 1, #ready do
            local entry = ready[i]
            safe_resume(entry.co, table.unpack(entry.args or {}))
        end

        -- 2. Fire expired timers.
        local now = socket.gettime()
        local fired = {}

        for co, wake_at in pairs(self.timer_waiters) do
            if wake_at <= now then
                fired[#fired + 1] = co
            end
        end

        for i = 1, #fired do
            self.timer_waiters[fired[i]] = nil
            safe_resume(fired[i])
        end

        -- Compute the next wake AFTER firing, not during the scan above. A
        -- coroutine we just resumed will usually register a fresh timer
        -- (a sleep loop re-arms; auth's PBKDF2 yield_fn does reactor:sleep(0)
        -- between iteration slices), and a deadline gathered before the resume
        -- can't see it. The loop then blocked on select for the full 1s cap
        -- with a runnable timer pending: every cooperative sleep(0) cost a
        -- second, push-mode delivery ran at 1s instead of push_interval, and a
        -- 600k-iteration AUTH needed ~73s — well past the handshake watchdog,
        -- so authentication could never complete.
        local soonest = math.huge
        for _, wake_at in pairs(self.timer_waiters) do
            if wake_at < soonest then soonest = wake_at end
        end

        -- 3. Build select sets.
        local read_set, write_set = {}, {}
        for sock in pairs(self.read_waiters)  do read_set[#read_set+1]  = sock end
        for sock in pairs(self.write_waiters) do write_set[#write_set+1] = sock end

        -- 4. Compute select timeout: shorter of (next timer) and a 1s cap
        --    so we always check `self.running` at least every second.
        local timeout
        if soonest == math.huge then
            timeout = 1.0
        else
            timeout = math.max(0, math.min(1.0, soonest - socket.gettime()))
        end

        -- Coroutines scheduled during this tick (e.g. a sender woken by
        -- Connection:send, or a just-fired timer) are sitting in self.ready
        -- but weren't in the snapshot we drained. Don't block on select/sleep
        -- while runnable work is pending — poll instead and loop right back to
        -- the drain, or they'd wait up to a full second to transmit.
        if #self.ready > 0 then
            timeout = 0
        end

        if #read_set == 0 and #write_set == 0 then
            socket.sleep(timeout)
        else
            -- pcall, not a bare call: socket.select RAISES on a descriptor it
            -- cannot represent. The accept path above keeps those out of the
            -- waiter sets, but sockets the reactor didn't accept (replication
            -- peers, cluster clients, anything a future caller parks here) can
            -- still cross the limit — and an unguarded raise here kills the
            -- broker rather than one connection. Drop the offenders and keep
            -- serving everyone else.
            local sel_ok, readable, writable =
                pcall(socket.select, read_set, write_set, timeout)
            if not sel_ok then
                log:error("select failed (%s); dropping unwatchable sockets",
                    tostring(readable))
                self:_drop_unwatchable()
                goto continue
            end
            for _, sock in ipairs(readable or {}) do
                local co = self.read_waiters[sock]
                if co then
                    self.read_waiters[sock] = nil
                    safe_resume(co)
                end
            end
            for _, sock in ipairs(writable or {}) do
                local co = self.write_waiters[sock]
                if co then
                    self.write_waiters[sock] = nil
                    safe_resume(co)
                end
            end
        end
        ::continue::
    end
end

-- Remove every waiter whose descriptor is outside the select set, closing the
-- socket and resuming its coroutine so the owner unwinds through its normal
-- error path (a closed socket surfaces as a read/write error, which every
-- caller already handles) rather than parking forever.
function Reactor:_drop_unwatchable()
    local dropped = 0

    for _, waiters in ipairs({ self.read_waiters, self.write_waiters }) do
        local doomed = {}
        for sock in pairs(waiters) do
            if not self:can_watch(sock) then doomed[#doomed + 1] = sock end
        end
        for i = 1, #doomed do
            local sock = doomed[i]
            local co   = waiters[sock]
            waiters[sock] = nil
            pcall(function() sock:close() end)
            if co then safe_resume(co) end
            dropped = dropped + 1
        end
    end

    if dropped == 0 then
        -- select failed for some other reason and we have no offender to
        -- blame. Returning here still beats propagating: the loop retries,
        -- and a persistent failure shows up as a repeating log line instead
        -- of a dead broker.
        log:error("select failed but every watched socket looks valid")
    else
        self.fd_rejected = self.fd_rejected + dropped
        metrics.inc("moonmq_connections_refused_fd_total", dropped)
        log:error("dropped %d unwatchable socket(s)", dropped)
    end
    return dropped
end

function Reactor:stop()
    self.running = false
end

-- Close all sockets the reactor is tracking. Call after run() returns so
-- peers see a clean FIN instead of waiting for GC to drop the fd.
function Reactor:shutdown()
    for listener in pairs(self.listeners) do
        pcall(function() listener:close() end)
    end
    self.listeners = {}

    for sock in pairs(self.read_waiters) do
        pcall(function() sock:close() end)
    end
    self.read_waiters = {}

    for sock in pairs(self.write_waiters) do
        pcall(function() sock:close() end)
    end
    self.write_waiters = {}
end

return Reactor