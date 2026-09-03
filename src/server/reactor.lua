local socket   = require("socket")
local os_utils = require("src.core.os")
local metrics  = require("src.metrics")
local tls      = require("src.io.tls")
local log      = require("src.log.logger").get("reactor")

local Reactor = {}
Reactor.__index = Reactor

Reactor.SELECT_LIMIT = socket._SETSIZE or 1024

function Reactor.new(opts)
    return setmetatable({
        running        = false,
        read_waiters   = {},
        write_waiters  = {},
        timer_waiters  = {},
        ready          = {},
        listeners      = {},

        fd_limit       = (opts and opts.fd_limit) or Reactor.SELECT_LIMIT,
        fd_rejected    = 0,
    }, Reactor)
end

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

-- Park the running coroutine on `waiters[sock]`. With a `deadline` (absolute
-- socket.gettime() seconds) the coroutine is also registered as a timer, so
-- a peer that never becomes ready cannot strand it: whichever fires first
-- wins and the other registration is removed. Returns true when woken by
-- socket readiness (or an external wake-up) and false on deadline.
function Reactor:_wait(sock, waiters, deadline)
    local co = assert(coroutine.running(), "wait needs a coroutine")
    waiters[sock] = co
    if deadline then self.timer_waiters[co] = deadline end
    local woke_by = coroutine.yield()
    if deadline then self.timer_waiters[co] = nil end
    if woke_by == "timeout" and waiters[sock] == co then
        waiters[sock] = nil
        return false
    end
    return true
end

function Reactor:wait_readable(sock, deadline)
    return self:_wait(sock, self.read_waiters, deadline)
end

function Reactor:wait_writable(sock, deadline)
    return self:_wait(sock, self.write_waiters, deadline)
end

-- Wake every coroutine parked on `sock` (typically because it was just
-- closed locally). They resume as if the socket became ready, retry their
-- I/O and observe "closed" instead of leaking in the waiter tables forever
-- (select silently skips closed sockets).
function Reactor:release(sock)
    local me = coroutine.running()
    for _, waiters in ipairs({ self.read_waiters, self.write_waiters }) do
        local co = waiters[sock]
        if co and co ~= me then
            waiters[sock] = nil
            self.timer_waiters[co] = nil
            self:schedule(co)
        end
    end
end

function Reactor:sleep(seconds)
    local co = assert(coroutine.running(), "sleep needs a coroutine")
    self.timer_waiters[co] = socket.gettime() + seconds
    coroutine.yield()
end

-- Returns false for a non-blocking error (nothing to park on). With a
-- deadline, also returns false, "deadline" when the timer fired first.
function Reactor:park(sock, err, fallback, deadline)
    local woke
    if err == "wantread" then
        woke = self:wait_readable(sock, deadline)
    elseif err == "wantwrite" then
        woke = self:wait_writable(sock, deadline)
    elseif err == "timeout" then
        if fallback == "write" then
            woke = self:wait_writable(sock, deadline)
        else
            woke = self:wait_readable(sock, deadline)
        end
    else
        return false
    end
    if not woke then return false, "deadline" end
    return true
end

function Reactor.would_block(err)
    return err == "timeout" or err == "wantread" or err == "wantwrite"
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

            if Reactor.would_block(err) then
                if got >= n then break end
                if deadline and socket.gettime() > deadline then
                    return nil, "deadline exceeded"
                end
                if not self:park(sock, err, "read", deadline) then
                    return nil, "deadline exceeded"
                end
            else
                return nil, err
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
            if Reactor.would_block(err) then
                if deadline and socket.gettime() > deadline then
                    return nil, "write deadline exceeded"
                end
                if not self:park(sock, err, "write", deadline) then
                    return nil, "write deadline exceeded"
                end
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

function Reactor:tls_handshake(sock, params, deadline)
    local wrapped, werr = tls.wrap(sock, params)
    if not wrapped then
        pcall(function() sock:close() end)
        return nil, werr or "tls wrap failed"
    end

    wrapped:settimeout(0)

    local function fail(reason)
        pcall(function() wrapped:close() end)
        return nil, reason
    end

    while true do
        local ok, herr = wrapped:dohandshake()
        if ok then return wrapped end

        if not Reactor.would_block(herr) then
            return fail(herr or "handshake failed")
        end
        if deadline and socket.gettime() > deadline then
            return fail("handshake deadline exceeded")
        end
        if not self:park(wrapped, herr, "read", deadline) then
            return fail("handshake deadline exceeded")
        end
    end
end

function Reactor:listen(host, port, on_accept, opts)
    local listener, err = socket.bind(host, port)
    if not listener then return nil, err end

    opts = opts or {}
    local tls_cfg = opts.tls

    self.listeners[listener] = on_accept

    self:spawn(function()
        while self.running do
            local client, aerr = self:accept(listener)
            if not client then
                if not self.running then break end
                log:error("accept: %s", aerr)
                self:sleep(0.1)
            elseif not self:can_watch(client) then
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

                self:spawn(function()
                    local sock = client
                    if tls_cfg then
                        if opts.pre_tls and not opts.pre_tls(sock, peer, ip) then
                            pcall(function() sock:close() end)
                            return
                        end
                        local deadline = socket.gettime()
                            + (tls_cfg.handshake_timeout or 10)
                        local secured, terr =
                            self:tls_handshake(sock, tls_cfg.params, deadline)
                        if not secured then
                            metrics.inc("moonmq_tls_handshake_failures_total")
                            log:warn("TLS handshake with %s failed: %s",
                                peer, tostring(terr))
                            return
                        end
                        metrics.inc("moonmq_tls_handshakes_total")
                        sock = secured
                    end
                    on_accept(sock, peer, ip)
                end)
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
        local ready = self.ready
        self.ready = {}
        for i = 1, #ready do
            local entry = ready[i]
            safe_resume(entry.co, table.unpack(entry.args or {}))
        end

        local now = socket.gettime()
        local fired = {}

        for co, wake_at in pairs(self.timer_waiters) do
            if wake_at <= now then
                fired[#fired + 1] = co
            end
        end

        for i = 1, #fired do
            self.timer_waiters[fired[i]] = nil
            safe_resume(fired[i], "timeout")
        end

        local soonest = math.huge
        for _, wake_at in pairs(self.timer_waiters) do
            if wake_at < soonest then soonest = wake_at end
        end

        local read_set, write_set = {}, {}
        for sock in pairs(self.read_waiters)  do read_set[#read_set+1]  = sock end
        for sock in pairs(self.write_waiters) do write_set[#write_set+1] = sock end

        local timeout
        if soonest == math.huge then
            timeout = 1.0
        else
            timeout = math.max(0, math.min(1.0, soonest - socket.gettime()))
        end

        if #self.ready > 0 then
            timeout = 0
        end

        if #read_set == 0 and #write_set == 0 then
            socket.sleep(timeout)
        else
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