-- socket.select-based event loop with coroutine handlers.
--
-- Each connection runs in a coroutine that uses blocking-style API
-- (read_exact, send_all, sleep). The reactor handles the actual
-- multiplexing. Single-threaded — no locks needed, but no parallelism
-- either, so beware blocking syscalls (disk I/O, os.execute) on hot
-- paths.

local socket = require("socket")

local Reactor = {}
Reactor.__index = Reactor

function Reactor.new()
    return setmetatable({
        running        = false,
        read_waiters   = {},   -- sock -> coroutine
        write_waiters  = {},   -- sock -> coroutine
        timer_waiters  = {},   -- coroutine -> wake_at_timestamp
        ready          = {},   -- coroutines to resume on next tick
        listeners      = {},   -- sock -> accept_handler(client_sock, peer)
    }, Reactor)
end

function Reactor:spawn(fn, ...)
    local co = coroutine.create(fn)
    local args = { ... }
    self.ready[#self.ready + 1] = { co = co, args = args }
    return co
end

local function safe_resume(co, ...)
    local ok, err = coroutine.resume(co, ...)
    if not ok then
        io.stderr:write(string.format("[reactor] coroutine error: %s\n",
            tostring(err)))
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
    assert(type(deadline) == "number", "deadline must be a number")

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

function Reactor:send_all(sock, data)
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
                io.stderr:write(string.format("[reactor] accept: %s\n", aerr))
                self:sleep(0.1)
            else
                local peer = client:getpeername() or "?:?"
                self:spawn(function() on_accept(client, peer) end)
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
        local soonest = math.huge
        local fired = {}

        for co, wake_at in pairs(self.timer_waiters) do
            if wake_at <= now then
                fired[#fired + 1] = co
            elseif wake_at < soonest then
                soonest = wake_at
            end
        end

        for i = 1, #fired do
            self.timer_waiters[fired[i]] = nil
            safe_resume(fired[i])
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

        if #read_set == 0 and #write_set == 0 then
            socket.sleep(timeout)
        else
            local readable, writable = socket.select(read_set, write_set, timeout)
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
    end
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