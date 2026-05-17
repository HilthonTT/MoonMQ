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

return Reactor