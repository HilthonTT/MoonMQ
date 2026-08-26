-- Regression cover for the crash documented at the top of reactor.lua:
-- socket.select raises "descriptor too large for set size" the moment any
-- watched descriptor reaches FD_SETSIZE, and the raise happens inside run()'s
-- own frame — so it killed the whole broker, not the offending connection.
-- Reproduced end-to-end at exactly the shipped MaxConnections of 1024.
--
-- These specs drive the same paths with a deliberately tiny fd_limit instead
-- of opening a thousand sockets.

local socket  = require("socket")
local Reactor = require("src.server.reactor")

-- A socket stand-in that reports whatever descriptor number we want.
local function fake_sock(fd)
    return {
        getfd     = function() return fd end,
        close     = function() return true end,
        settimeout = function() end,
    }
end

describe("Reactor descriptor budget", function()

    it("publishes the select limit it was compiled against", function()
        assert.is_number(Reactor.SELECT_LIMIT)
        assert.is_true(Reactor.SELECT_LIMIT >= 64)
    end)

    it("defaults fd_limit to the select limit", function()
        assert.are.equal(Reactor.SELECT_LIMIT, Reactor.new().fd_limit)
    end)

    it("accepts descriptors below the limit and refuses those at or above it", function()
        local r = Reactor.new({ fd_limit = 100 })
        assert.is_true(r:can_watch(fake_sock(0)))
        assert.is_true(r:can_watch(fake_sock(99)))
        assert.is_false(r:can_watch(fake_sock(100)))
        assert.is_false(r:can_watch(fake_sock(4096)))
    end)

    it("passes sockets with no usable descriptor rather than guessing", function()
        local r = Reactor.new({ fd_limit = 10 })
        assert.is_true(r:can_watch(fake_sock(-1)))          -- unconnected
        assert.is_true(r:can_watch({ close = function() end }))  -- no getfd
    end)

    it("does not blow up the loop when select raises", function()
        -- Park a coroutine on a descriptor that select cannot represent: the
        -- pre-fix behaviour was an uncaught error out of run(). Now the
        -- offender is dropped, its coroutine resumed, and the loop keeps going.
        local r = Reactor.new({ fd_limit = 4 })
        local closed, resumed = false, false

        local bad = {
            getfd      = function() return 99999 end,
            close      = function() closed = true end,
            settimeout = function() end,
        }

        local co = coroutine.create(function()
            r:wait_readable(bad)
            resumed = true
        end)
        coroutine.resume(co)              -- parks on bad
        r.read_waiters[bad] = co

        -- Stop after one tick so run() returns.
        r:spawn(function() r:sleep(0.01); r:stop() end)

        local ok, err = pcall(function() r:run() end)
        assert.is_true(ok, tostring(err))
        assert.is_true(closed, "unwatchable socket should be closed")
        assert.is_true(resumed, "parked coroutine should be resumed, not orphaned")
        assert.is_nil(r.read_waiters[bad])
        assert.is_true(r.fd_rejected > 0)
    end)

    it("refuses an accepted connection whose descriptor is over the limit", function()
        -- fd_limit of 1 makes every real accepted socket unwatchable, which is
        -- the same situation as a full fd table, minus a thousand sockets.
        local r = Reactor.new({ fd_limit = 1 })
        local handled = false

        local listener, lerr = r:listen("127.0.0.1", 0, function() handled = true end)
        assert.is_nil(lerr)
        local _, port = listener:getsockname()

        r:spawn(function()
            local c = socket.tcp()
            c:settimeout(1)
            c:connect("127.0.0.1", tonumber(port))
            -- Give the reactor a few ticks to accept and refuse.
            r:sleep(0.2)
            c:close()
            r:stop()
        end)

        local ok, err = pcall(function() r:run() end)
        assert.is_true(ok, tostring(err))
        assert.is_false(handled, "handler must not run for an unwatchable socket")
        assert.are.equal(1, r.fd_rejected)

        r:shutdown()
    end)

    it("serves normally when descriptors fit", function()
        local r = Reactor.new()   -- real limit
        local got_peer = nil

        local listener, lerr = r:listen("127.0.0.1", 0, function(sock, peer)
            got_peer = peer
            sock:close()
            r:stop()
        end)
        assert.is_nil(lerr)
        local _, port = listener:getsockname()

        r:spawn(function()
            local c = socket.tcp()
            c:settimeout(1)
            c:connect("127.0.0.1", tonumber(port))
            r:sleep(0.5)
            c:close()
            r:stop()
        end)

        r:run()
        assert.is_string(got_peer)
        assert.are.equal(0, r.fd_rejected)

        r:shutdown()
    end)
end)

describe("Server connection cap", function()
    local Server = require("src.server.server")

    it("clamps max_connections to what select can watch", function()
        local srv, err = Server.new({
            host = "127.0.0.1", port = 0, data_dir = "/tmp/lua_reactor_fd_test",
            max_connections = 100000,
            fd_limit = 256,
            fd_reserve = 56,
        })
        assert.is_nil(err)
        assert.are.equal(200, srv.max_connections)
    end)

    it("leaves a cap that already fits alone", function()
        local srv, err = Server.new({
            host = "127.0.0.1", port = 0, data_dir = "/tmp/lua_reactor_fd_test",
            max_connections = 50,
            fd_limit = 256,
            fd_reserve = 56,
        })
        assert.is_nil(err)
        assert.are.equal(50, srv.max_connections)
    end)
end)
