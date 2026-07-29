-- Regression tests for three defects fixed together:
--
--   1. Reactor:run computed the next-timer deadline BEFORE firing due timers,
--      so a timer armed by a coroutine it had just resumed was invisible and
--      the loop blocked on select for the full 1s cap. Every cooperative
--      reactor:sleep(0) cost a second — which made a default 600k-iteration
--      AUTH take ~73s and blow through the 5s handshake watchdog.
--   2. ClusterServer:_handle consulted the controller fence BEFORE checking
--      X-Cluster-Token. ControllerFence:observe durably adopts a higher epoch,
--      so an unauthenticated request could pin the epoch and permanently fence
--      the real controller out of the broker despite getting a 401 back.
--   3. Coordinator:begin overwrote a transaction sitting in PREPARE_COMMIT /
--      PREPARE_ABORT, destroying the only record of a decision that end_txn
--      and crash recovery both roll forward.

local json           = require("dkjson")
local socket         = require("socket")
local Reactor        = require("src.server.reactor")
local ClusterServer  = require("src.cluster.cluster_server")
local ControllerFence = require("src.cluster.controller_fence")
local Assignments    = require("src.cluster.assignments")
local brk_m          = require("src.broker")
local txn_m          = require("src.broker.txn_coordinator")
local os_utils       = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_bugfix3_test"
                                      or "/tmp/moonmq_bugfix3_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

-- An in-memory socket that hands `request` to the reader and collects
-- everything written back. Nothing ever blocks, so ClusterServer:_handle runs
-- to completion on the calling coroutine and the reactor never has to spin.
local function mock_sock(request)
    local pos, out = 1, {}
    return {
        _out       = out,
        settimeout = function() end,
        close      = function() end,
        receive    = function(_, n)
            if pos > #request then return nil, "closed" end
            local chunk = request:sub(pos, pos + n - 1)
            pos = pos + #chunk
            return chunk
        end,
        send = function(_, data, i, j)
            i, j = i or 1, j or #data
            out[#out + 1] = data:sub(i, j)
            return j
        end,
    }
end

local function response_of(sock)
    return table.concat(sock._out)
end

describe("reactor timer scheduling", function()
    it("does not stall a full select tick per cooperative sleep(0)", function()
        local r = Reactor.new()
        local n, elapsed = 6, nil
        r:spawn(function()
            local t0 = socket.gettime()
            for _ = 1, n do r:sleep(0) end
            elapsed = socket.gettime() - t0
            r:stop()
        end)
        r:run()

        assert.is_number(elapsed)
        -- Pre-fix this was ~1s per yield (n seconds total). Anything under a
        -- fraction of one tick proves the deadline is recomputed after firing.
        assert.is_true(elapsed < 0.5,
            string.format("%d x sleep(0) took %.3fs; expected well under 0.5s", n, elapsed))
    end)

    it("still honours a real future deadline", function()
        local r = Reactor.new()
        local elapsed
        r:spawn(function()
            local t0 = socket.gettime()
            r:sleep(0.2)
            elapsed = socket.gettime() - t0
            r:stop()
        end)
        r:run()

        assert.is_true(elapsed >= 0.15, "sleep(0.2) must not return early")
        assert.is_true(elapsed < 1.5,   "sleep(0.2) must not overshoot a whole tick")
    end)
end)

describe("handshake watchdog vs. in-flight credential verification", function()
    local Connection = require("src.server.connection")

    -- Minimum stub the watchdog + close path touch.
    local function fake_conn(reactor, deadline)
        local server = {
            reactor            = reactor,
            handshake_deadline = deadline,
            max_pending_bytes  = 1 << 20,
            _unregister_conn   = function() end,
        }
        local sock = {
            settimeout = function() end,
            close      = function() end,
            send       = function(_, d, i, j) return j or #d end,
        }
        return Connection.new(server, sock, "127.0.0.1:1", "127.0.0.1")
    end

    it("does not evict a connection whose AUTH the broker is still computing", function()
        local r = Reactor.new()
        -- Integer deadline: this test must exercise the wait-it-out logic, not
        -- be masked by the %d-on-a-float crash the next test covers.
        local conn = fake_conn(r, 1)

        -- The AUTH handler flags the derivation; it outlasts the deadline
        -- (pure-Lua PBKDF2 at any real iteration count does).
        conn.auth_in_progress = true
        r:spawn(function()
            r:sleep(1.6)
            conn.state = Connection.STATE_AUTHENTICATED
            conn.auth_in_progress = false
        end)
        r:spawn(function() conn:run_handshake_watchdog() end)
        r:spawn(function() r:sleep(2.2); r:stop() end)
        r:run()

        assert.are.equal(Connection.STATE_AUTHENTICATED, conn.state)
        assert.is_nil(conn.close_reason)
    end)

    it("still evicts a peer that never completes the handshake", function()
        local r = Reactor.new()
        -- Fractional deadline on purpose: the close message formatted it with
        -- %d, which raises on a non-integer and killed the watchdog coroutine
        -- before it could close anything.
        local conn = fake_conn(r, 0.1)

        r:spawn(function() conn:run_handshake_watchdog() end)
        r:spawn(function() r:sleep(0.6); r:stop() end)
        r:run()

        assert.are.equal(Connection.STATE_CLOSED, conn.state)
        assert.are.equal(Connection.REASON_HANDSHAKE_TIMEOUT, conn.close_reason)
    end)
end)

describe("cluster endpoint auth ordering", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    local function new_server(token)
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local assignments = assert(Assignments.new(BASE_DIR, "b1"))
        local fence = assert(ControllerFence.new(BASE_DIR))
        local cs = ClusterServer.new({
            reactor     = Reactor.new(),
            broker      = broker,
            assignments = assignments,
            broker_id   = "b1",
            port        = 0,
            token       = token,
            fence       = fence,
        })
        return cs, fence
    end

    it("rejects an untokened request WITHOUT adopting its controller epoch", function()
        local cs, fence = new_server("s3cret")
        assert.are.equal(0, (fence:highest()))

        local body = "{}"
        local sock = mock_sock(table.concat({
            "POST /cluster/owner HTTP/1.1\r\n",
            "X-Controller-Epoch: 2147483647\r\n",
            "X-Controller-Id: attacker\r\n",
            "Content-Length: " .. #body .. "\r\n",
            "\r\n", body,
        }))
        cs:_handle(sock)

        assert.is_truthy(response_of(sock):find("401", 1, true))
        -- The whole point: the fence must be untouched, in memory AND on disk.
        local epoch, claimant = fence:highest()
        assert.are.equal(0, epoch)
        assert.is_nil(claimant)

        local reloaded = assert(ControllerFence.new(BASE_DIR))
        assert.are.equal(0, (reloaded:highest()))

        -- And a legitimate controller can still claim epoch 1 afterwards.
        assert.are.equal(1, (fence:claim("b1")))
    end)

    it("still fences a stale controller once the token is valid", function()
        local cs, fence = new_server("s3cret")
        assert.are.equal(5, (fence:observe(5, "b2") and select(1, fence:highest())))

        local body = json.encode({ topic = "t", partition = 1, owner = "b2" })
        local sock = mock_sock(table.concat({
            "POST /cluster/owner HTTP/1.1\r\n",
            "X-Cluster-Token: s3cret\r\n",
            "X-Controller-Epoch: 3\r\n",
            "X-Controller-Id: b3\r\n",
            "Content-Length: " .. #body .. "\r\n",
            "\r\n", body,
        }))
        cs:_handle(sock)

        assert.is_truthy(response_of(sock):find("409", 1, true))
        assert.are.equal(5, (fence:highest()))
    end)

    it("accepts an authenticated current-epoch request", function()
        local cs, fence = new_server("s3cret")
        assert(fence:observe(7, "b1"))

        local body = json.encode({ topic = "t", partition = 1, owner = "b2" })
        local sock = mock_sock(table.concat({
            "POST /cluster/owner HTTP/1.1\r\n",
            "X-Cluster-Token: s3cret\r\n",
            "X-Controller-Epoch: 7\r\n",
            "X-Controller-Id: b1\r\n",
            "Content-Length: " .. #body .. "\r\n",
            "\r\n", body,
        }))
        cs:_handle(sock)

        assert.is_truthy(response_of(sock):find("200", 1, true))
    end)
end)

describe("transaction coordinator prepared-state protection", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("refuses to begin over a transaction left in PREPARE_COMMIT", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 1))
        assert(broker:create_topic("in", 1))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("txn-p"))

        assert(broker.transactions:begin("txn-p", pid, epoch))
        assert(broker.transactions:add_partition("txn-p", pid, epoch, "out", 1))
        assert(broker.transactions:add_offsets("txn-p", pid, epoch, "grp",
            { { topic = "in", partition = 1, offset = 42 } }))

        -- Fail the offset commit so end_txn leaves the txn in PREPARE_COMMIT.
        local real_commit = broker.offsets.commit
        broker.offsets.commit = function() return nil, "injected failure" end
        local ok = broker.transactions:end_txn("txn-p", pid, epoch, true)
        broker.offsets.commit = real_commit
        assert.is_nil(ok)
        assert.are.equal(txn_m.STATES.PREPARE_COMMIT,
            broker.transactions:current("txn-p").state)

        -- The client must NOT be able to wipe the durable decision by starting
        -- a fresh transaction on the same id.
        local bok, berr, bcode = broker.transactions:begin("txn-p", pid, epoch)
        assert.is_nil(bok)
        assert.are.equal("state", bcode)
        assert.is_string(berr)
        assert.are.equal(txn_m.STATES.PREPARE_COMMIT,
            broker.transactions:current("txn-p").state)

        -- Retrying END_TXN with the same decision still completes it, and the
        -- buffered offset lands.
        assert(broker.transactions:end_txn("txn-p", pid, epoch, true))
        assert.are.equal(txn_m.STATES.COMPLETE_COMMIT,
            broker.transactions:current("txn-p").state)
        assert.are.equal(42, broker:fetch_offset("grp", "in", 1))

        -- Once COMPLETE, the id is reusable for the next transaction.
        assert(broker.transactions:begin("txn-p", pid, epoch))
        assert.are.equal(txn_m.STATES.ONGOING,
            broker.transactions:current("txn-p").state)
    end)
end)
