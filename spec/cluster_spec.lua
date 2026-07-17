-- Cluster / reassignment layer tests. Two real Brokers run in-process; each
-- "peer" client drives the OTHER node's real ClusterServer route functions
-- directly (no HTTP, no reactor — but the exact production logic the HTTP
-- routes dispatch to).

local json         = require("dkjson")
local brk_m        = require("src.broker")
local prd_m        = require("src.broker.producer")
local consumer_m   = require("src.broker.consumer")
local msg_m        = require("src.record.message")
local os_utils     = require("src.core.os")
local Assignments  = require("src.cluster.assignments")
local ClusterServer = require("src.cluster.cluster_server")
local Reassigner   = require("src.cluster.reassigner")
local Router       = require("src.cluster.router")
local BalanceLoop  = require("src.cluster.balance_loop")
local Action       = require("src.autobalancer.common.action")

local BASE = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_cluster_test"
    or "/tmp/moonmq_cluster_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

-- A Peer implementation that calls the target node's ClusterServer route
-- functions in-process. Same surface as src/cluster/peer.lua.
local function local_peer(node)
    local cs = node.cluster_server
    local function unwrap(status, out)
        if status ~= 200 then return nil, tostring(out) end
        return out
    end
    return {
        id = node.id,
        ensure_topic = function(_, topic, partitions)
            local out, err = unwrap(cs:_ensure(
                json.encode({ topic = topic, partitions = partitions })))
            if not out then return nil, err end
            return true
        end,
        append = function(_, topic, partition, payload)
            local out, err = unwrap(cs:_append(topic, partition, payload))
            if not out then return nil, err end
            return out.offset
        end,
        leo = function(_, topic, partition)
            local out, err = unwrap(cs:_leo(
                string.format("topic=%s&partition=%d", topic, partition)))
            if not out then return nil, err end
            return out.offset
        end,
        set_owner = function(_, topic, partition, owner)
            local out, err = unwrap(cs:_owner(
                json.encode({ topic = topic, partition = partition, owner = owner })))
            if not out then return nil, err end
            return true
        end,
        loads = function(_)
            local out, err = unwrap(cs:_loads())
            if not out then return nil, err end
            return out.loads
        end,
    }
end

local function make_node(id)
    local dir = BASE .. "/" .. id
    local broker = assert(brk_m.Broker.new(dir))
    local assignments = assert(Assignments.new(dir, id))
    broker.cluster_assignments = assignments
    local node = {
        id = id, dir = dir, broker = broker, assignments = assignments,
    }
    node.cluster_server = ClusterServer.new({
        reactor = {}, broker = broker, assignments = assignments,
        broker_id = id, port = 1,   -- port unused: routes are driven directly
    })
    return node
end

-- Wire two nodes together: each gets a peers map pointing at the other, a
-- Reassigner, and a Producer with cluster routing.
local function make_cluster()
    local a, b = make_node("A"), make_node("B")
    a.peers = { B = local_peer(b) }
    b.peers = { A = local_peer(a) }
    for _, node in ipairs({ a, b }) do
        node.reassigner = Reassigner.new({
            broker = node.broker, assignments = node.assignments,
            peers = node.peers, self_id = node.id,
        })
        node.router = Router.new({
            assignments = node.assignments, peers = node.peers, self_id = node.id,
        })
        node.producer = prd_m.Producer.new(node.broker, prd_m.AckMode.AckLeader,
            { router = node.router })
    end
    return a, b
end

local function produce_n(node, topic, n, prefix)
    for i = 1, n do
        local _, _, err = node.producer:produce(topic,
            msg_m.Message.new("k" .. i, (prefix or "v") .. i, 0))
        assert.is_nil(err)
    end
end

local function partition_leo(node, topic, partition)
    local t = assert(node.broker:get_topic(topic))
    return t.partitions[partition].offset
end

describe("cluster assignments", function()
    before_each(function() rmdir(BASE) end)

    it("defaults every partition to self", function()
        local n = make_node("A")
        assert.are.equal("A", n.assignments:owner("orders", 1))
        assert.is_true(n.assignments:owned_by_self("orders", 1))
    end)

    it("persists ownership across reload", function()
        local n = make_node("A")
        assert.is_true(n.assignments:set_owner("orders", 2, "B"))
        local reloaded = assert(Assignments.new(n.dir, "A"))
        assert.are.equal("B", reloaded:owner("orders", 2))
        assert.are.equal("A", reloaded:owner("orders", 1))
    end)

    it("setting owner back to self removes the sparse entry", function()
        local n = make_node("A")
        assert.is_true(n.assignments:set_owner("orders", 2, "B"))
        assert.is_true(n.assignments:set_owner("orders", 2, "A"))
        assert.are.equal(0, #n.assignments:entries())
    end)
end)

describe("cluster server routes", function()
    before_each(function() rmdir(BASE) end)

    it("ensure is idempotent and validates names", function()
        local n = make_node("A")
        local peer = local_peer(n)
        assert.is_true(peer:ensure_topic("orders", 2))
        assert.is_true(peer:ensure_topic("orders", 2))       -- second call ok
        local ok, err = peer:ensure_topic("../evil", 1)
        assert.is_nil(ok)
        assert.is_not_nil(err)
    end)

    it("append rejects truncated bodies without writing", function()
        local n = make_node("A")
        local peer = local_peer(n)
        assert.is_true(peer:ensure_topic("orders", 1))
        local ok, err = peer:append("orders", 1, string.rep("\0", 7))
        assert.is_nil(ok)
        assert.matches("short record header", err)
        assert.are.equal(0, partition_leo(n, "orders", 1))
    end)

    it("loads excludes internal and moved-away partitions", function()
        local n = make_node("A")
        assert(n.broker:create_topic("orders", 2))
        assert.is_true(n.assignments:set_owner("orders", 2, "B"))
        local peer = local_peer(n)
        local loads = assert(peer:loads())
        assert.are.equal(1, #loads)                          -- partition 2 excluded
        assert.are.equal("orders", loads[1].topic)
        assert.are.equal(1, loads[1].partition)
    end)
end)

describe("reassigner MOVE", function()
    before_each(function() rmdir(BASE) end)

    it("migrates a partition byte-for-byte and flips ownership", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        produce_n(a, "orders", 20)
        local src_leo = partition_leo(a, "orders", 1)
        assert.is_true(src_leo > 0)

        local ok, err = a.reassigner:_move("orders", 1, "B")
        assert.is_nil(err)
        assert.is_true(ok)

        -- Byte-identical copy → same LEO on the destination.
        assert.are.equal(src_leo, partition_leo(b, "orders", 1))
        -- Ownership flipped on A; B's table treats unlisted as its own.
        assert.are.equal("B", a.assignments:owner("orders", 1))
        assert.is_true(b.assignments:owned_by_self("orders", 1))

        -- The records are readable on B.
        local t = assert(b.broker:get_topic("orders"))
        local msg = assert(t.partitions[1]:read_message(0))
        assert.are.equal("k1", msg.key)
    end)

    it("refuses to migrate onto a non-empty destination", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        assert(b.broker:create_topic("orders", 1))
        produce_n(a, "orders", 2)
        -- Write directly on B so its partition is non-empty.
        local bt = assert(b.broker:get_topic("orders"))
        assert(bt.partitions[1]:write_message(msg_m.Message.new("x", "y", 1)))

        local ok, err = a.reassigner:_move("orders", 1, "B")
        assert.is_nil(ok)
        assert.matches("refusing", err)
        -- Ownership unchanged.
        assert.is_true(a.assignments:owned_by_self("orders", 1))
    end)

    it("refuses unknown destination and partitions it does not own", function()
        local a = (make_cluster())
        assert(a.broker:create_topic("orders", 1))
        local ok, err = a.reassigner:_move("orders", 1, "Z")
        assert.is_nil(ok)
        assert.matches("unknown dest", err)

        assert.is_true(a.assignments:set_owner("orders", 1, "B"))
        local ok2, err2 = a.reassigner:_move("orders", 1, "B")
        assert.is_nil(ok2)
        assert.matches("not owned", err2)
    end)

    it("execute() runs local MOVEs and skips non-local actions", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("t1", 1))
        assert(a.broker:create_topic("t2", 1))
        produce_n(a, "t1", 3)
        produce_n(a, "t2", 3)

        local t1 = assert(a.broker:get_topic("t1"))
        local t2 = assert(a.broker:get_topic("t2"))
        local plan = {
            Action.new(Action.ActionType.MOVE, t1, "A", "B", nil, 1),
            Action.new(Action.ActionType.MOVE, t2, "C", "B", nil, 1),  -- not ours
        }
        local ok, err, skipped = a.reassigner:execute(plan)
        assert.is_nil(err)
        assert.is_true(ok)
        assert.are.equal(1, #skipped)
        assert.are.equal("B", a.assignments:owner("t1", 1))
        assert.is_true(partition_leo(b, "t1", 1) > 0)
        assert.is_true(a.assignments:owned_by_self("t2", 1))
    end)
end)

describe("produce routing after a move", function()
    before_each(function() rmdir(BASE) end)

    it("forwards produces for a moved partition to the owner", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        produce_n(a, "orders", 5)
        assert.is_true(a.reassigner:_move("orders", 1, "B"))

        local a_leo_frozen = partition_leo(a, "orders", 1)
        local b_leo_before = partition_leo(b, "orders", 1)

        -- Producing on A must land on B now.
        local pid, offset, err = a.producer:produce("orders",
            msg_m.Message.new("post-move", "value", 0))
        assert.is_nil(err)
        assert.are.equal(1, pid)
        assert.are.equal(b_leo_before, offset)   -- record starts at B's old LEO
        assert.are.equal(a_leo_frozen, partition_leo(a, "orders", 1))
        assert.is_true(partition_leo(b, "orders", 1) > b_leo_before)
    end)

    it("errors when the owner has no configured peer", function()
        local a = (make_cluster())
        assert(a.broker:create_topic("orders", 1))
        assert.is_true(a.assignments:set_owner("orders", 1, "GHOST"))
        local _, _, err = a.producer:produce("orders",
            msg_m.Message.new("k", "v", 0))
        assert.matches("no such peer", err)
    end)
end)

describe("consumer scoping after a move", function()
    before_each(function() rmdir(BASE) end)

    it("stops serving the stale local copy on the source broker", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        produce_n(a, "orders", 3)

        assert.is_true(a.reassigner:_move("orders", 1, "B"))

        local c = consumer_m.Consumer.new(a.broker, "g1")
        assert(c:subscribe("orders"))
        local records = assert(c:poll())
        assert.are.equal(0, #records)

        -- Same group on B sees everything.
        local cb = consumer_m.Consumer.new(b.broker, "g1")
        assert(cb:subscribe("orders"))
        local rb = assert(cb:poll())
        assert.is_true(#rb > 0)
    end)
end)

describe("balance loop", function()
    before_each(function() rmdir(BASE) end)

    local function loop_for(node, opts)
        local base = {
            broker = node.broker, assignments = node.assignments,
            peers = node.peers, self_id = node.id,
            reassigner = node.reassigner,
            emit_metrics = false, min_valid = 1,
        }
        for k, v in pairs(opts or {}) do base[k] = v end
        return BalanceLoop.new(base)
    end

    it("dry-run plans partition-count moves toward the empty peer", function()
        local a, _ = make_cluster()
        for i = 1, 4 do
            assert(a.broker:create_topic("t" .. i, 1))
            produce_n(a, "t" .. i, 2)
        end
        local loop = loop_for(a, { dry_run = true })
        local actions, err = loop:tick()
        assert.is_nil(err)
        assert.are.equal(2, #actions)
        -- Dry run: nothing actually moved.
        assert.are.equal(0, #a.assignments:entries())
    end)

    it("executes the plan end-to-end through the reassigner", function()
        local a, b = make_cluster()
        for i = 1, 4 do
            assert(a.broker:create_topic("t" .. i, 1))
            produce_n(a, "t" .. i, 2)
        end
        local loop = loop_for(a)
        local actions, err = loop:tick()
        assert.is_nil(err)
        assert.are.equal(2, #actions)
        -- Two partitions now live on B, and A's table says so.
        assert.are.equal(2, #a.assignments:entries())
        local moved = 0
        for i = 1, 4 do
            local t = b.broker.topic_manager.topics["t" .. i]
            if t and t.partitions[1].offset > 0 then moved = moved + 1 end
        end
        assert.are.equal(2, moved)
    end)

    it("marks an unreachable peer inactive instead of failing the pass", function()
        local a, _ = make_cluster()
        assert(a.broker:create_topic("t1", 1))
        a.peers.B.loads = function() return nil, "connection refused" end
        local loop = loop_for(a, { dry_run = true })
        local actions, err = loop:tick()
        assert.is_nil(err)
        -- Only one active broker → nothing to balance toward.
        assert.are.equal(0, #actions)
    end)
end)
