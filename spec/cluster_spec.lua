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
local Resource     = require("src.autobalancer.common.resource")

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
        append = function(_, topic, partition, payload, forwarded)
            local out, err = unwrap(cs:_append(topic, partition, payload, forwarded))
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
        push_offsets = function(_, topic, partition, offsets)
            local out, err = unwrap(cs:_offsets(json.encode(
                { topic = topic, partition = partition, offsets = offsets })))
            if not out then return nil, err end
            return out.applied
        end,
        loads = function(_)
            local out, err = unwrap(cs:_loads())
            if not out then return nil, err end
            return out.loads
        end,
        txn_enroll = function(_, txn, topic, partition, first_offset)
            local out, err = unwrap(cs:_txn_enroll(json.encode({
                txn = txn, topic = topic, partition = partition,
                first_offset = first_offset })))
            if not out then return nil, err end
            return true
        end,
        txn_resolve = function(_, txn, topic, partition, opts)
            opts = opts or {}
            local out, err = unwrap(cs:_txn_resolve(json.encode({
                txn = txn, topic = topic, partition = partition,
                aborted = opts.aborted or false, pid = opts.pid,
                epoch = opts.epoch, first = opts.first, upto = opts.upto })))
            if not out then return nil, err end
            return true
        end,
        group_join = function(_, group, member, topics, origin)
            local out, err = unwrap(cs:_group_join(json.encode({
                group = group, member = member, topics = topics, origin = origin })))
            if not out then return nil, err, "internal" end
            if out.ok == false then return nil, out.reason, out.code end
            return out.assignment
        end,
        group_heartbeat = function(_, group, member)
            local out, err = unwrap(cs:_group_heartbeat(json.encode({
                group = group, member = member })))
            if not out then return nil, err end
            if out.ok == false then return nil, out.reason end
            return out.assignment or {}
        end,
        group_leave = function(_, group, member)
            local out, err = unwrap(cs:_group_leave(json.encode({
                group = group, member = member })))
            if not out then return nil, err end
            return true
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

describe("offset migration on MOVE", function()
    before_each(function() rmdir(BASE) end)

    it("ships committed offsets to the new owner", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        produce_n(a, "orders", 5)

        -- Consume 3 records on A; auto-commit persists the position.
        local c = consumer_m.Consumer.new(a.broker, "g1")
        assert(c:subscribe("orders"))
        for _ = 1, 3 do assert(c:poll()) end
        local committed = a.broker:fetch_offset("g1", "orders", 1)
        assert.is_number(committed)
        assert.is_true(committed > 0)

        assert.is_true(a.reassigner:_move("orders", 1, "B"))

        -- B now holds g1's position; a consumer there resumes, not restarts.
        assert.are.equal(committed, b.broker:fetch_offset("g1", "orders", 1))
        local cb = consumer_m.Consumer.new(b.broker, "g1")
        assert(cb:subscribe("orders"))
        local seen = {}
        for _ = 1, 10 do
            for _, r in ipairs(assert(cb:poll())) do seen[#seen + 1] = r.value end
        end
        assert.are.same({ "v4", "v5" }, seen)
    end)

    it("never rolls back a higher offset already committed on the dest", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        produce_n(a, "orders", 3)

        local c = consumer_m.Consumer.new(a.broker, "g1")
        assert(c:subscribe("orders"))
        assert(c:poll())   -- commit a small offset on A
        local a_committed = a.broker:fetch_offset("g1", "orders", 1)

        -- Simulate the group having advanced further on B already.
        assert(b.broker:create_topic("orders", 1))
        assert(b.broker:commit_offset("g1", "orders", 1, a_committed + 1000))

        -- Push A's (lower) snapshot directly: dest keeps the higher value.
        local applied = assert(a.peers.B:push_offsets("orders", 1,
            { g1 = a_committed }))
        assert.are.equal(0, applied)
        assert.are.equal(a_committed + 1000, b.broker:fetch_offset("g1", "orders", 1))
    end)

    it("moves with no committed offsets still succeed", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        produce_n(a, "orders", 2)
        assert.is_true(a.reassigner:_move("orders", 1, "B"))
        assert.is_nil(b.broker:fetch_offset("nobody", "orders", 1))
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

describe("transactional produce to a peer-owned partition", function()
    before_each(function() rmdir(BASE) end)

    -- Drain every available record value (poll returns at most one record
    -- per partition per pass).
    local function drain(consumer, max_polls)
        local all = {}
        for _ = 1, max_polls or 20 do
            local records = assert(consumer:poll())
            if #records == 0 then break end
            for _, r in ipairs(records) do all[#all + 1] = r.value end
        end
        return all
    end

    -- A coordinates the transaction; B owns orders/partition-1.
    local function txn_setup()
        local a, b = make_cluster()
        a.broker.transactions:set_router(a.router)
        assert(a.broker:create_topic("orders", 1))
        assert(b.broker:create_topic("orders", 1))
        assert(a.assignments:set_owner("orders", 1, "B"))
        local pid, epoch =
            assert(a.broker.producer_state:get_or_create_producer("txn-x"))
        assert(a.broker.transactions:begin("txn-x", pid, epoch))
        return a, b, pid, epoch
    end

    -- Transactional produce exactly like the produce handler: pre_append
    -- enrols the partition (remotely here) before the forwarded append.
    local function txn_forward_produce(a, pid, epoch, key, value)
        local msg = msg_m.Message.new(key, value, 1, msg_m.ATTR_TXN, pid, epoch)
        return a.producer:produce("orders", msg, {
            pre_append = function(topic_name, partition_id, _p, remote)
                return a.broker.transactions:add_partition(
                    "txn-x", pid, epoch, topic_name, partition_id, remote)
            end,
        })
    end

    it("forwards the record, floors the owner's LSO, and commits atomically", function()
        local a, b, pid, epoch = txn_setup()

        local part_id, _, err = txn_forward_produce(a, pid, epoch, "k1", "cross")
        assert.is_nil(err)
        assert.are.equal(1, part_id)
        assert.is_true(partition_leo(b, "orders", 1) > 0, "record lands on the owner")
        assert.are.equal(0, partition_leo(a, "orders", 1), "stale local log untouched")

        -- While the txn is unresolved, the OWNER's read_committed consumers
        -- must not see the record (remote enrolment floored B's LSO).
        local rc = consumer_m.Consumer.new(b.broker, "g-rc",
            { isolation = "read_committed" })
        assert(rc:subscribe("orders"))
        assert.are.same({}, drain(rc))

        -- Commit on the coordinator: marker + resolve land on the owner.
        assert(a.broker.transactions:end_txn("txn-x", pid, epoch, true))
        assert.are.same({ "cross" }, drain(rc))
    end)

    it("abort records the aborted range in the owner's abort index", function()
        local a, b, pid, epoch = txn_setup()

        local _, _, perr = txn_forward_produce(a, pid, epoch, "k1", "doomed")
        assert.is_nil(perr)
        assert(a.broker.transactions:end_txn("txn-x", pid, epoch, false))

        -- The owner filters the aborted record for read_committed readers
        -- from its OWN durable abort index (fed by /cluster/txn/resolve)...
        local rc = consumer_m.Consumer.new(b.broker, "g-rc",
            { isolation = "read_committed" })
        assert(rc:subscribe("orders"))
        assert.are.same({}, drain(rc))
        assert.is_true(#b.broker.transactions.aborts:entries("orders", 1) > 0)

        -- ...while read_uncommitted still sees it.
        local ru = consumer_m.Consumer.new(b.broker, "g-ru")
        assert(ru:subscribe("orders"))
        assert.are.same({ "doomed" }, drain(ru))
    end)

    it("fails the produce when remote enrolment fails (LSO safety)", function()
        local a, b, pid, epoch = txn_setup()
        a.peers.B.txn_enroll = function() return nil, "connection refused" end

        local _, _, err = txn_forward_produce(a, pid, epoch, "k1", "v")
        assert.is_not_nil(err)
        assert.matches("enrol", err)
        -- The record must NOT have been forwarded to the owner.
        assert.are.equal(0, partition_leo(b, "orders", 1))
    end)
end)

describe("cluster-wide consumer groups", function()
    local GroupCoordinator = require("src.server.group_coordinator")

    before_each(function() rmdir(BASE) end)

    -- make_cluster plus a cluster-mode GroupCoordinator per node, wired into
    -- each node's ClusterServer (as Server:start does in production).
    local function group_cluster()
        local a, b = make_cluster()
        for _, node in ipairs({ a, b }) do
            node.coordinator = GroupCoordinator.new(node.broker, {
                max_groups = 16,
                cluster    = { self_id = node.id, peers = node.peers },
            })
            node.cluster_server.group_coordinator = node.coordinator
        end
        return a, b
    end

    -- Find a group id whose coordinator is `want` — hashing is deterministic,
    -- so probe suffixes until one lands there.
    local function group_for(coordinator, want)
        for i = 1, 64 do
            local gid = "g-" .. i
            if coordinator:coordinator_for(gid) == want then return gid end
        end
        error("no group id hashed to " .. want)
    end

    it("every broker agrees on each group's coordinator", function()
        local a, b = group_cluster()
        for i = 1, 20 do
            local gid = "group-" .. i
            assert.are.equal(a.coordinator:coordinator_for(gid),
                             b.coordinator:coordinator_for(gid))
        end
    end)

    it("membership spans brokers: a forwarded join rebalances remote members", function()
        local a, b = group_cluster()
        assert(a.broker:create_topic("orders", 2))
        assert(b.broker:create_topic("orders", 2))
        -- Coordinator is B; both partitions are owned by A (B's view must say
        -- so — ownership-aware assignment runs on the coordinator).
        local gid = group_for(a.coordinator, "B")
        assert(b.assignments:set_owner("orders", 1, "A"))
        assert(b.assignments:set_owner("orders", 2, "A"))

        -- First member joins via A (forwarded to B): gets both partitions.
        local asg1 = assert(a.coordinator:join(gid, "mA1", { "orders" }))
        table.sort(asg1.orders)
        assert.are.same({ 1, 2 }, asg1.orders)
        assert.is_true(a.coordinator:member_alive(gid, "mA1"))

        -- Second member joins via A too: coordinator B rebalances both.
        local asg2 = assert(a.coordinator:join(gid, "mA2", { "orders" }))
        assert.are.equal(1, #asg2.orders)

        -- B holds the membership (with origins), not A.
        local group = b.coordinator:get(gid)
        assert.is_not_nil(group)
        assert.are.equal("A", group.members.mA1.origin)
        assert.is_nil(a.coordinator:get(gid))

        -- mA1's cached assignment is stale until its next heartbeat, which
        -- carries the post-rebalance assignment back.
        assert.is_true(a.coordinator:heartbeat(gid, "mA1"))
        local cached = a.coordinator.remote_members[gid].mA1
        assert.are.equal(1, #cached.orders)
        local total = #cached.orders + #asg2.orders
        assert.are.equal(2, total, "the two members split the partitions")
    end)

    it("assignment is ownership-aware: members only get partitions their broker serves", function()
        local a, b = group_cluster()
        assert(a.broker:create_topic("orders", 2))
        assert(b.broker:create_topic("orders", 2))
        local gid = group_for(a.coordinator, "B")
        -- B's view: partition 1 owned by A, partition 2 by B (default self).
        assert(b.assignments:set_owner("orders", 1, "A"))

        local asg_a = assert(a.coordinator:join(gid, "mA", { "orders" }))   -- via A
        local asg_b = assert(b.coordinator:join(gid, "mB", { "orders" }))   -- local on B
        assert.are.same({ 1 }, asg_a.orders, "A-side member gets A-owned partition")
        assert.are.same({ 2 }, asg_b.orders, "B-side member gets B-owned partition")
    end)

    it("eviction on the coordinator fences the remote member on its next heartbeat", function()
        local a, b = group_cluster()
        assert(a.broker:create_topic("orders", 1))
        assert(b.broker:create_topic("orders", 1))
        local gid = group_for(a.coordinator, "B")

        assert(a.coordinator:join(gid, "mA", { "orders" }))
        assert.is_true(a.coordinator:member_alive(gid, "mA"))

        -- Coordinator-side eviction (as the reaper would on heartbeat timeout).
        b.coordinator:get(gid):leave("mA")

        local ok = a.coordinator:heartbeat(gid, "mA")
        assert.is_nil(ok)
        assert.is_false(a.coordinator:member_alive(gid, "mA"),
            "lapsed membership must fence commits on the origin broker")
    end)

    it("leave via the origin broker removes the member on the coordinator", function()
        local a, b = group_cluster()
        assert(a.broker:create_topic("orders", 1))
        assert(b.broker:create_topic("orders", 1))
        local gid = group_for(a.coordinator, "B")

        assert(a.coordinator:join(gid, "mA", { "orders" }))
        assert(a.coordinator:leave(gid, "mA"))
        assert.is_false(a.coordinator:member_alive(gid, "mA"))
        local group = b.coordinator:get(gid)
        assert.is_true(group == nil or group.members.mA == nil)
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

    it("feeds NW_IN/NW_OUT byte rates from the traffic counters", function()
        local a, _ = make_cluster()
        assert(a.broker:create_topic("t1", 1))
        produce_n(a, "t1", 2)

        local loop = loop_for(a, { dry_run = true })
        assert(loop:tick())
        -- First pass only primes the baselines: no rate samples yet.
        local snap1 = loop.ab.model:snapshot()
        assert.are.equal(0, snap1:broker_load("A", Resource.NW_IN))

        -- Traffic lands between passes; the next tick differences the
        -- cumulative counters into rates.
        a.broker.traffic:add_in("t1", 1, 10 * 1024 * 1024)
        a.broker.traffic:add_out("t1", 1, 5 * 1024 * 1024)
        assert(loop:tick())

        local snap2 = loop.ab.model:snapshot()
        assert.is_true(snap2:broker_load("A", Resource.NW_IN) > 0)
        assert.is_true(snap2:broker_load("A", Resource.NW_OUT) > 0)
        assert.is_true(snap2:broker_load("A", Resource.NW_IN)
            > snap2:broker_load("A", Resource.NW_OUT))
    end)

    it("counts produce bytes locally and on the forwarding owner", function()
        local a, b = make_cluster()
        assert(a.broker:create_topic("orders", 1))
        produce_n(a, "orders", 1)
        local bin = a.broker.traffic:totals("orders", 1)
        assert.is_true(bin > 0)

        -- After a move, produces on A are forwarded: the OWNER (B) counts
        -- them as produce traffic; the migration copy itself is not counted.
        assert.is_true(a.reassigner:_move("orders", 1, "B"))
        local b_bin_after_move = b.broker.traffic:totals("orders", 1)
        assert.are.equal(0, b_bin_after_move)

        assert(a.producer:produce("orders", msg_m.Message.new("k", "forwarded", 0)))
        local b_bin = b.broker.traffic:totals("orders", 1)
        assert.is_true(b_bin > 0)
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
