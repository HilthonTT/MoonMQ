local Reactor       = require("src.server.reactor")
local broker_m      = require("src.broker")
local prd_m         = require("src.broker.producer")
local message       = require("src.record.message")
local os_utils      = require("src.core.os")
local Group         = require("src.replication.group")
local Fetcher       = require("src.replication.fetcher")
local ReplicaServer = require("src.server.replica_server")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\lua_replication_failover_test"
                                      or "/tmp/lua_replication_failover_test"

local IDS   = { "1", "2", "3" }
local PORTS = { ["1"] = 19521, ["2"] = 19522, ["3"] = 19523 }

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function close_broker(b)
    for _, t in pairs(b.topic_manager.topics) do
        for _, p in ipairs(t.partitions) do p:close() end
    end
end

local function values(b, topic)
    local t = b.topic_manager.topics[topic]
    if not t then return {} end
    local p = t.partitions[1]
    local out = {}
    local off = p:oldest_offset()
    while off < p.offset do
        local msg, next_offset, _, at = p:read_message(off)
        if not msg then break end
        out[#out + 1] = msg.value
        off = (at and at + 1) or next_offset
    end
    return out
end

local function make_member(reactor, id, running)
    local dir = BASE_DIR .. "/" .. id
    local broker = assert(broker_m.Broker.new(dir, {
        transactions = { defer_recovery = true },
    }))

    local members = {}
    for _, other in ipairs(IDS) do
        members[#members + 1] = {
            id             = other,
            address        = "127.0.0.1:" .. PORTS[other],
            client_address = "127.0.0.1:" .. (PORTS[other] + 100),
        }
    end

    local group = assert(Group.new({
        id             = id,
        data_dir       = dir,
        broker         = broker,
        reactor        = reactor,
        members        = members,
        client_address = "127.0.0.1:" .. (PORTS[id] + 100),
        preferred      = id == "1",
        election_min   = 1.0,
        election_max   = 1.6,
        heartbeat_s    = 0.15,
        rpc_timeout    = 0.8,
        commit_wait    = 5,
        isr_lag_s      = 1.5,
        ack_timeout    = 8,
        max_fetch_wait = 0.1,
    }))
    Fetcher.new(group)
    group.on_promote = function()
        local ok, err = broker:reload_state()
        if not ok then return nil, err end
        return broker.transactions:recover()
    end

    local member = { id = id, broker = broker, group = group,
                     reachable = true, alive = false, generation = 0 }
    local rs = ReplicaServer.new({
        reactor = reactor, broker = broker, port = PORTS[id], group = group,
    })
    assert(reactor:listen("127.0.0.1", PORTS[id], function(sock)
        if not member.reachable then
            pcall(function() sock:close() end)
            return
        end
        rs:_handle(sock)
    end))

    function member.start()
        member.alive = true
        member.reachable = true
        member.generation = member.generation + 1
        local generation = member.generation
        reactor:spawn(function()
            group:run(function()
                return running() and member.alive and member.generation == generation
            end)
        end)
    end

    function member.kill()
        member.alive = false
        member.reachable = false
    end

    return member
end

describe("replication failover", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("elects a new leader from the in-sync replicas, keeps acked records, "
        .. "and truncates the old leader's unreplicated tail", function()
        local reactor = Reactor.new()
        local running = true
        local function is_running() return running end

        local members = {}
        for _, id in ipairs(IDS) do
            members[#members + 1] = make_member(reactor, id, is_running)
        end

        local function leaders()
            local out = {}
            for _, m in ipairs(members) do
                if m.alive and m.group:is_leader() then out[#out + 1] = m end
            end
            return out
        end

        local function wait_until(pred, seconds)
            local deadline = require("socket").gettime() + seconds
            while require("socket").gettime() < deadline do
                if pred() then return true end
                reactor:sleep(0.05)
            end
            return pred()
        end

        local outcome = {}
        reactor:spawn(function()
            local ok, err = pcall(function()
                for _, m in ipairs(members) do m.start() end

                assert(wait_until(function() return #leaders() == 1 end, 10),
                    "no first leader")
                local first = leaders()[1]
                outcome.first = first.id
                local first_epoch = first.group.state.epoch

                assert(first.broker:create_topic("orders", 1))
                assert(wait_until(function()
                    for _, m in ipairs(members) do
                        if #m.group.state.isr ~= 3 then return false end
                    end
                    return true
                end, 10), "isr never grew to all three replicas")

                local producer = prd_m.Producer.new(first.broker, prd_m.AckMode.AckAll,
                    { replicator = first.group })
                for i = 1, 5 do
                    local _, _, perr = producer:produce("orders",
                        message.Message.new("k", "acked-" .. i, 0))
                    assert(perr == nil, perr)
                end
                assert(first.broker:commit_offset("billing", "orders", 1, 3))

                for _, m in ipairs(members) do
                    assert.are.same(values(first.broker, "orders"), values(m.broker, "orders"))
                end

                first.reachable = false
                reactor:sleep(0.5)
                local p = first.broker.topic_manager.topics.orders.partitions[1]
                assert(p:write_message(message.Message.new("k", "unreplicated", 0)))
                reactor:sleep(0.3)
                first.kill()

                assert(wait_until(function() return #leaders() == 1 end, 10),
                    "no leader after the first one died")
                local second = leaders()[1]
                outcome.second = second.id
                outcome.epoch_advanced = second.group.state.epoch > first_epoch

                outcome.second_values = values(second.broker, "orders")
                outcome.committed = second.broker:fetch_offset("billing", "orders", 1)

                local producer2 = prd_m.Producer.new(second.broker, prd_m.AckMode.AckAll,
                    { replicator = second.group })
                for i = 6, 7 do
                    local _, _, perr = producer2:produce("orders",
                        message.Message.new("k", "acked-" .. i, 0))
                    assert(perr == nil, perr)
                end

                first.start()
                assert(wait_until(function()
                    local current = leaders()[1]
                    if not current or #current.group.state.isr ~= 3 then return false end
                    local want = table.concat(values(current.broker, "orders"), ",")
                    for _, m in ipairs(members) do
                        if table.concat(values(m.broker, "orders"), ",") ~= want then
                            return false
                        end
                    end
                    return true
                end, 20), "the replicas never converged after the old leader returned")
                outcome.final_leader = leaders()[1].id
                outcome.old_leader_values = values(first.broker, "orders")
                outcome.old_leader_follows = first.group.state.leader
            end)
            if not ok then outcome.error = err end
            running = false
            reactor:sleep(0.3)
            reactor:stop()
        end)

        reactor:spawn(function()
            reactor:sleep(60)
            running = false
            reactor:stop()
        end)

        reactor:run()
        reactor:shutdown()
        for _, m in ipairs(members) do close_broker(m.broker) end

        assert.is_nil(outcome.error)
        assert.are.equal("1", outcome.first)
        assert.are_not.equal("1", outcome.second)
        assert.is_true(outcome.epoch_advanced)
        assert.are.same({ "acked-1", "acked-2", "acked-3", "acked-4", "acked-5" },
            outcome.second_values)
        assert.are.equal(3, outcome.committed)
        assert.are.same({ "acked-1", "acked-2", "acked-3", "acked-4", "acked-5",
                          "acked-6", "acked-7" }, outcome.old_leader_values)
        assert.are.equal(outcome.final_leader, outcome.old_leader_follows)
    end)
end)

describe("replication group state machine", function()
    local Node = require("src.cluster.raft.node")

    it("seeds the in-sync set with the first leader and ignores stale isr changes", function()
        local state = { epoch = 0, isr = {} }
        Group.reduce(state, { kind = Node.KIND_CONTROLLER, data = { leader = "1", term = 2 } })
        assert.are.same({ "1" }, state.isr)
        assert.are.equal(2, state.epoch)

        Group.reduce(state, { kind = Group.KIND_ISR, data = { epoch = 2, isr = { "1", "2" } } })
        assert.are.same({ "1", "2" }, state.isr)

        Group.reduce(state, { kind = Group.KIND_ISR, data = { epoch = 1, isr = { "3" } } })
        assert.are.same({ "1", "2" }, state.isr)

        Group.reduce(state, { kind = Node.KIND_CONTROLLER, data = { leader = "2", term = 5 } })
        assert.are.equal("2", state.leader)
        assert.are.same({ "1", "2" }, state.isr)
    end)
end)
