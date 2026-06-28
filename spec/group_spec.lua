local brk_m   = require("src.storage.broker")
local group_m = require("src.client.groups")
local os_utils = require("src.core.os")

local STATES = group_m.STATES

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_group_test"
                                      or "/tmp/moonmq_group_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function new_broker_with_topic(topic, parts)
    local broker = assert(brk_m.Broker.new(BASE_DIR))
    assert(broker:create_topic(topic, parts))
    return broker
end

describe("consumer group lifecycle (FSM-driven)", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("starts empty", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")
        assert.are.equal(STATES.EMPTY, g:state())
    end)

    it("reaches stable after a member joins", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        local assigned = assert(g:join("m1", { "orders" }))
        assert.are.equal(STATES.STABLE, g:state())
        -- A lone member owns every partition.
        assert.are.same({ 1, 2, 3, 4 }, assigned["orders"])
    end)

    it("rebalances partitions across members on each join", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        assert(g:join("m1", { "orders" }))
        local a2 = assert(g:join("m2", { "orders" }))
        assert.are.equal(STATES.STABLE, g:state())

        -- range strategy: 4 partitions / 2 members = 2 each.
        assert.are.same({ 1, 2 }, g.members["m1"].partitions["orders"])
        assert.are.same({ 3, 4 }, a2["orders"])
    end)

    it("collapses back to empty when the last member leaves", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        assert(g:join("m1", { "orders" }))
        assert(g:leave("m1"))
        assert.are.equal(STATES.EMPTY, g:state())
    end)

    it("re-stabilizes with survivors when one of several members leaves", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        assert(g:join("m1", { "orders" }))
        assert(g:join("m2", { "orders" }))
        assert(g:leave("m1"))

        assert.are.equal(STATES.STABLE, g:state())
        -- m2 now owns everything again.
        assert.are.same({ 1, 2, 3, 4 }, g.members["m2"].partitions["orders"])
    end)

    it("a join to a missing topic leaves the group untouched", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        local ok, err = g:join("m1", { "orders", "nope" })
        assert.is_nil(ok)
        assert.matches("nope", err)

        -- No phantom member, and the FSM is not stranded in a rebalance.
        assert.are.equal(STATES.EMPTY, g:state())
        assert.is_nil(g.members["m1"])

        -- A subsequent valid join still works cleanly.
        local a = assert(g:join("m1", { "orders" }))
        assert.are.equal(STATES.STABLE, g:state())
        assert.are.same({ 1, 2, 3, 4 }, a["orders"])
    end)

    it("rejects operations once the group is dead", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        assert(g:join("m1", { "orders" }))
        assert(g:close())
        assert.are.equal(STATES.DEAD, g:state())

        local ok, err = g:join("m2", { "orders" })
        assert.is_nil(ok)
        assert.matches("dead", err)

        local hok, herr = g:heartbeat("m1")
        assert.is_false(hok)
        assert.matches("dead", herr)
    end)

    it("evicts stale members and reports who was dropped", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        assert(g:join("m1", { "orders" }))
        assert(g:join("m2", { "orders" }))

        -- Backdate m1's heartbeat past the 30s deadline.
        g.members["m1"].last_heartbeat = g.members["m1"].last_heartbeat - 31

        local expired = g:check_heartbeats()
        assert.are.same({ "m1" }, expired)
        assert.are.equal(STATES.STABLE, g:state())
        assert.is_nil(g.members["m1"])
        assert.are.same({ 1, 2, 3, 4 }, g.members["m2"].partitions["orders"])
    end)

    it("returns to empty when heartbeat eviction drains the group", function()
        local broker = new_broker_with_topic("orders", 4)
        local g = group_m.ConsumerGroup.new(broker, "g1")

        assert(g:join("m1", { "orders" }))
        g.members["m1"].last_heartbeat = g.members["m1"].last_heartbeat - 31

        g:check_heartbeats()
        assert.are.equal(STATES.EMPTY, g:state())
    end)
end)
