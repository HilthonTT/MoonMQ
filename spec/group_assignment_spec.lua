-- Proves consumer-group assignment is ENFORCED on the read path: a member
-- only polls the partitions the coordinator assigned it. Before the fix the
-- coordinator computed assignments but the Consumer ignored them and every
-- member read every partition (the group guarantee was decorative). See
-- Consumer:set_assignment / Consumer:owns and Server:_apply_group_assignment.

local brk_m      = require("src.storage.broker")
local group_m    = require("src.client.groups")
local consumer_m = require("src.client.consumer")
local msg_m      = require("src.record.message")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_group_assign_test"
                                      or "/tmp/moonmq_group_assign_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

-- Write one record straight into a specific partition so we know exactly which
-- partition each record lives in (the producer's hash partitioner wouldn't let
-- us target partitions deterministically).
local function seed_partition(broker, topic, pid, value)
    local t = assert(broker:get_topic(topic))
    local part = t.partitions[pid]
    local _, err = part:write_message(msg_m.Message.new("k", value, 1000))
    assert(not err, err)
end

-- Collect the set of partition ids a consumer returns across one poll per
-- partition (poll reads at most one record per partition per call).
local function polled_partitions(consumer)
    local records = assert(consumer:poll())
    local seen = {}
    for _, r in ipairs(records) do seen[r.partition] = true end
    return seen
end

describe("consumer-group assignment enforcement", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function setup()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 3))
        for pid = 1, 3 do
            seed_partition(broker, "orders", pid, "v" .. pid)
        end
        return broker
    end

    it("splits partitions so two members read disjoint sets", function()
        local broker = setup()
        local g = group_m.ConsumerGroup.new(broker, "g1")
        assert(g:join("m1", { "orders" }))
        assert(g:join("m2", { "orders" }))

        -- range strategy: 3 partitions / 2 members -> m1:{1,2}, m2:{3}
        assert.are.same({ 1, 2 }, g.members["m1"].partitions["orders"])
        assert.are.same({ 3 },    g.members["m2"].partitions["orders"])

        local c1 = consumer_m.Consumer.new(broker, "g1")
        assert(c1:subscribe("orders"))
        c1:set_assignment(g.members["m1"].partitions)

        local c2 = consumer_m.Consumer.new(broker, "g1")
        assert(c2:subscribe("orders"))
        c2:set_assignment(g.members["m2"].partitions)

        local p1 = polled_partitions(c1)
        local p2 = polled_partitions(c2)

        -- Each member reads exactly its assignment — no overlap, full coverage.
        assert.is_true(p1[1] and p1[2] and not p1[3], "m1 must read {1,2} only")
        assert.is_true(p2[3] and not p2[1] and not p2[2], "m2 must read {3} only")
    end)

    it("reads every partition when no assignment is set (raw fetch)", function()
        local broker = setup()
        local c = consumer_m.Consumer.new(broker, "g1")
        assert(c:subscribe("orders"))
        -- No set_assignment call: backward-compatible "own everything".
        local seen = polled_partitions(c)
        assert.is_true(seen[1] and seen[2] and seen[3], "must read all 3")
    end)

    it("owns nothing after being assigned an empty set (evicted member)", function()
        local broker = setup()
        local c = consumer_m.Consumer.new(broker, "g1")
        assert(c:subscribe("orders"))
        c:set_assignment({})  -- what Server:_apply_group_assignment pins an evicted member to
        local records = assert(c:poll())
        assert.are.equal(0, #records)
    end)
end)
