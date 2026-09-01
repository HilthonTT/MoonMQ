local brk_m      = require("src.broker")
local consumer_m = require("src.broker.consumer")
local message    = require("src.record.message")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_assign_test"
                                      or "/tmp/moonmq_assign_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function seed_all_partitions(broker, topic_name)
    local topic = assert(broker:get_topic(topic_name))
    for pid = 1, #topic.partitions do
        assert(topic.partitions[pid]:write_message(
            message.Message.new("k", "p" .. pid, 1)))
    end
    return #topic.partitions
end

local function partitions_seen(records)
    local set = {}
    for _, r in ipairs(records) do set[r.partition] = true end
    return set
end

describe("consumer group partition assignment", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("reads every partition when no assignment is set", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        seed_all_partitions(broker, "orders")

        local c = consumer_m.Consumer.new(broker, "g")
        assert(c:subscribe("orders"))
        local recs = assert(c:poll())
        local seen = partitions_seen(recs)
        for pid = 1, 4 do assert.is_true(seen[pid], "missing partition " .. pid) end
    end)

    it("restricts each member to its assigned partitions (no overlap, full cover)", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        seed_all_partitions(broker, "orders")

        local c1 = consumer_m.Consumer.new(broker, "g")
        assert(c1:subscribe("orders"))
        c1:set_assignment({ orders = { 1, 2 } })

        local c2 = consumer_m.Consumer.new(broker, "g")
        assert(c2:subscribe("orders"))
        c2:set_assignment({ orders = { 3, 4 } })

        local s1 = partitions_seen(assert(c1:poll()))
        local s2 = partitions_seen(assert(c2:poll()))

        assert.is_true(s1[1] and s1[2], "m1 should own 1,2")
        assert.is_nil(s1[3]); assert.is_nil(s1[4])
        assert.is_true(s2[3] and s2[4], "m2 should own 3,4")
        assert.is_nil(s2[1]); assert.is_nil(s2[2])
    end)

    it("reads nothing under an empty assignment (e.g. a reaped member)", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        seed_all_partitions(broker, "orders")

        local c = consumer_m.Consumer.new(broker, "g")
        assert(c:subscribe("orders"))
        c:set_assignment({})
        local recs = assert(c:poll())
        assert.are.equal(0, #recs)
    end)

    it("honours a reassignment on the next poll", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        seed_all_partitions(broker, "orders")

        local c = consumer_m.Consumer.new(broker, "g")
        assert(c:subscribe("orders"))

        c:set_assignment({ orders = { 1 } })
        assert.is_true(partitions_seen(assert(c:poll()))[1])

        c:set_assignment({ orders = { 3 } })
        local s = partitions_seen(assert(c:poll()))
        assert.is_true(s[3], "should read newly-assigned partition 3")
        assert.is_nil(s[1], "should no longer read partition 1")
    end)
end)
