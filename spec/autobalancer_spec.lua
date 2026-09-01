local Autobalancer  = require("src.autobalancer")
local Resource      = require("src.autobalancer.common.resource")
local Action        = require("src.autobalancer.common.action")
local Snapshot      = require("src.autobalancer.model.sample.snapshot")
local Windowed      = require("src.autobalancer.model.sample.windowed_samples")
local ClusterModel  = require("src.autobalancer.model.cluster_model")
local Topic         = require("src.storage.topic")

local MB = 1024 * 1024

local function topic(name) return Topic.new(name) end

describe("autobalancer sample layer", function()
    it("Snapshot percentile interpolates between ranks", function()
        local s = Snapshot.new({ 10, 20, 30, 40, 50 })
        assert.are.equal(10, s:percentile(0))
        assert.are.equal(50, s:percentile(1))
        assert.are.equal(30, s:percentile(0.5))
    end)

    it("Snapshot treats an empty window as zero load", function()
        assert.are.equal(0, Snapshot.new({}):percentile(0.9))
    end)

    it("WindowedSamples rings over the window and reports latest", function()
        local w = Windowed.new({ window = 3, min_valid = 2 })
        assert.is_false(w:is_trusted())
        w:append(1); w:append(2)
        assert.is_true(w:is_trusted())
        w:append(3); w:append(4)
        assert.are.equal(3, w:size())
        assert.are.equal(4, w:latest())
        assert.are.equal(2, w:snapshot():percentile(0))
    end)
end)

describe("ClusterModelSnapshot", function()
    it("aggregates broker load from replicas, counting partitions", function()
        local m = ClusterModel.new({ min_valid = 1 })
        m:register_broker("b1")
        local t1, t2 = topic("orders"), topic("events")
        m:register_replica("b1", t1, 1)
        m:register_replica("b1", t2, 1)
        m:update_replica_load("b1", "orders", 1, Resource.NW_IN, 3 * MB)
        m:update_replica_load("b1", "events", 1, Resource.NW_IN, 1 * MB)

        local snap = m:snapshot()
        assert.are.equal(2, snap:broker_load("b1", Resource.PARTITION_COUNT))
        assert.are.equal(4 * MB, snap:broker_load("b1", Resource.NW_IN))
    end)

    it("apply_action MOVE relocates a replica and rebalances loads", function()
        local m = ClusterModel.new({ min_valid = 1 })
        m:register_broker("b1"); m:register_broker("b2")
        local t = topic("orders")
        m:register_replica("b1", t, 1)
        m:update_replica_load("b1", "orders", 1, Resource.NW_IN, 5 * MB)

        local snap = m:snapshot()
        local ok = snap:apply_action(
            Action.new(Action.ActionType.MOVE, t, "b1", "b2", nil, 1))
        assert.is_true(ok)
        assert.are.equal(0, snap:broker_load("b1", Resource.NW_IN))
        assert.are.equal(5 * MB, snap:broker_load("b2", Resource.NW_IN))
        assert.are.equal(1, snap:broker_load("b2", Resource.PARTITION_COUNT))
    end)

    it("apply_action reports an error for an unknown replica", function()
        local m = ClusterModel.new()
        m:register_broker("b1"); m:register_broker("b2")
        local snap = m:snapshot()
        local ok, err = snap:apply_action(
            Action.new(Action.ActionType.MOVE, topic("ghost"), "b1", "b2", nil, 1))
        assert.is_nil(ok)
        assert.is_not_nil(err)
    end)
end)

describe("partition-count goal", function()
    it("evens out replicas across brokers", function()
        local ab = Autobalancer.new({
            emit_metrics = false,
            goals = { network_in = false, network_out = false, disk = false },
        })
        ab.model:register_broker("b1"); ab.model:register_broker("b2")
        for i = 1, 4 do ab.model:register_replica("b1", topic("t" .. i), 1) end

        local plan, snap = ab:detect()
        assert.are.equal(2, #plan)
        assert.are.equal(2, snap:broker_load("b1", Resource.PARTITION_COUNT))
        assert.are.equal(2, snap:broker_load("b2", Resource.PARTITION_COUNT))
    end)

    it("does nothing when already balanced", function()
        local ab = Autobalancer.new({
            emit_metrics = false,
            goals = { network_in = false, network_out = false, disk = false },
        })
        ab.model:register_broker("b1"); ab.model:register_broker("b2")
        ab.model:register_replica("b1", topic("a"), 1)
        ab.model:register_replica("b2", topic("b"), 1)

        assert.are.equal(0, #ab:detect())
    end)
end)

describe("network-in goal", function()
    it("moves hot replicas until throughput is balanced", function()
        local ab = Autobalancer.new({
            emit_metrics = false, min_valid = 1,
            goals = { network_out = false, disk = false, partition_count = false },
        })
        ab.model:register_broker("b1"); ab.model:register_broker("b2")
        for i = 1, 4 do
            local t = topic("hot" .. i)
            ab.model:register_replica("b1", t, 1)
            ab.model:update_replica_load("b1", t.name, 1, Resource.NW_IN, 2 * MB)
        end

        local plan, snap = ab:detect()
        assert.are.equal(2, #plan)
        assert.are.equal(4 * MB, snap:broker_load("b1", Resource.NW_IN))
        assert.are.equal(4 * MB, snap:broker_load("b2", Resource.NW_IN))
    end)

    it("stays idle below the detect threshold", function()
        local ab = Autobalancer.new({
            emit_metrics = false, min_valid = 1,
            goals = { network_out = false, disk = false, partition_count = false,
                      network_in = { detect_threshold = 100 * MB } },
        })
        ab.model:register_broker("b1"); ab.model:register_broker("b2")
        for i = 1, 4 do
            local t = topic("warm" .. i)
            ab.model:register_replica("b1", t, 1)
            ab.model:update_replica_load("b1", t.name, 1, Resource.NW_IN, 2 * MB)
        end
        assert.are.equal(0, #ab:detect())
    end)

    it("ignores untrusted (too few samples) replicas", function()
        local ab = Autobalancer.new({
            emit_metrics = false, min_valid = 5,
            goals = { network_out = false, disk = false, partition_count = false },
        })
        ab.model:register_broker("b1"); ab.model:register_broker("b2")
        for i = 1, 4 do
            local t = topic("new" .. i)
            ab.model:register_replica("b1", t, 1)
            ab.model:update_replica_load("b1", t.name, 1, Resource.NW_IN, 2 * MB)
        end
        assert.are.equal(0, #ab:detect())
    end)
end)

describe("detector run_once", function()
    it("passes the plan to the execute hook", function()
        local executed
        local ab = Autobalancer.new({
            emit_metrics = false,
            goals = { network_in = false, network_out = false, disk = false },
            execute = function(actions) executed = actions; return true end,
        })
        ab.model:register_broker("b1"); ab.model:register_broker("b2")
        for i = 1, 4 do ab.model:register_replica("b1", topic("p" .. i), 1) end

        local plan, err = ab:run_once()
        assert.is_nil(err)
        assert.are.equal(2, #plan)
        assert.are.equal(plan, executed)
    end)

    it("surfaces an execute-hook failure", function()
        local ab = Autobalancer.new({
            emit_metrics = false,
            goals = { network_in = false, network_out = false, disk = false },
            execute = function() return nil, "reassignment refused" end,
        })
        ab.model:register_broker("b1"); ab.model:register_broker("b2")
        for i = 1, 4 do ab.model:register_replica("b1", topic("q" .. i), 1) end

        local _, err = ab:run_once()
        assert.are.equal("reassignment refused", err)
    end)
end)
