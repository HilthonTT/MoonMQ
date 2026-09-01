local broker_m   = require("src.broker")
local producer_m = require("src.broker.producer")
local message_m  = require("src.record.message")
local chaos_m    = require("src.chaos.chaos")
local socket     = require("socket")
local os_utils = require("src.core.os")

local BASE_DIR   = os_utils.IS_WINDOWS and "C:\\Temp\\lua_chaos_test" or "/tmp/lua_chaos_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function drain_partition(partition)
    partition:sync()
    local out = {}
    local off = 0
    while off < partition.offset do
        local msg, next_off, err = partition:read_message(off)
        if err then break end
        out[#out + 1] = msg
        off = next_off
    end
    return out
end

local function build_pipeline(topic_name, failure_rate)
    rmdir(BASE_DIR)
    local broker, berr = broker_m.Broker.new(BASE_DIR)
    assert(not berr, berr)
    local topic, terr = broker:create_topic(topic_name, 1)
    assert(not terr, terr)
    local producer = producer_m.Producer.new(broker, producer_m.AckMode.AckNone)
    local chaos    = chaos_m.ChaosProducer.new(topic_name, producer, failure_rate)
    return broker, topic, producer, chaos
end

describe("ChaosProducer", function()

    before_each(function()
        math.randomseed(0xC0FFEE)
    end)

    after_each(function()
        rmdir(BASE_DIR)
    end)

    describe("MessageLossResilience", function()
        it("drops every message when loss probability is 1.0", function()
            local _, topic, _, chaos = build_pipeline("loss_all", 1.0)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorMessageLoss, 1.0)

            for i = 1, 50 do
                local m = message_m.Message.new("k", "v-" .. i, i)
                local pid, off, err = chaos:produce(m)
                assert.is_nil(err)
                assert.are.equal(-1, pid)
                assert.are.equal(-1, off)
            end

            local on_disk = drain_partition(topic.partitions[1])
            assert.are.equal(0, #on_disk)
            assert.are.equal(50, chaos.stats.lost)
            assert.are.equal(0,  chaos.stats.sent)
        end)

        it("loses only a fraction when failure_rate < 1.0", function()
            local _, topic, _, chaos = build_pipeline("loss_partial", 0.5)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorMessageLoss, 1.0)

            local N = 200
            for i = 1, N do
                chaos:produce(message_m.Message.new("k", "v-" .. i, i))
            end

            local on_disk = drain_partition(topic.partitions[1])
            assert.is_true(#on_disk > 70,  "too few messages survived: " .. #on_disk)
            assert.is_true(#on_disk < 130, "too many messages survived: " .. #on_disk)
            assert.are.equal(N, chaos.stats.attempts)
            assert.are.equal(N, chaos.stats.lost + chaos.stats.sent)
        end)

        it("delivers normally once loss is disabled (system recovers)", function()
            local _, topic, _, chaos = build_pipeline("loss_recover", 1.0)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorMessageLoss, 1.0)

            for i = 1, 10 do
                chaos:produce(message_m.Message.new("k", "before-" .. i, i))
            end
            assert.are.equal(0, #drain_partition(topic.partitions[1]))

            chaos.failure_rate = 0
            for i = 1, 10 do
                local _, _, err = chaos:produce(message_m.Message.new("k", "after-" .. i, i))
                assert.is_nil(err)
            end

            local on_disk = drain_partition(topic.partitions[1])
            assert.are.equal(10, #on_disk)
            for i, m in ipairs(on_disk) do
                assert.are.equal("after-" .. i, m.value)
            end
        end)
    end)

    describe("BackpressureResilience", function()
        it("delivers every message even when every call sleeps", function()
            local _, topic, _, chaos = build_pipeline("backpressure", 1.0)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorNetworkDelay, 1.0)
            chaos.network_delay = 0.002

            local N = 30
            local started = socket.gettime()
            for i = 1, N do
                local _, _, err = chaos:produce(message_m.Message.new("k", "v-" .. i, i))
                assert.is_nil(err)
            end
            local elapsed = socket.gettime() - started

            local on_disk = drain_partition(topic.partitions[1])
            assert.are.equal(N, #on_disk)
            for i, m in ipairs(on_disk) do
                assert.are.equal("v-" .. i, m.value)
            end
            assert.is_true(chaos.stats.delayed >= 1)
            assert.is_true(elapsed < 2.0, "backpressure run took too long: " .. elapsed)
        end)

        it("does not deadlock under sustained slow-path bursts", function()
            local _, topic, _, chaos = build_pipeline("backpressure_mixed", 1.0)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorNetworkDelay, 0.8)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorMessageLoss,  0.2)
            chaos.network_delay = 0.001

            local N = 50
            for i = 1, N do
                local _, _, err = chaos:produce(message_m.Message.new("k", "v-" .. i, i))
                assert.is_nil(err)
            end

            local on_disk = drain_partition(topic.partitions[1])
            assert.are.equal(N, chaos.stats.sent + chaos.stats.lost)
            assert.are.equal(#on_disk, chaos.stats.sent)
        end)
    end)

    describe("NetworkPartitionResilience", function()
        it("delivers messages once the partition heals", function()
            local _, topic, _, chaos = build_pipeline("netpart", 1.0)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorServiceRestart, 1.0)
            chaos.restart_delay = 0.005

            local N = 10
            for i = 1, N do
                local pid, off, err = chaos:produce(message_m.Message.new("k", "v-" .. i, i))
                assert.is_nil(err)
                assert.is_true(pid > 0)
                assert.is_true(off >= 0)
            end

            local on_disk = drain_partition(topic.partitions[1])
            assert.are.equal(N, #on_disk)
            for i, m in ipairs(on_disk) do
                assert.are.equal("v-" .. i, m.value)
            end
            assert.are.equal(N, chaos.stats.restarts)
        end)

        it("survives a partition that heals mid-stream", function()
            local _, topic, _, chaos = build_pipeline("netpart_heal", 1.0)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorServiceRestart, 1.0)
            chaos.restart_delay = 0.002

            for i = 1, 5 do
                chaos:produce(message_m.Message.new("k", "partitioned-" .. i, i))
            end
            assert.are.equal(5, chaos.stats.restarts)

            chaos.failure_rate = 0
            for i = 1, 5 do
                chaos:produce(message_m.Message.new("k", "healed-" .. i, i))
            end

            local on_disk = drain_partition(topic.partitions[1])
            assert.are.equal(10, #on_disk)
            for i = 1, 5 do
                assert.are.equal("partitioned-" .. i, on_disk[i].value)
            end
            for i = 1, 5 do
                assert.are.equal("healed-" .. i, on_disk[5 + i].value)
            end
        end)

        it("propagates errors from the underlying producer", function()
            local _, _, producer = build_pipeline("netpart_err", 1.0)
            local chaos = chaos_m.ChaosProducer.new("nonexistent", producer, 1.0)
            chaos:add_behavior(chaos_m.ChaosBehavior.BehaviorServiceRestart, 1.0)
            chaos.restart_delay = 0.001

            local pid, off, err = chaos:produce(message_m.Message.new("k", "v", 1))
            assert.is_not_nil(err)
            assert.are.equal(-1, pid)
            assert.are.equal(-1, off)
        end)
    end)
end)
