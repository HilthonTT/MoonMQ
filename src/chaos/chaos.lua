local socket = require("socket")
local prd_m  = require("src.broker.producer")
local time_m = require("src.core.time")
local msg_m  = require("src.record.message")

local M = {}

local ChaosBehavior = {
    BehaviorNetworkDelay   = 0,
    BehaviorMessageLoss    = 1,
    BehaviorDuplication    = 2,
    BehaviorCorruption     = 3,
    BehaviorReordering     = 4,
    BehaviorServiceRestart = 5,
}
M.ChaosBehavior = ChaosBehavior

local ChaosProducer = {}
ChaosProducer.__index = ChaosProducer

function ChaosProducer.new(topic_name, producer, failure_rate)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(getmetatable(producer) == prd_m.Producer, "producer must be a Producer instance")
    assert(type(failure_rate) == "number", "failure_rate must be a number")

    return setmetatable({
        topic_name     = topic_name,
        wrapped        = producer,
        behaviors      = {},
        failure_rate   = failure_rate,
        network_delay  = 100 * time_m.MILLISECOND,
        restart_delay  = 500 * time_m.MILLISECOND,
        message_store  = {},
        stats = {
            attempts   = 0,
            sent       = 0,
            lost       = 0,
            delayed    = 0,
            duplicated = 0,
            corrupted  = 0,
            reordered  = 0,
            restarts   = 0,
        },
    }, ChaosProducer)
end

function ChaosProducer:add_behavior(behavior, probability)
    assert(type(behavior) == "number", "behavior must be a number")
    assert(type(probability) == "number", "probability must be a number")
    self.behaviors[behavior] = probability
end

local function pick_behavior(behaviors)
    local fired = {}
    for behavior, probability in pairs(behaviors) do
        if math.random() < probability then
            fired[#fired + 1] = behavior
        end
    end
    if #fired == 0 then return nil end
    return fired[math.random(#fired)]
end

local function corrupt_value(message)
    if #message.value == 0 then return end
    local pos     = math.random(1, #message.value)
    local b       = string.byte(message.value, pos)
    local flipped = (b ~ 0xFF) & 0xFF
    message.value = message.value:sub(1, pos - 1)
                 .. string.char(flipped)
                 .. message.value:sub(pos + 1)
end

function ChaosProducer:produce(message)
    assert(getmetatable(message) == msg_m.Message, "message must be a Message instance")
    self.stats.attempts = self.stats.attempts + 1

    if math.random() < self.failure_rate then
        local behavior = pick_behavior(self.behaviors)

        if behavior == ChaosBehavior.BehaviorMessageLoss then
            self.stats.lost = self.stats.lost + 1
            return -1, -1, nil

        elseif behavior == ChaosBehavior.BehaviorNetworkDelay then
            socket.sleep(math.random() * self.network_delay)
            self.stats.delayed = self.stats.delayed + 1

        elseif behavior == ChaosBehavior.BehaviorDuplication then
            local pid, off, perr = self.wrapped:produce(self.topic_name, message)
            if perr then return -1, -1, perr end
            self.wrapped:produce(self.topic_name, message)
            self.stats.duplicated = self.stats.duplicated + 1
            self.stats.sent       = self.stats.sent + 1
            return pid, off, nil

        elseif behavior == ChaosBehavior.BehaviorCorruption then
            corrupt_value(message)
            self.stats.corrupted = self.stats.corrupted + 1

        elseif behavior == ChaosBehavior.BehaviorReordering then
            self.message_store[#self.message_store + 1] = message
            if #self.message_store > 1 and math.random() < 0.3 then
                for i = #self.message_store, 2, -1 do
                    local j = math.random(i)
                    self.message_store[i], self.message_store[j] =
                        self.message_store[j], self.message_store[i]
                end
                for _, m in ipairs(self.message_store) do
                    self.wrapped:produce(self.topic_name, m)
                end
                self.stats.reordered = self.stats.reordered + #self.message_store
                self.stats.sent      = self.stats.sent + #self.message_store
                self.message_store   = {}
            end
            return -1, -1, nil

        elseif behavior == ChaosBehavior.BehaviorServiceRestart then
            socket.sleep(self.restart_delay)
            self.stats.restarts = self.stats.restarts + 1
            local pid, off, perr = self.wrapped:produce(self.topic_name, message)
            if perr then return -1, -1, perr end
            self.stats.sent = self.stats.sent + 1
            return pid, off, nil
        end
    end

    local pid, off, perr = self.wrapped:produce(self.topic_name, message)
    if perr then return -1, -1, perr end
    self.stats.sent = self.stats.sent + 1
    return pid, off, nil
end

function ChaosProducer:flush()
    for _, m in ipairs(self.message_store) do
        self.wrapped:produce(self.topic_name, m)
        self.stats.sent = self.stats.sent + 1
    end
    self.message_store = {}
end

function ChaosProducer:close()
    self:flush()
end

M.ChaosProducer = ChaosProducer
M.ChaosBehavior = ChaosBehavior

return M
