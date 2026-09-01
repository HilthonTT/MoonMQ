local Resource         = require("src.autobalancer.common.resource")
local WindowedSamples  = require("src.autobalancer.model.sample.windowed_samples")

local Replica = {}
Replica.__index = Replica

function Replica.new(topic, partition, broker_id, loads, trusted)
    return setmetatable({
        topic     = topic,
        partition = partition,
        broker_id = broker_id,
        loads     = loads,
        trusted   = trusted,
    }, Replica)
end

function Replica:get_load(resource)
    return self.loads[resource] or 0
end

function Replica:set_load(resource, value)
    self.loads[resource] = value
end

function Replica:is_trusted(resource)
    return self.trusted[resource] == true
end

function Replica:key()
    return self.topic.name .. "-" .. tostring(self.partition)
end

local ReplicaUpdater = {}
ReplicaUpdater.__index = ReplicaUpdater

function ReplicaUpdater.new(topic, partition, broker_id, opts)
    assert(type(partition) == "number", "partition must be a number")
    assert(type(broker_id) == "string", "broker_id must be a string")

    return setmetatable({
        topic     = topic,
        partition = partition,
        broker_id = broker_id,
        opts      = opts or {},
        samples   = {},
    }, ReplicaUpdater)
end

function ReplicaUpdater:_series(resource)
    local s = self.samples[resource]
    if not s then
        s = WindowedSamples.new(self.opts)
        self.samples[resource] = s
    end
    return s
end

function ReplicaUpdater:update(resource, value)
    assert(Resource.is_measured(resource),
        "only measured resources are sampled; PARTITION_COUNT is synthesized")
    self:_series(resource):append(value)
end

function ReplicaUpdater:snapshot(percentile)
    local loads, trusted = {}, {}
    for _, r in ipairs(Resource.VALUES) do
        if r == Resource.PARTITION_COUNT then
            loads[r], trusted[r] = 1, true
        else
            local s = self.samples[r]
            if s then
                loads[r]   = s:snapshot():percentile(percentile)
                trusted[r] = s:is_trusted()
            else
                loads[r], trusted[r] = 0, false
            end
        end
    end
    return Replica.new(self.topic, self.partition, self.broker_id, loads, trusted)
end

ReplicaUpdater.Replica = Replica
return ReplicaUpdater
