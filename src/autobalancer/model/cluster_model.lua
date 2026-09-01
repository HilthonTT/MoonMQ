local Resource             = require("src.autobalancer.common.resource")
local BrokerUpdater        = require("src.autobalancer.model.broker_updater")
local ReplicaUpdater       = require("src.autobalancer.model.replica_updater")
local ClusterModelSnapshot = require("src.autobalancer.model.cluster_snapshot")

local ClusterModel = {}
ClusterModel.__index = ClusterModel

local DEFAULT_PERCENTILE = 0.9

function ClusterModel.new(opts)
    opts = opts or {}
    return setmetatable({
        percentile   = opts.percentile or DEFAULT_PERCENTILE,
        sample_opts  = { window = opts.window, min_valid = opts.min_valid },
        brokers      = {},
        replicas     = {},
    }, ClusterModel)
end

function ClusterModel:register_broker(id, opts)
    if not self.brokers[id] then
        self.brokers[id] = BrokerUpdater.new(id, opts)
        self.replicas[id] = self.replicas[id] or {}
    elseif opts then
        if opts.rack ~= nil then self.brokers[id].rack = opts.rack end
        if opts.active ~= nil then self.brokers[id]:set_active(opts.active) end
    end
    return self.brokers[id]
end

function ClusterModel:set_broker_active(id, active)
    local b = self.brokers[id]
    if b then b:set_active(active) end
end

local function replica_key(topic_name, partition)
    return topic_name .. "-" .. tostring(partition)
end

function ClusterModel:register_replica(broker_id, topic, partition)
    assert(self.brokers[broker_id],
        "register broker before its replicas: " .. tostring(broker_id))
    local bucket = self.replicas[broker_id]
    local key = replica_key(topic.name, partition)
    if not bucket[key] then
        bucket[key] = ReplicaUpdater.new(topic, partition, broker_id, self.sample_opts)
    end
    return bucket[key]
end

function ClusterModel:update_replica_load(broker_id, topic_name, partition, resource, value)
    local bucket = self.replicas[broker_id]
    if not bucket then return false end
    local updater = bucket[replica_key(topic_name, partition)]
    if not updater then return false end
    updater:update(resource, value)
    return true
end

function ClusterModel:snapshot()
    local snap = ClusterModelSnapshot.new()
    for id, updater in pairs(self.brokers) do
        snap:add_broker(updater:snapshot())
    end
    for _, bucket in pairs(self.replicas) do
        for _, updater in pairs(bucket) do
            if self.brokers[updater.broker_id] then
                snap:add_replica(updater:snapshot(self.percentile))
            end
        end
    end
    return snap:aggregate()
end

ClusterModel.Resource = Resource
return ClusterModel
