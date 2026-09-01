local Resource = require("src.autobalancer.common.resource")
local Action   = require("src.autobalancer.common.action")

local ActionType = Action.ActionType

local ClusterModelSnapshot = {}
ClusterModelSnapshot.__index = ClusterModelSnapshot

function ClusterModelSnapshot.new()
    return setmetatable({
        brokers            = {},
        replicas_by_broker = {},
    }, ClusterModelSnapshot)
end

function ClusterModelSnapshot:add_broker(broker)
    self.brokers[broker.id] = broker
    self.replicas_by_broker[broker.id] = self.replicas_by_broker[broker.id] or {}
end

function ClusterModelSnapshot:add_replica(replica)
    local bucket = self.replicas_by_broker[replica.broker_id]
    assert(bucket, "replica references unknown broker " .. tostring(replica.broker_id))
    bucket[replica:key()] = replica
end

function ClusterModelSnapshot:aggregate()
    for id, broker in pairs(self.brokers) do
        for _, r in ipairs(Resource.VALUES) do broker.loads[r] = 0 end
        for _, replica in pairs(self.replicas_by_broker[id]) do
            for _, r in ipairs(Resource.VALUES) do
                broker:add_load(r, replica:get_load(r))
            end
        end
    end
    return self
end

function ClusterModelSnapshot:broker(id)
    return self.brokers[id]
end

function ClusterModelSnapshot:broker_load(id, resource)
    local b = self.brokers[id]
    return b and b:get_load(resource) or 0
end

function ClusterModelSnapshot:active_broker_ids()
    local ids = {}
    for id, b in pairs(self.brokers) do
        if b.active then ids[#ids + 1] = id end
    end
    table.sort(ids)
    return ids
end

function ClusterModelSnapshot:replicas_of(id)
    local out = {}
    local bucket = self.replicas_by_broker[id]
    if bucket then
        for _, replica in pairs(bucket) do out[#out + 1] = replica end
    end
    return out
end

function ClusterModelSnapshot:replica_at(broker_id, topic_name, partition)
    local bucket = self.replicas_by_broker[broker_id]
    if not bucket then return nil end
    return bucket[topic_name .. "-" .. tostring(partition)]
end

local function move_one(self, replica, from_id, to_id)
    local from = self.replicas_by_broker[from_id]
    local to   = self.replicas_by_broker[to_id]
    from[replica:key()] = nil
    replica.broker_id = to_id
    to[replica:key()] = replica
    for _, r in ipairs(Resource.VALUES) do
        self.brokers[from_id]:add_load(r, -replica:get_load(r))
        self.brokers[to_id]:add_load(r, replica:get_load(r))
    end
end

function ClusterModelSnapshot:apply_action(action)
    if action.action_type == ActionType.MOVE then
        local replica = self:replica_at(action.src_broker_id,
            action.src_topic.name, action.src_partition)
        if not replica then return nil, "MOVE: no such replica on src broker" end
        if not self.brokers[action.dest_broker_id] then
            return nil, "MOVE: unknown dest broker"
        end
        move_one(self, replica, action.src_broker_id, action.dest_broker_id)
        return true
    end

    local src_replica = self:replica_at(action.src_broker_id,
        action.src_topic.name, action.src_partition)
    local dest_replica = self:replica_at(action.dest_broker_id,
        action.dest_topic.name, action.dest_partition)
    if not src_replica or not dest_replica then
        return nil, "SWAP: missing src or dest replica"
    end
    move_one(self, src_replica, action.src_broker_id, action.dest_broker_id)
    move_one(self, dest_replica, action.dest_broker_id, action.src_broker_id)
    return true
end

return ClusterModelSnapshot
