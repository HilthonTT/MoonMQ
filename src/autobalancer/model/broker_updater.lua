-- Broker is the immutable, per-detection view of one broker: identity plus the
-- per-resource loads that ClusterModelSnapshot aggregates from the broker's
-- replicas. Load values are filled in by the snapshot's aggregate() pass, not
-- here, so a broker with no replicas reads as zero load on every resource.
local Broker = {}
Broker.__index = Broker

function Broker.new(id, rack, active)
    return setmetatable({
        id     = id,
        rack   = rack,
        active = active,
        loads  = {},   -- resource -> number, filled by aggregation
    }, Broker)
end

function Broker:get_load(resource)
    return self.loads[resource] or 0
end

function Broker:set_load(resource, value)
    self.loads[resource] = value
end

function Broker:add_load(resource, delta)
    self.loads[resource] = (self.loads[resource] or 0) + delta
end

-- BrokerUpdater is the live side. Brokers have no sampled load of their own in
-- this model (load is the sum of their replicas), so the updater only tracks
-- membership: id, rack, and whether the broker is eligible to hold replicas.
local BrokerUpdater = {}
BrokerUpdater.__index = BrokerUpdater

function BrokerUpdater.new(id, opts)
    assert(type(id) == "string", "broker id must be a string")
    opts = opts or {}
    local active = opts.active
    if active == nil then active = true end

    return setmetatable({
        id     = id,
        rack   = opts.rack,
        active = active,
    }, BrokerUpdater)
end

function BrokerUpdater:set_active(active)
    self.active = active and true or false
end

function BrokerUpdater:snapshot()
    return Broker.new(self.id, self.rack, self.active)
end

BrokerUpdater.Broker = Broker
return BrokerUpdater
