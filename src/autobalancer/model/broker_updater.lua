local Broker = {}
Broker.__index = Broker

function Broker.new(id, rack, active)
    return setmetatable({
        id     = id,
        rack   = rack,
        active = active,
        loads  = {},
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
