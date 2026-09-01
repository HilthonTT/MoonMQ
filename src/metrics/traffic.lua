local Traffic = {}
Traffic.__index = Traffic

local function key(topic, partition)
    return topic .. "\0" .. tostring(partition)
end

function Traffic.new()
    return setmetatable({
        map = {},
    }, Traffic)
end

function Traffic:_slot(topic, partition)
    local k = key(topic, partition)
    local s = self.map[k]
    if not s then
        s = { bytes_in = 0, bytes_out = 0 }
        self.map[k] = s
    end
    return s
end

function Traffic:add_in(topic, partition, n)
    local s = self:_slot(topic, partition)
    s.bytes_in = s.bytes_in + (n or 0)
end

function Traffic:add_out(topic, partition, n)
    local s = self:_slot(topic, partition)
    s.bytes_out = s.bytes_out + (n or 0)
end

function Traffic:totals(topic, partition)
    local s = self.map[key(topic, partition)]
    if not s then return 0, 0 end
    return s.bytes_in, s.bytes_out
end

return Traffic
