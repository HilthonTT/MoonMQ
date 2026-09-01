local log = require("src.log.logger").get("replicator")

local Replicator = {}
Replicator.__index = Replicator

local DEFAULT_ACK_TIMEOUT = 5
local DEFAULT_LAG_MAX     = 0

function Replicator.new(reactor, replica_id, followers, opts)
    assert(type(replica_id) == "number", "replica_id must be a number")
    assert(type(followers) == "table", "followers must be a list")
    opts = opts or {}

    local self = setmetatable({
        reactor     = reactor,
        replica_id  = replica_id,
        followers   = followers,
        ack_timeout = opts.ack_timeout or DEFAULT_ACK_TIMEOUT,
        lag_max     = opts.lag_max or DEFAULT_LAG_MAX,
        tp          = {},
    }, Replicator)
    return self
end

function Replicator:enabled()
    return #self.followers > 0
end

function Replicator:_state(topic, partition)
    local key = topic .. "\0" .. partition
    local s = self.tp[key]
    if not s then
        s = { leo = 0, followers = {} }
        for i = 1, #self.followers do
            s.followers[i] = { last_leo = 0, queue = {}, head = 1, tail = 0, running = false }
        end
        self.tp[key] = s
    end
    return s
end

function Replicator:replicate(topic, partition, leo, bytes)
    if #self.followers == 0 then return end
    local s = self:_state(topic, partition)
    if leo > s.leo then s.leo = leo end

    for i = 1, #self.followers do
        local fs = s.followers[i]
        fs.tail = fs.tail + 1
        fs.queue[fs.tail] = { leo = leo, bytes = bytes }
        if not fs.running then
            fs.running = true
            if self.reactor then
                self.reactor:spawn(function() self:_drain(topic, partition, i) end)
            else
                self:_drain(topic, partition, i)
            end
        end
    end
end

function Replicator:_drain(topic, partition, i)
    local s  = self:_state(topic, partition)
    local fs = s.followers[i]
    local follower = self.followers[i]

    while fs.head <= fs.tail do
        local item = fs.queue[fs.head]
        fs.queue[fs.head] = nil
        fs.head = fs.head + 1

        local follower_leo, err = follower.client:send(
            topic, partition, self.replica_id, item.bytes)
        if err then
            log:error("replicate %s/partition-%d -> replica %s: %s",
                topic, partition, tostring(follower.id), err)
        elseif follower_leo and follower_leo > fs.last_leo then
            fs.last_leo = follower_leo
        end
    end
    fs.running = false
end

function Replicator:_in_sync(s, i)
    return (s.leo - s.followers[i].last_leo) <= self.lag_max
end

function Replicator:high_watermark(topic, partition)
    local s = self:_state(topic, partition)
    local hwm, any = math.huge, false
    for i = 1, #self.followers do
        if self:_in_sync(s, i) then
            any = true
            if s.followers[i].last_leo < hwm then hwm = s.followers[i].last_leo end
        end
    end
    if not any then return nil end
    return hwm
end

function Replicator:in_sync_count(topic, partition)
    local s = self:_state(topic, partition)
    local n = 0
    for i = 1, #self.followers do
        if self:_in_sync(s, i) then n = n + 1 end
    end
    return n
end

function Replicator:all_reached(topic, partition, leo)
    local s = self:_state(topic, partition)
    for i = 1, #self.followers do
        if s.followers[i].last_leo < leo then return false end
    end
    return true
end

function Replicator:wait_for(topic, partition, leo)
    if #self.followers == 0 then
        return nil, "acks=all requires configured replicas, none are set"
    end
    assert(self.reactor, "wait_for requires a reactor")
    local deadline = self:_now() + self.ack_timeout
    while true do
        if self:all_reached(topic, partition, leo) then
            return true
        end
        if self:_now() > deadline then
            return nil, string.format(
                "acks=all timed out after %.1fs waiting for replicas on %s/partition-%d",
                self.ack_timeout, topic, partition)
        end
        self.reactor:sleep(0.005)
    end
end

function Replicator:_now()
    if self._now_override then return self._now_override() end
    return require("socket").gettime()
end

return Replicator
