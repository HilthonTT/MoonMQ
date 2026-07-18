-- Traffic — per-partition cumulative byte counters, the feed the autobalancer's
-- NW_IN / NW_OUT goals were waiting for (docs/cluster.md: "no per-partition
-- byte-rate feed exists yet").
--
-- One instance per Broker (NOT module-level state — tests run several brokers
-- in one process). Producers add bytes-in on every local append (including
-- produces forwarded from a peer, counted on the owner); the delivery paths
-- add bytes-out per record handed to a consumer. Counters are monotonic and
-- process-lifetime only: readers (the balance loop) turn them into rates by
-- differencing successive snapshots, Prometheus-style, and must tolerate a
-- reset (restart) as a negative delta.

local Traffic = {}
Traffic.__index = Traffic

local function key(topic, partition)
    return topic .. "\0" .. tostring(partition)
end

function Traffic.new()
    return setmetatable({
        map = {},   -- key(topic, partition) -> { bytes_in, bytes_out }
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

-- totals returns (bytes_in, bytes_out) for one partition; (0, 0) if untouched.
function Traffic:totals(topic, partition)
    local s = self.map[key(topic, partition)]
    if not s then return 0, 0 end
    return s.bytes_in, s.bytes_out
end

return Traffic
