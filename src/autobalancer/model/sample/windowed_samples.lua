local Snapshot = require("src.autobalancer.model.sample.snapshot")

-- A fixed-size ring of the most recent metric values for one (instance,
-- resource) pair, mirroring AutoMQ's time-window Samples. append() overwrites
-- the oldest value once full; snapshot() freezes the current window into a
-- percentile-queryable Snapshot.
--
-- "Trusted" mirrors AutoMQ's notion that a series with too few samples is not
-- yet reliable enough to drive a reassignment: goals can down-weight or ignore
-- untrusted loads instead of acting on noise.
local WindowedSamples = {}
WindowedSamples.__index = WindowedSamples

local DEFAULT_WINDOW    = 60   -- values retained (e.g. ~1/min for an hour)
local DEFAULT_MIN_VALID = 1    -- samples needed before the series is trusted

function WindowedSamples.new(opts)
    opts = opts or {}
    local window = opts.window or DEFAULT_WINDOW
    assert(type(window) == "number" and window >= 1, "window must be a positive number")

    return setmetatable({
        window    = window,
        min_valid = opts.min_valid or DEFAULT_MIN_VALID,
        buf       = {},   -- ring buffer, 1..window
        head      = 0,    -- index of the most-recently written slot
        count     = 0,    -- live values, saturates at window
        _latest   = 0,
    }, WindowedSamples)
end

function WindowedSamples:append(value)
    assert(type(value) == "number", "value must be a number")
    self.head = (self.head % self.window) + 1
    self.buf[self.head] = value
    if self.count < self.window then self.count = self.count + 1 end
    self._latest = value
end

function WindowedSamples:is_trusted()
    return self.count >= self.min_valid
end

function WindowedSamples:size()
    return self.count
end

function WindowedSamples:latest()
    return self._latest
end

-- Freeze the current window into an immutable Snapshot. Order within the window
-- is irrelevant to percentile, so we hand over the raw live values.
function WindowedSamples:snapshot()
    local values = {}
    for i = 1, self.count do values[i] = self.buf[i] end
    return Snapshot.new(values, self._latest)
end

return WindowedSamples
