local Snapshot = require("src.autobalancer.model.sample.snapshot")

local WindowedSamples = {}
WindowedSamples.__index = WindowedSamples

local DEFAULT_WINDOW    = 60
local DEFAULT_MIN_VALID = 1

function WindowedSamples.new(opts)
    opts = opts or {}
    local window = opts.window or DEFAULT_WINDOW
    assert(type(window) == "number" and window >= 1, "window must be a positive number")

    return setmetatable({
        window    = window,
        min_valid = opts.min_valid or DEFAULT_MIN_VALID,
        buf       = {},
        head      = 0,
        count     = 0,
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

function WindowedSamples:snapshot()
    local values = {}
    for i = 1, self.count do values[i] = self.buf[i] end
    return Snapshot.new(values, self._latest)
end

return WindowedSamples
