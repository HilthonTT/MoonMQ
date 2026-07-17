-- An immutable percentile view over a window of metric values, mirroring
-- AutoMQ's Snapshot. Goals read a *percentile* (default p90) of the recent
-- window rather than the latest raw value so a single spike doesn't trigger a
-- reassignment and a single dip doesn't hide a hot broker.
--
--   local snap = Snapshot.new({ 3, 1, 2, 5, 4 })
--   snap:percentile(0.9)  -- 90th percentile, linear interpolation
--   snap:latest()         -- most recent appended value (last of the input)
local Snapshot = {}
Snapshot.__index = Snapshot

-- values: array of numbers in append order. Copied and sorted; the caller's
-- table is not retained or mutated.
function Snapshot.new(values, latest)
    assert(type(values) == "table", "values must be a table")

    local sorted = {}
    for i = 1, #values do sorted[i] = values[i] end
    table.sort(sorted)

    return setmetatable({
        sorted = sorted,
        n      = #sorted,
        _latest = latest,
    }, Snapshot)
end

function Snapshot:size()
    return self.n
end

function Snapshot:latest()
    return self._latest or 0
end

-- Percentile with linear interpolation between closest ranks. p in [0, 1].
-- Empty snapshot is treated as 0 load (a broker/replica with no samples yet
-- contributes nothing rather than skewing the mean).
function Snapshot:percentile(p)
    assert(type(p) == "number" and p >= 0 and p <= 1, "p must be in [0, 1]")
    if self.n == 0 then return 0 end
    if self.n == 1 then return self.sorted[1] end

    local rank = p * (self.n - 1) + 1
    local lo   = math.floor(rank)
    local hi   = math.ceil(rank)
    if lo == hi then return self.sorted[lo] end
    local frac = rank - lo
    return self.sorted[lo] + (self.sorted[hi] - self.sorted[lo]) * frac
end

return Snapshot
