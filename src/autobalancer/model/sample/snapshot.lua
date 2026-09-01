local Snapshot = {}
Snapshot.__index = Snapshot

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
