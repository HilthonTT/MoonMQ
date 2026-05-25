local socket = require("socket")

local M = {
    counters = {},
    gauges = {},
    histograms = {},
}

-- Labels as a stable string key so we don't lose to table identity.
local function key(name, labels)
    if not labels then return name end
    local parts = {}
    for k, v in pairs(labels) do
        parts[#parts+1] = k.. "=" ..tostring(v)
    end
    table.sort(parts)
    return name .. "{" .. table.concat(parts, ",") .. "}"
end

function M.inc(name, n, labels)
    local k = key(name, labels)
    M.counters[k] = (M.counters[k] or 0) + (n or 1)
end

function M.set(name, value, labels)
    M.gauges[key(name, labels)] = value
end

-- Histogram buckets: explicit boundaries, Prometheus-style. Stores
-- bucket counts + sum + count, so quantiles are reconstructable.
-- The "+Inf" bucket is implicit: its count equals h.count (Prometheus
-- convention). We surface it explicitly at render time so tail-latency
-- quantiles are computable.
local BUCKETS = { 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 }
function M.observe(name, value, labels)
    local k = key(name, labels)
    local h = M.histograms[k]
    if not h then
        h = { buckets = {}, sum = 0, count = 0 }
        for _, b in ipairs(BUCKETS) do
            h.buckets[b] = 0
        end
        M.histograms[k] = h
    end
    for _, b in ipairs(BUCKETS) do
        if value <= b then
            h.buckets[b] = h.buckets[b] + 1
        end
    end
    h.sum = h.sum + value
    h.count = h.count + 1
end

-- Convenience: returns a timer closure. Call it to record elapsed.
function M.timer(name, labels)
    local t0 = socket.gettime()
    return function() M.observe(name, socket.gettime() - t0, labels) end
end

-- Prometheus text exposition format.
function M.render_prometheus()
    local out = {}
    for k, v in pairs(M.counters) do
        out[#out+1] = string.format("%s %s", k, tostring(v))
    end
    for k, v in pairs(M.gauges) do
        out[#out+1] = string.format("%s %s", k, tostring(v))
    end
    for k, h in pairs(M.histograms) do
        -- Split key into base and inner labels by the first `{`. Lua
        -- patterns don't support `?` on groups, so don't try to do this
        -- with a single match.
        local base, inner
        local brace = k:find("{", 1, true)
        if brace then
            base  = k:sub(1, brace - 1)
            inner = k:sub(brace + 1, -2)  -- strip closing brace
        else
            base  = k
            inner = ""
        end
        local prefix = (inner == "") and "" or (inner .. ",")
        local label_suffix = (inner == "") and "" or ("{" .. inner .. "}")
        for _, b in ipairs(BUCKETS) do
            out[#out+1] = string.format("%s_bucket{%sle=\"%s\"} %d",
                base, prefix, b, h.buckets[b])
        end
        -- +Inf bucket = count (Prometheus requires this for quantile estimation).
        out[#out+1] = string.format("%s_bucket{%sle=\"+Inf\"} %d",
            base, prefix, h.count)
        out[#out+1] = string.format("%s_sum%s %s",
            base, label_suffix, tostring(h.sum))
        out[#out+1] = string.format("%s_count%s %d",
            base, label_suffix, h.count)
    end
    return table.concat(out, "\n") .. "\n"
end

return M