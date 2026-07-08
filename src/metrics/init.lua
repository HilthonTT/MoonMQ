local socket = require("socket")

local M = {
    counters = {},
    gauges = {},
    histograms = {},
    -- base metric name -> { type = "counter"|"gauge"|"histogram", help = "..." }
    -- Optional: populated via M.describe. Drives the # HELP/# TYPE headers so
    -- scrapers and Grafana's metric browser can discover metric families.
    meta = {},
}

-- describe registers HELP text and (optionally) an explicit type for a metric
-- family. Types are otherwise inferred from which store a series lands in, so
-- describe is only needed to attach human-readable HELP or to pin a type.
function M.describe(name, mtype, help)
    M.meta[name] = { type = mtype, help = help }
end

-- Per the Prometheus exposition format, label values are double-quoted
-- and must escape backslash, double-quote, and newline. Anything else
-- passes through verbatim. We escape at key() time so the same escaped
-- form is what we group by — otherwise two metrics whose labels only
-- differ by an escape would end up under the same series key.
local function escape_label_value(v)
    v = tostring(v)
    v = v:gsub("\\", "\\\\")
    v = v:gsub('"', '\\"')
    v = v:gsub("\n", "\\n")
    return v
end

-- Labels as a stable string key so we don't lose to table identity.
local function key(name, labels)
    if not labels then return name end
    local parts = {}
    for k, v in pairs(labels) do
        parts[#parts+1] = k .. '="' .. escape_label_value(v) .. '"'
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

-- delete drops a single gauge series. Needed for per-label series whose label
-- set is unbounded over a process's lifetime (e.g. one series per connection):
-- without removal they accumulate forever, leaking memory and bloating every
-- /metrics scrape with dead series.
function M.delete(name, labels)
    M.gauges[key(name, labels)] = nil
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

-- The base metric name is everything before the first `{` (the label set).
local function base_name(k)
    local brace = k:find("{", 1, true)
    return brace and k:sub(1, brace - 1) or k
end

-- Prometheus text exposition format. Series are grouped by metric family so a
-- single # HELP/# TYPE header precedes each family's series — the exposition
-- format forbids duplicate TYPE lines for the same family, which the previous
-- interleaved render couldn't guarantee.
function M.render_prometheus()
    local families = {}   -- base -> { type = , lines = {} }
    local order = {}

    local function family(base, mtype)
        local fam = families[base]
        if not fam then
            fam = { type = mtype, lines = {} }
            families[base] = fam
            order[#order + 1] = base
        end
        return fam
    end

    for k, v in pairs(M.counters) do
        local fam = family(base_name(k), "counter")
        fam.lines[#fam.lines + 1] = string.format("%s %s", k, tostring(v))
    end
    for k, v in pairs(M.gauges) do
        local fam = family(base_name(k), "gauge")
        fam.lines[#fam.lines + 1] = string.format("%s %s", k, tostring(v))
    end
    for k, h in pairs(M.histograms) do
        local base = base_name(k)
        local fam = family(base, "histogram")
        -- Split key into base and inner labels by the first `{`.
        local inner
        local brace = k:find("{", 1, true)
        inner = brace and k:sub(brace + 1, -2) or ""  -- strip closing brace
        local prefix = (inner == "") and "" or (inner .. ",")
        local label_suffix = (inner == "") and "" or ("{" .. inner .. "}")
        for _, b in ipairs(BUCKETS) do
            fam.lines[#fam.lines + 1] = string.format("%s_bucket{%sle=\"%s\"} %d",
                base, prefix, b, h.buckets[b])
        end
        -- +Inf bucket = count (Prometheus requires this for quantile estimation).
        fam.lines[#fam.lines + 1] = string.format("%s_bucket{%sle=\"+Inf\"} %d",
            base, prefix, h.count)
        fam.lines[#fam.lines + 1] = string.format("%s_sum%s %s",
            base, label_suffix, tostring(h.sum))
        fam.lines[#fam.lines + 1] = string.format("%s_count%s %d",
            base, label_suffix, h.count)
    end

    -- Deterministic family order so scrapes/diffs are stable.
    table.sort(order)

    local out = {}
    for _, base in ipairs(order) do
        local fam = families[base]
        local meta = M.meta[base]
        if meta and meta.help then
            out[#out + 1] = string.format("# HELP %s %s", base, meta.help)
        end
        out[#out + 1] = string.format("# TYPE %s %s",
            base, (meta and meta.type) or fam.type)
        table.sort(fam.lines)
        for _, line in ipairs(fam.lines) do
            out[#out + 1] = line
        end
    end
    return table.concat(out, "\n") .. "\n"
end

return M