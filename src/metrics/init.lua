local socket = require("socket")

local M = {
    counters = {},
    gauges = {},
    histograms = {},
    meta = {},
}

function M.describe(name, mtype, help)
    M.meta[name] = { type = mtype, help = help }
end

local function escape_label_value(v)
    v = tostring(v)
    v = v:gsub("\\", "\\\\")
    v = v:gsub('"', '\\"')
    v = v:gsub("\n", "\\n")
    return v
end

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

function M.delete(name, labels)
    M.gauges[key(name, labels)] = nil
end

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

function M.timer(name, labels)
    local t0 = socket.gettime()
    return function() M.observe(name, socket.gettime() - t0, labels) end
end

local function base_name(k)
    local brace = k:find("{", 1, true)
    return brace and k:sub(1, brace - 1) or k
end

function M.render_prometheus()
    local families = {}
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
        local inner
        local brace = k:find("{", 1, true)
        inner = brace and k:sub(brace + 1, -2) or ""
        local prefix = (inner == "") and "" or (inner .. ",")
        local label_suffix = (inner == "") and "" or ("{" .. inner .. "}")
        for _, b in ipairs(BUCKETS) do
            fam.lines[#fam.lines + 1] = string.format("%s_bucket{%sle=\"%s\"} %d",
                base, prefix, b, h.buckets[b])
        end
        fam.lines[#fam.lines + 1] = string.format("%s_bucket{%sle=\"+Inf\"} %d",
            base, prefix, h.count)
        fam.lines[#fam.lines + 1] = string.format("%s_sum%s %s",
            base, label_suffix, tostring(h.sum))
        fam.lines[#fam.lines + 1] = string.format("%s_count%s %d",
            base, label_suffix, h.count)
    end

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