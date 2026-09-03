local ok_colors, colors = pcall(require, "term.colors")

local M = {}

local function paint(color, s)
    if not ok_colors then return s end
    local c = colors[color]
    if type(c) == "function" then return c(s) end
    if type(c) == "string" then return c .. s .. colors.reset end
    -- lua-term exposes colors as callable tables (__call/__tostring).
    if type(c) == "table" then
        local mt = getmetatable(c)
        if mt and mt.__call then return c(s) end
        return tostring(c) .. s .. tostring(colors.reset)
    end
    return s
end

M.paint = paint

local function display_width(s)
    local line = tostring(s):gsub("[\r\n]", " ")
    return utf8.len(line) or #line
end

local function one_line(s)
    return (tostring(s):gsub("[\r\n]", " "))
end

function M.table(columns, rows)
    local widths = {}
    for i, h in ipairs(columns) do
        widths[i] = display_width(h)
    end
    for _, row in ipairs(rows) do
        for i = 1, #columns do
            local w = display_width(row[i] == nil and "" or row[i])
            if w > widths[i] then widths[i] = w end
        end
    end

    local function hline()
        local segs = {}
        for i = 1, #columns do
            segs[i] = string.rep("-", widths[i] + 2)
        end
        return "+" .. table.concat(segs, "+") .. "+"
    end

    local function render_row(cells, painter)
        local segs = {}
        for i = 1, #columns do
            local cell = one_line(cells[i] == nil and "" or cells[i])
            local pad = string.rep(" ", widths[i] - (utf8.len(cell) or #cell))
            local text = " " .. cell .. pad .. " "
            segs[i] = painter and painter(text) or text
        end
        return "|" .. table.concat(segs, "|") .. "|"
    end

    local out = {}
    out[#out + 1] = paint("dim", hline())
    out[#out + 1] = render_row(columns, function(t) return paint("cyan", t) end)
    out[#out + 1] = paint("dim", hline())
    for _, row in ipairs(rows) do
        out[#out + 1] = render_row(row, nil)
    end
    out[#out + 1] = paint("dim", hline())
    return table.concat(out, "\n")
end

return M
