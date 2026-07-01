-- Rendering helpers for the SQL-like console: an ASCII result-set table and
-- coloring. Colors are optional — if the `term` module isn't available the
-- functions degrade to plain text, which keeps this module usable in tests.

local ok_colors, colors = pcall(require, "term.colors")

local M = {}

-- Apply a color function/escape if colors are available, else return s.
local function paint(color, s)
    if not ok_colors then return s end
    local c = colors[color]
    if type(c) == "function" then return c(s) end
    if type(c) == "string" then return c .. s .. colors.reset end
    return s
end

M.paint = paint

local function display_width(s)
    -- Records may carry newlines; collapse them so a cell stays one line.
    return #(tostring(s):gsub("[\r\n]", " "))
end

local function one_line(s)
    return (tostring(s):gsub("[\r\n]", " "))
end

-- Render a result set as a bordered table. `columns` is an array of header
-- strings; `rows` is an array of arrays of cell values. Returns a string
-- (no trailing newline).
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
            local pad = string.rep(" ", widths[i] - #cell)
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
