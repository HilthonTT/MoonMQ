-- Small string helpers Lua's stdlib lacks. Kept deliberately thin: only what
-- the commitlog's filename parsing (src/commitlog/commitlog.lua) actually
-- needs, so this doesn't drift into an unused grab-bag.

local M = {}

function M.endswith(str, ending)
    return ending == "" or str:sub(-#ending) == ending
end

function M.trimsuffix(str, suffix)
    assert(type(str) == "string", "str must be a string")
    assert(type(suffix) == "string", "suffix must be a string")

    if suffix == "" or not M.endswith(str, suffix) then
        return str
    end

    return str:sub(1, #str - #suffix)
end

return M
