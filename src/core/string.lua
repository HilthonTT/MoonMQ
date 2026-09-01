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
