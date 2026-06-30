local M = {}

function M.contains(str, sub)
    assert(type(str) == "string", "str must be a string")
    assert(type(sub) == "string", "sub must be a string")
    return str:find(sub, 1, true) ~= nil
end

function M.startswith(str, start)
    assert(type(str) == "string", "str must be a string")
    assert(type(start) == "string", "start must be a string")
    return str:sub(1, #start) == start
end

function M.endswith(str, ending)
    return ending == "" or str:sub(-#ending) == ending
end

function M.replace(str, old, new)
    assert(type(str) == "string", "str must be a string")
    assert(type(old) == "string", "old must be a string")
    assert(type(new) == "string", "new must be a string")

    -- An empty `old` has no well-defined replacement and would match endlessly.
    if old == "" then return str end

    local s = str
    local search_start_idx = 1

    while true do
        local start_idx, end_idx = s:find(old, search_start_idx, true)
        if (not start_idx) then
            break
        end

        s = s:sub(1, (start_idx - 1)) .. new .. s:sub(end_idx + 1)

        -- Resume right after the inserted `new`, as an absolute (positive)
        -- index. The old `-#postfix` trick became -0 == 0 when the match ended
        -- at end-of-string; Lua clamps a find init of 0 up to 1, rescanning the
        -- just-inserted text and looping forever when `new` contains `old`.
        search_start_idx = start_idx + #new
    end

    return s
end

function M.insert(str, pos, text)
    assert(type(str) == "string", "str must be a string")
    assert(type(pos) == "number", "pos must be a number")
    assert(type(text) == "string", "text must be a string")
    return str:sub(1, pos - 1) .. text .. str:sub(pos)
end

function M.trimsuffix(str, suffix)
    assert(type(str) == "string", "str must be a string")
    assert(type(suffix) == "string", "suffix must be a string")

    if suffix == "" or not M.endswith(str, suffix) then
        return str
    end

    return str:sub(1, #str - #suffix)
end

function M.trimprefix(str, prefix)
    assert(type(str) == "string", "str must be a string")
    assert(type(prefix) == "string", "prefix must be a string")

    if prefix == "" or not M.startswith(str, prefix) then
        return str
    end

    return str:sub(#prefix + 1)
end

return M
