local M = {}

function M.equal(a, b)
    if type(a) ~= "string" or type(b) ~= "string" then return false end

    local la, lb = #a, #b
    local n = la > lb and la or lb
    if n == 0 then return la == lb end

    local diff = la ~ lb
    for i = 1, n do
        local ai = (i <= la) and a:byte(i) or 0
        local bi = (i <= lb) and b:byte(i) or 0
        diff = diff | (ai ~ bi)
    end
    return diff == 0
end

return M
