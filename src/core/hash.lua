local M = {}

function M.fnv1a(s)
    local hash = 2166136261
    for i = 1, #s do
        hash = (hash ~ s:byte(i)) & 0xFFFFFFFF
        hash = (hash * 16777619) & 0xFFFFFFFF
    end
    return hash
end

return M
