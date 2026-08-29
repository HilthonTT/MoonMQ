-- Constant-time comparison, in one place.
--
-- Extracted from src/server/auth.lua when SCRAM (src/server/scram.lua) needed
-- the same primitive for proof and signature checks. Two copies of a timing-
-- sensitive compare is how one of them ends up subtly weaker than the other.

local M = {}

-- Compare in constant time over max(|a|, |b|) so the LENGTH is also
-- constant-time. Mismatched lengths always return false, but take the same
-- time as a same-length mismatch — otherwise the comparison leaks how much of
-- a secret a guess got right by prefix.
function M.equal(a, b)
    if type(a) ~= "string" or type(b) ~= "string" then return false end

    local la, lb = #a, #b
    local n = la > lb and la or lb
    if n == 0 then return la == lb end

    local diff = la ~ lb  -- nonzero if lengths differ
    for i = 1, n do
        local ai = (i <= la) and a:byte(i) or 0
        local bi = (i <= lb) and b:byte(i) or 0
        diff = diff | (ai ~ bi)
    end
    return diff == 0
end

return M
