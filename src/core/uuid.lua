local rng = require("src.core.rng")

local M = {}

M.ZERO = string.rep("\0", 16)

function M.bytes()
    local b = rng.bytes(16)
    local b7 = (b:byte(7) & 0x0F) | 0x40
    local b9 = (b:byte(9) & 0x3F) | 0x80
    return b:sub(1, 6) .. string.char(b7) .. b:sub(8, 8)
        .. string.char(b9) .. b:sub(10, 16)
end

function M.format(b)
    if type(b) ~= "string" or #b ~= 16  then return "????????" end
    local o = { b:byte(1, 16) }
    return string.format(
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        o[1], o[2], o[3], o[4],
        o[5], o[6],
        o[7], o[8],
        o[9], o[10],
        o[11], o[12], o[13], o[14], o[15], o[16])
end

function M.short(b)
    if type(b) ~= "string" or #b ~= 16 then return "????????" end
    return string.format("%02x%02x%02x%02x", b:byte(1), b:byte(2), b:byte(3), b:byte(4))
end

return M