-- RFC 4122 v4 UUIDs. Used for connection identifiers (so log lines can
-- be correlated across a connection's lifetime) and for heartbeat
-- correlation IDs (so the heartbeat coroutine can match responses to
-- specific requests).

local rng = require("src.server.rng")

local M = {}

M.ZERO = string.rep("\0", 16)

-- Returns 16 raw bytes, with version=4 and variant=10 set per RFC 4122.
function M.bytes()
    local b= rng.bytes(16)
    local octets = { b:byte(1, 16) }
    octets[7] = (octets[7] & 0x0F) | 0x40  -- version 4 (random)
    octets[9] = (octets[9] & 0x3F) | 0x80  -- variant 10 (RFC 4122)
    return string.char(table.unpack(octets))
end

-- Canonical 8-4-4-4-12 hyphenated form.
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

-- First 8 hex chars of a UUID, suitable for compact log correlation
-- (32 bits of uniqueness — plenty inside a single process lifetime).
function M.short(b)
    if type(b) ~= "string" or #b ~= 16 then return "????????" end
    return string.format("%02x%02x%02x%02x", b:byte(1), b:byte(2), b:byte(3), b:byte(4))
end

return M