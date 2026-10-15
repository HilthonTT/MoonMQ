local M = {}

M.MAX_HEADERS     = 64
M.MAX_NAME_LEN    = 255
M.MAX_VALUE_LEN   = 1024 * 1024

local EMPTY = {}
M.EMPTY = EMPTY

function M.normalize(headers)
    if headers == nil then return EMPTY, nil end
    if type(headers) ~= "table" then
        return nil, "headers must be a table"
    end

    local list = {}
    if #headers > 0 then
        for i = 1, #headers do
            local h = headers[i]
            if type(h) ~= "table" then
                return nil, string.format("header %d must be a table", i)
            end
            list[i] = { name = h.name or h[1], value = h.value or h[2] }
        end
    else
        for name, value in pairs(headers) do
            list[#list + 1] = { name = name, value = value }
        end
        table.sort(list, function(a, b) return tostring(a.name) < tostring(b.name) end)
    end

    if #list > M.MAX_HEADERS then
        return nil, string.format("too many headers (%d > %d)", #list, M.MAX_HEADERS)
    end
    for i = 1, #list do
        local h = list[i]
        if type(h.name) ~= "string" or #h.name == 0 or #h.name > M.MAX_NAME_LEN then
            return nil, string.format("header %d: name must be a 1..%d byte string",
                i, M.MAX_NAME_LEN)
        end
        if type(h.value) ~= "string" or #h.value > M.MAX_VALUE_LEN then
            return nil, string.format("header %q: value must be a string of at most %d bytes",
                h.name, M.MAX_VALUE_LEN)
        end
    end
    return list, nil
end

function M.encode(list)
    local parts = { string.pack(">I2", #list) }
    for i = 1, #list do
        parts[#parts + 1] = string.pack(">s2s4", list[i].name, list[i].value)
    end
    return table.concat(parts)
end

function M.decode(buf, pos)
    pos = pos or 1
    if #buf - pos + 1 < 2 then return nil, "truncated header count" end
    local count
    count, pos = string.unpack(">I2", buf, pos)
    if count > M.MAX_HEADERS then
        return nil, string.format("too many headers (%d > %d)", count, M.MAX_HEADERS)
    end
    local list = {}
    for i = 1, count do
        if #buf - pos + 1 < 2 then return nil, "truncated header name length" end
        local nlen = string.unpack(">I2", buf, pos)
        if nlen == 0 or nlen > M.MAX_NAME_LEN or #buf - pos - 1 < nlen + 4 then
            return nil, "truncated or invalid header name"
        end
        local name = buf:sub(pos + 2, pos + 1 + nlen)
        pos = pos + 2 + nlen
        local vlen = string.unpack(">I4", buf, pos)
        if vlen > M.MAX_VALUE_LEN or #buf - pos - 3 < vlen then
            return nil, "truncated or invalid header value"
        end
        list[i] = { name = name, value = buf:sub(pos + 4, pos + 3 + vlen) }
        pos = pos + 4 + vlen
    end
    return list, pos
end

function M.size(list)
    if not list then return 0 end
    local n = 0
    for i = 1, #list do n = n + #list[i].name + #list[i].value end
    return n
end

return M
