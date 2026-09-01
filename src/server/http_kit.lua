local socket = require("socket")

local M = {}

M.MAX_HEADERS = 8192

M.STATUS_TEXT = {
    [200] = "OK", [400] = "Bad Request", [401] = "Unauthorized",
    [404] = "Not Found", [405] = "Method Not Allowed",
    [500] = "Internal Server Error",
    [503] = "Service Unavailable",
}

function M.read_headers(reactor, sock, deadline)
    sock:settimeout(0)
    local buf = ""
    while #buf < M.MAX_HEADERS do
        if socket.gettime() > deadline then return nil, "read deadline" end
        local chunk, err, partial = sock:receive(M.MAX_HEADERS - #buf)
        local progressed = false
        if chunk then buf = buf .. chunk; progressed = #chunk > 0
        elseif partial and #partial > 0 then buf = buf .. partial; progressed = true end

        local idx = buf:find("\r\n\r\n", 1, true)
        if idx then return buf:sub(1, idx + 3), buf:sub(idx + 4) end

        if reactor.would_block and reactor.would_block(err) then
            reactor:park(sock, err, "read")
        elseif err == "closed" then return nil, "peer closed"
        elseif err then return nil, err
        elseif not progressed then reactor:sleep(0.02) end
    end
    return nil, "headers too large"
end

function M.read_body(reactor, sock, have, want, deadline, max_body)
    if want <= 0 then return have end
    if want > max_body then return nil, "body too large" end
    sock:settimeout(0)
    local parts = { have }
    local got = #have
    while got < want do
        if socket.gettime() > deadline then return nil, "read deadline" end
        local chunk, err, partial = sock:receive(want - got)
        if chunk then parts[#parts + 1] = chunk; got = got + #chunk
        elseif partial and #partial > 0 then parts[#parts + 1] = partial; got = got + #partial end
        if got >= want then break end
        if reactor.would_block and reactor.would_block(err) then
            reactor:park(sock, err, "read")
        elseif err == "closed" then return nil, "peer closed"
        elseif err then return nil, err end
    end
    return table.concat(parts)
end

function M.respond(reactor, sock, status, ctype, body, opts)
    if type(opts) == "number" then opts = { deadline = opts } end
    opts = opts or {}
    local text = M.STATUS_TEXT[status] or "Status"
    local extra = opts.extra or ""
    if opts.no_store then extra = "Cache-Control: no-store\r\n" .. extra end
    local response = string.format(
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n" ..
        "Connection: close\r\n%s\r\n%s",
        status, text, ctype, #body, extra, body)
    reactor:send_all(sock, response, socket.gettime() + (opts.deadline or 5))
end

function M.header(headers, name)
    local pat = "[\r\n]" .. name:gsub("%a", function(c)
        return "[" .. c:lower() .. c:upper() .. "]"
    end) .. ":%s*([^\r\n]+)"
    return headers:match(pat)
end

function M.request_line(headers)
    local method, target = headers:match("^(%S+)%s+(%S+)")
    if not method then return nil end
    local path, query = target:match("^([^%?]*)%??(.*)$")
    return method, path, query ~= "" and query or nil
end

function M.parse_query(query)
    local out = {}
    if not query then return out end
    for pair in query:gmatch("[^&]+") do
        local k, v = pair:match("^([^=]+)=(.*)$")
        if k then
            v = v:gsub("%%(%x%x)", function(h)
                return string.char(tonumber(h, 16))
            end)
            out[k] = v
        end
    end
    return out
end

return M
