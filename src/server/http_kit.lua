-- Minimal reactor-friendly HTTP/1.1 server plumbing, shared by the
-- inter-broker endpoints (replica_server, cluster_server). Extracted from
-- replica_server so each endpoint keeps only its routing/business logic.
--
-- All functions are cooperative: they run inside a reactor coroutine and park
-- on reactor:wait_readable / reactor:sleep instead of blocking the process.

local socket = require("socket")

local M = {}

M.MAX_HEADERS = 8192

M.STATUS_TEXT = {
    [200] = "OK", [400] = "Bad Request", [401] = "Unauthorized",
    [404] = "Not Found", [405] = "Method Not Allowed",
    [500] = "Internal Server Error",
}

-- Read up to and including the header terminator, returning
-- (header_str, leftover_body_bytes) or (nil, err). Leftover is any body bytes
-- that arrived in the same recv as the end of the headers.
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

        if err == "timeout" then reactor:wait_readable(sock)
        elseif err == "closed" then return nil, "peer closed"
        elseif err then return nil, err
        elseif not progressed then reactor:sleep(0.02) end
    end
    return nil, "headers too large"
end

-- Read `want` body bytes given the `have` leftover from read_headers.
-- max_body bounds allocation. Returns (body, nil) or (nil, err).
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
        if err == "timeout" then reactor:wait_readable(sock)
        elseif err == "closed" then return nil, "peer closed"
        elseif err then return nil, err end
    end
    return table.concat(parts)
end

-- Write a full response. write_deadline is seconds from now.
function M.respond(reactor, sock, status, ctype, body, write_deadline)
    local text = M.STATUS_TEXT[status] or "Status"
    local response = string.format(
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n" ..
        "Connection: close\r\n\r\n%s",
        status, text, ctype, #body, body)
    reactor:send_all(sock, response, socket.gettime() + (write_deadline or 5))
end

-- Case-insensitive single-line header extraction. Header values are
-- attacker-influenced on unauthenticated endpoints: strip CR/LF is inherent
-- (the pattern stops at \r\n) and callers must validate further.
function M.header(headers, name)
    -- Build a case-insensitive pattern for the header name.
    local pat = "[\r\n]" .. name:gsub("%a", function(c)
        return "[" .. c:lower() .. c:upper() .. "]"
    end) .. ":%s*([^\r\n]+)"
    return headers:match(pat)
end

-- Parse "METHOD /path?query" from the request line. Returns (method, path,
-- query_string_or_nil).
function M.request_line(headers)
    local method, target = headers:match("^(%S+)%s+(%S+)")
    if not method then return nil end
    local path, query = target:match("^([^%?]*)%??(.*)$")
    return method, path, query ~= "" and query or nil
end

-- Decode an application/x-www-form-urlencoded-style query string into a table.
-- Only what the cluster endpoints need: k=v pairs split on &, %XX unescaped.
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
