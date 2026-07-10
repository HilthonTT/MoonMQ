-- Follower-side replication endpoint. Lives on its own port, shares the main
-- reactor (one event loop). The leader (src/server/replicator.lua) POSTs each
-- serialized record here; we apply it to the local partition and return the new
-- log-end offset so the leader can advance its high-watermark.
--
--   POST /replicate
--     X-Topic:     <topic name>
--     X-Partition: <1-based partition id>
--     body:        one serialized record (len(8) | body), as produced by
--                  src/record/message.serialize_message
--   → 200 { "offset": <follower LEO> }   on success
--   → 4xx/5xx text                        on error
--
-- HTTP/1.1, Connection: close. No auth — bind to a trusted network / firewall
-- the port, same caveat as the metrics endpoint.

local socket  = require("socket")
local json    = require("dkjson")
local msg_m   = require("src.record.message")
local log     = require("src.log.logger").get("replica_server")

local M = {}
M.__index = M

local READ_DEADLINE  = 10
local WRITE_DEADLINE = 5
local MAX_HEADERS    = 8192
local MAX_BODY       = 8 * 1024 * 1024   -- generous vs the 1 MiB frame cap

function M.new(opts)
    return setmetatable({
        reactor = assert(opts.reactor, "reactor required"),
        broker  = assert(opts.broker, "broker required"),
        host    = opts.host or "127.0.0.1",
        port    = assert(opts.port, "port required"),
    }, M)
end

-- Read up to and including the header terminator, returning
-- (header_str, leftover_body_bytes) or (nil, err). Leftover is any body bytes
-- that arrived in the same recv as the end of the headers.
local function read_headers(reactor, sock, deadline)
    sock:settimeout(0)
    local buf = ""
    while #buf < MAX_HEADERS do
        if socket.gettime() > deadline then return nil, "read deadline" end
        local chunk, err, partial = sock:receive(MAX_HEADERS - #buf)
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

local function read_body(reactor, sock, have, want, deadline)
    if want <= 0 then return have end
    if want > MAX_BODY then return nil, "body too large" end
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

local STATUS_TEXT = {
    [200] = "OK", [400] = "Bad Request", [404] = "Not Found",
    [405] = "Method Not Allowed", [500] = "Internal Server Error",
}

local function respond(reactor, sock, status, ctype, body)
    local text = STATUS_TEXT[status] or "Status"
    local response = string.format(
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n" ..
        "Connection: close\r\n\r\n%s",
        status, text, ctype, #body, body)
    reactor:send_all(sock, response, socket.gettime() + WRITE_DEADLINE)
end

-- Apply one serialized record (len(8) | body) to (topic, partition). Returns
-- (leo, nil) or (nil, err).
function M:_apply(topic_name, partition_id, payload)
    if #payload < 8 then return nil, "short record" end
    local total_size = string.unpack(">I8", payload)
    if #payload < 8 + total_size then return nil, "truncated record body" end
    local msg, derr = msg_m.decode_body(payload:sub(9, 8 + total_size))
    if not msg then return nil, "decode: " .. tostring(derr) end

    local topic, terr = self.broker:get_topic(topic_name)
    if not topic then return nil, "topic: " .. tostring(terr) end
    local part = topic.partitions[partition_id]
    if not part then return nil, "no such partition" end

    local _, werr = part:write_message(msg)
    if werr then return nil, "write: " .. tostring(werr) end
    if part.request_sync then part:request_sync() end
    -- Log-end offset after applying: what the leader compares its HWM against.
    return part.offset, nil
end

function M:_handle(sock)
    local deadline = socket.gettime() + READ_DEADLINE
    local headers, leftover = read_headers(self.reactor, sock, deadline)
    if not headers then pcall(function() sock:close() end); return end

    local method, path = headers:match("^(%S+)%s+(%S+)")
    path = path and path:gsub("%?.*", "") or ""
    if method ~= "POST" or path ~= "/replicate" then
        pcall(function()
            respond(self.reactor, sock, method ~= "POST" and 405 or 404,
                "text/plain", "replica endpoint: POST /replicate only\n")
            sock:close()
        end)
        return
    end

    local topic     = headers:match("[Xx]%-[Tt]opic:%s*([^\r\n]+)")
    local partition = tonumber(headers:match("[Xx]%-[Pp]artition:%s*(%d+)"))
    local clen      = tonumber(headers:match("[Cc]ontent%-[Ll]ength:%s*(%d+)")) or 0

    local status, ctype, body
    if not topic or not partition then
        status, ctype, body = 400, "text/plain", "missing X-Topic/X-Partition\n"
    else
        local payload, berr = read_body(self.reactor, sock, leftover, clen, deadline)
        if not payload then
            status, ctype, body = 400, "text/plain", "body: " .. tostring(berr) .. "\n"
        else
            local leo, aerr = self:_apply(topic, partition, payload)
            if not leo then
                status, ctype, body = 500, "text/plain", tostring(aerr) .. "\n"
                log:error("apply %s/partition-%s: %s", topic, tostring(partition), aerr)
            else
                status, ctype, body = 200, "application/json",
                    json.encode({ offset = leo })
            end
        end
    end

    pcall(function()
        respond(self.reactor, sock, status, ctype, body)
        sock:close()
    end)
end

function M:start()
    local _, lerr = self.reactor:listen(self.host, self.port,
        function(sock) self:_handle(sock) end)
    if lerr then
        log:error("replica listen failed on %s:%d: %s", self.host, self.port, lerr)
        return nil, lerr
    end
    log:info("replica endpoint listening on %s:%d (POST /replicate)",
        self.host, self.port)
    return true
end

return M
