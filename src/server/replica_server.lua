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
local httpk   = require("src.server.http_kit")
local log     = require("src.log.logger").get("replica_server")

local M = {}
M.__index = M

local READ_DEADLINE  = 10
local WRITE_DEADLINE = 5
local MAX_BODY       = 8 * 1024 * 1024   -- generous vs the 1 MiB frame cap

function M.new(opts)
    return setmetatable({
        reactor = assert(opts.reactor, "reactor required"),
        broker  = assert(opts.broker, "broker required"),
        host    = opts.host or "127.0.0.1",
        port    = assert(opts.port, "port required"),
    }, M)
end

-- HTTP plumbing (read_headers / read_body / respond) lives in
-- src/server/http_kit.lua, shared with the cluster endpoint.
local function respond(reactor, sock, status, ctype, body)
    httpk.respond(reactor, sock, status, ctype, body, WRITE_DEADLINE)
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
    local headers, leftover = httpk.read_headers(self.reactor, sock, deadline)
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
        local payload, berr = httpk.read_body(
            self.reactor, sock, leftover, clen, deadline, MAX_BODY)
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
