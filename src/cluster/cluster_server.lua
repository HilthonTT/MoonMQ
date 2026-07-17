-- Inter-broker cluster endpoint. Serves the peer-side of the reassignment
-- layer on its own port, sharing the main reactor (like replica_server):
--
--   POST /cluster/ensure   json {topic, partitions}      → 200 {"ok":true}
--       Create the topic if missing (idempotent).
--   POST /cluster/append   X-Topic / X-Partition headers → 200 {"offset":LEO}
--       body: 1..N serialized records (len(8)|body each), applied in order.
--       Used for both migration batches and forwarded produces.
--   GET  /cluster/leo?topic=T&partition=N                → 200 {"offset":LEO}
--   POST /cluster/owner    json {topic, partition, owner}→ 200 {"ok":true}
--       Record an ownership change in the local assignments table.
--   GET  /cluster/loads                                  → 200 {"broker_id":..,"loads":[..]}
--       Per-partition disk bytes, feeding peers' autobalancer models.
--
-- HTTP/1.1, Connection: close. Binds 127.0.0.1 by default. When opts.token is
-- set, every request must carry a matching X-Cluster-Token — set it whenever
-- the port is reachable beyond loopback.

local socket = require("socket")
local json   = require("dkjson")
local msg_m  = require("src.record.message")
local httpk  = require("src.server.http_kit")
local util_m = require("src.core.util")
local log    = require("src.log.logger").get("cluster_server")

local M = {}
M.__index = M

local READ_DEADLINE  = 10
local WRITE_DEADLINE = 5
local MAX_BODY       = 8 * 1024 * 1024
local MAX_PARTITIONS = 1024   -- same bound CREATE_TOPIC enforces on the wire

function M.new(opts)
    return setmetatable({
        reactor     = assert(opts.reactor, "reactor required"),
        broker      = assert(opts.broker, "broker required"),
        assignments = assert(opts.assignments, "assignments required"),
        broker_id   = assert(opts.broker_id, "broker_id required"),
        host        = opts.host or "127.0.0.1",
        port        = assert(opts.port, "port required"),
        token       = opts.token,
    }, M)
end

-- Constant-time token compare — don't leak the match length through timing.
local function token_ok(expect, got)
    if not expect then return true end
    if type(got) ~= "string" then return false end
    local diff = #expect ~ #got
    for i = 1, math.max(#expect, #got) do
        diff = diff | ((expect:byte(i) or 0) ~ (got:byte(i) or 0))
    end
    return diff == 0
end

-- ---------------------------------------------------------------------------
-- Route implementations. Each returns (status, body_table_or_text).

function M:_ensure(body)
    local req = json.decode(body or "")
    if type(req) ~= "table" or type(req.topic) ~= "string"
        or type(req.partitions) ~= "number" then
        return 400, "ensure: need {topic, partitions}"
    end
    local valid, verr = util_m.validate_topic_name(req.topic)
    if not valid then return 400, "ensure: " .. tostring(verr) end
    if req.partitions < 1 or req.partitions > MAX_PARTITIONS then
        return 400, "ensure: partitions out of range"
    end

    local existing = self.broker.topic_manager.topics[req.topic]
    if existing then
        if #existing.partitions < req.partitions then
            return 400, string.format(
                "ensure: topic exists with %d partitions, need %d",
                #existing.partitions, req.partitions)
        end
        return 200, { ok = true }
    end

    local _, cerr = self.broker:create_topic(req.topic, req.partitions)
    if cerr then return 500, "ensure: " .. tostring(cerr) end
    log:info("created topic %s (%d partitions) for reassignment",
        req.topic, req.partitions)
    return 200, { ok = true }
end

-- Apply 1..N concatenated serialized records to (topic, partition), in order.
-- One request_sync at the end so a migration batch costs one fsync, not N.
function M:_append(topic_name, partition_id, payload)
    local topic, terr = self.broker:get_topic(topic_name)
    if not topic then return 404, "append: " .. tostring(terr) end
    local part = topic.partitions[partition_id]
    if not part then return 404, "append: no such partition" end

    local pos, applied = 1, 0
    while pos <= #payload do
        if #payload - pos + 1 < 8 then return 400, "append: short record header" end
        local total_size = string.unpack(">I8", payload, pos)
        if total_size < msg_m.MIN_BODY or total_size > #payload - pos - 7 then
            return 400, "append: truncated or corrupt record body"
        end
        local msg, derr = msg_m.decode_body(payload:sub(pos + 8, pos + 7 + total_size))
        if not msg then return 400, "append: decode: " .. tostring(derr) end

        local _, werr = part:write_message(msg)
        if werr then return 500, "append: write: " .. tostring(werr) end
        applied = applied + 1
        pos = pos + 8 + total_size
    end

    if applied > 0 and part.request_sync then
        local sok, serr = part:request_sync()
        if not sok then return 500, "append: sync: " .. tostring(serr) end
    end
    return 200, { offset = part.offset, applied = applied }
end

function M:_leo(query)
    local q = httpk.parse_query(query)
    local partition_id = tonumber(q.partition)
    if type(q.topic) ~= "string" or not partition_id then
        return 400, "leo: need topic & partition"
    end
    local topic, terr = self.broker:get_topic(q.topic)
    if not topic then return 404, "leo: " .. tostring(terr) end
    local part = topic.partitions[partition_id]
    if not part then return 404, "leo: no such partition" end
    return 200, { offset = part.offset }
end

function M:_owner(body)
    local req = json.decode(body or "")
    if type(req) ~= "table" or type(req.topic) ~= "string"
        or type(req.partition) ~= "number" or type(req.owner) ~= "string" then
        return 400, "owner: need {topic, partition, owner}"
    end
    local ok, err = self.assignments:set_owner(req.topic, req.partition, req.owner)
    if not ok then return 500, "owner: " .. tostring(err) end
    log:info("ownership: %s/partition-%d -> %s", req.topic, req.partition, req.owner)
    return 200, { ok = true }
end

function M:_loads()
    local loads = {}
    for name, topic in pairs(self.broker.topic_manager.topics) do
        if name:sub(1, 2) ~= "__" then   -- internal topics never rebalance
            for _, p in ipairs(topic.partitions) do
                -- Only partitions this broker owns: a moved-away partition's
                -- leftover local data must not count as our load.
                if self.assignments:owned_by_self(name, p.id) then
                    loads[#loads + 1] = {
                        topic      = name,
                        partition  = p.id,
                        disk_bytes = p.offset or 0,
                    }
                end
            end
        end
    end
    return 200, { broker_id = self.broker_id, loads = loads }
end

-- ---------------------------------------------------------------------------

function M:_handle(sock)
    local deadline = socket.gettime() + READ_DEADLINE
    local headers, leftover = httpk.read_headers(self.reactor, sock, deadline)
    if not headers then pcall(function() sock:close() end); return end

    local method, path, query = httpk.request_line(headers)
    local clen = tonumber(httpk.header(headers, "Content%-Length")) or 0

    local status, out
    if not token_ok(self.token, httpk.header(headers, "X%-Cluster%-Token")) then
        status, out = 401, "bad or missing X-Cluster-Token"
    elseif method == "POST" and path == "/cluster/append" then
        local topic     = httpk.header(headers, "X%-Topic")
        local partition = tonumber(httpk.header(headers, "X%-Partition"))
        if not topic or not partition then
            status, out = 400, "append: missing X-Topic/X-Partition"
        else
            local payload, berr = httpk.read_body(
                self.reactor, sock, leftover, clen, deadline, MAX_BODY)
            if not payload then
                status, out = 400, "append: body: " .. tostring(berr)
            else
                status, out = self:_append(topic, partition, payload)
            end
        end
    elseif method == "POST" and (path == "/cluster/ensure" or path == "/cluster/owner") then
        local body, berr = httpk.read_body(
            self.reactor, sock, leftover, clen, deadline, MAX_BODY)
        if not body then
            status, out = 400, "body: " .. tostring(berr)
        elseif path == "/cluster/ensure" then
            status, out = self:_ensure(body)
        else
            status, out = self:_owner(body)
        end
    elseif method == "GET" and path == "/cluster/leo" then
        status, out = self:_leo(query)
    elseif method == "GET" and path == "/cluster/loads" then
        status, out = self:_loads()
    else
        status, out = 404, "unknown cluster route"
    end

    local ctype, body
    if type(out) == "table" then
        ctype, body = "application/json", json.encode(out)
    else
        ctype, body = "text/plain", tostring(out) .. "\n"
        if status >= 500 then log:error("%s %s: %s", method, path, out) end
    end

    pcall(function()
        httpk.respond(self.reactor, sock, status, ctype, body, WRITE_DEADLINE)
        sock:close()
    end)
end

function M:start()
    local _, lerr = self.reactor:listen(self.host, self.port,
        function(sock) self:_handle(sock) end)
    if lerr then
        log:error("cluster listen failed on %s:%d: %s", self.host, self.port, lerr)
        return nil, lerr
    end
    log:info("cluster endpoint listening on %s:%d (broker_id=%s%s)",
        self.host, self.port, self.broker_id,
        self.token and ", token auth on" or "")
    return true
end

return M
