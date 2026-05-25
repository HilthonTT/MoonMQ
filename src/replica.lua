local socket = require("socket")
local http   = require("socket.http")
local ltn12  = require("ltn12")
local json   = require("dkjson")        -- only needs json.decode
local msg_m  = require("src.message")

local DEFAULT_QUEUE_SIZE = 1000
local DEFAULT_TIMEOUT_IN_SECONDS = 5 -- seconds

local ReplicaClient = {}
ReplicaClient.__index = ReplicaClient

function ReplicaClient.new(address, timeout)
    assert(type(address) == "string", "address must be a string")

    return setmetatable({
        address = address,
        timeout = timeout or DEFAULT_TIMEOUT_IN_SECONDS,
    }, ReplicaClient)
end

-- POST a serialized message to the replica's /replicate endpoint.
-- Returns (offset, nil) on 200 OK with valid JSON, otherwise (nil, err).
--
-- We previously set socket.http.TIMEOUT module-globally, which races
-- when multiple replica coroutines share the same scheduler. The custom
-- `create` below binds the timeout to *this* request's TCP socket, so
-- concurrent senders no longer clobber each other.
function ReplicaClient:send(topic_name, partition_id, sender_replica_id, payload)
    local timeout = self.timeout
    local response_body = {}

    local _, code, _hdrs, status = http.request{
        url     = string.format("http://%s/replicate", self.address),
        method  = "POST",
        headers = {
            ["Content-Type"]   = "application/octet-stream",
            ["Content-Length"] = tostring(#payload),
            ["X-Replica-ID"]   = tostring(sender_replica_id),
            ["X-Topic"]        = topic_name,
            ["X-Partition"]    = tostring(partition_id),
        },
        source = ltn12.source.string(payload),
        sink   = ltn12.sink.table(response_body),
        create = function()
            local s = socket.tcp()
            s:settimeout(timeout)
            return s
        end,
    }

    if type(code) ~= "number" then
        return nil, string.format("request failed: %s", tostring(code or status))
    end
    if code ~= 200 then
        return nil, string.format("HTTP %d: %s", code, table.concat(response_body))
    end

    local body = table.concat(response_body)
    local parsed, _, perr = json.decode(body)
    if not parsed then
        return nil, string.format("invalid JSON response: %s", perr)
    end
    if type(parsed.offset) ~= "number" then
        return nil, "response missing 'offset' field"
    end

    return parsed.offset, nil
end

local PartitionReplica = {}
PartitionReplica.__index = PartitionReplica

function PartitionReplica.new(id, client)
    return setmetatable({
        id          = id,
        client      = client,
        last_offset = -1,
        queue       = {},     -- ring of pending payloads
        head        = 1,
        tail        = 0,
        suspended   = nil,    -- worker coroutine when waiting for work
        running     = false,
    }, PartitionReplica)
end

function PartitionReplica:_queue_len()
    return self.tail - self.head + 1
end

local ReplicatedPartition = {}
ReplicatedPartition.__index = ReplicatedPartition

-- replica_addresses can be:
--   * a 1-indexed array { [1] = "host:port", [2] = "host:port", ... }, or
--   * a map keyed by replica id { [id] = "host:port", ... }.
-- Entries whose key equals replica_id are skipped (don't replicate to self).
function ReplicatedPartition.new(partition, replica_id, is_leader, replica_addresses, opts) 
    assert(type(partition) == "table",  "partition must be a Partition instance")
    assert(type(replica_id) == "number", "replica_id must be a number")
    assert(type(is_leader) == "boolean", "is_leader must be a boolean")
    assert(type(replica_addresses) == "table", "replica_addresses must be a table")
    opts = opts or {}

    local self = setmetatable({
        partition = partition,
        replica_id = replica_id,
        is_leader = is_leader,
        replicas = {},
        max_queue = opts.max_queue or DEFAULT_QUEUE_SIZE,
        timeout = opts.timeout or DEFAULT_TIMEOUT_IN_SECONDS,
        running = false,
    }, ReplicatedPartition)

    for id, address in pairs(replica_addresses) do
        if id ~= replica_id then
            local client  = ReplicaClient.new(address, self.timeout)
            self.replicas[#self.replicas + 1] = PartitionReplica.new(id, client)
        end
    end

    return self
end

-- Mark the partition as live. Call before spawning workers; serves the
-- same role as `go rp.replicationLoop()` in the Go constructor but
-- without actually creating coroutines (the caller owns that).
function ReplicatedPartition:start()
    if not self.is_leader then return end
    self.running = true
    for i = 1, #self.replicas do
        self.replicas[i].running = true
    end
end

-- Write locally, then enqueue the serialized payload for every replica.
-- Returns (offset, nil) on local-write success, (-1, err) on failure.
-- Replication failures do NOT propagate; they're logged. Local write
-- success means the leader's log is durable enough for `acks=1`.
function ReplicatedPartition:write(scheduler, msg)
    local offset, err = self.partition:write_message(msg)
    if err then return -1, err end

    if not (self.is_leader and self.running) then
        return offset, nil
    end

    local payload, serr = msg_m.serialize_message(msg)
    if not payload then
        -- Local write already succeeded; we can't unwind it. Log and move on.
        io.stderr:write(string.format(
            "[replicate] partition %d: serialize failed: %s\n",
            self.partition.id, serr))
        return offset, nil
    end

    for i = 1, #self.replicas do
        local replica = self.replicas[i]
        if replica:_queue_len() >= self.max_queue then
            -- Same back-pressure semantics as Go's buffered chan(1000)
            -- when full: drop, since we can't block the writer without
            -- killing throughput.
            io.stderr:write(string.format(
                "[replicate] partition %d replica %d: queue full, dropping\n",
                self.partition.id, replica.id))
        else
            replica.tail = replica.tail + 1
            replica.queue[replica.tail] = payload

            if replica.suspended then
                local worker = replica.suspended
                replica.suspended = nil
                scheduler.resume(worker)
            end
        end
    end

    return offset, nil
end

-- Long-lived worker for one replica. Run as a coroutine on your scheduler,
-- one per entry in self.replicas:
--
--   rp:start()
--   for i = 1, #rp.replicas do
--       local r = rp.replicas[i]
--       scheduler.spawn(function() rp:run_replica(scheduler, r) end)
--   end
function ReplicatedPartition:run_replica(scheduler, replica)
    while replica.running do
        -- Drain the queue. Each send is synchronous within this coroutine,
        -- so per-replica ordering is preserved and a slow replica only
        -- stalls itself.

        while replica.head <= replica.tail and replica.running do
            local payload = replica.queue[replica.head]
            replica.queue[replica.head] = nil
            replica.head = replica.head + 1

            local new_offset, serr = replica.client:send(
                self.partition.topic.name,
                self.partition.id,
                self.replica_id,
                payload)

            if serr then
                io.stderr:write(string.format(
                    "[replicate] partition %d -> replica %d: %s\n",
                    self.partition.id, replica.id, serr))
            else
                replica.last_offset = new_offset
            end
        end

        -- Sleep until write() wakes us. The check-then-yield is safe
        -- because write() only resumes us if suspended is set, and
        -- we set it before yielding.
        if replica.running then
            replica.suspended = coroutine.running()
            coroutine.yield()
        end
    end
end

-- Stop accepting new replication and wake any sleeping workers so they
-- can exit their loop. Idempotent.
function ReplicatedPartition:close(scheduler)
    self.running = false
    for i = 1, #self.replicas do
        local replica = self.replicas[i]
        replica.running = false
        if replica.suspended then
            local worker      = replica.suspended
            replica.suspended = nil
            if scheduler then scheduler.resume(worker) end
        end
    end
end

return {
    ReplicatedPartition = ReplicatedPartition,
    PartitionReplica    = PartitionReplica,
    ReplicaClient       = ReplicaClient,
}
