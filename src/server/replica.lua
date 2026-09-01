local socket = require("socket")
local http   = require("socket.http")
local ltn12  = require("ltn12")
local json   = require("dkjson")
local msg_m  = require("src.record.message")
local tls_m  = require("src.io.tls")
local log    = require("src.log.logger").get("replicate")

local DEFAULT_QUEUE_SIZE = 1000
local DEFAULT_TIMEOUT_IN_SECONDS = 5

local ReplicaClient = {}
ReplicaClient.__index = ReplicaClient

function ReplicaClient.new(address, timeout, opts)
    assert(type(address) == "string", "address must be a string")
    opts = opts or {}

    return setmetatable({
        address = address,
        timeout = timeout or DEFAULT_TIMEOUT_IN_SECONDS,
        tls     = opts.tls,
    }, ReplicaClient)
end

function ReplicaClient:send(topic_name, partition_id, sender_replica_id, payload)
    local timeout = self.timeout
    local response_body = {}

    local _, code, _hdrs, status = http.request{
        url     = string.format("%s://%s/replicate",
                                self.tls and "https" or "http", self.address),
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
        create = self.tls
            and tls_m.http_create(self.tls.params, timeout, self.tls.server_name)
            or function()
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
        queue       = {},
        head        = 1,
        tail        = 0,
        suspended   = nil,
        running     = false,
    }, PartitionReplica)
end

function PartitionReplica:_queue_len()
    return self.tail - self.head + 1
end

local ReplicatedPartition = {}
ReplicatedPartition.__index = ReplicatedPartition

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

function ReplicatedPartition:start()
    if not self.is_leader then return end
    self.running = true
    for i = 1, #self.replicas do
        self.replicas[i].running = true
    end
end

function ReplicatedPartition:write(scheduler, msg)
    local offset, err = self.partition:write_message(msg)
    if err then return -1, err end

    if not (self.is_leader and self.running) then
        return offset, nil
    end

    local payload, serr = msg_m.serialize_message(msg)
    if not payload then
        log:error("partition %d: serialize failed: %s",
            self.partition.id, serr)
        return offset, nil
    end

    for i = 1, #self.replicas do
        local replica = self.replicas[i]
        if replica:_queue_len() >= self.max_queue then
            log:warn("partition %d replica %d: queue full, dropping",
                self.partition.id, replica.id)
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

function ReplicatedPartition:run_replica(scheduler, replica)
    while replica.running do

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
                log:error("partition %d -> replica %d: %s",
                    self.partition.id, replica.id, serr)
            else
                replica.last_offset = new_offset
            end
        end

        if replica.running then
            replica.suspended = coroutine.running()
            coroutine.yield()
        end
    end
end

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
