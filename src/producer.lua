local message = require("src.message")
local socket  = require("socket")
local brk     = require("src.broker")
local Future  = require("src.future")
local io_sync = require("src.io_sync")
local util    = require("src.util")

-- AckMode defines the producer acknowledgment levels
local AckMode = {
    -- AckNone means no acknowledgment is required
    AckNone     = 0,
    -- AckLeader means the leader must acknowledge (fsync after write)
    AckLeader   = 1,
    -- AckAll means all replicas must acknowledge (not implemented in this version)
    AckAll      = -1,
}

local function rand_intn(n)
    assert(type(n) == "number", "n must be a number")

    if n <= 0 then return 0 end
    return math.random(0, n-1)
end

local function fnv32a(key)
    local FNV_OFFSET_BASIS = 2166136261
    local FNV_PRIME        = 16777619

    local hash = FNV_OFFSET_BASIS

    for i = 1, #key do
        local byte = string.byte(key, i)
        hash = hash ~ byte
        hash = (hash * FNV_PRIME) & 0xFFFFFFFF
    end

    return hash
end

local function default_partitioner(key, numPartitions)
    assert(type(key) == "string", "key must be a string")
    assert(type(numPartitions) == "number", "numPartitions must be a number")

    if #key == 0 then
        return rand_intn(numPartitions)
    end

    local hash = fnv32a(key)
    return hash % numPartitions
end

-- Millisecond-resolution timestamps. We'd love nanosecond precision to
-- match the Go reference's UnixNano(), but ns-since-epoch is ~1.8e18 in
-- 2026 and Lua doubles only hold integers cleanly up to 2^53 ≈ 9e15.
-- ms-since-epoch is ~1.8e12, well within precision, and ms granularity
-- is more than enough for ordering messages within a partition.
local function now_ms()
    return math.floor(socket.gettime() * 1000)
end

local ProduceOptions = {}
ProduceOptions.__index = ProduceOptions

function ProduceOptions.new(ack_mode, timeout_in_seconds)
    return setmetatable({
        ack_mode           = ack_mode or AckMode.AckNone,
        timeout_in_seconds = timeout_in_seconds,    -- nil = no limit
    }, ProduceOptions)
end

local ProduceResult = {}
ProduceResult.__index = ProduceResult

function ProduceResult.new(topic, partition, offset, err)
    assert(type(topic) == "string", "topic must be a string")

    return setmetatable({
        topic     = topic,
        partition = partition,
        offset    = offset,
        error     = err,
    }, ProduceResult)
end

local Producer = {}
Producer.__index = Producer

function Producer.new(broker, acks)
    assert(getmetatable(broker) == brk.Broker, "broker must be a Broker instance")
    assert(type(acks) == "number", "acks must be a number")

    return setmetatable({
        broker = broker,
        acks = acks,
        partitioner = default_partitioner,
    }, Producer)
end

function Producer:produce(topic_name, msg)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")

    local valid, vErr = util.validate_topic_name(topic_name)
    if not valid then return -1, -1, vErr end

    if msg.timestamp == 0 then
        msg.timestamp = now_ms()
    end

    local topic, err = self.broker:get_topic(topic_name)
    if not topic then
        return -1, -1, string.format("failed to get topic: %s", err)
    end

    local num_partitions = #topic.partitions
    local partition_id   = self.partitioner(msg.key, num_partitions) + 1

    local partition = topic.partitions[partition_id]
    if not partition then
        return -1, -1, string.format("partition %d does not exist", partition_id)
    end

    local offset, werr = partition:write_message(msg)
    if werr then
        return -1, -1, string.format("failed to write message: %s", werr)
    end

    if self.acks > 0 then
        -- True fsync, not just userspace flush; otherwise acks=1 lies.
        local sok, serr = io_sync.sync(partition.file)
        if not sok then
            return -1, -1, string.format("failed to sync partition: %s", serr)
        end
        partition.last_sync = socket.gettime()
    end

    return partition_id, offset, nil
end

-- Async variant. Returns a Future that resolves to a ProduceResult.
-- NOTE: with the trivial scheduler (synchronous coroutine.resume), this
-- runs to completion on the calling resume — it does not actually overlap
-- with other I/O. Use it for the caller-friendly Future API, not for
-- concurrency gains.
function Producer:produce_async(scheduler, topic_name, msg, opts)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")
    opts = opts or {}
    local ack_mode = opts.ack_mode or self.acks
    local timeout  = opts.timeout_in_seconds   -- nil = no limit

    if msg.timestamp == 0 then
        msg.timestamp = now_ms()
    end

    local future = Future.new(scheduler)

    local co = coroutine.create(function()
        local started = socket.gettime()

        local valid, vErr = util.validate_topic_name(topic_name)
        if not valid then
            future:resolve(ProduceResult.new(topic_name, -1, -1, vErr))
            return
        end

        local topic, err = self.broker:get_topic(topic_name)
        if not topic then
            future:resolve(ProduceResult.new(topic_name, -1, -1,
                string.format("failed to get topic: %s", err)))
            return
        end

        local num_partitions = #topic.partitions
        local partition_id   = self.partitioner(msg.key, num_partitions) + 1
        local partition      = topic.partitions[partition_id]
        if not partition then
            future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                string.format("partition %d does not exist", partition_id)))
            return
        end

        local offset, werr = partition:write_message(msg)
        if werr then
            future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                string.format("failed to write message: %s", werr)))
            return
        end

        if ack_mode == AckMode.AckLeader then
            local sok, serr = io_sync.sync(partition.file)
            if not sok then
                future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                    string.format("failed to sync partition: %s", serr)))
                return
            end
            partition.last_sync = socket.gettime()
        end

        -- Soft timeout: the write itself isn't cancellable. The write has
        -- already succeeded; resolve with the offset and no error so the
        -- caller doesn't retry and duplicate. Caller can compare elapsed
        -- vs timeout themselves if monitoring is needed.
        local elapsed = socket.gettime() - started
        if timeout and elapsed > timeout then
            io.stderr:write(
                string.format("warn: produce exceeded timeout: %.3fs > %.3fs\n",
                    elapsed, timeout))
        end

        future:resolve(ProduceResult.new(topic_name, partition_id, offset, nil))
    end)

    scheduler.resume(co)
    return future
end

return {
    AckMode        = AckMode,
    ProduceOptions = ProduceOptions,
    ProduceResult  = ProduceResult,
    Producer       = Producer,
}
