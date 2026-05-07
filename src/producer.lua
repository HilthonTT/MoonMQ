local message = require("src.message")
local socket = require("socket")
local brk = require("src.broker")
local Future  = require("src.future")

-- AckMode defines the producer acknowledgment levels
local AckMode = {
    -- AckNone means no acknowledgment is required
    AckNone     = 0,
    -- AckLeader means the leader must acknowledge
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
        hash = hash ~ byte                     -- XOR
        hash = (hash * FNV_PRIME) & 0xFFFFFFFF -- Multiply + keep 32 bits
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

local ProduceOptions = {}
ProduceOptions.__index = ProduceOptions

function ProduceOptions.new(ack_mode, timeout_in_seconds)
    assert(type(timeout_in_seconds) == "number", "timeout_in_seconds must be a number")

    return setmetatable({
        ack_mode           = ack_mode or AckMode.AckNone,
        timeout_in_seconds = timeout_in_seconds,
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

    if msg.timestamp == 0 then
        msg.timestamp = os.time()
    end

    local topic, err = self.broker:get_topic(topic_name)
    if not topic or err then
        return -1, -1, ("failed to get topic: %s"):format(err)
    end

    -- Determine partition
    local num_partitions = #topic.partitions
    local partition_id = self.partitioner(msg.key, num_partitions) + 1

    -- Get partition
    local partition = topic.partitions[partition_id]
    if not partition then
        return -1, -1, ("partition %d does not exist"):format(partition_id)
    end

    -- Write message to partition
    local offset, err = partition:write_message(msg)
    if err then
        return -1, -1, ("failed to write message: %s"):format(err)
    end

    if self.acks > 0 then
        partition.file:flush()
        partition.last_sync = socket.gettime()
    end

    return partition_id, offset, nil
end

-- Async variant, returns a Future that
-- resolves to a ProduceResult. Caller awaits when convenient.
function Producer:produce_async(scheduler, topic_name, msg, opts)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")
    opts = opts or {}
    local ack_mode = opts.ack_mode or self.acks
    local timeout  = opts.timeout_in_seconds   -- nil = no limit

    if msg.timestamp == 0 then
        msg.timestamp = os.time()
    end

    local future = Future.new(scheduler)

    -- The "goroutine". Spawned now, runs on the next tick.
    local co = coroutine.create(function()
        local started = socket.gettime()

        local topic, err = self.broker:get_topic(topic_name)
        if not topic or err then
            future:resolve(ProduceResult.new(topic_name, -1, -1,
                ("failed to get topic: %s"):format(err)))
            return
        end

        local num_partitions = #topic.partitions
        local partition_id   = self.partitioner(msg.key, num_partitions) + 1
        local partition      = topic.partitions[partition_id]
        if not partition then
            future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                ("partition %d does not exist"):format(partition_id)))
            return
        end

        local offset, werr = partition:write_message(msg)
        if werr then
            future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                ("failed to write message: %s"):format(werr)))
            return
        end

        if ack_mode == AckMode.AckLeader then
            partition.file:flush()
            partition.last_sync = socket.gettime()
        end

        -- Soft timeout, matching Go's decorative context.WithTimeout:
        -- the write itself isn't cancellable, so we report the deadline
        -- miss after the fact. Still useful for monitoring.
        local elapsed = socket.gettime() - started
        if timeout and elapsed > timeout then
            future:resolve(ProduceResult.new(topic_name, partition_id, offset,
                ("produce exceeded timeout: %.3fs > %.3fs"):format(elapsed, timeout)))
            return
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
