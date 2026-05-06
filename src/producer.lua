local message = require("src.message")
local socket = require("socket")
local brk = require("src.broker")

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

return {
    Producer = Producer,
}
