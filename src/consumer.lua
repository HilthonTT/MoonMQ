local brk = require("src.broker")

-- ConsumerRecord holds a consumed message with its metadata
local ConsumerRecord = {}
ConsumerRecord.__index = ConsumerRecord

function ConsumerRecord.new(topic, partition, offset, key, value, timestamp)
    return setmetatable({
        topic     = topic,
        partition = partition,
        offset    = offset,
        key       = key,
        value     = value,
        timestamp = timestamp,
    }, ConsumerRecord)
end

-- Consumer reads messages from topics
local Consumer = {}
Consumer.__index = Consumer

function Consumer.new(broker, group_id)
    assert(getmetatable(broker) == brk.Broker, "broker must be a Broker instance")
    assert(type(group_id) == "string", "group_id must be a string")

    return setmetatable({
        broker      = broker,
        group_id    = group_id,
        offsets     = {}, -- topic_name -> partition_id -> offset
        auto_commit = true,
    }, Consumer)
end

-- Subscribe subscribes the consumer to a topic and initializes its offsets
function Consumer:subscribe(topic_name)
    assert(type(topic_name) == "string", "topic_name must be a string")

    local topic, err = self.broker:get_topic(topic_name)
    if not topic or err then
        return err
    end

    -- Initialize the offset table for this topic if needed
    if not self.offsets[topic_name] then
        self.offsets[topic_name] = {}
    end

    -- Initialize offsets for each partition
    for _, partition in ipairs(topic.partitions) do
        -- If we already have an offset for this partition, keep it
        if self.offsets[topic_name][partition.id] then
            goto continue
        end

        -- Otherwise load from storage, or start from the beginning
        local stored_offset, load_err = self:load_offset(topic_name, partition.id)
        if load_err then
            self.offsets[topic_name][partition.id] = 0
        else
            self.offsets[topic_name][partition.id] = stored_offset
        end

        ::continue::
    end

    return nil
end

-- Poll reads one available message per topic from all subscribed topics
function Consumer:poll()
    local records = {}

    for topic_name, partition_offsets in pairs(self.offsets) do
        local topic, err = self.broker:get_topic(topic_name)
        if not topic then
            return nil, ("failed to get topic: %s"):format(err)
        end

        for partition_id, offset in pairs(partition_offsets) do
            local partition = topic.partitions[partition_id]
            if not partition then goto continue end

            -- Skip partitions we've fully consumed
            if offset >= partition.offset then goto continue end

            local msg, next_offset, read_err = partition:read_message(offset)
            if not msg then
                -- Skip past corrupted / truncated messages
                if read_err and (read_err:find("EOF") or read_err:find("unexpected EOF")) then
                    self.offsets[topic_name][partition_id] = partition.offset
                    goto continue
                end
                return nil, ("failed to read message: %s"):format(read_err or "unknown error")
            end

            records[#records + 1] = ConsumerRecord.new(
                topic_name,
                partition_id,
                offset,
                msg.key,
                msg.value,
                msg.timestamp
            )

            -- Advance the offset
            self.offsets[topic_name][partition_id] = next_offset

            if self.auto_commit then
                local commit_err = self:commit_offset(topic_name, partition_id, next_offset)
                if commit_err then
                    return nil, commit_err
                end
            end

            -- Only read one message per partition per poll to be fair
            break

            ::continue::
        end
    end

    return records, nil
end

-- CommitOffsets flushes all current offsets to storage
function Consumer:commit_offsets()
    for topic_name, partition_offsets in pairs(self.offsets) do
        for partition_id, offset in pairs(partition_offsets) do
            local err = self:commit_offset(topic_name, partition_id, offset)
            if err then
                return err
            end
        end
    end
    return nil
end

-- commit_offset persists a single offset (stub — extend for file/DB storage)
function Consumer:commit_offset(topic_name, partition_id, offset)
    print(("Committed offset %d for topic '%s', partition %d")
        :format(offset, topic_name, partition_id))
    return nil
end

-- load_offset loads a persisted offset from storage (stub — always returns "no stored offset")
function Consumer:load_offset(topic_name, partition_id)
    return 0, "no stored offset"
end

return {
    Consumer       = Consumer,
    ConsumerRecord = ConsumerRecord,
}
