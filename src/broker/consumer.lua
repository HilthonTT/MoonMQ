local brk_m  = require("src.broker")
local util_m = require("src.core.util")
local compression = require("src.record.compression")
local log    = require("src.log.logger").get("consumer")

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

local Consumer = {}
Consumer.__index = Consumer

function Consumer.new(broker, group_id, opts)
    assert(getmetatable(broker) == brk_m.Broker, "broker must be a Broker instance")
    assert(type(group_id) == "string", "group_id must be a string")
    opts = opts or {}
    local isolation = opts.isolation or "read_uncommitted"
    assert(isolation == "read_uncommitted" or isolation == "read_committed",
        "isolation must be read_uncommitted or read_committed")

    return setmetatable({
        broker      = broker,
        group_id    = group_id,
        isolation   = isolation,
        assignment     = nil,
        assignment_src = nil,
        offsets     = {},
        topics      = {},
        auto_commit = true,
    }, Consumer)
end

function Consumer:subscribe(topic_name)
    assert(type(topic_name) == "string", "topic_name must be a string")

    local valid, vErr = util_m.validate_topic_name(topic_name)
    if not valid then return nil, vErr end

    local topic, err = self.broker:get_topic(topic_name)
    if not topic then return nil, err end

    self.topics[topic_name] = topic
    if not self.offsets[topic_name] then
        self.offsets[topic_name] = {}
    end

    for _, partition in ipairs(topic.partitions) do
        if self.offsets[topic_name][partition.id] == nil then
            local stored_offset, load_err = self:load_offset(topic_name, partition.id)
            if stored_offset then
                self.offsets[topic_name][partition.id] = stored_offset
            else
                self.offsets[topic_name][partition.id] = 0
                _ = load_err
            end
        end
    end

    return true, nil
end

function Consumer:set_assignment(by_topic)
    if by_topic == self.assignment_src then return end
    self.assignment_src = by_topic
    if by_topic == nil then
        self.assignment = nil
        return
    end
    local set = {}
    for topic_name, ids in pairs(by_topic) do
        local owned = {}
        for _, id in ipairs(ids) do owned[id] = true end
        set[topic_name] = owned
    end
    self.assignment = set
end

function Consumer:owns(topic_name, partition_id)
    if self.assignment == nil then return true end
    local owned = self.assignment[topic_name]
    return owned ~= nil and owned[partition_id] == true
end

local function is_eof_error(read_err)
    return read_err ~= nil and read_err:find("EOF", 1, true) ~= nil
end

function Consumer:_pollable_partition_count()
    local n = 0
    for topic_name, partition_offsets in pairs(self.offsets) do
        for partition_id in pairs(partition_offsets) do
            if self:owns(topic_name, partition_id)
                and self.broker:serves_partition(topic_name, partition_id) then
                n = n + 1
            end
        end
    end
    return n
end

function Consumer:poll(opts)
    opts = opts or {}
    local records = {}

    local max_records       = opts.max_records
    local max_per_partition = opts.max_per_partition
    if not max_per_partition then
        if max_records then
            local n = self:_pollable_partition_count()
            max_per_partition = n > 0
                and math.max(1, math.ceil(max_records / n))
                or max_records
        else
            max_per_partition = 1
        end
    end

    for topic_name, partition_offsets in pairs(self.offsets) do
        local topic = self.topics[topic_name]
        if not topic then
            local t, err = self.broker:get_topic(topic_name)
            if not t then
                return nil, string.format("failed to get topic: %s", err)
            end
            topic = t
            self.topics[topic_name] = t
        end

        for partition_id, offset in pairs(partition_offsets) do
            local partition = topic.partitions[partition_id]
            local hi = partition and partition.offset or 0
            if self.isolation == "read_committed" and self.broker.transactions then
                local lso = self.broker.transactions:lso(topic_name, partition_id)
                if lso ~= nil and lso < hi then hi = lso end
            end
            local taken = 0
            while taken < max_per_partition
                and self:owns(topic_name, partition_id)
                and self.broker:serves_partition(topic_name, partition_id)
                and partition and offset < hi do
                local msg, next_offset, read_err = partition:read_message(offset)
                local skip_aborted = msg ~= nil and not msg:is_control()
                    and self.isolation == "read_committed"
                    and msg:is_txn() and self.broker.transactions ~= nil
                    and self.broker.transactions:is_aborted(
                        topic_name, partition_id, msg.pid, msg.epoch, offset)
                if msg and (msg:is_control() or skip_aborted) then
                    if self.auto_commit then
                        local cok, cerr =
                            self:commit_offset(topic_name, partition_id, next_offset)
                        if not cok then
                            if #records > 0 then return records, nil end
                            return nil, cerr
                        end
                    end
                    self.offsets[topic_name][partition_id] = next_offset
                    offset = next_offset
                elseif msg then
                    local value = msg.value
                    local mcodec = msg:codec()
                    if mcodec ~= 0 then
                        local plain, derr = compression.decompress(mcodec, msg.value)
                        if not plain then
                            if #records > 0 then return records, nil end
                            return nil, string.format(
                                "decompress %s/partition-%d offset %d: %s",
                                topic_name, partition_id, offset, tostring(derr))
                        end
                        value = plain
                    end
                    if self.auto_commit then
                        local cok, cerr = self:commit_offset(topic_name, partition_id, next_offset)
                        if not cok then
                            if #records > 0 then
                                return records, nil
                            end
                            return nil, cerr
                        end
                    end

                    self.offsets[topic_name][partition_id] = next_offset
                    records[#records + 1] = ConsumerRecord.new(
                        topic_name,
                        partition_id,
                        offset,
                        msg.key,
                        value,
                        msg.timestamp
                    )
                    taken  = taken + 1
                    offset = next_offset
                    if max_records and #records >= max_records then
                        return records, nil
                    end
                else
                    if is_eof_error(read_err) then
                        local oldest = partition.oldest_offset
                            and partition:oldest_offset() or 0
                        if offset < oldest then
                            self.offsets[topic_name][partition_id] = oldest
                        else
                            self.offsets[topic_name][partition_id] = partition.offset
                        end
                    else
                        return nil, string.format("failed to read message: %s", read_err or "unknown error")
                    end
                    break
                end
            end
        end
    end

    return records, nil
end

function Consumer:commit_offset(topic_name, partition_id, offset)
    local ok, err = self.broker:commit_offset(
        self.group_id, topic_name, partition_id, offset)
    if not ok then
        return nil, err
    end
    log:debug("committed offset %d for group '%s' topic '%s' partition %d",
        offset, self.group_id, topic_name, partition_id)
    return true, nil
end

function Consumer:load_offset(topic_name, partition_id)
    local offset = self.broker:fetch_offset(self.group_id, topic_name, partition_id)
    if offset == nil then
        return nil, "no stored offset"
    end
    return offset, nil
end

return {
    Consumer       = Consumer,
    ConsumerRecord = ConsumerRecord,
}
