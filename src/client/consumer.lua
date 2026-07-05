local brk_m  = require("src.storage.broker")
local util_m = require("src.core.util")
local log    = require("src.log.logger").get("consumer")

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
    assert(getmetatable(broker) == brk_m.Broker, "broker must be a Broker instance")
    assert(type(group_id) == "string", "group_id must be a string")

    return setmetatable({
        broker      = broker,
        group_id    = group_id,
        offsets     = {}, -- topic_name -> partition_id -> offset
        -- Topic-ref cache. Populated at subscribe time; consulted in
        -- poll() instead of going through broker:get_topic on every
        -- iteration. The broker's topic table doesn't move references
        -- around (topics are not re-created in place), so the cached
        -- ref stays valid for the consumer's lifetime — when a topic
        -- is dropped (not currently supported) we'd invalidate here.
        topics      = {},
        auto_commit = true,
        -- Optional partition-assignment filter: topic_name -> { [pid]=true }.
        -- nil means "no restriction" — read every partition of every subscribed
        -- topic (the standalone / non-group-member case). When set (a joined
        -- group member), poll() reads ONLY the partitions this member owns, so
        -- two members of the same group never both consume a partition
        -- (duplicate delivery) nor both commit the same (group,topic,partition)
        -- offset key (clobbering). See Consumer:set_assignment.
        assignment  = nil,
    }, Consumer)
end

-- set_assignment restricts which partitions poll() will read to those this
-- consumer owns. `assignment` is a group member's { topic -> { pid, ... } } map,
-- or nil to lift the restriction entirely. The list form is normalised into a
-- lookup set. The server calls this before every poll from the live group
-- state, so a rebalance is honoured on the very next poll without pushing a new
-- assignment to the client.
function Consumer:set_assignment(assignment)
    if assignment == nil then
        self.assignment = nil
        return
    end
    local set = {}
    for topic_name, pids in pairs(assignment) do
        local s = {}
        for _, pid in ipairs(pids) do s[pid] = true end
        set[topic_name] = s
    end
    self.assignment = set
end

-- _partition_allowed reports whether poll() may read (topic, partition) under
-- the current assignment. With no assignment set, everything is allowed. With
-- an assignment set, a topic absent from it (e.g. a member subscribed but
-- assigned no partitions, or a reaped member with an empty assignment) yields
-- nothing.
function Consumer:_partition_allowed(topic_name, partition_id)
    if self.assignment == nil then return true end
    local s = self.assignment[topic_name]
    if not s then return false end
    return s[partition_id] == true
end

-- Subscribe subscribes the consumer to a topic and initializes its offsets.
-- Returns (true, nil) on success, (nil, err) on failure.
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
                -- No persisted offset (or an I/O error we can't recover from
                -- here); start from the beginning. load_err is informational.
                self.offsets[topic_name][partition.id] = 0
                _ = load_err
            end
        end
    end

    return true, nil
end

-- Poll reads at most one message PER partition across all subscribed topics.
-- Returns (records, nil) on success, (nil, err) on failure.
local function is_eof_error(read_err)
    return read_err ~= nil and read_err:find("EOF", 1, true) ~= nil
end

function Consumer:poll()
    local records = {}

    for topic_name, partition_offsets in pairs(self.offsets) do
        -- Use the cached topic ref from subscribe(); fall back to
        -- broker:get_topic only if it's missing (defensive — should
        -- only happen if offsets[] is populated without going through
        -- subscribe(), which the public API doesn't allow).
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
            -- Skip partitions this consumer isn't assigned (group member),
            -- partitions that vanished, or ones we've consumed up to the tail.
            if self:_partition_allowed(topic_name, partition_id)
               and partition and offset < partition.offset then
                local msg, next_offset, read_err = partition:read_message(offset)
                if msg then
                    records[#records + 1] = ConsumerRecord.new(
                        topic_name,
                        partition_id,
                        offset,
                        msg.key,
                        msg.value,
                        msg.timestamp
                    )
                    self.offsets[topic_name][partition_id] = next_offset

                    if self.auto_commit then
                        local cok, cerr = self:commit_offset(topic_name, partition_id, next_offset)
                        if not cok then
                            return nil, cerr
                        end
                    end
                else
                    -- Skip past a torn / corrupted tail; otherwise surface.
                    if is_eof_error(read_err) then
                        self.offsets[topic_name][partition_id] = partition.offset
                    else
                        return nil, string.format("failed to read message: %s", read_err or "unknown error")
                    end
                end
            end
        end
    end

    return records, nil
end

-- CommitOffsets flushes all current offsets to storage.
-- Returns (true, nil) on success, (nil, err) on failure.
function Consumer:commit_offsets()
    for topic_name, partition_offsets in pairs(self.offsets) do
        for partition_id, offset in pairs(partition_offsets) do
            local ok, err = self:commit_offset(topic_name, partition_id, offset)
            if not ok then
                return nil, err
            end
        end
    end
    return true, nil
end

-- commit_offset persists a single offset via the broker's OffsetManager
-- (durable, keyed by this consumer's group). Returns (true, nil) or (nil, err).
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

-- load_offset reads the durable committed offset for this group from the
-- broker's OffsetManager. Returns (offset, nil) when found, or
-- (nil, "no stored offset") when this group has never committed it.
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
