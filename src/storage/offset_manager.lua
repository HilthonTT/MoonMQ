local msg_m = require("src.record.message")
local log = require("src.log.logger").get("offset_manager")
local hash_m = require("src.core.hash")
local time_m = require("src.core.time")

local OFFSETS_TOPIC = "__consumer_offsets"
local DEFAULT_PARTITIONS = 16

local OFFSETS_BACKEND          = "commitlog"
local OFFSETS_MAX_SEGMENT_SIZE = 8 * 1024 * 1024

local KEY_FMT   = ">s2s2I4"
local VALUE_FMT = ">I8"

local OffsetManager = {}
OffsetManager.__index = OffsetManager

function OffsetManager.new(topic_manager, opts)
    assert(type(topic_manager) == "table", "topic_manager must be a TopicManager")
    opts = opts or {}

    local nparts = opts.num_partitions or DEFAULT_PARTITIONS
    assert(type(nparts) == "number" and nparts >= 1, "num_partitions must be >= 1")

    local topic = topic_manager.topics[OFFSETS_TOPIC]
    if not topic then
        local created, err = topic_manager:create_topic(OFFSETS_TOPIC, nparts, {
            backend          = OFFSETS_BACKEND,
            cleanup_policy   = "compact",
            max_segment_size = OFFSETS_MAX_SEGMENT_SIZE,
        })
        if not created then
            return nil, string.format("failed to create %s: %s", OFFSETS_TOPIC, err)
        end
        topic = created
    end

    local self = setmetatable({
        topic  = topic,
        nparts = #topic.partitions,
        map    = {},
    }, OffsetManager)

    local rerr = self:recover()
    if rerr then
        return nil, rerr
    end

    return self, nil
end

function OffsetManager.topic_name()
    return OFFSETS_TOPIC
end

local function set_in_map(map, group, topic, partition, offset)
    local g = map[group]
    if not g then g = {}; map[group] = g end
    local t = g[topic]
    if not t then t = {}; g[topic] = t end
    t[partition] = offset
end

local function unset_in_map(map, group, topic, partition)
    local g = map[group]; if not g then return end
    local t = g[topic];   if not t then return end
    t[partition] = nil
    if next(t) == nil then g[topic] = nil end
    if next(g) == nil then map[group] = nil end
end

function OffsetManager:_partition_for(group)
    return (hash_m.fnv1a(group) % self.nparts) + 1
end

function OffsetManager:_append(group, topic, partition, val)
    local key = string.pack(KEY_FMT, group, topic, partition)
    local rec = msg_m.Message.new(key, val, time_m.now_ms())

    local part = self.topic.partitions[self:_partition_for(group)]
    local _, werr = part:write_message(rec)
    if werr then
        return nil, string.format("offset commit write failed: %s", werr)
    end

    if part.request_sync then
        local ok, serr = part:request_sync()
        if not ok then
            return nil, string.format("offset commit sync failed: %s", tostring(serr))
        end
    end

    return true, nil
end

function OffsetManager:commit(group, topic, partition, offset)
    assert(type(group) == "string", "group must be a string")
    assert(type(topic) == "string", "topic must be a string")
    assert(type(partition) == "number", "partition must be a number")
    assert(type(offset) == "number", "offset must be a number")

    local ok, err = self:_append(group, topic, partition,
        string.pack(VALUE_FMT, offset))
    if not ok then return nil, err end

    set_in_map(self.map, group, topic, partition, offset)
    return true, nil
end

function OffsetManager:delete(group, topic, partition)
    assert(type(group) == "string", "group must be a string")
    assert(type(topic) == "string", "topic must be a string")
    assert(type(partition) == "number", "partition must be a number")

    local ok, err = self:_append(group, topic, partition, "")
    if not ok then return nil, err end

    unset_in_map(self.map, group, topic, partition)
    return true, nil
end

function OffsetManager:delete_group(group)
    assert(type(group) == "string", "group must be a string")

    local n = 0
    for _, o in ipairs(self:offsets_for_group(group)) do
        local ok, err = self:delete(group, o.topic, o.partition)
        if not ok then return nil, err end
        n = n + 1
    end
    return n, nil
end

function OffsetManager:delete_topic_offsets(topic)
    assert(type(topic) == "string", "topic must be a string")

    local doomed = {}
    for group, topics in pairs(self.map) do
        local t = topics[topic]
        if t then
            for partition in pairs(t) do
                doomed[#doomed + 1] = { group = group, partition = partition }
            end
        end
    end

    local n = 0
    for _, d in ipairs(doomed) do
        local ok, err = self:delete(d.group, topic, d.partition)
        if not ok then return nil, err end
        n = n + 1
    end
    return n, nil
end

function OffsetManager:groups()
    local out = {}
    for group in pairs(self.map) do out[#out + 1] = group end
    table.sort(out)
    return out
end

function OffsetManager:offsets_for_group(group)
    assert(type(group) == "string", "group must be a string")

    local out = {}
    local topics = self.map[group]
    if not topics then return out end

    for topic, parts in pairs(topics) do
        for partition, offset in pairs(parts) do
            out[#out + 1] = {
                topic = topic, partition = partition, offset = offset,
            }
        end
    end
    table.sort(out, function(a, b)
        if a.topic ~= b.topic then return a.topic < b.topic end
        return a.partition < b.partition
    end)
    return out
end

function OffsetManager:fetch(group, topic, partition)
    local g = self.map[group]; if not g then return nil end
    local t = g[topic];        if not t then return nil end
    return t[partition]
end

function OffsetManager:offsets_for_partition(topic, partition)
    local out = {}
    for group, topics in pairs(self.map) do
        local t = topics[topic]
        if t and t[partition] ~= nil then
            out[group] = t[partition]
        end
    end
    return out
end

function OffsetManager:recover()
    local restored = 0

    for _, part in ipairs(self.topic.partitions) do
        local serr = part:scan(function(_offset, msg)
            local ok, group, topic, partition = pcall(string.unpack, KEY_FMT, msg.key)
            if ok and #msg.value == 0 then
                unset_in_map(self.map, group, topic, partition)
                return
            end
            local vok, stored = pcall(string.unpack, VALUE_FMT, msg.value)
            if ok and vok then
                set_in_map(self.map, group, topic, partition, stored)
                restored = restored + 1
            else
                log:warn("%s/partition-%d: skipping undecodable offset record",
                    OFFSETS_TOPIC, part.id)
            end
        end)
        if serr then
            log:warn("%s/partition-%d: replay stopped early: %s",
                OFFSETS_TOPIC, part.id, serr)
        end
    end

    local live = 0
    for _, topics in pairs(self.map) do
        for _, parts in pairs(topics) do
            for _ in pairs(parts) do live = live + 1 end
        end
    end

    if live > 0 then
        log:info("recovered %d committed offset(s) from %s (%d record(s) replayed)",
            live, OFFSETS_TOPIC, restored)
    end
    return nil
end

return {
    OffsetManager = OffsetManager,
    OFFSETS_TOPIC = OFFSETS_TOPIC,
}