-- OffsetManager — durable consumer-group offsets via an internal topic.
--
-- Instead of a bespoke offset store, committed offsets are written as
-- ordinary records into an internal partitioned topic (__consumer_offsets),
-- exactly like Kafka. This reuses the broker's existing CRC-checked,
-- crash-recovering append-only logs: nothing new on the persistence side.
--
--   * commit(group, topic, partition, offset)
--       - append a record keyed by (group, topic, partition) whose value is
--         the committed offset, and update the in-memory map (latest wins).
--   * fetch(group, topic, partition) -> offset | nil
--       - pure in-memory read of the map.
--   * recover()
--       - replay every internal partition front-to-back at startup, rebuilding
--         the map. Because records are read in append order, the last write
--         for a key wins, which is the committed offset.
--
-- A group's commits all land in one internal partition — chosen by
-- hash(group) % N — so a single group's offset history stays append-ordered
-- within that partition, which is what makes "latest wins" on replay correct.
--
-- The internal topic is log-compaction-friendly (one live record per key),
-- so pointing the compaction cleaner at it later keeps it bounded. That's a
-- config concern, not this module's job.
--
-- OFFSET OPACITY. The stored offset value is whatever cursor the partition
-- backend hands the consumer: a byte offset for the segmented backend, a
-- message-count for the commitlog backend. OffsetManager never interprets it;
-- it round-trips the u64 verbatim.

local msg_m = require("src.record.message")
local log = require("src.log.logger")

local OFFSETS_TOPIC = "__consumer_offsets"
local DEFAULT_PARTITIONS = 16 -- Kafka defaults to 50; smaller suits a single broker

-- Key:   u16 group | u16 topic | u32 partition   (group/topic length-prefixed)
-- Value: u64 offset
-- string.pack ">s2" writes a 2-byte big-endian length followed by the bytes,
-- so group/topic never need to be \0-free — we don't rely on a delimiter.
local KEY_FMT   = ">s2s2I4"
local VALUE_FMT = ">I8"

local OffsetManager = {}
OffsetManager.__index = OffsetManager

-- FNV-1a (32-bit). Deterministic across restarts and platforms, which is
-- required: the partition a group hashes to must not move, or replay would
-- read a group's history from the wrong partition.
local function fnv1a(s)
    local hash = 2166136261
    for i = 1, #s do
        hash = (hash ~ s:byte(i)) & 0xFFFFFFFF
        hash = (hash * 16777619) & 0xFFFFFFFF
    end
    return hash
end

-- Ensure the internal topic exists, then build the manager and replay it.
-- `topic_manager` is the broker's TopicManager. We go straight through it
-- (not Broker:create_topic) so this works during Broker.new, before any
-- committer factory or reactor is attached.
function OffsetManager.new(topic_manager, opts)
    assert(type(topic_manager) == "table", "topic_manager must be a TopicManager")
    opts = opts or {}

    local nparts = opts.num_partitions or DEFAULT_PARTITIONS
    assert(type(nparts) == "number" and nparts >= 1, "num_partitions must be >= 1")

    -- On restart Broker:load_topics has already recreated the internal topic
    -- from its on-disk partition dirs; in that case the dir count is
    -- authoritative and we must not try to create it again.
    local topic = topic_manager.topics[OFFSETS_TOPIC]
    if not topic then
        local created, err = topic_manager:create_topic(OFFSETS_TOPIC, nparts, {
            -- Compaction keeps only the latest record per key — the natural
            -- fit for an offsets log. Falls back to the default backend if the
            -- cleaner isn't wired; correctness doesn't depend on it.
            cleanup_policy = "compact",
        })
        if not created then
            return nil, string.format("failed to create %s: %s", OFFSETS_TOPIC, err)
        end
        topic = created
    end

    local self = setmetatable({
        topic  = topic,
        nparts = #topic.partitions,
        -- map[group][topic][partition] = offset
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

-- commit appends an offset record and updates the in-memory map.
-- Returns (true, nil) or (nil, err).
function OffsetManager:commit(group, topic, partition, offset)
    assert(type(group) == "string", "group must be a string")
    assert(type(topic) == "string", "topic must be a string")
    assert(type(partition) == "number", "partition must be a number")
    assert(type(offset) == "number", "offset must be a number")

    local key = string.pack(KEY_FMT, group, topic, partition)
    local val = string.pack(VALUE_FMT, offset)
    local rec = msg_m.Message.new(key, val, os.time() * 1000)

    local part = self.topic.partitions[self:_partition_for(group)]
    local _, werr = part:write_message(rec)
    if werr then
        return nil, string.format("offset commit write failed: %s", werr)
    end

    set_in_map(self.map, group, topic, partition, offset)
    return true, nil
end

-- fetch returns the committed offset for (group, topic, partition), or nil
-- when none has been committed.
function OffsetManager:fetch(group, topic, partition)
    local g = self.map[group]; if not g then return nil end
    local t = g[topic];        if not t then return nil end
    return t[partition]
end
 
-- recover replays every internal partition front-to-back. Read errors at the
-- tail (torn final record from a crash mid-write) end that partition's scan
-- rather than aborting recovery — a half-written offset record just means the
-- previous committed value stands, which is safe.
function OffsetManager:recover()
    local restored = 0

    for _, part in ipairs(self.topic.partitions) do
        local offset = 0
        local tail   = part.offset

        while offset < tail do
            local msg, next_offset, rerr = part:read_message(offset)
            if not msg then
                log:warn("%s/partition-%d: stopping replay at offset %d: %s",
                    OFFSETS_TOPIC, part.id, offset, rerr or "unknown")
                break
            end

            local ok, group, topic, partition = pcall(string.unpack, KEY_FMT, msg.key)
            local vok, stored = pcall(string.unpack, VALUE_FMT, msg.value)
            if ok and vok then
                set_in_map(self.map, group, topic, partition, stored)
                restored = restored + 1
            else
                -- A record whose key/value we can't decode isn't ours to
                -- interpret; skip it rather than poison the whole replay.
                log:warn("%s/partition-%d: undecodable offset record at %d",
                    OFFSETS_TOPIC, part.id, offset)
            end

            offset = next_offset
        end
    end

    if restored > 0 then
        log:info("recovered %d committed offset(s) from %s", restored, OFFSETS_TOPIC)
    end
    return nil
end

return {
    OffsetManager = OffsetManager,
    OFFSETS_TOPIC = OFFSETS_TOPIC,
}