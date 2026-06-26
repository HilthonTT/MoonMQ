local fs_m        = require("src.io.fs")
local Topic     = require("src.storage.topic")
local SegmentedPartition = require("src.storage.segmentation").SegmentedPartition
local topic_config = require("src.storage.topic_config")
local utl_m      = require("src.core.util")

local TopicManager = {}
TopicManager.__index = TopicManager

function TopicManager.new(baseDir)
    assert(type(baseDir) == "string", "baseDir must be a string")

    return setmetatable({
        baseDir = baseDir,
        topics  = {},
    }, TopicManager)
end

-- opts (optional): per-topic config forwarded to every SegmentedPartition
-- under this topic. See src/storage/segmentation.lua SegmentedPartition.new for
-- the supported keys (max_segment_size, retention, cleaner_interval).
-- Unknown keys are ignored.
function TopicManager:create_topic(name, numPartitions, opts)
    assert(type(name) == "string", "name must be a string")
    assert(type(numPartitions) == "number", "numPartitions must be a number")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end

    local valid, vErr = utl_m.validate_topic_name(name)
    if not valid then return nil, vErr end

    if self.topics[name] then
        return nil, string.format("topic '%s' already exists", name)
    end

    local topicDir = fs_m.join_path(self.baseDir, name)
    local ok, err  = fs_m.mkdir(topicDir)
    if not ok then return nil, err end

    -- Persist per-topic opts so Broker:load_topics can restore them on
    -- restart. Done before opening any partitions so a sidecar failure
    -- needs no partition cleanup. Only write when at least one known
    -- key is set — that way a load-rebuild call (which passes whatever
    -- the sidecar held, including `{}` for predates-this-feature
    -- topics) doesn't add a spurious sidecar.
    if opts and next(opts) ~= nil then
        local sok, serr = topic_config.save(topicDir, opts)
        if not sok then
            return nil, string.format(
                "failed to persist topic config: %s", serr)
        end
    end

    local topic = Topic.new(name)

    -- Track partitions we've successfully opened so we can release their
    -- file handles if a later partition fails. Without this, partial
    -- failure leaks descriptors until GC, which is awful under retries.
    local opened = {}
    for i = 1, numPartitions do
        local partition, pErr = SegmentedPartition.new(topic, i, topicDir, opts)
        if not partition then
            for _, p in ipairs(opened) do p:close() end
            return nil, string.format("failed to create partition %d: %s", i, pErr)
        end
        opened[#opened + 1] = partition
        topic.partitions[i] = partition
    end

    self.topics[name] = topic
    return topic, nil
end

return TopicManager
