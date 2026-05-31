local fs_m        = require("src.fs")
local Topic     = require("src.topic")
local SegmentedPartition = require("src.segmentation").SegmentedPartition
local utl_m      = require("src.util")

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
-- under this topic. See src/segmentation.lua SegmentedPartition.new for
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
