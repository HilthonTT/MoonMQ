local fs        = require("src.fs")
local Topic     = require("src.topic")
local Partition = require("src.partition").Partition

local TopicManager = {}
TopicManager.__index = TopicManager

function TopicManager.new(baseDir)
    assert(type(baseDir) == "string", "baseDir must be a string")

    return setmetatable({
        baseDir = baseDir,
        topics  = {},
    }, TopicManager)
end

function TopicManager:create_topic(name, numPartitions)
    assert(type(name) == "string", "name must be a string")
    assert(type(numPartitions) == "number", "numPartitions must be a number")

    if self.topics[name] then
        return nil, ("topic '%s' already exists"):format(name)
    end

    local topicDir = fs.join_path(self.baseDir, name)
    local ok, err  = fs.mkdir(topicDir)
    if not ok then return nil, err end

    local topic = Topic.new(name)

    for i = 1, numPartitions do
        local partition, pErr = Partition.new(topic, i, topicDir)
        if pErr then
            return nil, ("failed to create partition %d: %s"):format(i, pErr)
        end

        if partition then
            partition:sync_loop()
        end
        topic.partitions[i] = partition
    end

    self.topics[name] = topic
    return topic, nil
end

return TopicManager
