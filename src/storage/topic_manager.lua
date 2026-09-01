local fs_m        = require("src.io.fs")
local Topic     = require("src.storage.topic")
local SegmentedPartition = require("src.storage.segmentation").SegmentedPartition
local CommitLogPartition = require("src.storage.commitlog_partition")
local topic_config = require("src.storage.topic_config")
local utl_m      = require("src.core.util")

local BACKENDS = {
    segmented = function(topic, id, dir, opts)
        return SegmentedPartition.new(topic, id, dir, opts)
    end,
    commitlog = function(topic, id, dir, opts)
        return CommitLogPartition.new(topic, id, dir, opts)
    end,
}

local DEFAULT_BACKEND = "segmented"

local TopicManager = {}
TopicManager.__index = TopicManager

function TopicManager.new(baseDir, opts)
    assert(type(baseDir) == "string", "baseDir must be a string")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end
    opts = opts or {}

    local default_backend = opts.default_backend or DEFAULT_BACKEND
    assert(BACKENDS[default_backend],
        string.format("unknown default_backend %q", tostring(default_backend)))

    return setmetatable({
        baseDir         = baseDir,
        topics          = {},
        default_backend = default_backend,
    }, TopicManager)
end

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

    local backend_name = (opts and opts.backend) or self.default_backend
    local factory = BACKENDS[backend_name]
    if not factory then
        return nil, string.format("unknown storage backend %q", tostring(backend_name))
    end

    local topicDir = fs_m.join_path(self.baseDir, name)
    local ok, err  = fs_m.mkdir(topicDir)
    if not ok then return nil, err end

    local persist = {}
    if opts then
        for k, v in pairs(opts) do persist[k] = v end
    end
    if backend_name ~= DEFAULT_BACKEND then
        persist.backend = backend_name
    end
    if next(persist) ~= nil then
        local sok, serr = topic_config.save(topicDir, persist)
        if not sok then
            return nil, string.format(
                "failed to persist topic config: %s", serr)
        end
    end

    local topic = Topic.new(name)

    local opened = {}
    for i = 1, numPartitions do
        local partition, pErr = factory(topic, i, topicDir, opts)
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

function TopicManager:topic_dir(name)
    return fs_m.join_path(self.baseDir, name)
end

function TopicManager:config(name)
    assert(type(name) == "string", "name must be a string")
    return topic_config.load(self:topic_dir(name))
end

function TopicManager:set_config(name, opts)
    assert(type(name) == "string", "name must be a string")
    assert(type(opts) == "table", "opts must be a table")
    return topic_config.save(self:topic_dir(name), opts)
end

function TopicManager:delete_topic(name)
    assert(type(name) == "string", "name must be a string")

    local topic = self.topics[name]
    if not topic then
        return nil, string.format("topic '%s' does not exist", name)
    end

    for _, p in ipairs(topic.partitions) do
        if p.detach_committer then p:detach_committer() end
    end
    for _, p in ipairs(topic.partitions) do
        p:close()
    end

    local ok, err = fs_m.remove_all(self:topic_dir(name))
    if not ok then
        return nil, string.format("failed to remove topic '%s': %s", name, tostring(err))
    end

    self.topics[name] = nil
    return true, nil
end

return TopicManager
