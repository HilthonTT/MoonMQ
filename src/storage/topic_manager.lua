local fs_m        = require("src.io.fs")
local Topic     = require("src.storage.topic")
local SegmentedPartition = require("src.storage.segmentation").SegmentedPartition
local CommitLogPartition = require("src.storage.commitlog_partition")
local topic_config = require("src.storage.topic_config")
local utl_m      = require("src.core.util")

-- Pluggable storage backends. Each factory has the signature
-- (topic, id, dir, opts) -> (partition, err) and yields an object satisfying
-- the duck-typed partition interface (write_message/read_message/.offset/etc).
-- "segmented" is the default byte-offset SegmentedPartition; "commitlog" is the
-- jocko-style message-offset CommitLog (src/commitlog) via its adapter.
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

-- opts (optional):
--   default_backend  backend name used when a topic doesn't specify one
--                    ("segmented" | "commitlog"; default "segmented").
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

-- opts (optional): per-topic config forwarded to every partition under this
-- topic. `opts.backend` selects the storage engine ("segmented" default, or
-- "commitlog"); the rest are backend-specific tuning keys (segmented:
-- max_segment_size/retention/cleaner_interval — see SegmentedPartition.new;
-- commitlog: max_segment_size/max_log_bytes/cleanup_policy — see
-- CommitLogPartition.new). Unknown keys are ignored by each backend.
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

    -- Resolve the storage backend: explicit per-topic opt wins, else the
    -- manager default. Persisted in the sidecar (below) so a restart restores
    -- the same backend rather than silently falling back to the default.
    local backend_name = (opts and opts.backend) or self.default_backend
    local factory = BACKENDS[backend_name]
    if not factory then
        return nil, string.format("unknown storage backend %q", tostring(backend_name))
    end

    local topicDir = fs_m.join_path(self.baseDir, name)
    local ok, err  = fs_m.mkdir(topicDir)
    if not ok then return nil, err end

    -- Persist per-topic opts so Broker:load_topics can restore them on restart.
    -- Done before opening any partitions so a sidecar failure needs no partition
    -- cleanup. The persisted table is the caller's opts plus the resolved backend
    -- (added only when it isn't the default). We write only when something is set
    -- — that way default-backend topics with no opts keep writing no sidecar, and
    -- a load-rebuild call doesn't add a spurious one.
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

    -- Track partitions we've successfully opened so we can release their
    -- file handles if a later partition fails. Without this, partial
    -- failure leaks descriptors until GC, which is awful under retries.
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

return TopicManager
