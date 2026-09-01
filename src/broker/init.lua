local fs_m       = require("src.io.fs")
local tpm_m      = require("src.storage.topic_manager")
local topic_config = require("src.storage.topic_config")
local util_m     = require("src.core.util")
local offmgr_m   = require("src.storage.offset_manager")
local prodstate_m = require("src.storage.producer_state")
local txn_m      = require("src.broker.txn_coordinator")
local dlq_m      = require("src.broker.dlq")
local traffic_m  = require("src.metrics.traffic")
local uuid = require("src.core.uuid")

local Broker = {}
Broker.__index = Broker

function Broker.new(data_dir, opts)
    assert(type(data_dir) == "string", "data dir must be a string")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end
    opts = opts or {}

    local success, err = fs_m.mkdir(data_dir)
    if not success then return nil, err end

    local topic_manager = tpm_m.new(data_dir, {
        default_backend = opts.default_backend,
    })
    local broker = setmetatable({
        id = uuid.bytes(),
        topic_manager = topic_manager,
        traffic = traffic_m.new(),
    }, Broker)

    local lerr = broker:load_topics()
    if lerr then return nil, lerr end

    local offsets, oerr = offmgr_m.OffsetManager.new(topic_manager, opts.offsets)
    if not offsets then return nil, oerr end
    broker.offsets = offsets

    local prod_state, perr =
        prodstate_m.ProducerStateManager.new(topic_manager, opts.producer_state)
    if not prod_state then return nil, perr end

    broker.producer_state = prod_state

    local txn, terr = txn_m.Coordinator.new(broker, opts.transactions)
    if not txn then return nil, terr end

    broker.transactions = txn

    broker.dlq = dlq_m.DlqManager.new(broker, opts.dlq)

    return broker, nil
end

function Broker:load_topics()
    local baseDir = self.topic_manager.baseDir

    local entries, err = fs_m.read_dir(baseDir)
    if not entries then
        return err
    end

    for _, name in ipairs(entries) do
        local topic_dir = fs_m.join_path(baseDir, name)

        if fs_m.is_dir(topic_dir) then
            local valid = util_m.validate_topic_name(name)
            if valid then
                local partition_dirs, gErr = fs_m.glob(topic_dir, "^partition%-(%d+)$")
                if not partition_dirs then
                    return string.format("failed to glob partitions for topic %s: %s", name, gErr)
                end

                local ids = {}
                for _, dir_path in ipairs(partition_dirs) do
                    if fs_m.is_dir(dir_path) then
                        local basename = dir_path:match("[^/\\]+$") or dir_path
                        local id = tonumber(basename:match("^partition%-(%d+)$"))
                        if id then ids[#ids + 1] = id end
                    end
                end
                table.sort(ids)

                if #ids > 0 then
                    local max_id = ids[#ids]
                    for i = 1, max_id do
                        if ids[i] ~= i then
                            return string.format("topic %s has non-contiguous partition dirs (missing partition %d)", name, i)
                        end
                    end

                    local opts, oerr = topic_config.load(topic_dir)
                    if not opts then
                        return string.format(
                            "failed to load topic %s config: %s", name, oerr)
                    end

                    local _, cErr = self.topic_manager:create_topic(name, max_id, opts)
                    if cErr then
                        return string.format("failed to load topic %s: %s", name, cErr)
                    end
                end
            end
        end
    end

    return nil
end

function Broker:create_topic(name, num_partitions, opts)
    assert(type(name) == "string", "name must be a string")
    assert(type(num_partitions) == "number", "num_partitions must be a number")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end

    local topic, err = self.topic_manager:create_topic(name, num_partitions, opts)
    if topic and self._committer_factory then
        for _, p in ipairs(topic.partitions) do
            self._committer_factory(p)
        end
    end
    return topic, err
end

function Broker.is_internal(name)
    return name:sub(1, 2) == "__"
end

function Broker:delete_topic(name)
    assert(type(name) == "string", "name must be a string")

    if Broker.is_internal(name) then
        return nil, string.format("refusing to delete internal topic '%s'", name)
    end
    if not self.topic_manager.topics[name] then
        return nil, string.format("topic %s does not exist", name)
    end

    if self.group_coordinator then
        self.group_coordinator:forget_topic(name)
    end

    local ok, err = self.topic_manager:delete_topic(name)
    if not ok then return nil, err end

    if self.offsets then
        local _, oerr = self.offsets:delete_topic_offsets(name)
        if oerr then
            return true, string.format(
                "topic deleted, but clearing its committed offsets failed: %s", oerr)
        end
    end

    if self.dlq and self.dlq.forget_topic then
        self.dlq:forget_topic(name)
    end

    return true, nil
end

function Broker:describe_topic(name)
    assert(type(name) == "string", "name must be a string")

    local topic, terr = self:get_topic(name)
    if not topic then return nil, terr end

    local config, cerr = self.topic_manager:config(name)
    if not config then return nil, cerr end

    return {
        name           = name,
        num_partitions = #topic.partitions,
        config         = config,
    }, nil
end

local ALTERABLE = {
    max_segment_size = "number",
    retention        = "number",
    cleaner_interval = "number",
    max_log_bytes    = "number",
    cleanup_policy   = "string",
}

local LIVE_PARTITION_FIELDS = {
    max_segment_size = true,
    retention        = true,
    cleaner_interval = true,
}

function Broker:alter_topic_config(name, changes)
    assert(type(name) == "string", "name must be a string")
    assert(type(changes) == "table", "changes must be a table")

    if Broker.is_internal(name) then
        return nil, string.format(
            "refusing to reconfigure internal topic '%s'", name)
    end

    local topic, terr = self:get_topic(name)
    if not topic then return nil, terr end

    local parsed = {}
    for key, raw in pairs(changes) do
        local kind = ALTERABLE[key]
        if not kind then
            return nil, string.format("unknown or immutable config key '%s'", key)
        end
        if kind == "number" then
            local n = tonumber(raw)
            if not n then
                return nil, string.format(
                    "config key '%s' needs a number, got %q", key, tostring(raw))
            end
            if n <= 0 then
                return nil, string.format(
                    "config key '%s' must be positive, got %s", key, tostring(n))
            end
            parsed[key] = math.floor(n)
        else
            parsed[key] = tostring(raw)
        end
    end

    if next(parsed) == nil then return {}, nil end

    local merged, cerr = self.topic_manager:config(name)
    if not merged then return nil, cerr end
    for k, v in pairs(parsed) do merged[k] = v end

    local ok, serr = self.topic_manager:set_config(name, merged)
    if not ok then return nil, serr end

    for key, value in pairs(parsed) do
        if LIVE_PARTITION_FIELDS[key] then
            for _, p in ipairs(topic.partitions) do
                if p[key] ~= nil then p[key] = value end
            end
        end
    end

    return parsed, nil
end

function Broker:attach_committer_factory(fn)
    assert(type(fn) == "function", "factory must be a function")
    self._committer_factory = fn
    for _, topic in pairs(self.topic_manager.topics) do
        for _, p in ipairs(topic.partitions) do
            fn(p)
        end
    end
end

function Broker:detach_committers()
    self._committer_factory = nil
    for _, topic in pairs(self.topic_manager.topics) do
        for _, p in ipairs(topic.partitions) do
            if p.detach_committer then p:detach_committer() end
        end
    end
end

function Broker:tick_cleaners()
    local ran = 0
    for name, topic in pairs(self.topic_manager.topics) do
        if name:sub(1, 2) ~= "__" then
            for _, p in ipairs(topic.partitions) do
                if p.tick_cleaner and p:tick_cleaner() then
                    ran = ran + 1
                    if self.transactions and p.oldest_offset then
                        self.transactions.aborts:prune(name, p.id, p:oldest_offset())
                    end
                end
            end
        end
    end
    return ran
end

function Broker:expire_idle_producers(max_idle_ms, opts)
    opts = opts or {}
    local caller_active = opts.is_active
    return self.producer_state:expire_idle(max_idle_ms, {
        now_ms    = opts.now_ms,
        is_active = function(name, pid)
            if self.transactions and self.transactions:has_unresolved(name) then
                return true
            end
            return caller_active ~= nil and caller_active(name, pid) or false
        end,
    })
end

function Broker:serves_partition(topic_name, partition_id)
    local a = self.cluster_assignments
    if not a then return true end
    return a:owned_by_self(topic_name, partition_id)
end

function Broker:get_topic(name)
    assert(type(name) == "string", "name must be a string")

    local existing_topic = self.topic_manager.topics[name]
    if not existing_topic then
        return nil, string.format("topic %s does not exist", name)
    end

    return existing_topic, nil
end

function Broker:list_topics()
    local topics = {}
    for topic_name, _ in pairs(self.topic_manager.topics) do
        if topic_name:sub(1, 2) ~= "__" then
            topics[#topics + 1] = topic_name
        end
    end
    return topics
end

function Broker:commit_offset(group, topic, partition, offset)
    return self.offsets:commit(group, topic, partition, offset)
end

function Broker:fetch_offset(group, topic, partition)
    return self.offsets:fetch(group, topic, partition)
end

return {
    Broker = Broker,
}
