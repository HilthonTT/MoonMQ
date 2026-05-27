local fs_m       = require("src.fs")
local tpm_m      = require("src.topic_manager")
local util_m     = require("src.util")

local Broker = {}
Broker.__index = Broker

function Broker.new(data_dir)
    assert(type(data_dir) == "string", "data dir must be a string")

    local success, err = fs_m.mkdir(data_dir)
    if not success then
        return nil, err
    end

    local topic_manager = tpm_m.new(data_dir)
    local broker = setmetatable({
        topic_manager = topic_manager,
    }, Broker)

    local lerr = broker:load_topics()
    if lerr then
        return nil, lerr
    end

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
            -- Skip names we'd reject anyway. Without this, a file/dir
            -- left behind by some other tool would crash load.
            local valid = util_m.validate_topic_name(name)
            if valid then
                -- Each partition lives under topic_dir/partition-<id>/ as
                -- a directory containing one or more <base_offset>.log
                -- segment files plus a recovery-checkpoint sidecar.
                local partition_dirs, gErr = fs_m.glob(topic_dir, "^partition%-(%d+)$")
                if not partition_dirs then
                    return string.format("failed to glob partitions for topic %s: %s", name, gErr)
                end

                -- Extract partition IDs from dir names; reject gaps.
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
                    -- Verify contiguous 1..max_id.
                    for i = 1, max_id do
                        if ids[i] ~= i then
                            return string.format("topic %s has non-contiguous partition dirs (missing partition %d)", name, i)
                        end
                    end

                    -- SegmentedPartition.new performs its own crash recovery
                    -- (verify_file with checkpoint/clean-shutdown protocol)
                    -- when it opens the partition dir, so the broker doesn't
                    -- need a separate recovery pass here.
                    local _, cErr = self.topic_manager:create_topic(name, max_id)
                    if cErr then
                        return string.format("failed to load topic %s: %s", name, cErr)
                    end
                end
            end
        end
    end

    return nil
end

function Broker:create_topic(name, num_partitions)
    assert(type(name) == "string", "name must be a string")
    assert(type(num_partitions) == "number", "num_partitions must be a number")

    return self.topic_manager:create_topic(name, num_partitions)
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
        topics[#topics + 1] = topic_name
    end
    return topics
end

return {
    Broker = Broker,
}
