local fs       = require("src.fs")
local tpm      = require("src.topic_manager")
local util     = require("src.util")
local recover  = require("src.crash_recovery")

local Broker = {}
Broker.__index = Broker

function Broker.new(data_dir)
    assert(type(data_dir) == "string", "data dir must be a string")

    local success, err = fs.mkdir(data_dir)
    if not success then
        return nil, err
    end

    local topic_manager = tpm.new(data_dir)
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

    local entries, err = fs.read_dir(baseDir)
    if not entries then
        return err
    end

    for _, name in ipairs(entries) do
        local topic_dir = fs.join_path(baseDir, name)

        if fs.is_dir(topic_dir) then
            -- Skip names we'd reject anyway. Without this, a file/dir
            -- left behind by some other tool would crash load.
            local valid = util.validate_topic_name(name)
            if valid then
                local partition_files, gErr = fs.glob(topic_dir, "^partition%-(%d+)%.log$")
                if not partition_files then
                    return ("failed to glob partitions for topic %s: %s"):format(name, gErr)
                end

                -- Extract partition IDs from filenames; reject gaps.
                -- Trusting #partition_files as the partition count would
                -- silently lose partitions when filenames are non-contiguous.
                local ids = {}
                for _, file_path in ipairs(partition_files) do
                    local basename = file_path:match("[^/\\]+$") or file_path
                    local id = tonumber(basename:match("^partition%-(%d+)%.log$"))
                    if id then ids[#ids + 1] = id end
                end
                table.sort(ids)

                if #ids > 0 then
                    -- Run crash recovery on each partition file BEFORE
                    -- topic_manager opens it for append. Recovery truncates
                    -- the tail at the last CRC-valid record boundary.
                    for _, id in ipairs(ids) do
                        local _, rerr = recover(topic_dir, id)
                        if rerr then
                            return ("failed to recover topic %s partition %d: %s"):format(name, id, rerr)
                        end
                    end

                    local max_id = ids[#ids]
                    -- Verify contiguous 1..max_id.
                    for i = 1, max_id do
                        if ids[i] ~= i then
                            return ("topic %s has non-contiguous partition files (missing partition %d)"):format(name, i)
                        end
                    end

                    local _, cErr = self.topic_manager:create_topic(name, max_id)
                    if cErr then
                        return ("failed to load topic %s: %s"):format(name, cErr)
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
        return nil, ("topic %s does not exist"):format(name)
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
