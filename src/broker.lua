local fs = require("src.fs")
local tpm = require("src.topic_manager")

local Broker = {}
Broker.__index = Broker

function Broker.new(data_dir)
    assert(type(data_dir) == "string", "data dir must be a string")

    local success, err = fs.mkdir(data_dir)
    if not success or err then
        return nil, err
    end

    local topic_manager = tpm.new(data_dir)
    local broker = setmetatable({
        topic_manager = topic_manager,
    }, Broker)

    err = broker:load_topics()
    if err then
        return nil, err
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

        if not fs.is_dir(topic_dir) then
            goto continue
        end

        local partition_files, err = fs.glob(topic_dir, "^partition%-.*%.log$")
        if not partition_files then
            error("glob failed: " .. err)
        end

        local num_partitions = #partition_files
        if num_partitions > 0 then
            _, err = self.topic_manager:create_topic(name, num_partitions)
            if err then
                return ("failed to load topic %s: %s"):format(name, err)
            end
        end

        ::continue::
    end

    return nil
end

function Broker:create_topic(name, num_partitions)
    assert(type(name) == "string", "name must be a string")
    assert(type(num_partitions) == "number", "num_partitions must be a number")

    local _, err = self.topic_manager:create_topic(name, num_partitions)
    return err
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
