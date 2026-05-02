-- spec/partition_spec.lua
local TopicManager = require("src.topic_manager")
local socket       = require("socket")

local IS_WINDOWS   = package.config:sub(1,1) == "\\"
local BASE_DIR     = IS_WINDOWS and "C:\\Temp\\lua_kafka_test" or "/tmp/lua_kafka_test"

local function rmdir(path)
    if IS_WINDOWS then
        os.execute(('rmdir /s /q "%s" 2>nul'):format(path:gsub("/", "\\")))
    else
        os.execute(("rm -rf '%s'"):format(path))
    end
end

describe("Partition", function()
    local tm, topic

    before_each(function()
        rmdir(BASE_DIR)
        tm = TopicManager.new(BASE_DIR)
        topic = tm:create_topic("orders", 3)
    end)

    after_each(function()
        for _, p in ipairs(topic.partitions) do
            pcall(function() p:close() end)
        end
    end)

    it("starts with offset 0 for a new file", function()
        assert.are.equal(topic.partitions[1].offset, 0)
    end)

    it("grows offset after write", function()
        local p = topic.partitions[1]
        p:write("abc")
        assert.are.equal(p.offset, 3)
    end)

    it("rejects non-string data in write()", function()
        assert.has_error(function() topic.partitions[1]:write(42) end)
    end)

    it("drives sync coroutine on tick()", function()
        socket.sleep(0.1)
        local p      = topic.partitions[1]
        local before = p.last_sync
        p:tick()
        assert.is_true(p.last_sync >= before)
    end)

    it("closes cleanly", function()
        local p = topic.partitions[1]
        p:close()
        assert.is_nil(p.file)
        assert.is_false(p.running)
    end)
end)
