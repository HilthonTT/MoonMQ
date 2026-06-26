local TopicManager = require("src.storage.topic_manager")
local os_utils     = require("src.core.os")

local BASE_DIR     = os_utils.IS_WINDOWS and "C:\\Temp\\lua_kafka_test" or "/tmp/lua_kafka_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

describe("TopicManager", function()
    local tm

    before_each(function()
        rmdir(BASE_DIR)
        tm = TopicManager.new(BASE_DIR)
    end)

    it("creates a manager with the correct baseDir", function()
        assert.are.equal(tm.baseDir, BASE_DIR)
    end)

    it("rejects a non-string baseDir", function()
        assert.has_error(function() TopicManager.new(123) end)
    end)

    describe("create_topic", function()
        it("succeeds with valid args", function()
            local topic, err = tm:create_topic("orders", 3)
            assert.is_nil(err)
            assert.are.equal(topic.name, "orders")
        end)

        it("creates the correct number of partitions", function()
            local topic = tm:create_topic("orders", 3)
            assert.are.equal(#topic.partitions, 3)
        end)

        it("rejects a duplicate topic name", function()
            tm:create_topic("orders", 3)
            local t, err = tm:create_topic("orders", 3)
            assert.is_nil(t)
            assert.is_not_nil(err)
        end)

        it("rejects a non-string name", function()
            assert.has_error(function() tm:create_topic(42, 3) end)
        end)

        it("rejects a non-number numPartitions", function()
            assert.has_error(function() tm:create_topic("logs", "three") end)
        end)
    end)
end)
