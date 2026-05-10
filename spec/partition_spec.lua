-- spec/partition_spec.lua
local TopicManager = require("src.topic_manager")
local message      = require("src.message")
local recover      = require("src.crash_recovery")

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

    it("round-trips a message through write_message/read_message", function()
        local p = topic.partitions[1]
        local m = message.Message.new("k1", "v-payload", 12345)
        local off, werr = p:write_message(m)
        assert.is_nil(werr)
        assert.are.equal(off, 0)

        local got, _, rerr = p:read_message(0)
        assert.is_nil(rerr)
        assert.are.equal(got.key, "k1")
        assert.are.equal(got.value, "v-payload")
        assert.are.equal(got.timestamp, 12345)
    end)

    it("read_message detects payload corruption", function()
        local p = topic.partitions[1]
        local m = message.Message.new("k", "secret", 99)
        p:write_message(m)
        p.file:flush()

        -- Partition opens its file in "a+b" (append mode). On POSIX, writes
        -- to an append-mode fd always go to EOF regardless of seek, so we
        -- can't corrupt in-place through p.file. Use a separate "r+b" fd.
        local IS_WIN = package.config:sub(1,1) == "\\"
        local sep    = IS_WIN and "\\" or "/"
        local path   = BASE_DIR .. sep .. "orders" .. sep .. "partition-1.log"

        -- Layout: len(8) | header(12) | hdr_crc(4) | payload | pay_crc(4)
        -- Flip a byte at offset 24 (start of payload, 0-based).
        local f = assert(io.open(path, "r+b"))
        f:seek("set", 24)
        f:write(string.char(0xFF))
        f:close()

        local got, _, rerr = p:read_message(0)
        assert.is_nil(got)
        assert.is_not_nil(rerr)
        assert.is_truthy(rerr:find("checksum"))
    end)

    it("crash recovery truncates trailing garbage", function()
        local p = topic.partitions[1]
        local m1 = message.Message.new("a", "alpha", 1)
        local m2 = message.Message.new("b", "beta",  2)
        p:write_message(m1)
        p:write_message(m2)
        local good_offset = p.offset
        p:close()

        local IS_WIN = package.config:sub(1,1) == "\\"
        local sep    = IS_WIN and "\\" or "/"
        local path   = BASE_DIR .. sep .. "orders" .. sep .. "partition-1.log"

        -- Append garbage that looks like a length prefix but has no body.
        local f = io.open(path, "ab")
        f:write(string.char(0,0,0,0,0,0,0xFF,0xFF)) -- absurd uint64 length
        f:close()

        local recovered, rerr = recover(BASE_DIR .. sep .. "orders", 1)
        assert.is_nil(rerr)
        assert.are.equal(recovered, good_offset)
    end)

    it("closes cleanly", function()
        local p = topic.partitions[1]
        p:close()
        assert.is_nil(p.file)
    end)
end)
