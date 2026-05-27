local TopicManager = require("src.topic_manager")
local message      = require("src.message")
local os_utils     = require("src.utils.os")

local BASE_DIR     = os_utils.IS_WINDOWS and "C:\\Temp\\lua_kafka_test" or "/tmp/lua_kafka_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local SEP = os_utils.IS_WINDOWS and "\\" or "/"
local FIRST_SEGMENT_NAME = string.format("%020d.log", 0)

-- Path to partition <pid>'s first segment file. The current
-- SegmentedPartition opens one segment at base_offset=0 on fresh create.
local function first_segment_path(topic_name, pid)
    return table.concat({
        BASE_DIR, topic_name,
        string.format("partition-%d", pid),
        FIRST_SEGMENT_NAME,
    }, SEP)
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
        p:sync()

        -- SegmentedPartition opens the segment in "a+b"; on POSIX writes
        -- ignore seek and go to EOF, so we corrupt through a separate
        -- "r+b" handle.
        local path = first_segment_path("orders", 1)

        -- Layout: len(8) | header(12) | hdr_crc(4) | payload | pay_crc(4)
        -- Payload starts at byte 24, where the first key byte ("k") lives.
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

        -- Close all partition handles first. This writes .clean-shutdown
        -- and persists the checkpoint at good_offset (the corruption below
        -- happens behind our back, so the checkpoint reflects only the
        -- legitimate writes).
        for _, part in ipairs(topic.partitions) do
            pcall(function() part:close() end)
        end

        local path = first_segment_path("orders", 1)

        -- Append garbage that looks like a length prefix but has no body.
        -- Simulates a torn append from an OS-level crash.
        local f = assert(io.open(path, "ab"))
        f:write(string.char(0,0,0,0,0,0,0xFF,0xFF)) -- absurd uint64 length
        f:close()

        -- Drop the clean-shutdown flag for partition-1 so recovery runs.
        -- (close() wrote it after we set good_offset; remove it now to
        -- simulate a crash that happened between corruption and the next
        -- clean shutdown.)
        local clean_path = table.concat({
            BASE_DIR, "orders", "partition-1", ".clean-shutdown",
        }, SEP)
        os.remove(clean_path)

        -- Fresh TopicManager picks up the existing partition dirs and
        -- runs SegmentedPartition.load_segments, which invokes verify_file
        -- and truncates the torn frame.
        local tm2 = TopicManager.new(BASE_DIR)
        local topic2, terr = tm2:create_topic("orders", 3)
        assert.is_nil(terr)
        topic = topic2  -- so after_each closes the new handles

        assert.are.equal(topic2.partitions[1].offset, good_offset)
    end)

    it("closes cleanly", function()
        local p = topic.partitions[1]
        p:close()
        -- SegmentedPartition holds files inside its segments; after close,
        -- every segment's file handle should be nil.
        for _, seg in ipairs(p.segments) do
            assert.is_nil(seg.file)
        end
    end)
end)
