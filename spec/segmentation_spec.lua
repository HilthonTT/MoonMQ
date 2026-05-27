local Topic              = require("src.topic")
local seg_m              = require("src.segmentation")
local SegmentedPartition = seg_m.SegmentedPartition
local message            = require("src.message")
local os_utils           = require("src.utils.os")
local fs_m               = require("src.fs")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\lua_seg_test" or "/tmp/lua_seg_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local SEP = os_utils.IS_WINDOWS and "\\" or "/"

local function part_dir(topic_name, pid)
    return table.concat({ BASE_DIR, topic_name, string.format("partition-%d", pid) }, SEP)
end

local function new_partition(topic_name, pid, opts)
    fs_m.mkdir(BASE_DIR)
    local topic_dir = table.concat({ BASE_DIR, topic_name }, SEP)
    fs_m.mkdir(topic_dir)
    local topic = Topic.new(topic_name)
    local p, err = SegmentedPartition.new(topic, pid, topic_dir)
    assert(not err, err)
    if opts and opts.max_segment_size then
        p.max_segment_size = opts.max_segment_size
    end
    return p
end

describe("SegmentedPartition", function()

    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("starts with offset 0 and one empty segment", function()
        local p = new_partition("t1", 1)
        assert.are.equal(p.offset, 0)
        assert.are.equal(#p.segments, 1)
        assert.are.equal(p.segments[1].base_offset, 0)
        assert.are.equal(p.segments[1].bytes_written, 0)
        p:close()
    end)

    it("round-trips write_message + read_message in one segment", function()
        local p = new_partition("t1", 1)
        local m = message.Message.new("k", "v", 42)
        local off, werr = p:write_message(m)
        assert.is_nil(werr)
        assert.are.equal(off, 0)

        local got, next_off, rerr = p:read_message(0)
        assert.is_nil(rerr)
        assert.are.equal(got.key, "k")
        assert.are.equal(got.value, "v")
        assert.are.equal(got.timestamp, 42)
        assert.are.equal(next_off, p.offset)
        p:close()
    end)

    it("rolls when the active segment exceeds max_segment_size", function()
        -- Tiny cap so each message rolls on its own.
        local p = new_partition("t1", 1, { max_segment_size = 64 })

        local offsets = {}
        for i = 1, 3 do
            local m = message.Message.new("k" .. i, "value" .. i, i)
            local off, werr = p:write_message(m)
            assert.is_nil(werr)
            offsets[i] = off
        end

        -- We expect more than one segment after a few writes.
        assert.is_true(#p.segments >= 2,
            string.format("expected >=2 segments, got %d", #p.segments))

        -- Reading by global offset must follow the segment boundaries.
        for i = 1, 3 do
            local got, _, rerr = p:read_message(offsets[i])
            assert.is_nil(rerr)
            assert.are.equal(got.value, "value" .. i)
        end
        p:close()
    end)

    it("writes recovery-checkpoint when rolling", function()
        local p = new_partition("t1", 1, { max_segment_size = 64 })

        for i = 1, 3 do
            local m = message.Message.new("k" .. i, "value" .. i, i)
            p:write_message(m)
        end

        local cp_path = table.concat({ part_dir("t1", 1), "recovery-checkpoint" }, SEP)
        local f = assert(io.open(cp_path, "rb"))
        local cp = tonumber(f:read("*a"))
        f:close()

        -- The checkpoint should be the base offset of the active (newest)
        -- segment, i.e., the boundary between sealed and live data.
        assert.are.equal(cp, p.active_segment.base_offset)
        p:close()
    end)

    it("recovers a torn tail when there's no clean-shutdown flag", function()
        local p = new_partition("t1", 1)
        local m1 = message.Message.new("a", "alpha", 1)
        local m2 = message.Message.new("b", "beta",  2)
        p:write_message(m1)
        p:write_message(m2)
        local good_offset = p.offset
        p:close()

        local seg_path = table.concat(
            { part_dir("t1", 1), string.format("%020d.log", 0) }, SEP)

        local f = assert(io.open(seg_path, "ab"))
        f:write(string.char(0,0,0,0,0,0,0xFF,0xFF)) -- bogus length prefix
        f:close()

        -- Delete clean-shutdown so load_segments runs verify_file.
        os.remove(table.concat({ part_dir("t1", 1), ".clean-shutdown" }, SEP))

        local p2 = new_partition("t1", 1)
        assert.are.equal(p2.offset, good_offset)
        p2:close()
    end)

    it("skips verification when clean-shutdown is set", function()
        local p = new_partition("t1", 1)
        p:write_message(message.Message.new("k", "v", 1))
        local good_offset = p.offset
        p:close()

        -- Reopen: clean_shutdown flag is present, recovery should be a
        -- no-op on sealed segments. (The active segment is always
        -- verified, but with no tear there's nothing to truncate.)
        local p2 = new_partition("t1", 1)
        assert.are.equal(p2.offset, good_offset)
        p2:close()
    end)

    it("persists segment creation time via .meta sidecar", function()
        local p = new_partition("t1", 1)
        p:close()

        local meta_path = table.concat(
            { part_dir("t1", 1), string.format("%020d.meta", 0) }, SEP)
        local f = assert(io.open(meta_path, "rb"))
        local ts = tonumber(f:read("*a"))
        f:close()

        assert.is_not_nil(ts)
        assert.is_true(ts > 0)
    end)

    it("write() appends raw bytes and advances offset", function()
        local p = new_partition("t1", 1)
        local ok, err = p:write("xyz")
        assert.is_true(ok)
        assert.is_nil(err)
        assert.are.equal(p.offset, 3)
        p:close()
    end)
end)
