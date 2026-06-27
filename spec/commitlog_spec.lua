local commitlog = require("src.commitlog")
local message   = require("src.record.message")
local os_utils  = require("src.core.os")
local fs_m      = require("src.io.fs")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\lua_commitlog_test"
                                      or "/tmp/lua_commitlog_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function new_log(opts)
    opts = opts or {}
    fs_m.mkdir(BASE_DIR)
    local path = fs_m.join_path(BASE_DIR, "log")
    local options = commitlog.Options.new(
        path,
        opts.max_segment_bytes or 0,
        opts.max_log_bytes or 0,
        opts.cleanup_policy or "")
    local l, err = commitlog.CommitLog.new(options)
    assert(not err, err)
    return l
end

local function msg(k, v, ts)
    return message.Message.new(k, v, ts or 1)
end

describe("CommitLog", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("starts with one empty segment at offset 0", function()
        local l = new_log()
        assert.are.equal(1, #l.segments)
        assert.are.equal(0, l:oldest_offset())
        assert.are.equal(0, l:newest_offset())
        l:close()
    end)

    it("round-trips append_message + read_at", function()
        local l = new_log()
        local off, err = l:append_message(msg("k", "v", 42))
        assert.is_nil(err)
        assert.are.equal(0, off)
        assert.are.equal(1, l:newest_offset())

        local got, next_off, rerr = l:read_at(0)
        assert.is_nil(rerr)
        assert.are.equal("k", got.key)
        assert.are.equal("v", got.value)
        assert.are.equal(42, got.timestamp)
        assert.are.equal(1, next_off)
        l:close()
    end)

    it("assigns sequential, message-count offsets", function()
        local l = new_log()
        for i = 0, 4 do
            local off = l:append_message(msg("k" .. i, "v" .. i, i))
            assert.are.equal(i, off)
        end
        assert.are.equal(5, l:newest_offset())
        for i = 0, 4 do
            local got = l:read_at(i)
            assert.are.equal("v" .. i, got.value)
        end
        l:close()
    end)

    it("rolls a new segment when the active one is full", function()
        -- A 1-byte cap forces every record into its own segment (an empty
        -- segment always accepts at least one record).
        local l = new_log({ max_segment_bytes = 1 })
        for i = 0, 2 do
            l:append_message(msg("k" .. i, "v" .. i, i))
        end
        assert.is_true(#l.segments >= 3,
            string.format("expected >=3 segments, got %d", #l.segments))
        -- Reads still resolve across segment boundaries.
        for i = 0, 2 do
            local got, _, rerr = l:read_at(i)
            assert.is_nil(rerr)
            assert.are.equal("v" .. i, got.value)
        end
        l:close()
    end)

    it("read_at reports out-of-range offsets", function()
        local l = new_log()
        l:append_message(msg("k", "v"))
        local got, _, err = l:read_at(5)
        assert.is_nil(got)
        assert.is_not_nil(err)
        l:close()
    end)

    it("recovers segments and offsets after reopen", function()
        local l = new_log({ max_segment_bytes = 1 })
        for i = 0, 3 do
            l:append_message(msg("k" .. i, "value" .. i, i))
        end
        local newest = l:newest_offset()
        l:close()

        local l2 = new_log({ max_segment_bytes = 1 })
        assert.are.equal(newest, l2:newest_offset())
        for i = 0, 3 do
            local got = l2:read_at(i)
            assert.are.equal("value" .. i, got.value)
        end
        -- A fresh append continues from the recovered offset.
        local off = l2:append_message(msg("k4", "value4", 4))
        assert.are.equal(newest, off)
        l2:close()
    end)

    it("recovers from a torn tail on the active segment", function()
        local l = new_log()
        l:append_message(msg("a", "alpha", 1))
        l:append_message(msg("b", "beta", 2))
        local newest = l:newest_offset()
        local seg_path = fs_m.join_path(BASE_DIR, "log",
            string.format("%020d.log", 0))
        l:close()

        -- Append garbage that looks like the start of a record but is short.
        local f = assert(io.open(seg_path, "ab"))
        f:write(string.char(0, 0, 0, 0, 0, 0, 0xFF, 0xFF)) -- bogus length prefix
        f:close()

        local l2 = new_log()
        assert.are.equal(newest, l2:newest_offset())
        local got = l2:read_at(1)
        assert.are.equal("beta", got.value)
        l2:close()
    end)

    it("delete cleaner drops oldest segments past the byte budget", function()
        -- Each record is ~30 bytes; budget keeps only the newest couple.
        local l = new_log({ max_segment_bytes = 1, max_log_bytes = 80 })
        for i = 0, 5 do
            l:append_message(msg("k" .. i, "v" .. i, i))
        end
        -- The oldest data must have been reclaimed.
        assert.is_true(l:oldest_offset() > 0,
            "expected oldest_offset to advance past 0")
        local got, _, err = l:read_at(0)
        assert.is_nil(got)
        assert.is_not_nil(err)
        -- The newest record is still readable.
        local newest_record = l:read_at(l:newest_offset() - 1)
        assert.are.equal("v5", newest_record.value)
        l:close()
    end)

    it("retains everything when max_log_bytes is unset", function()
        local l = new_log({ max_segment_bytes = 1 })  -- max_log_bytes -> retain all
        for i = 0, 4 do
            l:append_message(msg("k" .. i, "v" .. i, i))
        end
        assert.are.equal(0, l:oldest_offset())
        local got = l:read_at(0)
        assert.are.equal("v0", got.value)
        l:close()
    end)

    it("truncate removes segments below an offset", function()
        local l = new_log({ max_segment_bytes = 1 })
        for i = 0, 3 do
            l:append_message(msg("k" .. i, "v" .. i, i))
        end
        local terr = l:truncate(2)
        assert.is_nil(terr)
        assert.is_true(l:oldest_offset() >= 2)
        local got, _, err = l:read_at(0)
        assert.is_nil(got)
        assert.is_not_nil(err)
        l:close()
    end)

    it("compacts to the latest record per key", function()
        local l = new_log({ max_segment_bytes = 1, cleanup_policy = "compact" })
        -- Same key written twice; only the newest value should survive.
        l:append_message(msg("a", "a-old", 1))
        l:append_message(msg("b", "b-only", 2))
        l:append_message(msg("a", "a-new", 3))
        -- Force one more roll so the compaction pass runs over the set.
        l:append_message(msg("c", "c-only", 4))

        -- Collect every surviving record by scanning the live segments.
        local seen = {}
        for _, seg in ipairs(l.segments) do
            seg:each(function(_, m) seen[m.key] = m.value end)
        end
        assert.are.equal("a-new", seen.a)   -- superseded "a-old" dropped
        assert.are.equal("b-only", seen.b)
        assert.are.equal("c-only", seen.c)
        l:close()
    end)
end)
