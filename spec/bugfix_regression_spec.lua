local Topic              = require("src.storage.topic")
local seg_m              = require("src.storage.segmentation")
local SegmentedPartition = seg_m.SegmentedPartition
local BatchWriter        = require("src.storage.batch_writer")
local message            = require("src.record.message")
local commitlog          = require("src.commitlog.commitlog")
local auth               = require("src.server.auth")
local os_utils           = require("src.core.os")
local fs_m               = require("src.io.fs")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\lua_bugfix_test"
                                     or  "/tmp/lua_bugfix_test"
local SEP = os_utils.IS_WINDOWS and "\\" or "/"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function new_partition(topic_name, pid, opts)
    fs_m.mkdir(BASE_DIR)
    local topic_dir = table.concat({ BASE_DIR, topic_name }, SEP)
    fs_m.mkdir(topic_dir)
    local topic = Topic.new(topic_name)
    local p, err = SegmentedPartition.new(topic, pid, topic_dir, opts)
    assert(not err, err)
    return p
end

describe("bugfix regressions", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    describe("unbounded record-size allocation (DoS/OOM)", function()
        it("deserialize_record rejects a corrupt oversized length prefix without crashing", function()
            local path = table.concat({ BASE_DIR, "corrupt.bin" }, SEP)
            fs_m.mkdir(BASE_DIR)
            local f = assert(io.open(path, "wb"))
            f:write(string.rep("\255", 8) .. "garbage")
            f:close()

            local g = assert(io.open(path, "rb"))
            local ok, msg, framed, err = pcall(message.deserialize_record, g)
            g:close()

            assert.is_true(ok, "deserialize_record must not raise on a corrupt length prefix")
            assert.is_nil(msg)
            assert.is_nil(framed)
            assert.is_string(err)
        end)

        it("SegmentedPartition:read_message returns an error (no raise) for a misaligned offset", function()
            local p = new_partition("t1", 1)
            p:write_message(message.Message.new("k", "v", 42))

            local ok, m, _next, rerr = pcall(p.read_message, p, 1)
            p:close()

            assert.is_true(ok, "read_message must not raise on a misaligned offset")
            assert.is_nil(m)
            assert.is_string(rerr)
        end)
    end)

    describe("commitlog max_segment_bytes u32 index overflow", function()
        it("rejects a segment size that would overflow the u32 index position", function()
            local dir = table.concat({ BASE_DIR, "cl" }, SEP)
            fs_m.mkdir(BASE_DIR)
            local options = commitlog.Options.new(dir, 5 * 1024 * 1024 * 1024, 0, "")
            local cl, err = commitlog.CommitLog.new(options)
            assert.is_nil(cl)
            assert.is_string(err)
            assert.is_truthy(err:find("exceeds limit", 1, true))
        end)

        it("accepts a normal segment size", function()
            local dir = table.concat({ BASE_DIR, "cl2" }, SEP)
            fs_m.mkdir(BASE_DIR)
            local options = commitlog.Options.new(dir, 64 * 1024 * 1024, 0, "")
            local cl, err = commitlog.CommitLog.new(options)
            assert.is_nil(err)
            assert.is_not_nil(cl)
            if cl then cl:close() end
        end)
    end)

    describe("commitlog compaction offset-gap consumer wedge", function()
        it("next_readable_offset skips a gap to the next segment's base offset", function()
            local fake = { segments = {
                { base_offset = 0,  next_offset = 3  },
                { base_offset = 10, next_offset = 12 },
            } }
            local nro = commitlog.CommitLog.next_readable_offset

            assert.are.equal(1,   nro(fake, 1))
            assert.are.equal(10,  nro(fake, 5))
            assert.are.equal(10,  nro(fake, 10))
            assert.are.equal(11,  nro(fake, 11))
            assert.is_nil(nro(fake, 12))
            assert.is_nil(nro(fake, 99))
        end)
    end)

    describe("BatchWriter duplicate-on-fsync-failure", function()
        it("clears the buffer after a successful write even if the sync fails", function()
            local p = new_partition("bw", 1)
            local bw = BatchWriter.new(p, 1 << 20, 1000)

            assert.is_true((bw:add(message.Message.new("k", "v", 1))))

            local real_sync = p.sync
            p.sync = function() return nil, "simulated disk error" end

            local ok, err = bw:flush()
            assert.is_nil(ok)
            assert.is_string(err)
            assert.are.equal(0, #bw.buffer)

            p.sync = real_sync
            assert.is_true((bw:flush()))

            local m, next_off, rerr = p:read_message(0)
            assert.is_nil(rerr)
            assert.are.equal("v", m.value)
            local _, _, eof = p:read_message(next_off)
            assert.is_string(eof)
            p:close()
        end)

        it("add() surfaces a serialize error instead of raising", function()
            local p = new_partition("bw2", 1)
            local bw = BatchWriter.new(p, 1 << 20, 1000)
            local m = message.Message.new("k", "v", -1)
            local ok, res, err = pcall(bw.add, bw, m)
            p:close()
            assert.is_true(ok, "add() must not raise on a serialize failure")
            assert.is_nil(res)
            assert.is_string(err)
        end)
    end)

    describe("auth iteration ceiling", function()
        it("exposes a shared max and parse_hash rejects hashes above it", function()
            assert.is_number(auth.MAX_PBKDF2_ITERATIONS)
            local over = auth.MAX_PBKDF2_ITERATIONS + 1
            local stored = string.format(
                "pbkdf2-sha256$%d$%s$%s", over, string.rep("ab", 16), string.rep("cd", 32))
            local parsed, err = auth.parse_hash(stored)
            assert.is_nil(parsed)
            assert.is_string(err)
        end)
    end)
end)
