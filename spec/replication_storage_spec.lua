local broker_m   = require("src.broker")
local message    = require("src.record.message")
local os_utils   = require("src.core.os")
local EpochCache = require("src.replication.epoch_cache")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\lua_replication_storage_test"
                                      or "/tmp/lua_replication_storage_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function close_broker(b)
    for _, t in pairs(b.topic_manager.topics) do
        for _, p in ipairs(t.partitions) do p:close() end
    end
end

local function new_broker(name)
    local b, err = broker_m.Broker.new(BASE_DIR .. "/" .. name)
    assert(b, err)
    return b
end

local function write(p, i)
    local off, err = p:write_message(message.Message.new(
        "k" .. i, "value-" .. i, 1700000000000 + i))
    assert(off, err)
    return off
end

local function copy(src, dst)
    local off = dst.offset
    while off < src.offset do
        local bytes, next_offset, err, at = src:read_raw(off)
        assert(bytes, err)
        local ok, aerr = dst:append_raw(at or off, bytes, 0)
        assert(ok, aerr)
        off = next_offset
    end
end

local function values(p)
    local out = {}
    local off = p:oldest_offset()
    while off < p.offset do
        local msg, next_offset, err = p:read_message(off)
        assert(msg, err)
        out[#out + 1] = msg.value
        off = next_offset
    end
    return out
end

for _, backend in ipairs({ "segmented", "commitlog" }) do
    describe("replica log primitives (" .. backend .. ")", function()
        before_each(function() rmdir(BASE_DIR) end)
        after_each(function() rmdir(BASE_DIR) end)

        local function topic(b)
            local t, err = b:create_topic("orders", 1,
                backend == "commitlog" and { backend = "commitlog" } or nil)
            assert(t, err)
            return t.partitions[1]
        end

        it("copies raw records so the follower's offsets match the leader's", function()
            local lb, fb = new_broker("leader"), new_broker("follower")
            local lp, fp = topic(lb), topic(fb)
            local offsets = {}
            for i = 1, 5 do offsets[i] = write(lp, i) end

            copy(lp, fp)
            assert.are.equal(lp.offset, fp.offset)
            assert.are.same(values(lp), values(fp))
            for i = 1, 5 do
                local msg = fp:read_message(offsets[i])
                assert.are.equal("value-" .. i, msg.value)
            end
            close_broker(lb); close_broker(fb)
        end)

        it("refuses an append that is not at the follower's log end", function()
            local lb, fb = new_broker("leader"), new_broker("follower")
            local lp, fp = topic(lb), topic(fb)
            write(lp, 1)
            local second = write(lp, 2)
            local bytes = lp:read_raw(second)
            write(fp, 1)
            local ok = fp:append_raw(0, bytes, 0)
            assert.is_nil(ok)
            close_broker(lb); close_broker(fb)
        end)

        it("truncates the tail and keeps accepting appends afterwards", function()
            local b = new_broker("one")
            local p = topic(b)
            local offs = {}
            for i = 1, 6 do offs[i] = write(p, i) end

            assert.is_true(p:truncate_to(offs[4]))
            assert.are.equal(offs[4], p.offset)
            assert.are.same({ "value-1", "value-2", "value-3" }, values(p))

            local again = write(p, 7)
            assert.are.equal(offs[4], again)
            assert.are.same({ "value-1", "value-2", "value-3", "value-7" }, values(p))
            close_broker(b)
        end)

        it("keeps a truncation across a restart", function()
            local b = new_broker("one")
            local p = topic(b)
            local offs = {}
            for i = 1, 4 do offs[i] = write(p, i) end
            assert.is_true(p:truncate_to(offs[3]))
            close_broker(b)

            local b2 = new_broker("one")
            local p2 = b2.topic_manager.topics.orders.partitions[1]
            assert.are.equal(offs[3], p2.offset)
            assert.are.same({ "value-1", "value-2" }, values(p2))
            close_broker(b2)
        end)

        it("resets an empty or stale log to start at a new offset", function()
            local lb, fb = new_broker("leader"), new_broker("follower")
            local lp, fp = topic(lb), topic(fb)
            local offs = {}
            for i = 1, 3 do offs[i] = write(lp, i) end
            write(fp, 99)

            assert.is_true(fp:reset_to(offs[2]))
            assert.are.equal(offs[2], fp.offset)
            assert.are.equal(offs[2], fp:oldest_offset())

            local bytes, _, err, at = lp:read_raw(offs[2])
            assert(bytes, err)
            assert.truthy(fp:append_raw(at or offs[2], bytes, 0))
            assert.are.same({ "value-2" }, values(fp))
            close_broker(lb); close_broker(fb)
        end)
    end)
end

describe("leader epoch cache", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function cache()
        local b = new_broker("cache")
        close_broker(b)
        local c, err = EpochCache.new(BASE_DIR .. "/cache")
        assert(c, err)
        return c
    end

    it("answers the end offset of an epoch from the start of the next one", function()
        local c = cache()
        assert.is_true(c:assign("t", 1, 1, 0))
        assert.is_true(c:assign("t", 1, 3, 100))
        assert.is_true(c:assign("t", 1, 5, 250))

        assert.are.same({ 1, 100 }, { c:end_offset_for("t", 1, 1, 400) })
        assert.are.same({ 3, 250 }, { c:end_offset_for("t", 1, 4, 400) })
        assert.are.same({ 5, 400 }, { c:end_offset_for("t", 1, 5, 400) })
        assert.are.same({ 0, 0 },   { c:end_offset_for("t", 1, 0, 400) })
        assert.are.same({ 7, 42 },  { c:end_offset_for("x", 1, 7, 42) })
    end)

    it("ignores stale epochs and maps offsets back to the epoch that wrote them", function()
        local c = cache()
        c:assign("t", 1, 2, 10)
        c:assign("t", 1, 1, 50)
        assert.are.equal(2, (c:latest("t", 1)))
        assert.are.equal(0, c:epoch_at("t", 1, 5))
        assert.are.equal(2, c:epoch_at("t", 1, 10))
        assert.are.equal(2, c:epoch_at("t", 1, 99))
    end)

    it("drops epochs that start at or after a truncation point", function()
        local c = cache()
        c:assign("t", 1, 1, 0)
        c:assign("t", 1, 2, 100)
        c:truncate_from("t", 1, 100)
        assert.are.equal(1, (c:latest("t", 1)))
        c:truncate_from("t", 1, 0)
        assert.is_false(c:has("t", 1))
    end)

    it("persists epochs and topic ids, and forgets a deleted topic", function()
        local c = cache()
        c:assign("t", 1, 3, 7)
        c:assign("t", 2, 3, 0)
        c:assign("t.dlq", 1, 3, 0)
        c:set_uid("t", "abc")

        local reopened = EpochCache.new(BASE_DIR .. "/cache")
        assert.are.equal(3, (reopened:latest("t", 1)))
        assert.are.equal("abc", reopened:uid("t"))

        reopened:forget_topic("t")
        assert.is_false(reopened:has("t", 1))
        assert.is_false(reopened:has("t", 2))
        assert.is_true(reopened:has("t.dlq", 1))
        assert.is_nil(reopened:uid("t"))
    end)
end)
