local Replicator = require("src.server.replicator")

local function mock(leo, err)
    return {
        sent = 0,
        send = function(self, _topic, _part, _leader, _payload)
            self.sent = self.sent + 1
            if err then return nil, err end
            return leo, nil
        end,
    }
end

local function followers(...)
    local list = {}
    for i, c in ipairs({ ... }) do list[i] = { id = i + 1, client = c } end
    return list
end

describe("Replicator", function()
    it("is disabled with no followers", function()
        local r = Replicator.new(nil, 1, {})
        assert.is_false(r:enabled())
    end)

    it("ships records and advances a follower's LEO (sync drain)", function()
        local c = mock(100)
        local r = Replicator.new(nil, 1, followers(c))
        r:replicate("t", 1, 100, "record-bytes")
        assert.are.equal(1, c.sent)
        assert.is_true(r:all_reached("t", 1, 100))
        assert.are.equal(100, r:high_watermark("t", 1))
        assert.are.equal(1, r:in_sync_count("t", 1))
    end)

    it("requires ALL followers to reach the offset (acks=all predicate)", function()
        local ahead, behind = mock(100), mock(50)
        local r = Replicator.new(nil, 1, followers(ahead, behind))
        r:replicate("t", 1, 100, "x")
        assert.is_false(r:all_reached("t", 1, 100),
            "a lagging follower must block acks=all")
        assert.is_true(r:all_reached("t", 1, 50))
    end)

    it("drops a lagging follower out of the in-sync set", function()
        local r = Replicator.new(nil, 1, followers(mock(50)), { lag_max = 0 })
        r:replicate("t", 1, 100, "x")
        assert.are.equal(0, r:in_sync_count("t", 1))
        assert.is_nil(r:high_watermark("t", 1), "no in-sync followers → no HWM")
    end)

    it("tolerates lag within lag_max", function()
        local r = Replicator.new(nil, 1, followers(mock(90)), { lag_max = 20 })
        r:replicate("t", 1, 100, "x")
        assert.are.equal(1, r:in_sync_count("t", 1))
        assert.are.equal(90, r:high_watermark("t", 1))
    end)

    it("a send error leaves the follower behind", function()
        local r = Replicator.new(nil, 1, followers(mock(nil, "connection refused")))
        r:replicate("t", 1, 100, "x")
        assert.is_false(r:all_reached("t", 1, 100))
    end)

    it("wait_for returns true once every follower has the record", function()
        local r = Replicator.new(nil, 1, followers(mock(100)))
        r:replicate("t", 1, 100, "x")
        r.reactor = { sleep = function() end }
        assert.is_true(r:wait_for("t", 1, 100))
    end)

    it("wait_for times out when a follower never acks", function()
        local r = Replicator.new({ sleep = function() end }, 1,
            followers(mock(0)), { ack_timeout = 1 })
        local t = 0
        r._now_override = function() t = t + 0.6; return t end
        local ok, err = r:wait_for("t", 1, 100)
        assert.is_nil(ok)
        assert.matches("timed out", err)
    end)

    it("errors on wait_for with no followers configured", function()
        local r = Replicator.new({ sleep = function() end }, 1, {})
        local ok, err = r:wait_for("t", 1, 5)
        assert.is_nil(ok)
        assert.matches("requires configured replicas", err)
    end)
end)
