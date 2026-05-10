-- spec/future_spec.lua
local Future = require("src.future")

-- Minimal scheduler stub: resume() just dispatches to coroutine.resume.
-- That is enough for the Future contract since :await() returns whatever
-- the scheduler hands the waiter when it resumes it.
local function make_scheduler()
    return {
        resume = function(co, value)
            local ok, err = coroutine.resume(co, value)
            if not ok then error(err) end
        end,
    }
end

describe("Future", function()
    it("is not ready when freshly created", function()
        local f = Future.new(make_scheduler())
        assert.is_false(f:is_ready())
    end)

    it("becomes ready after resolve()", function()
        local f = Future.new(make_scheduler())
        f:resolve("v")
        assert.is_true(f:is_ready())
        assert.are.equal("v", f.value)
    end)

    it("returns the value synchronously if already resolved", function()
        local f = Future.new(make_scheduler())
        f:resolve(42)
        local co = coroutine.create(function()
            local v = f:await()
            assert.are.equal(42, v)
        end)
        local ok, err = coroutine.resume(co)
        assert.is_true(ok, err)
        assert.are.equal("dead", coroutine.status(co))
    end)

    it("delivers the resolved value to waiters", function()
        local f = Future.new(make_scheduler())
        local delivered
        local co = coroutine.create(function()
            delivered = f:await()
        end)
        coroutine.resume(co)
        assert.are.equal("suspended", coroutine.status(co))
        f:resolve("hello")
        assert.are.equal("hello", delivered)
        assert.are.equal("dead", coroutine.status(co))
    end)

    it("delivers to multiple waiters", function()
        local f = Future.new(make_scheduler())
        local got = {}
        for i = 1, 3 do
            local co = coroutine.create(function()
                got[i] = f:await()
            end)
            coroutine.resume(co)
        end
        f:resolve("ok")
        assert.are.same({"ok", "ok", "ok"}, got)
    end)

    it("ignores re-resolve (one-shot)", function()
        local f = Future.new(make_scheduler())
        f:resolve("first")
        f:resolve("second")
        assert.are.equal("first", f.value)
    end)

    it("errors when await() is called outside a coroutine", function()
        local f = Future.new(make_scheduler())
        assert.has_error(function() f:await() end)
    end)
end)
