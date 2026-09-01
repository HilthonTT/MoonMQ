local quota_m = require("src.server.quota")

local function clock()
    local t = { now = 1000 }
    function t.fn() return t.now end
    function t.advance(s) t.now = t.now + s end
    return t
end

describe("quota spec parsing", function()

    it("accepts both the JSON and internal key spellings", function()
        local a = assert(quota_m.spec({ ProduceRecordsPerSec = 10 }))
        local b = assert(quota_m.spec({ produce_records_per_sec = 10 }))
        assert.are.equal(10, a[quota_m.DIM_PRODUCE_RECORDS])
        assert.are.equal(10, b[quota_m.DIM_PRODUCE_RECORDS])
    end)

    it("rejects unknown keys instead of ignoring them", function()
        local spec, err = quota_m.spec({ ProduceRecordsPerSecond = 10 })
        assert.is_nil(spec)
        assert.is_truthy(err:find("unknown quota key"))
    end)

    it("rejects negative and non-numeric limits", function()
        assert.is_nil((quota_m.spec({ ProduceRecordsPerSec = -1 })))
        assert.is_nil((quota_m.spec({ ProduceRecordsPerSec = "lots" })))
    end)

    it("skips _comment keys, matching the config convention", function()
        local spec = assert(quota_m.spec({ _note = "hi", ProduceRecordsPerSec = 5 }))
        assert.are.equal(5, spec[quota_m.DIM_PRODUCE_RECORDS])
    end)

    it("treats an all-zero block as an explicit unlimited, not as absent", function()
        local spec = assert(quota_m.spec({ ProduceRecordsPerSec = 0 }))
        assert.are.same({}, spec)
        assert.is_nil((quota_m.spec(nil)))
    end)
end)

describe("token bucket", function()

    it("allows a burst then refills at the configured rate", function()
        local c = clock()
        local b = quota_m.bucket(10, 2, c.fn)

        assert.is_true(b:take(20))
        local ok, retry = b:take(1)
        assert.is_false(ok)
        assert.is_true(retry > 0)

        c.advance(1)
        assert.is_true(b:take(10))
        assert.is_false((b:take(1)))
    end)

    it("caps refill at the burst capacity", function()
        local c = clock()
        local b = quota_m.bucket(10, 2, c.fn)
        b:take(20)
        c.advance(3600)
        assert.is_true(b:take(20))
        assert.is_false((b:take(1)))
    end)

    it("passes a request larger than the bucket rather than refusing forever", function()
        local c = clock()
        local b = quota_m.bucket(10, 1, c.fn)
        assert.is_true(b:take(1000))
        c.advance(1)
        assert.is_true(b:take(1000))
    end)

    it("carries bounded debt so after-the-fact charges are not free", function()
        local c = clock()
        local b = quota_m.bucket(10, 1, c.fn)
        b:consume(10)
        b:consume(10)
        assert.is_false((b:peek(1)))
        c.advance(1)
        assert.is_false((b:peek(1)), "debt must still be owed after 1s")
        c.advance(1)
        assert.is_true(b:peek(1))
    end)
end)

describe("quota manager", function()

    local function manager(opts, c)
        opts.now_fn = c.fn
        opts.burst_seconds = opts.burst_seconds or 1
        return quota_m.new(opts)
    end

    it("applies the default to principals without an override", function()
        local c = clock()
        local m = manager({ default = quota_m.spec({ ProduceRecordsPerSec = 5 }) }, c)
        assert.is_true(m:check("alice", nil, quota_m.DIM_PRODUCE_RECORDS, 5))
        assert.is_false((m:check("alice", nil, quota_m.DIM_PRODUCE_RECORDS, 1)))
        assert.is_true(m:check("bob", nil, quota_m.DIM_PRODUCE_RECORDS, 5))
    end)

    it("lets a per-user override replace the default, including unlimited", function()
        local c = clock()
        local m = manager({
            default = quota_m.spec({ ProduceRecordsPerSec = 5 }),
            users   = {
                heavy = quota_m.spec({ ProduceRecordsPerSec = 100 }),
                admin = quota_m.spec({ ProduceRecordsPerSec = 0 }),
            },
        }, c)
        assert.is_true(m:check("heavy", nil, quota_m.DIM_PRODUCE_RECORDS, 100))
        assert.is_true(m:check("admin", nil, quota_m.DIM_PRODUCE_RECORDS, 1000000))
        assert.is_true(m:check("alice", nil, quota_m.DIM_PRODUCE_RECORDS, 5))
        assert.is_false((m:check("alice", nil, quota_m.DIM_PRODUCE_RECORDS, 1)))
    end)

    it("enforces the more restrictive of the user and topic buckets", function()
        local c = clock()
        local m = manager({
            default = quota_m.spec({ ProduceRecordsPerSec = 100 }),
            topics  = { hot = quota_m.spec({ ProduceRecordsPerSec = 4 }) },
        }, c)
        assert.is_true(m:check("alice", "hot", quota_m.DIM_PRODUCE_RECORDS, 4))
        assert.is_false((m:check("alice", "hot", quota_m.DIM_PRODUCE_RECORDS, 1)))
    end)

    it("does not spend the user bucket when the topic bucket refuses", function()
        local c = clock()
        local m = manager({
            default = quota_m.spec({ ProduceRecordsPerSec = 100 }),
            topics  = { hot = quota_m.spec({ ProduceRecordsPerSec = 4 }) },
        }, c)
        m:check("alice", "hot", quota_m.DIM_PRODUCE_RECORDS, 4)
        assert.is_false((m:check("alice", "hot", quota_m.DIM_PRODUCE_RECORDS, 90)))
        assert.is_true(m:check("alice", "cold", quota_m.DIM_PRODUCE_RECORDS, 96))
    end)

    it("keeps dimensions independent", function()
        local c = clock()
        local m = manager({ default = quota_m.spec({
            ProduceRecordsPerSec = 5, FetchRecordsPerSec = 5 }) }, c)
        assert.is_true(m:check("alice", nil, quota_m.DIM_PRODUCE_RECORDS, 5))
        assert.is_true(m:check("alice", nil, quota_m.DIM_FETCH_RECORDS, 5))
    end)

    it("passes everything through when nothing is configured", function()
        local c = clock()
        local m = manager({}, c)
        assert.is_true(m:is_empty())
        assert.is_true(m:check("alice", "orders", quota_m.DIM_PRODUCE_RECORDS, 1e6))
    end)

    it("ignores quotas for an unauthenticated (OPEN mode) principal", function()
        local c = clock()
        local m = manager({ default = quota_m.spec({ ProduceRecordsPerSec = 1 }) }, c)
        for _ = 1, 10 do
            assert.is_true(m:check(nil, nil, quota_m.DIM_PRODUCE_RECORDS, 1))
        end
    end)

    it("charges unconditionally via consume, for already-delivered work", function()
        local c = clock()
        local m = manager({ default = quota_m.spec({ FetchRecordsPerSec = 10 }) }, c)
        m:consume("alice", nil, quota_m.DIM_FETCH_RECORDS, 10)
        assert.is_false((m:check("alice", nil, quota_m.DIM_FETCH_RECORDS, 1)))
    end)

    it("returns a delay, and charges it, on the push path", function()
        local c = clock()
        local m = manager({ default = quota_m.spec({ FetchRecordsPerSec = 10 }) }, c)
        assert.are.equal(0, m:delay("alice", nil, quota_m.DIM_FETCH_RECORDS, 10))

        local wait = m:delay("alice", nil, quota_m.DIM_FETCH_RECORDS, 5)
        assert.is_true(wait > 0)
        local again = m:delay("alice", nil, quota_m.DIM_FETCH_RECORDS, 5)
        assert.is_true(again > wait)
    end)

    it("evicts idle buckets so topic churn does not leak memory", function()
        local c = clock()
        local m = manager({ topics = {} }, c)
        m.topics["t1"] = quota_m.spec({ ProduceRecordsPerSec = 1 })
        m:check("alice", "t1", quota_m.DIM_PRODUCE_RECORDS, 1)

        local n = 0
        for _ in pairs(m.buckets) do n = n + 1 end
        assert.are.equal(1, n)

        c.advance(3600)
        m:check("alice", nil, quota_m.DIM_REQUESTS, 1)
        n = 0
        for _ in pairs(m.buckets) do n = n + 1 end
        assert.are.equal(0, n)
    end)
end)
