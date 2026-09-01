local socket  = require("socket")
local metrics = require("src.metrics")

local M = {}

M.DIM_REQUESTS        = "requests_per_sec"
M.DIM_PRODUCE_RECORDS = "produce_records_per_sec"
M.DIM_PRODUCE_BYTES   = "produce_bytes_per_sec"
M.DIM_FETCH_RECORDS   = "fetch_records_per_sec"
M.DIM_FETCH_BYTES     = "fetch_bytes_per_sec"

local JSON_KEYS = {
    RequestsPerSec        = M.DIM_REQUESTS,
    ProduceRecordsPerSec  = M.DIM_PRODUCE_RECORDS,
    ProduceBytesPerSec    = M.DIM_PRODUCE_BYTES,
    FetchRecordsPerSec    = M.DIM_FETCH_RECORDS,
    FetchBytesPerSec      = M.DIM_FETCH_BYTES,
}

local DIMENSIONS = {}
for json_key, dim in pairs(JSON_KEYS) do
    DIMENSIONS[json_key] = dim
    DIMENSIONS[dim]      = dim
end

M.DIMENSIONS = DIMENSIONS

local DEFAULT_BURST_SECONDS = 2

local IDLE_EVICT_SECONDS = 600
local SWEEP_INTERVAL     = 60

function M.spec(cfg)
    if cfg == nil then return nil, nil end
    if type(cfg) ~= "table" then return nil, "quota must be an object" end

    local out = {}
    for k, v in pairs(cfg) do
        if k:sub(1, 1) ~= "_" then
            local dim = DIMENSIONS[k]
            if not dim then
                local known = {}
                for name in pairs(JSON_KEYS) do known[#known + 1] = name end
                table.sort(known)
                return nil, string.format("unknown quota key %q (known: %s)",
                    tostring(k), table.concat(known, ", "))
            end
            if type(v) ~= "number" or v < 0 then
                return nil, string.format("quota %s must be a non-negative number", k)
            end
            if v > 0 then out[dim] = v end
        end
    end

    return out, nil
end

local Bucket = {}
Bucket.__index = Bucket

function M.bucket(rate, burst_seconds, now_fn)
    assert(type(rate) == "number" and rate > 0, "rate must be positive")
    local now = now_fn or socket.gettime
    local capacity = rate * (burst_seconds or DEFAULT_BURST_SECONDS)
    return setmetatable({
        rate      = rate,
        capacity  = capacity,
        tokens    = capacity,
        now_fn    = now,
        last_fill = now(),
        last_used = now(),
    }, Bucket)
end

function Bucket:_refill(now)
    local elapsed = now - self.last_fill
    if elapsed > 0 then
        self.tokens = math.min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_fill = now
    end
end

function Bucket:peek(n)
    local now = self.now_fn()
    self:_refill(now)
    self.last_used = now

    local want = math.min(n, self.capacity)
    if self.tokens >= want then return true end

    local deficit = want - self.tokens
    return false, deficit / self.rate
end

function Bucket:consume(n)
    self.tokens = self.tokens - math.min(n, self.capacity)
    if self.tokens < -self.capacity then self.tokens = -self.capacity end
end

function Bucket:take(n)
    local ok, retry = self:peek(n)
    if not ok then return false, retry end
    self:consume(n)
    return true
end

local Manager = {}
Manager.__index = Manager

function M.new(opts)
    opts = opts or {}
    return setmetatable({
        default       = opts.default,
        users         = opts.users  or {},
        topics        = opts.topics or {},
        burst_seconds = opts.burst_seconds or DEFAULT_BURST_SECONDS,
        now_fn        = opts.now_fn or socket.gettime,
        buckets       = {},
        last_sweep    = (opts.now_fn or socket.gettime)(),
    }, Manager)
end

function Manager:is_empty()
    return self.default == nil
        and next(self.users) == nil
        and next(self.topics) == nil
end

function Manager:_spec_for(scope, key)
    if scope == "user" then
        return self.users[key] or self.default
    end
    return self.topics[key]
end

function Manager:_bucket(scope, key, dim, rate)
    local id = scope .. "\0" .. key .. "\0" .. dim
    local bucket = self.buckets[id]
    if bucket and bucket.rate ~= rate then
        bucket = nil
    end
    if not bucket then
        bucket = M.bucket(rate, self.burst_seconds, self.now_fn)
        self.buckets[id] = bucket
    end
    return bucket
end

function Manager:_maybe_sweep(now)
    if now - self.last_sweep < SWEEP_INTERVAL then return end
    self.last_sweep = now
    for id, bucket in pairs(self.buckets) do
        if now - bucket.last_used > IDLE_EVICT_SECONDS then
            self.buckets[id] = nil
        end
    end
end

function Manager:check(principal, topic, dim, amount)
    amount = amount or 1
    if amount <= 0 then return true end

    local now = self.now_fn()
    self:_maybe_sweep(now)

    local pending = {}

    if principal then
        local spec = self:_spec_for("user", principal)
        local rate = spec and spec[dim]
        if rate then
            pending[#pending + 1] =
                { bucket = self:_bucket("user", principal, dim, rate), scope = "user" }
        end
    end

    if topic then
        local spec = self:_spec_for("topic", topic)
        local rate = spec and spec[dim]
        if rate then
            pending[#pending + 1] =
                { bucket = self:_bucket("topic", topic, dim, rate), scope = "topic" }
        end
    end

    if #pending == 0 then return true end

    for _, entry in ipairs(pending) do
        local ok, retry = entry.bucket:peek(amount)
        if not ok then
            metrics.inc("moonmq_quota_throttled_total", 1,
                { scope = entry.scope, dimension = dim })
            return false, retry, entry.scope
        end
    end

    for _, entry in ipairs(pending) do
        entry.bucket:consume(amount)
    end
    return true
end

function Manager:available(principal, topic, dim, amount)
    amount = amount or 1

    if principal then
        local spec = self:_spec_for("user", principal)
        local rate = spec and spec[dim]
        if rate then
            local ok, retry = self:_bucket("user", principal, dim, rate):peek(amount)
            if not ok then
                metrics.inc("moonmq_quota_throttled_total", 1,
                    { scope = "user", dimension = dim })
                return false, retry, "user"
            end
        end
    end
    if topic then
        local spec = self:_spec_for("topic", topic)
        local rate = spec and spec[dim]
        if rate then
            local ok, retry = self:_bucket("topic", topic, dim, rate):peek(amount)
            if not ok then
                metrics.inc("moonmq_quota_throttled_total", 1,
                    { scope = "topic", dimension = dim })
                return false, retry, "topic"
            end
        end
    end
    return true
end

function Manager:consume(principal, topic, dim, amount)
    if not amount or amount <= 0 then return end

    if principal then
        local spec = self:_spec_for("user", principal)
        local rate = spec and spec[dim]
        if rate then
            self:_bucket("user", principal, dim, rate):consume(amount)
        end
    end
    if topic then
        local spec = self:_spec_for("topic", topic)
        local rate = spec and spec[dim]
        if rate then
            self:_bucket("topic", topic, dim, rate):consume(amount)
        end
    end
end

function Manager:delay(principal, topic, dim, amount)
    local ok, retry = self:check(principal, topic, dim, amount)
    if ok then return 0 end

    if principal then
        local spec = self:_spec_for("user", principal)
        if spec and spec[dim] then
            self:_bucket("user", principal, dim, spec[dim]):consume(amount)
        end
    end
    if topic then
        local spec = self:_spec_for("topic", topic)
        if spec and spec[dim] then
            self:_bucket("topic", topic, dim, spec[dim]):consume(amount)
        end
    end

    return retry or 0
end

M.DEFAULT_BURST_SECONDS = DEFAULT_BURST_SECONDS

return M
