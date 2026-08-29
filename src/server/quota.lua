-- Per-user and per-topic quotas: token buckets over the dimensions a tenant
-- can actually exhaust.
--
-- The connection-scoped `rate_limiter` hook this sits beside bounds one
-- socket. That is not a quota: a tenant with ten connections gets ten times
-- the budget, and a tenant sharing a topic with another tenant can starve it
-- while staying inside its own limit. Quotas are keyed by PRINCIPAL and by
-- TOPIC, so they hold however many connections a client opens.
--
-- Five dimensions, each independently optional (0 or absent = unlimited):
--
--     requests_per_sec          every gated request, produce or admin
--     produce_records_per_sec   records appended
--     produce_bytes_per_sec     key+value bytes appended
--     fetch_records_per_sec     records delivered, pull or push
--     fetch_bytes_per_sec       key+value bytes delivered
--
-- Both the user's bucket and the topic's bucket must have room; the more
-- restrictive one decides. Tokens are only consumed once BOTH have been
-- checked, so a request refused by the topic bucket does not silently burn
-- the user's budget (which would make one busy topic degrade a tenant's
-- access to every other topic it owns).
--
-- Enforcement style differs by path, deliberately:
--   * request/response paths (PRODUCE, FETCH, admin) get ERR_RATE_LIMITED
--     with a retry-after, so a client can back off with information;
--   * the SUBSCRIBE push loop gets a delay instead — there is no request to
--     fail, and stalling the stream for the computed interval is exactly the
--     backpressure the quota is asking for.

local socket  = require("socket")
local metrics = require("src.metrics")

local M = {}

M.DIM_REQUESTS        = "requests_per_sec"
M.DIM_PRODUCE_RECORDS = "produce_records_per_sec"
M.DIM_PRODUCE_BYTES   = "produce_bytes_per_sec"
M.DIM_FETCH_RECORDS   = "fetch_records_per_sec"
M.DIM_FETCH_BYTES     = "fetch_bytes_per_sec"

-- Config key → normalized dimension. Both the JSON PascalCase form and the
-- internal snake_case form are accepted, so a spec built in a test reads the
-- same as one loaded from appsettings.json.
local JSON_KEYS = {
    RequestsPerSec        = M.DIM_REQUESTS,
    ProduceRecordsPerSec  = M.DIM_PRODUCE_RECORDS,
    ProduceBytesPerSec    = M.DIM_PRODUCE_BYTES,
    FetchRecordsPerSec    = M.DIM_FETCH_RECORDS,
    FetchBytesPerSec      = M.DIM_FETCH_BYTES,
}

-- Built into a SEPARATE table: adding keys to the table being traversed is
-- undefined behaviour in Lua, and the version that did so dropped aliases
-- non-deterministically — a config key that parsed in one process and was
-- rejected as unknown in the next.
local DIMENSIONS = {}
for json_key, dim in pairs(JSON_KEYS) do
    DIMENSIONS[json_key] = dim
    DIMENSIONS[dim]      = dim
end

M.DIMENSIONS = DIMENSIONS

-- How much of a second's allowance may be saved up and spent at once. A
-- producer that writes in batches is normal traffic, not abuse; without a
-- burst window every batch larger than the per-second rate would be refused
-- no matter how idle the client had been.
local DEFAULT_BURST_SECONDS = 2

-- Buckets are created on first use and swept when they go cold, so a broker
-- with many short-lived topics does not accumulate one bucket per topic name
-- for the life of the process.
local IDLE_EVICT_SECONDS = 600
local SWEEP_INTERVAL     = 60

-- spec normalizes a config quota block. Returns (spec, nil) or (nil, err);
-- unknown keys are an error rather than a silent no-op, because a quota that
-- looks configured but is not is the worst of both worlds.
function M.spec(cfg)
    if cfg == nil then return nil, nil end
    if type(cfg) ~= "table" then return nil, "quota must be an object" end

    local out = {}
    for k, v in pairs(cfg) do
        -- Keys starting with _ are the codebase's convention for JSON
        -- comments (see appsettings.json), so they are skipped, not rejected.
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
            -- 0 is the documented "unlimited" spelling; storing it as absent
            -- keeps the hot path a single nil check.
            if v > 0 then out[dim] = v end
        end
    end

    -- An explicitly-present block with every dimension at 0 returns an EMPTY
    -- spec, not nil. That is how a user opts OUT of the default quota: nil
    -- means "no opinion, inherit the default", {} means "unlimited".
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

-- Can this bucket afford `n`? Returns (true) or (false, retry_after_seconds).
-- A request larger than the whole bucket is charged the bucket instead of
-- being refused forever: a 10k-record batch against a 1k/s quota should be
-- slowed to the quota's pace, not rejected in perpetuity.
function Bucket:peek(n)
    local now = self.now_fn()
    self:_refill(now)
    self.last_used = now

    local want = math.min(n, self.capacity)
    if self.tokens >= want then return true end

    local deficit = want - self.tokens
    return false, deficit / self.rate
end

-- Tokens may go NEGATIVE, down to one full bucket of debt. Delivery paths can
-- only charge after the fact — a FETCH does not know how many records it will
-- return until it has read them — so the overshoot has to be remembered
-- somewhere, and the next request is where it gets paid. Clamping at zero
-- instead would make every over-quota fetch free.
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

-- opts = { default = spec, users = { name = spec }, topics = { name = spec },
--          burst_seconds = 2, now_fn = fn }
--
-- `default` applies to every principal without its own entry, which is what
-- makes a single line of config ("nobody may exceed 5k records/s") cover
-- users added later.
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
        -- Rate changed under us (a reload); rebuild rather than let the old
        -- capacity linger.
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

-- The one call every enforcement point makes.
--
--   principal — username, or nil in OPEN mode (no quota to apply)
--   topic     — topic name, or nil for requests that name no topic
--   dim       — one of the M.DIM_* constants
--   amount    — records, bytes, or 1 for a request
--
-- Returns true when the charge went through, or (false, retry_after, scope)
-- when it did not. Nothing is consumed on refusal.
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

    -- Check every bucket BEFORE consuming from any, so a refusal by one does
    -- not spend the other's tokens.
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

-- Is there room for `amount` (default 1) right now? Consumes NOTHING: the
-- delivery paths have to ask before reading the log, then charge what they
-- actually delivered, and a probe that spent tokens would bill every consumer
-- for polls that returned nothing.
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

-- Charge without the possibility of refusal, for work that has already
-- happened: the records a FETCH read before anyone could count them. The debt
-- lands in the bucket and the NEXT request pays it, which is how a
-- deliver-then-meter path stays honest.
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

-- Push-path variant: how long should the subscriber loop pause before
-- delivering this batch? Always charges (the records are going out either
-- way); returns the seconds of backpressure the quota implies.
function Manager:delay(principal, topic, dim, amount)
    local ok, retry = self:check(principal, topic, dim, amount)
    if ok then return 0 end

    -- Charge past the refusal: the delay IS the enforcement here, so the
    -- tokens must be spent or the next loop would compute the same wait
    -- forever.
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
