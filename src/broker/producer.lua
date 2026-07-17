local message = require("src.record.message")
local socket  = require("socket")
local brk     = require("src.broker")
local Future  = require("src.core.future")
local util    = require("src.core.util")
local log     = require("src.log.logger").get("producer")

-- AckMode defines the producer acknowledgment levels
local AckMode = {
    -- AckNone means no acknowledgment is required
    AckNone     = 0,
    -- AckLeader means the leader must acknowledge (fsync after write)
    AckLeader   = 1,
    -- AckAll means all replicas must acknowledge. Currently rejected at
    -- the API boundary: src/server/replica.lua drains fire-and-forget over HTTP
    -- and has no high-watermark, so we cannot honour the contract. The
    -- prior behaviour silently degraded acks=-1 to acks=0 (because the
    -- `self.acks > 0` guard is false for -1), which is worse than failing
    -- — callers thought they had stronger durability than they did.
    AckAll      = -1,
}

local ERR_ACKS_ALL_UNSUPPORTED =
    "acks=all is not supported: replication is fire-and-forget without HWM. " ..
    "Use acks=0 (None) or acks=1 (Leader)."

local function rand_intn(n)
    assert(type(n) == "number", "n must be a number")

    if n <= 0 then return 0 end
    return math.random(0, n-1)
end

local function fnv32a(key)
    local FNV_OFFSET_BASIS = 2166136261
    local FNV_PRIME        = 16777619

    local hash = FNV_OFFSET_BASIS

    for i = 1, #key do
        local byte = string.byte(key, i)
        hash = hash ~ byte
        hash = (hash * FNV_PRIME) & 0xFFFFFFFF
    end

    return hash
end

local function default_partitioner(key, numPartitions)
    assert(type(key) == "string", "key must be a string")
    assert(type(numPartitions) == "number", "numPartitions must be a number")

    if #key == 0 then
        return rand_intn(numPartitions)
    end

    local hash = fnv32a(key)
    return hash % numPartitions
end

-- Sticky-partitioner parameters. Kafka 2.4's sticky partitioner pins
-- empty-key sends to one partition until a batch is closed (size or
-- linger). We don't yet thread batch boundaries through the produce
-- path, so we approximate with a count + wall-clock rotation: every N
-- records or T seconds, reroll to a new partition. This keeps small
-- bursts together (huge for batching/compression once it's wired) but
-- still spreads load over time.
local STICKY_MAX_RECORDS = 16
local STICKY_MAX_AGE_S   = 0.010   -- 10ms

-- pick_partition centralises keyed vs sticky routing. Returns the 0-based
-- partition index (the caller adds 1 for the partitions[] lookup).
local function pick_partition(self, topic_name, key, num_partitions)
    if num_partitions <= 0 then return 0 end

    -- Keyed: deterministic. No stickiness — same key always lands on the
    -- same partition so ordering-per-key is preserved.
    if key ~= nil and #key > 0 then
        return self.partitioner(key, num_partitions)
    end

    -- Empty key: sticky path.
    local s = self.sticky[topic_name]
    local now = socket.gettime()
    if s and s.num_partitions == num_partitions
       and s.count < STICKY_MAX_RECORDS
       and (now - s.started_at) < STICKY_MAX_AGE_S
    then
        s.count = s.count + 1
        return s.partition
    end

    -- Reroll. Avoid landing on the same partition twice in a row when N>1
    -- so we don't accidentally pin everything to one slot under low load.
    local prev = s and s.partition or -1
    local p = rand_intn(num_partitions)
    if num_partitions > 1 and p == prev then
        p = (p + 1) % num_partitions
    end
    self.sticky[topic_name] = {
        partition       = p,
        num_partitions  = num_partitions,
        count           = 1,
        started_at      = now,
    }
    return p
end

-- Millisecond-resolution timestamps. We'd love nanosecond precision to
-- match the Go reference's UnixNano(), but ns-since-epoch is ~1.8e18 in
-- 2026 and Lua doubles only hold integers cleanly up to 2^53 ≈ 9e15.
-- ms-since-epoch is ~1.8e12, well within precision, and ms granularity
-- is more than enough for ordering messages within a partition.
local function now_ms()
    return math.floor(socket.gettime() * 1000)
end

local ProduceOptions = {}
ProduceOptions.__index = ProduceOptions

function ProduceOptions.new(ack_mode, timeout_in_seconds)
    return setmetatable({
        ack_mode           = ack_mode or AckMode.AckNone,
        timeout_in_seconds = timeout_in_seconds,    -- nil = no limit
    }, ProduceOptions)
end

local ProduceResult = {}
ProduceResult.__index = ProduceResult

function ProduceResult.new(topic, partition, offset, err)
    assert(type(topic) == "string", "topic must be a string")

    return setmetatable({
        topic     = topic,
        partition = partition,
        offset    = offset,
        error     = err,
    }, ProduceResult)
end

local Producer = {}
Producer.__index = Producer

-- opts (optional):
--   replicator  a Replicator (src/server/replicator.lua). Required for
--               acks=all: without configured followers there is no
--               high-watermark to wait on. When present, EVERY produced record
--               is also shipped to followers (async for acks<all; awaited for
--               acks=all).
--   router      a cluster Router (src/cluster/router.lua). When present,
--               produce consults partition ownership: records for a partition
--               owned by a peer broker are forwarded to that peer instead of
--               written locally. nil = single-broker behaviour.
function Producer.new(broker, acks, opts)
    assert(getmetatable(broker) == brk.Broker, "broker must be a Broker instance")
    assert(type(acks) == "number", "acks must be a number")
    opts = opts or {}
    if acks == AckMode.AckAll then
        assert(opts.replicator, ERR_ACKS_ALL_UNSUPPORTED)
    end

    return setmetatable({
        broker = broker,
        acks = acks,
        replicator = opts.replicator,
        router = opts.router,
        partitioner = default_partitioner,
        -- Sticky-partition state for empty-key sends. Keyed by topic name
        -- since num_partitions varies per topic. See default_partitioner.
        sticky = {},
    }, Producer)
end

function Producer:produce(topic_name, msg)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")

    if self.acks == AckMode.AckAll
        and not (self.replicator and self.replicator:enabled()) then
        return -1, -1, ERR_ACKS_ALL_UNSUPPORTED
    end

    local valid, vErr = util.validate_topic_name(topic_name)
    if not valid then return -1, -1, vErr end

    if msg.timestamp == 0 then
        msg.timestamp = now_ms()
    end

    local topic, err = self.broker:get_topic(topic_name)
    if not topic then
        return -1, -1, string.format("failed to get topic: %s", err)
    end

    local num_partitions = #topic.partitions
    local partition_id   = pick_partition(self, topic_name, msg.key, num_partitions) + 1

    local partition = topic.partitions[partition_id]
    if not partition then
        return -1, -1, string.format("partition %d does not exist", partition_id)
    end

    -- Cluster routing: a partition owned by a peer broker gets its record
    -- forwarded there rather than appended to the local (stale) log. Ordering
    -- per partition is preserved — every producer routes through the same
    -- ownership table, so all writes for the partition land on the owner.
    if self.router then
        local peer, rerr = self.router:route(topic_name, partition_id)
        if rerr then return -1, -1, rerr end
        if peer then
            local roffset, ferr = self.router:forward(peer, topic_name, partition_id, msg)
            if not roffset then return -1, -1, ferr end
            return partition_id, roffset, nil
        end
    end

    local offset, werr = partition:write_message(msg)
    if werr then
        return -1, -1, string.format("failed to write message: %s", werr)
    end

    -- Capture the log-end offset covering THIS record now: request_sync can
    -- park this coroutine on the group committer, and other producers may
    -- append meanwhile. Waiting on a later re-read of partition.offset would
    -- block this produce on records it never wrote (and fail it on someone
    -- else's replication lag).
    local record_leo = partition.offset

    -- Ship to followers whenever replication is configured. `record_leo` is
    -- the leader's log-end offset after this write — what a follower must
    -- reach to hold this record. Async for acks<all; acks=all awaits it below.
    if self.replicator and self.replicator:enabled() then
        local bytes, berr = message.serialize_message(msg)
        if bytes then
            self.replicator:replicate(topic_name, partition_id, record_leo, bytes)
        else
            log:error("replication skipped: serialize failed: %s", tostring(berr))
        end
    end

    if self.acks == AckMode.AckLeader then
        -- request_sync coalesces concurrent acks=1 waiters into one fsync
        -- when the partition has a committer attached (set at server boot
        -- via Broker:attach_committer_factory). Without a committer it
        -- behaves exactly like :sync() — single fsync per call. Either
        -- way, this is a TRUE fsync, not just userspace flush.
        local sok, serr = partition:request_sync()
        if not sok then
            return -1, -1, string.format("failed to sync partition: %s", serr)
        end
    elseif self.acks == AckMode.AckAll then
        -- Leader durability first, then block until every follower has the
        -- record (or the ack timeout fires).
        local sok, serr = partition:request_sync()
        if not sok then
            return -1, -1, string.format("failed to sync partition: %s", serr)
        end
        local rok, rerr = self.replicator:wait_for(
            topic_name, partition_id, record_leo)
        if not rok then
            return -1, -1, rerr
        end
    end

    return partition_id, offset, nil
end

-- Async variant. Returns a Future that resolves to a ProduceResult.
-- NOTE: with the trivial scheduler (synchronous coroutine.resume), this
-- runs to completion on the calling resume — it does not actually overlap
-- with other I/O. Use it for the caller-friendly Future API, not for
-- concurrency gains.
function Producer:produce_async(scheduler, topic_name, msg, opts)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")
    opts = opts or {}
    local ack_mode = opts.ack_mode or self.acks
    local timeout  = opts.timeout_in_seconds   -- nil = no limit

    -- Reject acks=all at the API boundary rather than silently degrading
    -- to acks=0 (the old `ack_mode == AckLeader` branch skipped the sync
    -- for AckAll == -1, so callers got no durability at all).
    if ack_mode == AckMode.AckAll then
        local f = Future.new(scheduler)
        f:resolve(ProduceResult.new(topic_name, -1, -1, ERR_ACKS_ALL_UNSUPPORTED))
        return f
    end

    if msg.timestamp == 0 then
        msg.timestamp = now_ms()
    end

    local future = Future.new(scheduler)

    local co = coroutine.create(function()
        local started = socket.gettime()

        local valid, vErr = util.validate_topic_name(topic_name)
        if not valid then
            future:resolve(ProduceResult.new(topic_name, -1, -1, vErr))
            return
        end

        local topic, err = self.broker:get_topic(topic_name)
        if not topic then
            future:resolve(ProduceResult.new(topic_name, -1, -1,
                string.format("failed to get topic: %s", err)))
            return
        end

        local num_partitions = #topic.partitions
        local partition_id   = pick_partition(self, topic_name, msg.key, num_partitions) + 1
        local partition      = topic.partitions[partition_id]
        if not partition then
            future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                string.format("partition %d does not exist", partition_id)))
            return
        end

        local offset, werr = partition:write_message(msg)
        if werr then
            future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                string.format("failed to write message: %s", werr)))
            return
        end

        if ack_mode == AckMode.AckLeader then
            local sok, serr = partition:request_sync()
            if not sok then
                future:resolve(ProduceResult.new(topic_name, partition_id, -1,
                    string.format("failed to sync partition: %s", serr)))
                return
            end
        end

        -- Soft timeout: the write itself isn't cancellable. The write has
        -- already succeeded; resolve with the offset and no error so the
        -- caller doesn't retry and duplicate. Caller can compare elapsed
        -- vs timeout themselves if monitoring is needed.
        local elapsed = socket.gettime() - started
        if timeout and elapsed > timeout then
            log:warn("produce exceeded timeout: %.3fs > %.3fs", elapsed, timeout)
        end

        future:resolve(ProduceResult.new(topic_name, partition_id, offset, nil))
    end)

    scheduler.resume(co)
    return future
end

return {
    AckMode        = AckMode,
    ProduceOptions = ProduceOptions,
    ProduceResult  = ProduceResult,
    Producer       = Producer,
}
