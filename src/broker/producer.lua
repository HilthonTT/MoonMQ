local message = require("src.record.message")
local socket  = require("socket")
local brk     = require("src.broker")
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

-- opts (optional):
--   pre_append  fn(topic_name, partition_id, partition, remote) -> (ok, err),
--               invoked after partition selection but BEFORE the record is
--               appended. The transactional produce path uses it to enrol the
--               partition with the txn coordinator while the record's offset
--               is still the partition's LEO (see Coordinator:add_partition).
--               A falsy return aborts the produce. For a partition owned by a
--               peer broker, `partition` is nil and `remote` is
--               { peer = Peer, leo = owner's log-end offset } — captured
--               BEFORE the forwarded append so it lower-bounds this record on
--               the OWNER's log (conservative: another producer may append in
--               between, but the abort filter also matches pid/epoch, so
--               covering extra offsets is safe).
function Producer:produce(topic_name, msg, opts)
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
            if opts and opts.pre_append then
                -- Transactional produce to a peer-owned partition: capture
                -- the OWNER's LEO and enrol before forwarding, so the txn's
                -- first offset there is recorded (and the owner's LSO
                -- floored) before any of its records land.
                local rleo, lerr = peer:leo(topic_name, partition_id)
                if not rleo then
                    return -1, -1, string.format(
                        "leo of %s/partition-%d on peer %s: %s",
                        topic_name, partition_id, peer.id, tostring(lerr))
                end
                local pok, perr = opts.pre_append(topic_name, partition_id, nil,
                    { peer = peer, leo = rleo })
                if not pok then return -1, -1, perr end
            end
            local roffset, ferr = self.router:forward(peer, topic_name, partition_id, msg)
            if not roffset then return -1, -1, ferr end
            return partition_id, roffset, nil
        end
    end

    if opts and opts.pre_append then
        local pok, perr = opts.pre_append(topic_name, partition_id, partition)
        if not pok then return -1, -1, perr end
    end

    local offset, werr = partition:write_message(msg)
    if werr then
        return -1, -1, string.format("failed to write message: %s", werr)
    end

    -- Produce byte accounting for the autobalancer's NW_IN goal.
    if self.broker.traffic then
        self.broker.traffic:add_in(topic_name, partition_id, #msg.key + #msg.value)
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

-- produce_batch appends many messages to one topic in a single pass.
--
-- The win over N calls to produce() is durability cost, not the appends
-- themselves: acks>=1 issues ONE request_sync per distinct partition the batch
-- touched, at the end, instead of one per record. A 500-record batch on a
-- 4-partition topic goes from 500 fsyncs to 4. Appends are still per-record
-- (both storage backends index per message), and partitioning is unchanged —
-- each record is routed by its own key, so a batch may fan out across
-- partitions.
--
-- Ordering is preserved: records are appended in list order, so two records
-- landing on the same partition keep their relative order.
--
-- opts (optional):
--   pre_append  as in produce(), but invoked ONCE PER DISTINCT PARTITION —
--               on the first record routed there. The txn coordinator's
--               enrolment is idempotent per partition and captures the LEO
--               before any of the batch's records land, which is exactly the
--               bound the LSO needs.
--
-- Returns (acks, err) where `acks` is a list of { partition, offset } for the
-- records that were appended — always a PREFIX of `msgs`. On failure the acks
-- collected so far are returned alongside the error, already fsynced: a
-- partial batch is durable up to the failure, never silently dropped.
function Producer:produce_batch(topic_name, msgs, opts)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(type(msgs) == "table" and #msgs > 0, "msgs must be a non-empty list")

    local acks = {}

    if self.acks == AckMode.AckAll
        and not (self.replicator and self.replicator:enabled()) then
        return acks, ERR_ACKS_ALL_UNSUPPORTED
    end

    local valid, vErr = util.validate_topic_name(topic_name)
    if not valid then return acks, vErr end

    local topic, err = self.broker:get_topic(topic_name)
    if not topic then
        return acks, string.format("failed to get topic: %s", err)
    end

    local num_partitions = #topic.partitions

    -- Partitions this batch appended to locally, in first-touch order, each
    -- with the highest log-end offset the batch produced there. Both the
    -- single fsync per partition and the acks=all wait key off this.
    local touched, touched_order = {}, {}
    local enrolled = {}
    local batch_err

    for i = 1, #msgs do
        local msg = msgs[i]
        assert(getmetatable(msg) == message.Message, "msgs must contain Messages")
        if msg.timestamp == 0 then
            msg.timestamp = now_ms()
        end

        local partition_id = pick_partition(self, topic_name, msg.key, num_partitions) + 1
        local partition    = topic.partitions[partition_id]
        if not partition then
            batch_err = string.format("partition %d does not exist", partition_id)
            break
        end

        -- Cluster routing, per record: a peer-owned partition's records are
        -- forwarded to the owner one at a time. The batch's fsync amortisation
        -- is a local-append property, so a forwarded record costs the same as
        -- it does through produce() — the batch still saves the client N-1
        -- round trips.
        local peer
        if self.router then
            local rerr
            peer, rerr = self.router:route(topic_name, partition_id)
            if rerr then batch_err = rerr; break end
        end

        if peer then
            if opts and opts.pre_append and not enrolled[partition_id] then
                local rleo, lerr = peer:leo(topic_name, partition_id)
                if not rleo then
                    batch_err = string.format("leo of %s/partition-%d on peer %s: %s",
                        topic_name, partition_id, peer.id, tostring(lerr))
                    break
                end
                local pok, perr = opts.pre_append(topic_name, partition_id, nil,
                    { peer = peer, leo = rleo })
                if not pok then batch_err = perr; break end
                enrolled[partition_id] = true
            end
            local roffset, ferr = self.router:forward(peer, topic_name, partition_id, msg)
            if not roffset then batch_err = ferr; break end
            acks[#acks + 1] = { partition = partition_id, offset = roffset }
        else
            if opts and opts.pre_append and not enrolled[partition_id] then
                local pok, perr = opts.pre_append(topic_name, partition_id, partition)
                if not pok then batch_err = perr; break end
                enrolled[partition_id] = true
            end

            local offset, werr = partition:write_message(msg)
            if werr then
                batch_err = string.format("failed to write message: %s", werr)
                break
            end
            acks[#acks + 1] = { partition = partition_id, offset = offset }

            if self.broker.traffic then
                self.broker.traffic:add_in(topic_name, partition_id, #msg.key + #msg.value)
            end

            -- Log-end offset covering this record, captured before any sync
            -- can park this coroutine (see the same note in produce()).
            if not touched[partition_id] then
                touched_order[#touched_order + 1] = partition_id
                touched[partition_id] = { partition = partition }
            end
            touched[partition_id].leo = partition.offset

            if self.replicator and self.replicator:enabled() then
                local bytes, berr = message.serialize_message(msg)
                if bytes then
                    self.replicator:replicate(topic_name, partition_id,
                        partition.offset, bytes)
                else
                    log:error("replication skipped: serialize failed: %s", tostring(berr))
                end
            end
        end
    end

    -- One fsync per partition for the whole batch. Runs even on a partial
    -- failure so the prefix we're about to ack is genuinely durable.
    if self.acks == AckMode.AckLeader or self.acks == AckMode.AckAll then
        for _, partition_id in ipairs(touched_order) do
            local sok, serr = touched[partition_id].partition:request_sync()
            if not sok then
                return acks, string.format("failed to sync partition %d: %s",
                    partition_id, tostring(serr))
            end
        end
    end

    if self.acks == AckMode.AckAll then
        for _, partition_id in ipairs(touched_order) do
            local rok, rerr = self.replicator:wait_for(
                topic_name, partition_id, touched[partition_id].leo)
            if not rok then return acks, rerr end
        end
    end

    return acks, batch_err
end

return {
    AckMode  = AckMode,
    Producer = Producer,
}
