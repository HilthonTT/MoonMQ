-- DlqManager — dead-letter queue for records a consumer group cannot process.
--
-- MoonMQ's only redelivery mechanism is "don't commit → re-poll", which loops
-- forever on a poison record. The DLQ breaks that loop: a consumer that fails
-- to process a record NACKs it (OP_NACK), the broker counts delivery attempts
-- per (group, topic, partition, offset), and once the configured maximum is
-- reached it moves the record to the topic's dead-letter topic
-- (<topic><suffix>, default <topic>.dlq) and the group advances past it.
--
--   * record_failure(group, topic, partition, offset, reason)
--       - below max_deliveries: bump the attempt counter; the caller rewinds
--         the group's offset so the record is redelivered.
--       - at max_deliveries: append the record (wrapped in a dlq_envelope
--         carrying its provenance) to the dead-letter topic, clear the
--         counter, and hand back next_offset so the caller can advance the
--         group past the poison record.
--
-- The dead-letter topic is created lazily on first use, with the SAME
-- partition count as the source topic; a failed record lands in the SAME
-- partition id it came from, so per-partition failure order is preserved and
-- no partitioner is needed. It is an ordinary user-visible topic (no "__"
-- prefix): consumers subscribe to it like any other.
--
-- ATTEMPT COUNTERS ARE IN-MEMORY ONLY. A broker restart forgets them, which
-- merely restarts the count — the record is redelivered max_deliveries more
-- times before dead-lettering, an acceptable at-least-once outcome that keeps
-- this module free of its own persistence. The map is bounded
-- (opts.max_tracked); when full, the stalest counter is evicted, which again
-- only delays dead-lettering for that record.
--
-- CLUSTER NOTE. The dead-letter append is written to the LOCAL partition, like
-- __consumer_offsets commits: a NACK is only accepted by the broker that
-- serves the source partition (the handler checks serves_partition), and the
-- DLQ record lands on that same broker.

local msg_m       = require("src.record.message")
local compression = require("src.record.compression")
local envelope    = require("src.record.dlq_envelope")
local util_m      = require("src.core.util")
local log         = require("src.log.logger").get("dlq")

local DEFAULT_SUFFIX         = ".dlq"
local DEFAULT_MAX_DELIVERIES = 3
local DEFAULT_MAX_TRACKED    = 4096

local DlqManager = {}
DlqManager.__index = DlqManager

-- opts (optional):
--   suffix          dead-letter topic suffix (default ".dlq")
--   max_deliveries  processing attempts before a record is dead-lettered
--                   (default 3; 1 = dead-letter on the first NACK)
--   max_tracked     bound on live attempt counters (default 4096)
--   now_ms          clock override for tests
function DlqManager.new(broker, opts)
    assert(type(broker) == "table", "broker must be a Broker instance")
    opts = opts or {}
    local max_deliveries = opts.max_deliveries or DEFAULT_MAX_DELIVERIES
    assert(type(max_deliveries) == "number" and max_deliveries >= 1,
        "max_deliveries must be >= 1")

    return setmetatable({
        broker         = broker,
        suffix         = opts.suffix or DEFAULT_SUFFIX,
        max_deliveries = max_deliveries,
        max_tracked    = opts.max_tracked or DEFAULT_MAX_TRACKED,
        now_ms         = opts.now_ms or function() return os.time() * 1000 end,
        -- attempt_key -> { count, ts }
        attempts       = {},
        tracked        = 0,
    }, DlqManager)
end

function DlqManager:topic_for(topic_name)
    return topic_name .. self.suffix
end

-- string.pack ">s2" length-prefixes, so group/topic never need to be
-- delimiter-free — same keying trick as OffsetManager.
local function attempt_key(group, topic, partition, offset)
    return string.pack(">s2s2I4I8", group, topic, partition, offset)
end

-- attempts_for reports the live counter (0 when none) — introspection/tests.
function DlqManager:attempts_for(group, topic, partition, offset)
    local e = self.attempts[attempt_key(group, topic, partition, offset)]
    return e and e.count or 0
end

-- record_failure registers one failed processing attempt. Returns
-- ({ dead_lettered, attempts, next_offset, dlq_topic?, dlq_partition?,
--    dlq_offset? }, nil) or (nil, err).
--
-- next_offset is the cursor just past the failed record (offsets are opaque
-- per-backend cursors, so the caller cannot compute it as offset+1); on
-- dead-letter the caller commits it to advance the group past the record.
function DlqManager:record_failure(group, topic_name, partition_id, offset, reason)
    -- Read the failed record up front, before touching any counter. This both
    -- validates the caller-supplied offset (a bogus one must not rewind the
    -- group into a torn read) and yields the record + next_offset needed on
    -- the dead-letter path.
    local topic, terr = self.broker:get_topic(topic_name)
    if not topic then return nil, terr end
    local part = topic.partitions[partition_id]
    if not part then
        return nil, string.format("partition %d does not exist", partition_id)
    end
    local msg, next_offset, rerr = part:read_message(offset)
    if not msg then
        return nil, string.format("cannot nack %s/partition-%d offset %d: %s",
            topic_name, partition_id, offset, rerr or "unreadable record")
    end
    if msg:is_control() then
        return nil, "cannot nack a control record"
    end

    local key = attempt_key(group, topic_name, partition_id, offset)
    local entry = self.attempts[key]
    if not entry then
        self:_evict_if_full()
        entry = { count = 0 }
        self.attempts[key] = entry
        self.tracked = self.tracked + 1
    end
    entry.count = entry.count + 1
    entry.ts = self.now_ms()

    if entry.count < self.max_deliveries then
        return {
            dead_lettered = false,
            attempts      = entry.count,
            next_offset   = next_offset,
        }, nil
    end

    local dlq_topic, dlq_partition, dlq_offset, derr = self:_dead_letter(
        group, topic, topic_name, partition_id, offset, msg, entry.count, reason)
    if not dlq_topic then return nil, derr end

    -- Counter cleared only after the dead-letter append succeeded; a failed
    -- append leaves it in place so the next NACK retries the move.
    self.attempts[key] = nil
    self.tracked = self.tracked - 1

    log:info("dead-lettered %s/partition-%d offset %d for group '%s' after %d attempt(s) -> %s",
        topic_name, partition_id, offset, group, entry.count, dlq_topic)

    return {
        dead_lettered = true,
        attempts      = entry.count,
        next_offset   = next_offset,
        dlq_topic     = dlq_topic,
        dlq_partition = dlq_partition,
        dlq_offset    = dlq_offset,
    }, nil
end

-- _dead_letter wraps the record in a provenance envelope and appends it to
-- the (lazily created) dead-letter topic. Returns (dlq_topic, dlq_partition,
-- dlq_offset, nil) or (nil, nil, nil, err).
function DlqManager:_dead_letter(group, topic, topic_name, partition_id, offset,
                                 msg, attempts, reason)
    -- Store the envelope's payload decompressed: the DLQ record is written
    -- uncompressed, so leaving the value in its stored codec would strand
    -- bytes no read path knows to decompress.
    local value = msg.value
    local codec = msg:codec()
    if codec ~= 0 then
        local plain, derr = compression.decompress(codec, msg.value)
        if not plain then
            return nil, nil, nil, string.format(
                "decompress %s/partition-%d offset %d: %s",
                topic_name, partition_id, offset, tostring(derr))
        end
        value = plain
    end

    local dlq_name = self:topic_for(topic_name)
    local valid, verr = util_m.validate_topic_name(dlq_name)
    if not valid then
        return nil, nil, nil, string.format(
            "invalid dead-letter topic name %q: %s", dlq_name, verr)
    end

    local dlq_topic = self.broker.topic_manager.topics[dlq_name]
    if not dlq_topic then
        local created, cerr = self.broker:create_topic(dlq_name, #topic.partitions)
        if not created then
            return nil, nil, nil, string.format(
                "failed to create dead-letter topic %s: %s", dlq_name, cerr)
        end
        dlq_topic = created
        log:info("created dead-letter topic %s (%d partition(s))",
            dlq_name, #dlq_topic.partitions)
    end

    -- Same partition id as the source. A pre-existing (user-created) DLQ
    -- topic may have fewer partitions; wrap around rather than fail.
    local dlq_part = dlq_topic.partitions[partition_id]
        or dlq_topic.partitions[((partition_id - 1) % #dlq_topic.partitions) + 1]

    local env = envelope.encode({
        topic     = topic_name,
        partition = partition_id,
        offset    = offset,
        timestamp = msg.timestamp,
        group     = group,
        attempts  = attempts,
        reason    = reason or "",
        value     = value,
    })
    local rec = msg_m.Message.new(msg.key, env, self.now_ms())

    local dlq_offset, werr = dlq_part:write_message(rec)
    if werr then
        return nil, nil, nil, string.format(
            "dead-letter append to %s failed: %s", dlq_name, werr)
    end
    -- Same durability contract as offset commits: the NACK_ACK must not
    -- outlive a crash that loses the dead-lettered record, or the group has
    -- advanced past data that exists nowhere.
    if dlq_part.request_sync then
        local ok, serr = dlq_part:request_sync()
        if not ok then
            return nil, nil, nil, string.format(
                "dead-letter sync to %s failed: %s", dlq_name, tostring(serr))
        end
    end

    return dlq_name, dlq_part.id, dlq_offset, nil
end

-- Bound the counter map. Evicting the stalest entry is safe: losing a counter
-- only means that record survives max_deliveries more NACKs before
-- dead-lettering.
function DlqManager:_evict_if_full()
    if self.tracked < self.max_tracked then return end
    local oldest_key, oldest_ts
    for k, e in pairs(self.attempts) do
        if oldest_ts == nil or e.ts < oldest_ts then
            oldest_key, oldest_ts = k, e.ts
        end
    end
    if oldest_key then
        self.attempts[oldest_key] = nil
        self.tracked = self.tracked - 1
    end
end

return {
    DlqManager = DlqManager,
}
