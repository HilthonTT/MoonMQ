-- ProducerStateManager — durable producer identities + idempotent-produce
-- sequence memos, stored in an internal topic exactly like OffsetManager
-- (see src/storage/offset_manager.lua for the pattern this mirrors).
--
-- Two things are persisted, both as ordinary compacted records in the internal
-- __producer_state topic (commitlog backend, so latest-per-key wins):
--
--   1. Identity: producer_name -> (pid, epoch). A *named* (a.k.a. durable /
--      transactional) producer keeps the SAME pid across reconnects and broker
--      restarts; each new session bumps the epoch so a zombie old session is
--      fenced (its stale-epoch writes are rejected).
--   2. Sequence memo: (pid, topic) -> (last_seq, last_offset, last_partition),
--      so an idempotent retry that arrives after a reconnect/restart replays the
--      original ack instead of appending a duplicate.
--
-- Ephemeral (unnamed) producers keep today's session-scoped behaviour: they get
-- a pid from the same durable allocator (so pids never repeat across a restart)
-- but NOTHING is persisted — their sequence state lives on the connection.
--
-- Records land in a partition chosen by hash(key) % N, so every record with a
-- given key stays append-ordered within one partition, which is what makes
-- "latest wins" correct on replay.

local msg_m = require("src.record.message")
local log   = require("src.log.logger").get("producer_state")

local STATE_TOPIC = "__producer_state"
local DEFAULT_PARTITIONS = 16

-- Must run on the commitlog backend for the same reason as __consumer_offsets:
-- it's the only backend that key-compacts, so the log stays bounded and a quiet
-- producer never loses its latest state to time-based retention.
local STATE_BACKEND          = "commitlog"
local STATE_MAX_SEGMENT_SIZE = 8 * 1024 * 1024

-- Record key type tags (first byte of the record key).
local K_IDENTITY = "\1"   -- "\1" .. producer_name
local K_MEMO     = "\2"   -- "\2" .. u64 pid .. topic

local IDENTITY_VALUE_FMT = ">I8I2"        -- pid, epoch
local MEMO_VALUE_FMT     = ">I4I8I4"      -- last_seq, last_offset, last_partition

local ProducerStateManager = {}
ProducerStateManager.__index = ProducerStateManager

-- FNV-1a (32-bit), identical to OffsetManager's — deterministic across
-- restarts/platforms so a key never moves partitions between runs.
local function fnv1a(s)
    local hash = 2166136261
    for i = 1, #s do
        hash = (hash ~ s:byte(i)) & 0xFFFFFFFF
        hash = (hash * 16777619) & 0xFFFFFFFF
    end
    return hash
end

function ProducerStateManager.new(topic_manager, opts)
    assert(type(topic_manager) == "table", "topic_manager must be a TopicManager")
    opts = opts or {}

    local nparts = opts.num_partitions or DEFAULT_PARTITIONS
    assert(type(nparts) == "number" and nparts >= 1, "num_partitions must be >= 1")

    local topic = topic_manager.topics[STATE_TOPIC]
    if not topic then
        local created, err = topic_manager:create_topic(STATE_TOPIC, nparts, {
            backend          = STATE_BACKEND,
            cleanup_policy   = "compact",
            max_segment_size = STATE_MAX_SEGMENT_SIZE,
        })
        if not created then
            return nil, string.format("failed to create %s: %s", STATE_TOPIC, err)
        end
        topic = created
    end

    local self = setmetatable({
        topic       = topic,
        nparts      = #topic.partitions,
        by_name     = {},   -- producer_name -> { pid=, epoch= }
        epoch_by_pid= {},   -- pid -> current epoch (fencing)
        memo        = {},   -- pid -> topic -> { last_seq, last_offset, last_partition }
        next_pid    = 1,    -- monotonic allocator; 0 reserved as "unassigned"
    }, ProducerStateManager)

    local rerr = self:recover()
    if rerr then
        return nil, rerr
    end
    return self, nil
end

function ProducerStateManager.topic_name()
    return STATE_TOPIC
end

function ProducerStateManager:_partition_for(key)
    return (fnv1a(key) % self.nparts) + 1
end

-- Append a record and fsync it durable. Returns (true, nil) or (nil, err).
function ProducerStateManager:_write(key, value)
    local rec  = msg_m.Message.new(key, value, os.time() * 1000)
    local part = self.topic.partitions[self:_partition_for(key)]
    local _, werr = part:write_message(rec)
    if werr then
        return nil, string.format("producer_state write failed: %s", werr)
    end
    if part.request_sync then
        local ok, serr = part:request_sync()
        if not ok then
            return nil, string.format("producer_state sync failed: %s", tostring(serr))
        end
    end
    return true, nil
end

-- get_or_create_producer resolves a *named* producer identity. A brand-new name
-- allocates a fresh pid at epoch 0; a known name keeps its pid and bumps the
-- epoch (a new session — the old one is now fenced). Persists the identity and
-- returns (pid, epoch, nil) or (nil, nil, err).
function ProducerStateManager:get_or_create_producer(name)
    assert(type(name) == "string" and #name > 0, "producer name must be non-empty")

    local existing = self.by_name[name]
    local pid, epoch
    if existing then
        pid   = existing.pid
        epoch = existing.epoch + 1
    else
        pid   = self.next_pid
        self.next_pid = self.next_pid + 1
        epoch = 0
    end

    local ok, err = self:_write(K_IDENTITY .. name,
        string.pack(IDENTITY_VALUE_FMT, pid, epoch))
    if not ok then return nil, nil, err end

    self.by_name[name]      = { pid = pid, epoch = epoch }
    self.epoch_by_pid[pid]  = epoch
    return pid, epoch, nil
end

-- allocate_ephemeral hands out a pid from the same durable allocator but
-- persists nothing (session-scoped producer). Guarantees the pid won't collide
-- with a persisted named pid across a restart.
function ProducerStateManager:allocate_ephemeral()
    local pid = self.next_pid
    self.next_pid = self.next_pid + 1
    return pid
end

-- current_epoch returns the live epoch for a named pid, or nil if the pid isn't
-- a known durable producer (ephemeral pids are never registered here).
function ProducerStateManager:current_epoch(pid)
    return self.epoch_by_pid[pid]
end

-- pid_for returns the durable pid bound to a producer name, or nil.
function ProducerStateManager:pid_for(name)
    local e = self.by_name[name]
    return e and e.pid or nil
end

-- lookup_memo returns the dedup memo for (pid, topic) or nil.
function ProducerStateManager:lookup_memo(pid, topic)
    local t = self.memo[pid]
    if not t then return nil end
    return t[topic]
end

-- record_produce persists (and memoizes) the sequence/offset for a freshly
-- appended idempotent record. Returns (true, nil) or (nil, err).
function ProducerStateManager:record_produce(pid, topic, seq, offset, partition)
    local key = K_MEMO .. string.pack(">I8", pid) .. topic
    local val = string.pack(MEMO_VALUE_FMT, seq, offset, partition)
    local ok, err = self:_write(key, val)
    if not ok then return nil, err end

    local t = self.memo[pid]
    if not t then t = {}; self.memo[pid] = t end
    t[topic] = { last_seq = seq, last_offset = offset, last_partition = partition }
    return true, nil
end

-- recover replays every internal partition front-to-back, rebuilding the
-- in-memory identity/memo maps (last write per key wins) and the pid allocator.
function ProducerStateManager:recover()
    local max_pid = 0
    local restored = 0

    for _, part in ipairs(self.topic.partitions) do
        local serr = part:scan(function(_offset, m)
            local key = m.key
            if #key < 1 then return end
            local tag = key:sub(1, 1)

            if tag == K_IDENTITY then
                local name = key:sub(2)
                local ok, pid, epoch = pcall(string.unpack, IDENTITY_VALUE_FMT, m.value)
                if ok then
                    self.by_name[name]     = { pid = pid, epoch = epoch }
                    self.epoch_by_pid[pid] = epoch
                    if pid > max_pid then max_pid = pid end
                    restored = restored + 1
                end
            elseif tag == K_MEMO then
                -- key = "\2" .. u64 pid .. topic
                if #key >= 1 + 8 then
                    local pid = string.unpack(">I8", key, 2)
                    local topic = key:sub(1 + 8 + 1)
                    local ok, s, off, prt = pcall(string.unpack, MEMO_VALUE_FMT, m.value)
                    if ok then
                        local t = self.memo[pid]
                        if not t then t = {}; self.memo[pid] = t end
                        t[topic] = { last_seq = s, last_offset = off, last_partition = prt }
                        if pid > max_pid then max_pid = pid end
                        restored = restored + 1
                    end
                end
            else
                log:warn("%s: skipping record with unknown key tag", STATE_TOPIC)
            end
        end)
        if serr then
            log:warn("%s/partition-%d: replay stopped early: %s",
                STATE_TOPIC, part.id, serr)
        end
    end

    self.next_pid = max_pid + 1
    if restored > 0 then
        log:info("recovered %d producer-state record(s) from %s; next_pid=%d",
            restored, STATE_TOPIC, self.next_pid)
    end
    return nil
end

return {
    ProducerStateManager = ProducerStateManager,
    STATE_TOPIC          = STATE_TOPIC,
}
