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
-- Allocator watermark: a single fixed-key record holding next_pid. Only
-- written when expiry tombstones producer records — without it, deleting the
-- highest pid's identity would let a restart re-issue that pid to a NEW
-- producer while a zombie of the old one still holds it (epoch fencing can't
-- tell them apart: both start their epoch history at 0).
local K_ALLOC    = "\3"
local ALLOC_KEY  = K_ALLOC .. "next_pid"
local ALLOC_VALUE_FMT = ">I8"

-- A zero-length value is a tombstone: the key's prior record is dead. The
-- commitlog compactor drops tombstones (and everything they superseded) on
-- its next pass, so expired producer state actually leaves the disk.
local TOMBSTONE = ""

local IDENTITY_VALUE_FMT = ">I8I2"        -- pid, epoch
-- Memo carries the epoch that wrote it: dedup state is only valid within the
-- SAME producer session. A reconnect bumps the epoch and the client restarts
-- its sequences at 0 (Kafka KIP-360 semantics); without the epoch scope, the
-- new session's seq 0 would collide with the old session's memo and be
-- swallowed as a "retry" — silently losing the record.
local MEMO_VALUE_FMT     = ">I4I8I4I2"    -- last_seq, last_offset, last_partition, epoch
local MEMO_VALUE_FMT_V1  = ">I4I8I4"      -- pre-epoch memos (read-compat only)

-- A memo written by PRODUCE_BATCH appends a tail to the v2 value:
--
--   <v2 value> | u32 base_seq | u32 count | (u32 partition | u64 offset)*
--
-- One record's ack is not enough to answer a duplicate BATCH: a batch spans
-- partitions and offsets are opaque per-backend cursors (on the segmented
-- backend they are byte positions), so offsets cannot be reconstructed from a
-- base. The acks are therefore stored verbatim.
--
-- The tail is strictly additive: string.unpack ignores trailing bytes, so a
-- reader that only knows v2 still parses last_seq/last_offset/last_partition/
-- epoch out of a v3 value exactly as before. Single-record produces keep
-- writing plain v2 values (no tail), which is what makes an interleaving of
-- batch and single produces on one (pid, topic) work without special cases.
local MEMO_BATCH_HDR_FMT = ">I4I4"        -- base_seq, count
local MEMO_ACK_FMT       = ">I4I8"        -- partition, offset
-- Matches proto.MAX_IDEMPOTENT_BATCH. Duplicated rather than required so the
-- storage layer keeps no dependency on the wire module; the decoder uses it
-- only to refuse an implausible count from a corrupt record.
local MAX_MEMO_ACKS      = 1024

-- The identity epoch packs as u16; the 65,536th session of one name would
-- overflow string.pack mid-write. Fail the INIT instead (fresh name = fresh pid).
local MAX_EPOCH = 0xFFFF

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
        last_active = {},   -- pid -> ms of last identity/memo write (expiry input)
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
function ProducerStateManager:_write(key, value, now_ms)
    local rec  = msg_m.Message.new(key, value, now_ms or os.time() * 1000)
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
        if epoch > MAX_EPOCH then
            return nil, nil, string.format(
                "producer %q exhausted its epoch space (%d sessions); use a new name",
                name, MAX_EPOCH + 1)
        end
    else
        pid   = self.next_pid
        self.next_pid = self.next_pid + 1
        epoch = 0
    end

    local now = os.time() * 1000
    local ok, err = self:_write(K_IDENTITY .. name,
        string.pack(IDENTITY_VALUE_FMT, pid, epoch), now)
    if not ok then return nil, nil, err end

    self.by_name[name]      = { pid = pid, epoch = epoch }
    self.epoch_by_pid[pid]  = epoch
    self.last_active[pid]   = now
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
-- appended idempotent record, tagged with the epoch of the session that wrote
-- it. Returns (true, nil) or (nil, err).
--
-- batch (optional) = { base_seq = , acks = { { partition, offset }, ... } }:
-- set by the PRODUCE_BATCH path so a duplicate batch can be answered with the
-- original per-record acks. `seq`/`offset`/`partition` must describe the
-- batch's LAST record, which keeps a following single-record produce working
-- off the same last_seq with no knowledge that a batch came before it.
-- Omitting `batch` writes a plain v2 memo and clears any stored batch.
function ProducerStateManager:record_produce(pid, topic, seq, offset, partition, epoch, batch)
    epoch = epoch or 0
    local key = K_MEMO .. string.pack(">I8", pid) .. topic
    local val = string.pack(MEMO_VALUE_FMT, seq, offset, partition, epoch)
    if batch then
        assert(#batch.acks <= MAX_MEMO_ACKS,
            "batch too large to memoize for idempotent replay")
        local parts = { val, string.pack(MEMO_BATCH_HDR_FMT, batch.base_seq, #batch.acks) }
        for i = 1, #batch.acks do
            local a = batch.acks[i]
            parts[#parts + 1] = string.pack(MEMO_ACK_FMT, a.partition, a.offset)
        end
        val = table.concat(parts)
    end
    local now = os.time() * 1000
    local ok, err = self:_write(key, val, now)
    if not ok then return nil, err end

    local t = self.memo[pid]
    if not t then t = {}; self.memo[pid] = t end
    t[topic] = { last_seq = seq, last_offset = offset,
                 last_partition = partition, epoch = epoch,
                 base_seq = batch and batch.base_seq or nil,
                 acks     = batch and batch.acks or nil }
    self.last_active[pid] = now
    return true, nil
end

-- Parse the optional batch tail of a v3 memo value. `pos` is the position
-- string.unpack stopped at after the v2 prefix. Returns (base_seq, acks) or
-- nil when there is no tail (v1/v2 value) or it is truncated — a corrupt tail
-- degrades to "no batch memo", i.e. a duplicate batch is rejected as
-- out-of-order rather than replayed with wrong offsets.
local function decode_batch_tail(value, pos)
    if #value - pos + 1 < 8 then return nil end
    local ok, base_seq, count, p = pcall(string.unpack, MEMO_BATCH_HDR_FMT, value, pos)
    if not ok or count == 0 or count > MAX_MEMO_ACKS then return nil end
    if #value - p + 1 < count * 12 then return nil end
    local acks = {}
    for i = 1, count do
        local partition, offset, np = string.unpack(MEMO_ACK_FMT, value, p)
        acks[i] = { partition = partition, offset = offset }
        p = np
    end
    return base_seq, acks
end

-- expire_idle garbage-collects durable producers whose last identity/memo
-- write is at least max_idle_ms old. Expiry tombstones the producer's
-- identity and every sequence memo in __producer_state (compaction later
-- drops both the tombstones and everything they superseded), so a quiet
-- producer no longer pins its state forever.
--
-- opts:
--   now_ms     override for tests (default: wall clock).
--   is_active  fn(name, pid) -> bool. A truthy return vetoes expiry — the
--              broker uses it to protect producers with an unresolved
--              transaction, the server to protect pids bound to a live
--              connection.
--
-- Before the first tombstone, the pid allocator's watermark is persisted:
-- otherwise deleting the highest pid's identity would let a restart hand the
-- same pid to a NEW producer while a zombie session still holds it, and
-- epoch fencing could not tell the two apart.
--
-- Returns (expired_count, nil) or (expired_count_so_far, err) if a write
-- failed mid-sweep (in-memory state stays consistent with what was written;
-- the next sweep retries the rest).
function ProducerStateManager:expire_idle(max_idle_ms, opts)
    assert(type(max_idle_ms) == "number" and max_idle_ms > 0,
        "max_idle_ms must be a positive number")
    opts = opts or {}
    local now       = opts.now_ms or os.time() * 1000
    local is_active = opts.is_active

    local victims = {}
    for name, e in pairs(self.by_name) do
        local last = self.last_active[e.pid] or 0
        if (now - last) >= max_idle_ms
            and not (is_active and is_active(name, e.pid)) then
            victims[#victims + 1] = { name = name, pid = e.pid, idle_ms = now - last }
        end
    end
    if #victims == 0 then return 0, nil end

    local ok, err = self:_write(ALLOC_KEY,
        string.pack(ALLOC_VALUE_FMT, self.next_pid), now)
    if not ok then return 0, err end

    local expired = 0
    for _, v in ipairs(victims) do
        local wok, werr = self:_write(K_IDENTITY .. v.name, TOMBSTONE, now)
        if not wok then return expired, werr end
        for topic in pairs(self.memo[v.pid] or {}) do
            local mok, merr = self:_write(
                K_MEMO .. string.pack(">I8", v.pid) .. topic, TOMBSTONE, now)
            if not mok then
                -- Identity is already tombstoned; drop the in-memory entry so
                -- serving state never claims more than the log does. Leftover
                -- memo records are harmless (no identity → pid never revived)
                -- and get swept next pass.
                self.by_name[v.name]      = nil
                self.epoch_by_pid[v.pid]  = nil
                self.memo[v.pid]          = nil
                self.last_active[v.pid]   = nil
                return expired + 1, merr
            end
        end
        self.by_name[v.name]      = nil
        self.epoch_by_pid[v.pid]  = nil
        self.memo[v.pid]          = nil
        self.last_active[v.pid]   = nil
        expired = expired + 1
        log:info("expired idle producer %q (pid=%d, idle %.0fs)",
            v.name, v.pid, v.idle_ms / 1000)
    end
    return expired, nil
end

-- recover replays every internal partition front-to-back, rebuilding the
-- in-memory identity/memo maps (last write per key wins) and the pid allocator.
function ProducerStateManager:recover()
    local max_pid = 0
    local alloc_floor = 0   -- persisted allocator watermark (see expire_idle)
    local restored = 0

    -- Bump a pid's last_active from a record's timestamp (replay is in
    -- append order per partition, but identity and memo records for one pid
    -- can land on different partitions — keep the max).
    local function touch(pid, ts)
        local cur = self.last_active[pid]
        if ts and (not cur or ts > cur) then self.last_active[pid] = ts end
    end

    for _, part in ipairs(self.topic.partitions) do
        local serr = part:scan(function(_offset, m)
            local key = m.key
            if #key < 1 then return end
            local tag = key:sub(1, 1)

            if tag == K_IDENTITY then
                local name = key:sub(2)
                if #m.value == 0 then
                    -- Tombstone: this producer was expired. Per-key replay
                    -- order guarantees this supersedes any earlier identity.
                    local prev = self.by_name[name]
                    if prev then
                        self.epoch_by_pid[prev.pid] = nil
                        self.memo[prev.pid]         = nil
                        self.last_active[prev.pid]  = nil
                        self.by_name[name]          = nil
                    end
                    return
                end
                local ok, pid, epoch = pcall(string.unpack, IDENTITY_VALUE_FMT, m.value)
                if ok then
                    self.by_name[name]     = { pid = pid, epoch = epoch }
                    self.epoch_by_pid[pid] = epoch
                    touch(pid, m.timestamp)
                    if pid > max_pid then max_pid = pid end
                    restored = restored + 1
                end
            elseif tag == K_MEMO then
                -- key = "\2" .. u64 pid .. topic
                if #key >= 1 + 8 then
                    local pid = string.unpack(">I8", key, 2)
                    local topic = key:sub(1 + 8 + 1)
                    if #m.value == 0 then
                        -- Tombstoned memo (producer expiry).
                        local t = self.memo[pid]
                        if t then
                            t[topic] = nil
                            if next(t) == nil then self.memo[pid] = nil end
                        end
                        return
                    end
                    local ok, s, off, prt, ep, vpos =
                        pcall(string.unpack, MEMO_VALUE_FMT, m.value)
                    if not ok then
                        -- Pre-epoch memo (v1 format). Epoch 0: any session
                        -- after a reconnect has epoch >= 1 and correctly
                        -- treats this memo as belonging to an older session.
                        ok, s, off, prt = pcall(string.unpack, MEMO_VALUE_FMT_V1, m.value)
                        ep, vpos = 0, nil
                    end
                    if ok then
                        -- v3 batch tail, when present: the per-record acks a
                        -- duplicate PRODUCE_BATCH replays.
                        local base_seq, acks
                        if vpos then base_seq, acks = decode_batch_tail(m.value, vpos) end
                        local t = self.memo[pid]
                        if not t then t = {}; self.memo[pid] = t end
                        t[topic] = { last_seq = s, last_offset = off,
                                     last_partition = prt, epoch = ep,
                                     base_seq = base_seq, acks = acks }
                        touch(pid, m.timestamp)
                        if pid > max_pid then max_pid = pid end
                        restored = restored + 1
                    end
                end
            elseif tag == K_ALLOC then
                local ok, floor = pcall(string.unpack, ALLOC_VALUE_FMT, m.value)
                if ok and floor > alloc_floor then alloc_floor = floor end
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
    if alloc_floor > self.next_pid then self.next_pid = alloc_floor end
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
