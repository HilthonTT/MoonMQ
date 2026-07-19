-- TransactionCoordinator — atomic multi-partition transactions.
--
-- SCOPE (see docs/transactions.md): this implements crash-safe, epoch-fenced,
-- multi-partition transactions with transactional offset commit, but NOT
-- read_committed isolation. Consumers still read every record; aborted data
-- stays visible. What a transaction buys here is:
--   * all-or-nothing DURABLE resolution: a txn either reaches COMPLETE_COMMIT
--     (COMMIT markers on every participant partition + buffered offset commits
--     applied) or is aborted — decided by the coordinator's durable state, and
--     re-driven idempotently on crash recovery.
--   * producer fencing: a zombie session (stale epoch) can't commit/abort.
--
-- State is kept in the internal __transaction_state topic, keyed by
-- transactional_id (== the producer_name from src/storage/producer_state.lua),
-- exactly like OffsetManager / ProducerStateManager. The full record is
-- rewritten on each change; commitlog compaction keeps latest-per-key.
--
-- Lifecycle:  EMPTY → ONGOING → PREPARE_{COMMIT,ABORT} → COMPLETE_{COMMIT,ABORT}
-- A COMPLETE_* txn can begin again (same id, next txn), returning to ONGOING.

local msg_m      = require("src.record.message")
local AbortIndex = require("src.broker.abort_index")
local log        = require("src.log.logger").get("txn_coordinator")

local STATE_TOPIC = "__transaction_state"
local DEFAULT_PARTITIONS = 16
local BACKEND          = "commitlog"
local MAX_SEGMENT_SIZE = 8 * 1024 * 1024

-- Durable state ids.
local S = {
    EMPTY           = 0,
    ONGOING         = 1,
    PREPARE_COMMIT  = 2,
    PREPARE_ABORT   = 3,
    COMPLETE_COMMIT = 4,
    COMPLETE_ABORT  = 5,
}
local S_NAME = {}
for k, v in pairs(S) do S_NAME[v] = k end

-- Control-record marker types (the value of a control record written to a
-- participant partition). Consumers skip control records entirely.
local MARKER_ABORT  = 0
local MARKER_COMMIT = 1
local CONTROL_VALUE_FMT = ">BI8I2"   -- marker_type, pid, epoch

local Coordinator = {}
Coordinator.__index = Coordinator

local function fnv1a(s)
    local hash = 2166136261
    for i = 1, #s do
        hash = (hash ~ s:byte(i)) & 0xFFFFFFFF
        hash = (hash * 16777619) & 0xFFFFFFFF
    end
    return hash
end

-- broker must already have topic_manager, offsets, and producer_state built
-- (Broker.new constructs the coordinator last, so those are all available —
-- including during this constructor's crash-recovery pass).
function Coordinator.new(broker, opts)
    assert(type(broker) == "table", "broker required")
    opts = opts or {}
    local topic_manager = broker.topic_manager
    local nparts = opts.num_partitions or DEFAULT_PARTITIONS

    local topic = topic_manager.topics[STATE_TOPIC]
    if not topic then
        local created, err = topic_manager:create_topic(STATE_TOPIC, nparts, {
            backend          = BACKEND,
            cleanup_policy   = "compact",
            max_segment_size = MAX_SEGMENT_SIZE,
        })
        if not created then
            return nil, string.format("failed to create %s: %s", STATE_TOPIC, err)
        end
        topic = created
    end

    -- Durable aborted-range index, keyed per partition. Lives beside the
    -- topic dirs in the broker's data dir.
    local aborts, aerr = AbortIndex.new(topic_manager.baseDir)
    if not aborts then return nil, aerr end

    local self = setmetatable({
        broker = broker,
        topic  = topic,
        nparts = #topic.partitions,
        aborts = aborts,
        txns   = {},   -- txn_id -> { pid, epoch, state, participants, pending_offsets }
        -- Owner-side registry of REMOTE coordinators' unresolved transactions
        -- touching partitions this broker owns:
        --   key(topic, partition) -> { [txn_id] = first_offset }
        -- Feeds lso() so a read_committed consumer here never reads past a
        -- transaction still being driven by a peer broker. In-memory only:
        -- if this broker restarts mid-remote-txn, the floor is lost until the
        -- peer's markers arrive (see docs/transactions.md §2 caveats).
        remote = {},
        -- Cluster produce router (set by the Server via set_router). When
        -- present, commit/abort markers for a participant partition owned by
        -- a peer are appended on that peer instead of the stale local log.
        router = nil,
    }, Coordinator)

    local rerr = self:_recover()
    if rerr then return nil, rerr end
    return self, nil
end

function Coordinator.topic_name() return STATE_TOPIC end

-- set_router installs the cluster produce router (src/cluster/router.lua).
-- Nil-safe: without one, every participant is assumed local (single-broker).
function Coordinator:set_router(router) self.router = router end

function Coordinator:_partition_for(txn_id)
    return (fnv1a(txn_id) % self.nparts) + 1
end

-- ---- durable encode/decode of a txn record --------------------------------

-- Durable txn record value: pid(8) epoch(2) state(1) | participants | offsets.
-- Each participant carries first_offset — the partition LEO captured when the
-- txn enrolled it (i.e. before the txn's first append there). It bounds both
-- the LSO and the aborted range recorded on abort.
local TXN_HEAD_FMT = ">I8I2B"

local function encode_txn(t)
    local parts = { string.pack(TXN_HEAD_FMT, t.pid, t.epoch, t.state) }
    -- participants
    local plist = {}
    for _, p in pairs(t.participants) do plist[#plist + 1] = p end
    parts[#parts + 1] = string.pack(">I4", #plist)
    for _, p in ipairs(plist) do
        parts[#parts + 1] = string.pack(">s2", p.topic)
        parts[#parts + 1] = string.pack(">I4I8", p.partition, p.first_offset or 0)
    end
    -- pending offsets
    parts[#parts + 1] = string.pack(">I4", #t.pending_offsets)
    for _, o in ipairs(t.pending_offsets) do
        parts[#parts + 1] = string.pack(">s2s2I4I8",
            o.group, o.topic, o.partition, o.offset)
    end
    return table.concat(parts)
end

local function decode_txn(bytes)
    local pid, epoch, state, pos = string.unpack(TXN_HEAD_FMT, bytes)
    local t = { pid = pid, epoch = epoch, state = state,
                participants = {}, pending_offsets = {} }
    local pcount; pcount, pos = string.unpack(">I4", bytes, pos)
    for _ = 1, pcount do
        local topic, partition, first_offset
        topic, pos = string.unpack(">s2", bytes, pos)
        partition, first_offset, pos = string.unpack(">I4I8", bytes, pos)
        t.participants[topic .. "\0" .. partition] =
            { topic = topic, partition = partition, first_offset = first_offset }
    end
    local ocount; ocount, pos = string.unpack(">I4", bytes, pos)
    for _ = 1, ocount do
        local group, topic, partition, offset, npos =
            string.unpack(">s2s2I4I8", bytes, pos)
        pos = npos
        t.pending_offsets[#t.pending_offsets + 1] =
            { group = group, topic = topic, partition = partition, offset = offset }
    end
    return t
end

-- Persist the current in-memory txn record durably. (true, nil) or (nil, err).
function Coordinator:_persist(txn_id)
    local t = self.txns[txn_id]
    local rec = msg_m.Message.new(txn_id, encode_txn(t), os.time() * 1000)
    local part = self.topic.partitions[self:_partition_for(txn_id)]
    local _, werr = part:write_message(rec)
    if werr then return nil, string.format("txn state write failed: %s", werr) end
    if part.request_sync then
        local ok, serr = part:request_sync()
        if not ok then return nil, string.format("txn state sync failed: %s", tostring(serr)) end
    end
    return true, nil
end

-- ---- producer fencing ------------------------------------------------------

-- Validate that (pid, epoch) is the live identity for txn_id. The producer_state
-- manager is the source of truth for the current epoch.
function Coordinator:_check_producer(txn_id, pid, epoch)
    local ps = self.broker.producer_state
    -- The pid must be the one bound to this transactional_id — a defence against
    -- a caller pairing someone else's pid with this txn_id.
    if ps:pid_for(txn_id) ~= pid then
        return false, "producer id does not match transactional_id"
    end
    local cur = ps:current_epoch(pid)
    if cur == nil then
        return false, "unknown producer id for transaction"
    end
    if epoch ~= cur then
        return false, string.format("producer fenced: epoch %d != current %d", epoch, cur)
    end
    return true
end

-- ---- public API ------------------------------------------------------------

-- begin starts (or restarts) a transaction for txn_id. Requires a durable
-- producer identity. Returns (true, nil) or (nil, err, code) where code is a
-- proto error hint ("fenced" | "state").
function Coordinator:begin(txn_id, pid, epoch)
    local okp, perr = self:_check_producer(txn_id, pid, epoch)
    if not okp then return nil, perr, "fenced" end

    local t = self.txns[txn_id]
    if t and t.state == S.ONGOING then
        return nil, "transaction already in progress", "state"
    end
    self.txns[txn_id] = {
        pid = pid, epoch = epoch, state = S.ONGOING,
        participants = {}, pending_offsets = {},
    }
    local ok, err = self:_persist(txn_id)
    if not ok then return nil, err end
    return true
end

-- add_partition registers (topic, partition) as a participant. Called from the
-- produce path BEFORE the transaction's first append to that partition — the
-- partition's current LEO is captured as first_offset, which lower-bounds every
-- record this txn will write there. That ordering is what makes the LSO and
-- the aborted-range filter conservative-but-correct: registering after the
-- append would leave the first record outside the recorded range. Only persists
-- when the participant set actually grows, so repeated produces to the same
-- partition don't amplify writes. Returns (true, nil) or (nil, err, code).
--
-- remote (optional) marks a participant partition owned by a peer broker:
-- { peer = Peer, leo = owner's log-end offset captured before this txn's
-- first forwarded append }. The owner is enrolled too (peer:txn_enroll), so
-- ITS read_committed consumers floor their LSO on this transaction — the
-- coordinator's local lso() is irrelevant there. A failed remote enrolment
-- fails the produce: forwarding the record without the owner knowing the txn
-- is unresolved would let read_committed readers on the owner see it early.
function Coordinator:add_partition(txn_id, pid, epoch, topic, partition, remote)
    local t = self.txns[txn_id]
    if not t or t.state ~= S.ONGOING then
        return nil, "no transaction in progress", "state"
    end
    local okp, perr = self:_check_producer(txn_id, pid, epoch)
    if not okp then return nil, perr, "fenced" end

    local key = topic .. "\0" .. partition
    if t.participants[key] then return true end   -- already tracked

    local first_offset = 0
    if remote then
        first_offset = remote.leo or 0
        local rok, rerr = remote.peer:txn_enroll(txn_id, topic, partition, first_offset)
        if not rok then
            return nil, string.format("remote txn enrol on %s failed: %s",
                tostring(remote.peer.id), tostring(rerr)), "state"
        end
    else
        local tp = self.broker.topic_manager.topics[topic]
        local part = tp and tp.partitions[partition]
        if part then first_offset = part.offset or 0 end
    end

    t.participants[key] =
        { topic = topic, partition = partition, first_offset = first_offset }
    local ok, err = self:_persist(txn_id)
    if not ok then return nil, err end
    return true
end

-- add_offsets buffers offset commits into the transaction. They only take
-- effect when the txn commits. Returns (true, nil) or (nil, err, code).
function Coordinator:add_offsets(txn_id, pid, epoch, group, offsets)
    local t = self.txns[txn_id]
    if not t or t.state ~= S.ONGOING then
        return nil, "no transaction in progress", "state"
    end
    local okp, perr = self:_check_producer(txn_id, pid, epoch)
    if not okp then return nil, perr, "fenced" end

    for _, o in ipairs(offsets) do
        t.pending_offsets[#t.pending_offsets + 1] = {
            group = group, topic = o.topic, partition = o.partition, offset = o.offset,
        }
    end
    local ok, err = self:_persist(txn_id)
    if not ok then return nil, err end
    return true
end

-- Write a COMMIT/ABORT marker on the PEER that owns the participant
-- partition, then resolve the transaction there (clears the owner's LSO
-- floor; on abort, also records the aborted range in the owner's abort index
-- — the owner's consumers are the ones that filter). Returns
-- (true, marker_offset, true) or (nil, err). Idempotent: a recovery retry
-- re-appends a duplicate marker (harmless — consumers skip control records)
-- and remote_resolve dedups the abort range.
function Coordinator:_write_remote_marker(peer, participant, marker, pid, epoch, txn_id)
    local value = string.pack(CONTROL_VALUE_FMT, marker, pid, epoch)
    local rec = msg_m.Message.new("", value, os.time() * 1000, msg_m.ATTR_CONTROL)
    local bytes, serr = msg_m.serialize_message(rec)
    if not bytes then return nil, tostring(serr) end

    local leo, aerr = peer:append(participant.topic, participant.partition, bytes)
    if not leo then
        return nil, string.format("marker forward to %s for %s/partition-%d failed: %s",
            tostring(peer.id), participant.topic, participant.partition, tostring(aerr))
    end
    local moffset = leo - #bytes

    local rok, rerr = peer:txn_resolve(txn_id, participant.topic, participant.partition, {
        aborted = (marker == MARKER_ABORT),
        pid     = pid,
        epoch   = epoch,
        first   = participant.first_offset or 0,
        upto    = moffset,
    })
    if not rok then
        return nil, string.format("txn resolve on %s for %s/partition-%d failed: %s",
            tostring(peer.id), participant.topic, participant.partition, tostring(rerr))
    end
    return true, moffset, true
end

-- Write a COMMIT/ABORT control marker to one participant partition. When a
-- cluster router says a peer owns the partition (ownership is re-checked NOW,
-- not at enrol time, in case it moved), the marker goes to the owner — see
-- _write_remote_marker. A missing local topic/partition (shouldn't happen —
-- topic deletion isn't supported) is logged and treated as success; an actual
-- WRITE failure is returned so the transaction is not marked complete with
-- markers missing. Returns (true, marker_offset_or_nil, is_remote) or
-- (nil, err) — the offset lets the abort path record the aborted range's
-- upper bound; is_remote tells _finish the owner already recorded that range.
function Coordinator:_write_marker(participant, marker, pid, epoch, txn_id)
    if self.router then
        local peer, rerr = self.router:route(participant.topic, participant.partition)
        if rerr then return nil, rerr end
        if peer then
            return self:_write_remote_marker(peer, participant, marker, pid, epoch, txn_id)
        end
    end

    local topic = self.broker.topic_manager.topics[participant.topic]
    if not topic then
        log:warn("participant topic %s vanished; skipping marker", participant.topic)
        return true, nil
    end
    local part = topic.partitions[participant.partition]
    if not part then
        log:warn("participant %s/partition-%d vanished; skipping marker",
            participant.topic, participant.partition)
        return true, nil
    end
    local value = string.pack(CONTROL_VALUE_FMT, marker, pid, epoch)
    -- Control record: attrs control bit set, empty key.
    local rec = msg_m.Message.new("", value, os.time() * 1000, msg_m.ATTR_CONTROL)
    local moffset, werr = part:write_message(rec)
    if werr then
        return nil, string.format("marker write to %s/partition-%d failed: %s",
            participant.topic, participant.partition, tostring(werr))
    end
    if part.request_sync then part:request_sync() end
    return true, moffset
end

-- _finish drives the second half of a commit/abort: write markers to every
-- participant, apply (commit) or drop (abort) buffered offsets, then persist the
-- terminal state. Idempotent so crash recovery can re-run it.
--
-- Any marker/offset failure leaves the txn in its PREPARE_* state and returns
-- the error: the decision is durable, the completion is not — recovery (or a
-- retried END_TXN, which end_txn allows for a matching PREPARE state) re-runs
-- this until everything lands. Advancing to COMPLETE_* past a failed offset
-- commit would silently break the txn's atomicity contract: output records
-- committed, input offsets not, and the client told "success".
function Coordinator:_finish(txn_id, commit)
    local t = self.txns[txn_id]
    local marker = commit and MARKER_COMMIT or MARKER_ABORT

    for _, p in pairs(t.participants) do
        local ok, moffset_or_err, is_remote =
            self:_write_marker(p, marker, t.pid, t.epoch, txn_id)
        if not ok then
            log:error("txn %s: %s (left in %s for retry/recovery)",
                txn_id, moffset_or_err, S_NAME[t.state])
            return nil, moffset_or_err
        end
        -- On abort, durably record the aborted range [first_offset, marker)
        -- so read_committed consumers can filter this txn's data records.
        -- Failing to persist it must NOT complete the txn — the records would
        -- become permanently visible — so it's treated like a marker failure.
        -- Remote participants are excluded: the OWNER's abort index recorded
        -- the range during txn_resolve (its consumers do the filtering).
        if not commit and moffset_or_err ~= nil and not is_remote then
            local aok, aerr = self.aborts:add(p.topic, p.partition,
                t.pid, t.epoch, p.first_offset or 0, moffset_or_err)
            if not aok then
                log:error("txn %s: %s (left in %s for retry/recovery)",
                    txn_id, aerr, S_NAME[t.state])
                return nil, aerr
            end
        end
    end

    if commit then
        for _, o in ipairs(t.pending_offsets) do
            local ok, cerr = self.broker.offsets:commit(
                o.group, o.topic, o.partition, o.offset)
            if not ok then
                local err = string.format("offset commit failed: %s", tostring(cerr))
                log:error("txn %s: %s (left in %s for retry/recovery)",
                    txn_id, err, S_NAME[t.state])
                return nil, err
            end
        end
    end

    t.state = commit and S.COMPLETE_COMMIT or S.COMPLETE_ABORT
    return self:_persist(txn_id)
end

-- end_txn commits or aborts the in-progress transaction. Returns (true, nil) or
-- (nil, err, code).
function Coordinator:end_txn(txn_id, pid, epoch, commit)
    local t = self.txns[txn_id]
    if not t then
        return nil, "no transaction in progress", "state"
    end

    -- Retry path: a previous END_TXN durably recorded this same decision but
    -- failed while completing (marker/offset write error). Re-run the finish;
    -- it's idempotent. A retry with the OPPOSITE decision is refused — the
    -- prepared decision is already durable and recovery will enforce it.
    if t.state == S.PREPARE_COMMIT or t.state == S.PREPARE_ABORT then
        local okp0, perr0 = self:_check_producer(txn_id, pid, epoch)
        if not okp0 then return nil, perr0, "fenced" end
        local prepared_commit = (t.state == S.PREPARE_COMMIT)
        if prepared_commit ~= commit then
            return nil, string.format(
                "transaction already prepared to %s; cannot %s",
                prepared_commit and "commit" or "abort",
                commit and "commit" or "abort"), "state"
        end
        return self:_finish(txn_id, commit)
    end

    if t.state ~= S.ONGOING then
        return nil, "no transaction in progress", "state"
    end
    local okp, perr = self:_check_producer(txn_id, pid, epoch)
    if not okp then return nil, perr, "fenced" end

    -- Durably record the intent (PREPARE) BEFORE touching participant logs, so a
    -- crash mid-finish is rolled forward to the same decision on recovery.
    t.state = commit and S.PREPARE_COMMIT or S.PREPARE_ABORT
    local ok, err = self:_persist(txn_id)
    if not ok then return nil, err end

    return self:_finish(txn_id, commit)
end

-- current exposes a txn's durable snapshot (for tests/observability).
function Coordinator:current(txn_id) return self.txns[txn_id] end

-- has_unresolved reports whether txn_id has a transaction that hasn't reached
-- a COMPLETE_* state. Producer-state expiry uses it as a veto: expiring a
-- producer mid-transaction would drop the epoch fencing that recovery relies
-- on to resolve it.
function Coordinator:has_unresolved(txn_id)
    local t = self.txns[txn_id]
    return t ~= nil
        and t.state ~= S.COMPLETE_COMMIT
        and t.state ~= S.COMPLETE_ABORT
end

-- ---- read_committed queries ------------------------------------------------

-- lso returns the Last Stable Offset for (topic, partition): the smallest
-- first_offset among transactions that are still unresolved (ONGOING or
-- PREPARE_* — their markers aren't all durable yet) and enrolled this
-- partition. Every record below the LSO belongs to a resolved transaction
-- (or none), so a read_committed consumer may read up to it, filtering
-- aborted ranges via is_aborted. Returns nil when no unresolved transaction
-- touches the partition — the caller should then use the log-end offset.
function Coordinator:lso(topic, partition)
    local key = topic .. "\0" .. partition
    local min
    for _, t in pairs(self.txns) do
        if t.state ~= S.COMPLETE_COMMIT and t.state ~= S.COMPLETE_ABORT then
            local p = t.participants[key]
            if p and (min == nil or (p.first_offset or 0) < min) then
                min = p.first_offset or 0
            end
        end
    end
    -- Transactions driven by a PEER coordinator that enrolled this partition
    -- (this broker owns it; the txn state lives elsewhere) floor the LSO too.
    local rt = self.remote[key]
    if rt then
        for _, fo in pairs(rt) do
            if min == nil or fo < min then min = fo end
        end
    end
    return min
end

-- ---- remote-coordinator hooks (owner side) ---------------------------------
-- Called by the cluster server when a PEER broker coordinates a transaction
-- that produces to a partition THIS broker owns.

-- remote_enroll floors this partition's LSO at first_offset until the peer
-- resolves the transaction. Idempotent per (txn_id, partition).
function Coordinator:remote_enroll(txn_id, topic, partition, first_offset)
    assert(type(txn_id) == "string" and #txn_id > 0, "txn_id required")
    local key = topic .. "\0" .. partition
    local t = self.remote[key]
    if not t then t = {}; self.remote[key] = t end
    -- Keep the lowest floor if the peer retries with a different LEO.
    if t[txn_id] == nil or first_offset < t[txn_id] then
        t[txn_id] = first_offset
    end
    return true
end

-- remote_resolve ends a peer-coordinated transaction's hold on this
-- partition. On abort, the aborted range is recorded in OUR abort index
-- FIRST (this broker's consumers filter with it) and only then is the LSO
-- floor released — the other order would open a window where the aborted
-- records are below the LSO but not yet filtered. Returns (true) or (nil, err).
function Coordinator:remote_resolve(txn_id, topic, partition, opts)
    opts = opts or {}
    if opts.aborted then
        local ok, err = self.aborts:add(topic, partition,
            opts.pid or 0, opts.epoch or 0, opts.first or 0, opts.upto or 0)
        if not ok then return nil, err end
    end
    local key = topic .. "\0" .. partition
    local t = self.remote[key]
    if t then
        t[txn_id] = nil
        if next(t) == nil then self.remote[key] = nil end
    end
    return true
end

-- is_aborted reports whether the transactional data record at `offset` of
-- (topic, partition), written by producer session (pid, epoch), belongs to an
-- aborted transaction. Only meaningful below the LSO.
function Coordinator:is_aborted(topic, partition, pid, epoch, offset)
    return self.aborts:is_aborted(topic, partition, pid, epoch, offset)
end

-- ---- crash recovery --------------------------------------------------------

-- Replay __transaction_state, then resolve any txn caught mid-flight:
--   ONGOING       → abort (a producer that never reached END_TXN)
--   PREPARE_COMMIT→ roll forward the commit
--   PREPARE_ABORT → roll forward the abort
--   COMPLETE_*    → done
function Coordinator:_recover()
    for _, part in ipairs(self.topic.partitions) do
        local serr = part:scan(function(_offset, m)
            local ok, decoded = pcall(decode_txn, m.value)
            if ok then
                self.txns[m.key] = decoded
            else
                log:warn("%s: skipping undecodable txn record for %q",
                    STATE_TOPIC, m.key)
            end
        end)
        if serr then
            log:warn("%s/partition-%d: replay stopped early: %s",
                STATE_TOPIC, part.id, serr)
        end
    end

    local resolved = 0
    for txn_id, t in pairs(self.txns) do
        if t.state == S.ONGOING or t.state == S.PREPARE_ABORT then
            t.state = S.PREPARE_ABORT
            self:_persist(txn_id)
            local ok, err = self:_finish(txn_id, false)
            if not ok then
                -- Still PREPARE_ABORT; the next recovery (or a client retry)
                -- re-runs the finish. Loud, not fatal — the broker can serve.
                log:error("recovery: txn %s abort incomplete: %s", txn_id, tostring(err))
            end
            resolved = resolved + 1
        elseif t.state == S.PREPARE_COMMIT then
            local ok, err = self:_finish(txn_id, true)
            if not ok then
                log:error("recovery: txn %s commit incomplete: %s", txn_id, tostring(err))
            end
            resolved = resolved + 1
        end
    end
    if resolved > 0 then
        log:info("resolved %d in-flight transaction(s) from %s on recovery",
            resolved, STATE_TOPIC)
    end
    return nil
end

return {
    Coordinator = Coordinator,
    STATE_TOPIC = STATE_TOPIC,
    STATES      = S,
    STATE_NAMES = S_NAME,
    MARKER_COMMIT = MARKER_COMMIT,
    MARKER_ABORT  = MARKER_ABORT,
    CONTROL_VALUE_FMT = CONTROL_VALUE_FMT,
}
