local msg_m      = require("src.record.message")
local AbortIndex = require("src.broker.abort_index")
local log        = require("src.log.logger").get("txn_coordinator")
local hash_m = require("src.core.hash")
local time_m = require("src.core.time")

local STATE_TOPIC = "__transaction_state"
local DEFAULT_PARTITIONS = 16
local BACKEND          = "commitlog"
local MAX_SEGMENT_SIZE = 8 * 1024 * 1024

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

local MARKER_ABORT  = 0
local MARKER_COMMIT = 1
local CONTROL_VALUE_FMT = ">BI8I2"

local Coordinator = {}
Coordinator.__index = Coordinator

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

    local aborts, aerr = AbortIndex.new(topic_manager.baseDir)
    if not aborts then return nil, aerr end

    local self = setmetatable({
        broker = broker,
        topic  = topic,
        nparts = #topic.partitions,
        aborts = aborts,
        txns   = {},
        remote = {},
        router = nil,
        recovery_pending = false,
    }, Coordinator)

    -- In a cluster, in-flight transactions may have participants owned by
    -- peers; finishing them needs the router, which is attached later. The
    -- server defers recovery until then (see Coordinator:recover) so the
    -- owners get their txn_resolve instead of markers landing in local
    -- non-owner copies and the owner's LSO staying pinned forever.
    self:_load_txns()
    if opts and opts.defer_recovery then
        self.recovery_pending = true
    else
        local rerr = self:_resolve_inflight()
        if rerr then return nil, rerr end
    end
    return self, nil
end

function Coordinator.topic_name() return STATE_TOPIC end

function Coordinator:set_router(router) self.router = router end

-- Run deferred crash recovery (no-op unless `defer_recovery` was set).
function Coordinator:recover()
    if not self.recovery_pending then return true end
    self.recovery_pending = false
    local rerr = self:_resolve_inflight()
    if rerr then return nil, rerr end
    return true
end

function Coordinator:_partition_for(txn_id)
    return (hash_m.fnv1a(txn_id) % self.nparts) + 1
end


local TXN_HEAD_FMT = ">I8I2B"

local function encode_txn(t)
    local parts = { string.pack(TXN_HEAD_FMT, t.pid, t.epoch, t.state) }
    local plist = {}
    for _, p in pairs(t.participants) do plist[#plist + 1] = p end
    parts[#parts + 1] = string.pack(">I4", #plist)
    for _, p in ipairs(plist) do
        parts[#parts + 1] = string.pack(">s2", p.topic)
        parts[#parts + 1] = string.pack(">I4I8", p.partition, p.first_offset or 0)
    end
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

function Coordinator:_persist(txn_id)
    local t = self.txns[txn_id]
    local rec = msg_m.Message.new(txn_id, encode_txn(t), time_m.now_ms())
    local part = self.topic.partitions[self:_partition_for(txn_id)]
    local _, werr = part:write_message(rec)
    if werr then return nil, string.format("txn state write failed: %s", werr) end
    if part.request_sync then
        local ok, serr = part:request_sync()
        if not ok then return nil, string.format("txn state sync failed: %s", tostring(serr)) end
    end
    return true, nil
end


function Coordinator:_check_producer(txn_id, pid, epoch)
    local ps = self.broker.producer_state
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


function Coordinator:begin(txn_id, pid, epoch)
    local okp, perr = self:_check_producer(txn_id, pid, epoch)
    if not okp then return nil, perr, "fenced" end

    local t = self.txns[txn_id]
    if t and t.state == S.ONGOING then
        return nil, "transaction already in progress", "state"
    end
    if t and (t.state == S.PREPARE_COMMIT or t.state == S.PREPARE_ABORT) then
        return nil, string.format(
            "transaction is prepared to %s and not yet complete; retry END_TXN",
            t.state == S.PREPARE_COMMIT and "commit" or "abort"), "state"
    end
    self.txns[txn_id] = {
        pid = pid, epoch = epoch, state = S.ONGOING,
        participants = {}, pending_offsets = {},
    }
    local ok, err = self:_persist(txn_id)
    if not ok then return nil, err end
    return true
end

function Coordinator:add_partition(txn_id, pid, epoch, topic, partition, remote)
    local t = self.txns[txn_id]
    if not t or t.state ~= S.ONGOING then
        return nil, "no transaction in progress", "state"
    end
    local okp, perr = self:_check_producer(txn_id, pid, epoch)
    if not okp then return nil, perr, "fenced" end

    local key = topic .. "\0" .. partition
    if t.participants[key] then return true end

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

function Coordinator:_write_remote_marker(peer, participant, marker, pid, epoch, txn_id)
    local value = string.pack(CONTROL_VALUE_FMT, marker, pid, epoch)
    local rec = msg_m.Message.new("", value, time_m.now_ms(), msg_m.ATTR_CONTROL)
    local bytes, serr = msg_m.serialize_message(rec)
    if not bytes then return nil, tostring(serr) end

    local leo, first_or_err = peer:append(participant.topic, participant.partition, bytes)
    if not leo then
        return nil, string.format("marker forward to %s for %s/partition-%d failed: %s",
            tostring(peer.id), participant.topic, participant.partition,
            tostring(first_or_err))
    end
    -- Prefer the owner's reported record offset: LEO minus byte length is
    -- only right for byte-addressed backends, and a wrong `upto` makes the
    -- owner's AbortIndex silently ignore the range.
    local moffset = type(first_or_err) == "number" and first_or_err or (leo - #bytes)

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
    local rec = msg_m.Message.new("", value, time_m.now_ms(), msg_m.ATTR_CONTROL)
    local moffset, werr = part:write_message(rec)
    if werr then
        return nil, string.format("marker write to %s/partition-%d failed: %s",
            participant.topic, participant.partition, tostring(werr))
    end
    if part.request_sync then part:request_sync() end
    return true, moffset
end

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

function Coordinator:end_txn(txn_id, pid, epoch, commit)
    local t = self.txns[txn_id]
    if not t then
        return nil, "no transaction in progress", "state"
    end

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

    t.state = commit and S.PREPARE_COMMIT or S.PREPARE_ABORT
    local ok, err = self:_persist(txn_id)
    if not ok then return nil, err end

    return self:_finish(txn_id, commit)
end

function Coordinator:current(txn_id) return self.txns[txn_id] end

function Coordinator:has_unresolved(txn_id)
    local t = self.txns[txn_id]
    return t ~= nil
        and t.state ~= S.COMPLETE_COMMIT
        and t.state ~= S.COMPLETE_ABORT
end


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
    local rt = self.remote[key]
    if rt then
        for _, fo in pairs(rt) do
            if min == nil or fo < min then min = fo end
        end
    end
    return min
end


function Coordinator:remote_enroll(txn_id, topic, partition, first_offset)
    assert(type(txn_id) == "string" and #txn_id > 0, "txn_id required")
    local key = topic .. "\0" .. partition
    local t = self.remote[key]
    if not t then t = {}; self.remote[key] = t end
    if t[txn_id] == nil or first_offset < t[txn_id] then
        t[txn_id] = first_offset
    end
    return true
end

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

function Coordinator:is_aborted(topic, partition, pid, epoch, offset)
    return self.aborts:is_aborted(topic, partition, pid, epoch, offset)
end


-- Replay __transaction_state into memory (no side effects on other topics).
function Coordinator:_load_txns()
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
end

-- Finish every transaction the previous process left in flight.
function Coordinator:_resolve_inflight()
    local resolved = 0
    for txn_id, t in pairs(self.txns) do
        if t.state == S.ONGOING or t.state == S.PREPARE_ABORT then
            t.state = S.PREPARE_ABORT
            self:_persist(txn_id)
            local ok, err = self:_finish(txn_id, false)
            if not ok then
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
