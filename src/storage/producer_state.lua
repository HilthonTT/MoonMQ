local msg_m = require("src.record.message")
local log   = require("src.log.logger").get("producer_state")
local hash_m = require("src.core.hash")
local time_m = require("src.core.time")

local STATE_TOPIC = "__producer_state"
local DEFAULT_PARTITIONS = 16

local STATE_BACKEND          = "commitlog"
local STATE_MAX_SEGMENT_SIZE = 8 * 1024 * 1024

local K_IDENTITY = "\1"
local K_MEMO     = "\2"
local K_ALLOC    = "\3"
local ALLOC_KEY  = K_ALLOC .. "next_pid"
local ALLOC_VALUE_FMT = ">I8"

local TOMBSTONE = ""

local IDENTITY_VALUE_FMT = ">I8I2"
local MEMO_VALUE_FMT     = ">I4I8I4I2"
local MEMO_VALUE_FMT_V1  = ">I4I8I4"

local MEMO_BATCH_HDR_FMT = ">I4I4"
local MEMO_ACK_FMT       = ">I4I8"
local MAX_MEMO_ACKS      = 1024

local MAX_EPOCH = 0xFFFF

local ProducerStateManager = {}
ProducerStateManager.__index = ProducerStateManager

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
        by_name     = {},
        epoch_by_pid= {},
        memo        = {},
        last_active = {},
        next_pid    = 1,
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
    return (hash_m.fnv1a(key) % self.nparts) + 1
end

function ProducerStateManager:_write(key, value, now_ms)
    local rec  = msg_m.Message.new(key, value, now_ms or time_m.now_ms())
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

    local now = time_m.now_ms()
    local ok, err = self:_write(K_IDENTITY .. name,
        string.pack(IDENTITY_VALUE_FMT, pid, epoch), now)
    if not ok then return nil, nil, err end

    self.by_name[name]      = { pid = pid, epoch = epoch }
    self.epoch_by_pid[pid]  = epoch
    self.last_active[pid]   = now
    return pid, epoch, nil
end

function ProducerStateManager:allocate_ephemeral()
    local pid = self.next_pid
    self.next_pid = self.next_pid + 1
    return pid
end

function ProducerStateManager:current_epoch(pid)
    return self.epoch_by_pid[pid]
end

function ProducerStateManager:pid_for(name)
    local e = self.by_name[name]
    return e and e.pid or nil
end

function ProducerStateManager:lookup_memo(pid, topic)
    local t = self.memo[pid]
    if not t then return nil end
    return t[topic]
end

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
    local now = time_m.now_ms()
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

function ProducerStateManager:expire_idle(max_idle_ms, opts)
    assert(type(max_idle_ms) == "number" and max_idle_ms > 0,
        "max_idle_ms must be a positive number")
    opts = opts or {}
    local now       = opts.now_ms or time_m.now_ms()
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

function ProducerStateManager:recover()
    local max_pid = 0
    local alloc_floor = 0
    local restored = 0

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
                if #key >= 1 + 8 then
                    local pid = string.unpack(">I8", key, 2)
                    local topic = key:sub(1 + 8 + 1)
                    if #m.value == 0 then
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
                        ok, s, off, prt = pcall(string.unpack, MEMO_VALUE_FMT_V1, m.value)
                        ep, vpos = 0, nil
                    end
                    if ok then
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
