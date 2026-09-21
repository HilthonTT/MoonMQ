local socket     = require("socket")
local json       = require("dkjson")
local Node       = require("src.cluster.raft.node")
local Service    = require("src.cluster.raft.service")
local Store      = require("src.cluster.raft.store")
local EpochCache = require("src.replication.epoch_cache")
local wire       = require("src.replication.wire")
local uuid       = require("src.core.uuid")
local metrics    = require("src.metrics")
local log        = require("src.log.logger").get("replication")

local STORE_FILE = "replication-raft.json"

local DEFAULT_ISR_LAG_S       = 10
local DEFAULT_ACK_TIMEOUT_S   = 5
local DEFAULT_MIN_ISR         = 1
local DEFAULT_MAX_FETCH_WAIT  = 0.5
local DEFAULT_MAX_FETCH_BYTES = 1024 * 1024
local TICK_S                  = 0.05

local Group = {}
Group.__index = Group

Group.KIND_ISR = "isr"

local function initial_state()
    return { leader = nil, epoch = 0, isr = {} }
end

local function copy_state(s)
    s = type(s) == "table" and s or initial_state()
    local isr = {}
    for _, id in ipairs(type(s.isr) == "table" and s.isr or {}) do
        if type(id) == "string" then isr[#isr + 1] = id end
    end
    return {
        leader = type(s.leader) == "string" and s.leader or nil,
        epoch  = type(s.epoch) == "number" and s.epoch or 0,
        isr    = isr,
    }
end

local function reduce(state, entry)
    local d = type(entry.data) == "table" and entry.data or {}
    if entry.kind == Node.KIND_CONTROLLER then
        if type(d.leader) == "string" and type(d.term) == "number" then
            state.leader = d.leader
            state.epoch  = d.term
            if #state.isr == 0 then state.isr = { d.leader } end
        end
    elseif entry.kind == Group.KIND_ISR then
        if type(d.isr) == "table" and d.epoch == state.epoch then
            local isr = copy_state({ isr = d.isr }).isr
            if #isr > 0 then state.isr = isr end
        end
    end
    return state
end

Group.reduce = reduce

local function contains(list, id)
    for _, v in ipairs(list) do
        if v == id then return true end
    end
    return false
end

local function tp_key(topic, partition)
    return topic .. "\0" .. tostring(partition)
end

function Group.new(opts)
    assert(type(opts) == "table", "opts must be a table")
    local id = assert(opts.id, "id required")
    assert(type(id) == "string", "id must be a string")

    local cache = opts.cache
    if not cache then
        local cerr
        cache, cerr = EpochCache.new(assert(opts.data_dir, "data_dir required"))
        if not cache then return nil, cerr end
    end

    local store = opts.store
    if not store then
        local serr
        store, serr = Store.new(opts.data_dir, STORE_FILE)
        if not store then return nil, serr end
    end

    local member_ids, addresses, client_addresses = { id }, {}, {}
    client_addresses[id] = opts.client_address
    for _, m in ipairs(opts.members or {}) do
        if m.id ~= id then
            member_ids[#member_ids + 1] = m.id
            addresses[m.id] = m.address
            client_addresses[m.id] = m.client_address
        end
    end

    local self = setmetatable({
        id               = id,
        broker           = assert(opts.broker, "broker required"),
        reactor          = assert(opts.reactor, "reactor required"),
        cache            = cache,
        members          = member_ids,
        addresses        = addresses,
        client_addresses = client_addresses,
        preferred        = opts.preferred == true,
        token            = opts.token,
        tls              = opts.tls,
        rpc_timeout      = opts.rpc_timeout or 1.0,
        isr_lag_s        = opts.isr_lag_s or DEFAULT_ISR_LAG_S,
        ack_timeout      = opts.ack_timeout or DEFAULT_ACK_TIMEOUT_S,
        min_isr          = opts.min_isr or DEFAULT_MIN_ISR,
        max_fetch_wait   = opts.max_fetch_wait or DEFAULT_MAX_FETCH_WAIT,
        max_fetch_bytes  = opts.max_fetch_bytes or DEFAULT_MAX_FETCH_BYTES,
        on_promote       = opts.on_promote,
        on_demote        = opts.on_demote,
        now              = opts.now or socket.gettime,
        state            = initial_state(),
        serving          = false,
        promoted_at      = 0,
        progress         = {},
        caught_up        = {},
        isr_pending      = nil,
        meta_version     = 0,
    }, Group)

    self.node = Node.new({
        id              = id,
        peers           = member_ids,
        store           = store,
        election_min    = opts.election_min,
        election_max    = opts.election_max,
        max_log_entries = opts.max_log_entries,
        apply = function(entry)
            reduce(self.state, entry)
            return true
        end,
        snapshot = function() return copy_state(self.state) end,
        restore  = function(snap)
            self.state = copy_state(snap)
            return true
        end,
    })

    self.service = Service.new({
        node          = self.node,
        reactor       = self.reactor,
        addresses     = addresses,
        token         = opts.token,
        tls           = opts.tls,
        heartbeat_s   = opts.heartbeat_s,
        rpc_timeout   = opts.rpc_timeout,
        commit_wait   = opts.commit_wait,
        path_prefix   = "/replication/raft",
        metric_prefix = "moonmq_replication_raft",
        can_campaign  = function() return self:eligible() end,
    })

    self.broker.on_topic_change = function(kind, name, topic)
        self:_topic_changed(kind, name, topic)
    end
    self:_set_read_only(true)

    return self
end

function Group:_set_read_only(flag)
    self:each_partition(function(_, _, p) p.read_only = flag end)
end

function Group:log_view()
    local view = copy_state(self.node.state.snapshot)
    for _, entry in ipairs(self.node.state.entries) do
        reduce(view, entry)
    end
    return view
end

function Group:eligible()
    local view = self:log_view()
    if #view.isr == 0 then return self.preferred end
    return contains(view.isr, self.id)
end

function Group:holds_leadership()
    return self.node:is_leader()
        and self.state.leader == self.id
        and self.state.epoch == self.node:term()
end

function Group:is_leader()
    return self.serving and self:holds_leadership()
end

function Group:enabled()
    return true
end

function Group:replicate() end

function Group:leader_client_address()
    local leader = self.state.leader
    if not leader or leader == self.id then return nil end
    return self.client_addresses[leader]
end

function Group:not_leader_message()
    local address = self:leader_client_address()
    if address then
        return string.format("not the leader; leader=%s", address)
    end
    return "not the leader; no leader elected yet"
end

function Group:ack_set()
    local set = {}
    for _, m in ipairs(self.state.isr) do set[#set + 1] = m end
    for _, m in ipairs(self.isr_pending or {}) do
        if not contains(set, m) then set[#set + 1] = m end
    end
    if not contains(set, self.id) then set[#set + 1] = self.id end
    return set
end

function Group:high_watermark(topic, partition)
    local t = self.broker.topic_manager.topics[topic]
    local p = t and t.partitions[partition]
    if not p then return nil end
    local hwm = p.offset
    local k = tp_key(topic, partition)
    for _, m in ipairs(self:ack_set()) do
        if m ~= self.id then
            local pr = self.progress[m]
            local off = pr and pr[k] or 0
            if off < hwm then hwm = off end
        end
    end
    return hwm
end

function Group:wait_for(topic, partition, leo)
    local k = tp_key(topic, partition)
    local deadline = self.now() + self.ack_timeout
    while true do
        if not self:is_leader() then
            return nil, "acks=all: this broker stopped being the replication leader"
        end
        local set = self:ack_set()
        if #set < self.min_isr then
            return nil, string.format(
                "acks=all: %d in-sync replica(s), MinInsyncReplicas is %d",
                #set, self.min_isr)
        end
        local reached = true
        for _, m in ipairs(set) do
            if m ~= self.id then
                local pr = self.progress[m]
                if not pr or (pr[k] or -1) < leo then
                    reached = false
                    break
                end
            end
        end
        if reached then return true end
        if self.now() > deadline then
            return nil, string.format(
                "acks=all timed out after %.1fs waiting for in-sync replicas on %s/partition-%d",
                self.ack_timeout, topic, partition)
        end
        self.reactor:sleep(0.005)
    end
end

function Group:each_partition(fn)
    for name, topic in pairs(self.broker.topic_manager.topics) do
        for id, p in ipairs(topic.partitions) do fn(name, id, p) end
    end
end

function Group:_ensure_uid(name)
    if self.cache:uid(name) then return true end
    return self.cache:set_uid(name, uuid.format(uuid.bytes()))
end

function Group:_ensure_epochs(topic, partition, p)
    if self.cache:has(topic, partition) then return true end
    return self.cache:assign(topic, partition, self.state.epoch, p:oldest_offset())
end

function Group:_topic_changed(kind, name, topic)
    self.meta_version = self.meta_version + 1
    if kind == "create" and topic then
        for _, p in ipairs(topic.partitions) do p.read_only = not self.serving end
    end
    if not self:is_leader() then return end
    if kind == "create" and topic then
        self.cache:forget_topic(name)
        self:_ensure_uid(name)
        local items = {}
        for id, p in ipairs(topic.partitions) do
            items[#items + 1] = {
                topic = name, partition = id,
                epoch = self.state.epoch, start = p:oldest_offset(),
            }
        end
        local ok, err = self.cache:assign_many(items)
        if not ok then log:error("leader epochs for new topic %s: %s", name, tostring(err)) end
    elseif kind == "delete" then
        self.cache:forget_topic(name)
    end
end

function Group:_promote()
    local epoch = self.state.epoch
    local items = {}
    self:each_partition(function(name, id, p)
        items[#items + 1] = { topic = name, partition = id, epoch = epoch, start = p.offset }
    end)
    local ok, err = self.cache:assign_many(items)
    if not ok then return nil, err end
    for name in pairs(self.broker.topic_manager.topics) do
        local uok, uerr = self:_ensure_uid(name)
        if not uok then return nil, uerr end
    end

    self:_set_read_only(false)
    if self.on_promote then
        local pok, perr = self.on_promote(epoch)
        if not pok then
            self:_set_read_only(true)
            return nil, perr
        end
    end

    local now = self.now()
    self.progress, self.caught_up = {}, {}
    for _, m in ipairs(self.state.isr) do self.caught_up[m] = now end
    self.promoted_at = now
    self.isr_pending = nil
    self.serving = true
    self.meta_version = self.meta_version + 1
    metrics.inc("moonmq_replication_leader_changes_total")
    log:info("replica %s is now the leader for epoch %d (isr=%s)",
        self.id, epoch, table.concat(self.state.isr, ","))
    return true
end

function Group:_demote()
    self.serving = false
    self:_set_read_only(true)
    self.isr_pending = nil
    self.progress, self.caught_up = {}, {}
    log:info("replica %s stepped down from leadership (epoch %d, leader now %s)",
        self.id, self.state.epoch, tostring(self.state.leader))
    if self.on_demote then
        local ok, err = pcall(self.on_demote)
        if not ok then log:error("on_demote: %s", tostring(err)) end
    end
end

function Group:_propose_isr(isr, reason)
    if self.isr_pending then return end
    self.isr_pending = isr
    local epoch = self.state.epoch
    log:info("proposing isr=%s for epoch %d (%s)", table.concat(isr, ","), epoch, reason)
    self.reactor:spawn(function()
        local index, err = self.service:commit(Group.KIND_ISR, { epoch = epoch, isr = isr })
        if not index then
            log:warn("isr change to %s not committed: %s",
                table.concat(isr, ","), tostring(err))
        end
        if self.isr_pending == isr then self.isr_pending = nil end
    end)
end

function Group:_maintain_isr()
    if self.isr_pending then return end
    local now = self.now()
    local kept, dropped = {}, {}
    for _, m in ipairs(self.state.isr) do
        if m == self.id or now - (self.caught_up[m] or self.promoted_at) <= self.isr_lag_s then
            kept[#kept + 1] = m
        else
            dropped[#dropped + 1] = m
        end
    end
    if #dropped > 0 then
        if not contains(kept, self.id) then kept[#kept + 1] = self.id end
        metrics.inc("moonmq_replication_isr_shrinks_total", #dropped)
        self:_propose_isr(kept, "replica(s) " .. table.concat(dropped, ",")
            .. " not caught up within " .. tostring(self.isr_lag_s) .. "s")
    end
end

function Group:_note_progress(member)
    local pr = self.progress[member] or {}
    local at_end, at_hwm = true, true
    self:each_partition(function(name, id, p)
        local off = pr[tp_key(name, id)]
        if not off or off < p.offset then at_end = false end
        if not off or off < self:high_watermark(name, id) then at_hwm = false end
    end)
    if at_end then self.caught_up[member] = self.now() end
    if at_hwm and not self.isr_pending and not contains(self:ack_set(), member) then
        self.caught_up[member] = self.now()
        local isr = {}
        for _, m in ipairs(self.state.isr) do isr[#isr + 1] = m end
        isr[#isr + 1] = member
        metrics.inc("moonmq_replication_isr_expands_total")
        self:_propose_isr(isr, "replica " .. member .. " caught up")
    end
end

function Group:tick()
    local want = self:holds_leadership()
    if want and not self.serving then
        if not (self.fetcher and self.fetcher.busy) then
            local ok, err = self:_promote()
            if not ok then
                log:error("promotion failed, giving up leadership: %s", tostring(err))
                self.node:become_follower(self.node:term())
            end
        end
    elseif not want and self.serving then
        self:_demote()
    end
    if self.serving then self:_maintain_isr() end

    metrics.set("moonmq_replication_is_leader", self.serving and 1 or 0)
    metrics.set("moonmq_replication_epoch", self.state.epoch)
    metrics.set("moonmq_replication_isr_size", #self.state.isr)
end

function Group:run(running)
    self.reactor:spawn(function() self.service:run(running) end)
    if self.fetcher then
        self.reactor:spawn(function() self.fetcher:run(running) end)
    end
    while running() do
        self.reactor:sleep(TICK_S)
        if not running() then break end
        local ok, err = pcall(self.tick, self)
        if not ok then log:error("replication tick failed: %s", tostring(err)) end
    end
end

function Group:digest()
    local aborts = self.broker.transactions and self.broker.transactions.aborts
    return string.format("%d:%d:%d", self.state.epoch, self.meta_version,
        aborts and aborts.version or 0)
end

function Group:_fence(req)
    if not self:is_leader() then
        return { error = "not_leader", leader = self.state.leader, epoch = self.state.epoch }
    end
    if type(req) ~= "table" or req.epoch ~= self.state.epoch then
        return { error = "epoch", leader = self.state.leader, epoch = self.state.epoch }
    end
    return nil
end

function Group:handle_manifest(req)
    local fenced = self:_fence(req)
    if fenced then return 409, fenced end

    local topics = {}
    local digest = self:digest()
    for name, topic in pairs(self.broker.topic_manager.topics) do
        self:_ensure_uid(name)
        local config = self.broker.topic_manager:config(name) or {}
        topics[#topics + 1] = {
            name       = name,
            partitions = #topic.partitions,
            uid        = self.cache:uid(name),
            config     = config,
        }
    end
    table.sort(topics, function(a, b) return a.name < b.name end)

    local aborts = self.broker.transactions and self.broker.transactions.aborts
    return 200, {
        epoch  = self.state.epoch,
        digest = digest,
        topics = topics,
        aborts = aborts and aborts:all() or {},
    }
end

function Group:handle_epochs(req)
    local fenced = self:_fence(req)
    if fenced then return 409, fenced end
    if type(req.partitions) ~= "table" then return 400, { error = "partitions required" } end

    local out = {}
    for _, q in ipairs(req.partitions) do
        if type(q) == "table" and type(q.t) == "string" and type(q.p) == "number"
           and type(q.e) == "number" then
            local topic = self.broker.topic_manager.topics[q.t]
            local p = topic and topic.partitions[q.p]
            if not p then
                out[#out + 1] = { t = q.t, p = q.p, missing = true }
            else
                self:_ensure_epochs(q.t, q.p, p)
                local e, end_offset = self.cache:end_offset_for(q.t, q.p, q.e, p.offset)
                out[#out + 1] = {
                    t = q.t, p = q.p, e = e, ["end"] = end_offset, start = p:oldest_offset(),
                }
            end
        end
    end
    return 200, { epoch = self.state.epoch, partitions = out }
end

function Group:_collect(partitions, max_bytes)
    local records, chunks, resets, errors = {}, {}, {}, {}
    local total = 0
    for _, q in ipairs(partitions) do
        if total >= max_bytes then break end
        local topic = self.broker.topic_manager.topics[q.t]
        local p = topic and topic.partitions[q.p]
        if p then
            if q.o < p:oldest_offset() then
                resets[#resets + 1] = { q.t, q.p, p:oldest_offset() }
            elseif q.o > p.offset then
                errors[#errors + 1] = { q.t, q.p, "ahead" }
            elseif q.o < p.offset then
                self:_ensure_epochs(q.t, q.p, p)
                local off = q.o
                while off < p.offset and total < max_bytes do
                    local bytes, next_offset, err, at = p:read_raw(off)
                    if not bytes then
                        errors[#errors + 1] = { q.t, q.p, tostring(err) }
                        break
                    end
                    at = at or off
                    records[#records + 1] = {
                        q.t, q.p, at, self.cache:epoch_at(q.t, q.p, at), #bytes,
                    }
                    chunks[#chunks + 1] = bytes
                    total = total + #bytes
                    off = next_offset
                end
            end
        end
    end
    return records, chunks, resets, errors
end

function Group:handle_fetch(req)
    local fenced = self:_fence(req)
    if fenced then return 409, "application/json", json.encode(fenced) end
    if type(req.replica_id) ~= "string" or req.replica_id == self.id
       or not self.addresses[req.replica_id] or type(req.partitions) ~= "table" then
        return 400, "application/json", json.encode({ error = "bad fetch request" })
    end

    local member = req.replica_id
    local wanted, pr = {}, {}
    for _, q in ipairs(req.partitions) do
        if type(q) == "table" and type(q.t) == "string" and type(q.p) == "number"
           and type(q.o) == "number" then
            wanted[#wanted + 1] = q
            pr[tp_key(q.t, q.p)] = q.o
        end
    end
    self.progress[member] = pr
    self:_note_progress(member)

    local max_bytes = math.min(tonumber(req.max_bytes) or self.max_fetch_bytes,
        self.max_fetch_bytes)
    local wait = math.min(tonumber(req.max_wait) or 0, self.max_fetch_wait)
    local deadline = self.now() + wait

    local records, chunks, resets, errors
    while true do
        records, chunks, resets, errors = self:_collect(wanted, max_bytes)
        if #records > 0 or #resets > 0 or #errors > 0 then break end
        if self.now() >= deadline or not self:is_leader() then break end
        self.reactor:sleep(0.005)
    end

    local body = wire.encode_fetch({
        epoch   = self.state.epoch,
        digest  = self:digest(),
        records = records,
        resets  = resets,
        errors  = errors,
    }, chunks)
    return 200, "application/octet-stream", body
end

function Group:handle_raft(kind, args)
    if kind == "vote" then return self.node:handle_vote(args) end
    if kind == "append" then return self.node:handle_append(args) end
    if kind == "snapshot" then return self.node:handle_snapshot(args) end
    return nil, "unknown raft route"
end

function Group:status()
    return {
        id          = self.id,
        role        = self:is_leader() and "leader" or "follower",
        leader      = self.state.leader,
        epoch       = self.state.epoch,
        isr         = self.state.isr,
        raft_term   = self.node:term(),
        raft_leader = self.node.leader_id,
    }
end

return Group
