local json    = require("dkjson")
local rpc     = require("src.cluster.raft.rpc")
local wire    = require("src.replication.wire")
local msg_m   = require("src.record.message")
local metrics = require("src.metrics")
local log     = require("src.log.logger").get("replication")

local IDLE_S                = 0.05
local RETRY_S               = 0.25
local MAX_TRUNCATION_ROUNDS = 16

local LIVE_FIELDS = {
    max_segment_size = true,
    retention        = true,
    cleaner_interval = true,
}

local Fetcher = {}
Fetcher.__index = Fetcher

local function is_internal(name)
    return name:sub(1, 2) == "__"
end

local function canonical(config)
    local keys = {}
    for k in pairs(config) do keys[#keys + 1] = k end
    table.sort(keys)
    local parts = {}
    for _, k in ipairs(keys) do parts[#parts + 1] = k .. "=" .. tostring(config[k]) end
    return table.concat(parts, ";")
end

local function decode_record(bytes)
    if #bytes < 8 then return nil, "record shorter than its length prefix" end
    local total = string.unpack(">I8", bytes)
    if total ~= #bytes - 8 then return nil, "record length prefix does not match its size" end
    return msg_m.decode_body(bytes:sub(9))
end

function Fetcher.new(group, opts)
    opts = opts or {}
    local self = setmetatable({
        group     = group,
        reactor   = group.reactor,
        broker    = group.broker,
        cache     = group.cache,
        max_wait  = opts.max_wait or group.max_fetch_wait,
        max_bytes = opts.max_bytes or group.max_fetch_bytes,
        epoch     = nil,
        leader    = nil,
        digest    = nil,
        truncated = false,
        busy      = false,
    }, Fetcher)
    group.fetcher = self
    return self
end

function Fetcher:_partition(topic, partition)
    local t = self.broker.topic_manager.topics[topic]
    return t and t.partitions[partition]
end

function Fetcher:_post(path, payload, raw)
    local g = self.group
    return rpc.post(self.reactor, {
        address = g.addresses[self.leader],
        path    = path,
        body    = json.encode(payload),
        token   = g.token,
        tls     = g.tls,
        timeout = g.rpc_timeout + (raw and self.max_wait or 0),
        raw     = raw,
    })
end

function Fetcher:_current()
    local g = self.group
    return not g.serving and not g:holds_leadership()
        and g.state.leader == self.leader and g.state.epoch == self.epoch
end

function Fetcher:step()
    local g = self.group
    local st = g.state
    if g.serving or g:holds_leadership() or not st.leader or st.leader == g.id
       or not g.addresses[st.leader] then
        self.epoch = nil
        self.reactor:sleep(IDLE_S)
        return
    end

    if st.epoch ~= self.epoch or st.leader ~= self.leader then
        self.epoch, self.leader = st.epoch, st.leader
        self.digest, self.truncated = nil, false
        log:info("replica %s following %s for epoch %d", g.id, self.leader, self.epoch)
    end

    if not self.digest and not self:_sync_manifest() then
        self.reactor:sleep(RETRY_S)
        return
    end
    if not self.truncated and not self:_truncate() then
        self.reactor:sleep(RETRY_S)
        return
    end
    if not self:_fetch() then
        self.reactor:sleep(RETRY_S)
    end
end

function Fetcher:run(running)
    while running() do
        local ok, err = pcall(self.step, self)
        self.busy = false
        if not ok then
            log:error("replica fetch loop: %s", tostring(err))
            self.reactor:sleep(RETRY_S)
        end
    end
end

function Fetcher:_sync_manifest()
    local epoch = self.epoch
    local resp, err = self:_post("/replication/manifest", { epoch = epoch })
    if not resp then
        log:debug("manifest from %s: %s", tostring(self.leader), tostring(err))
        return false
    end
    if not self:_current() or resp.epoch ~= epoch then return false end

    self.busy = true
    local ok, aerr = self:_apply_manifest(resp)
    self.busy = false
    if not ok then
        log:error("applying the leader's topic manifest: %s", tostring(aerr))
        return false
    end
    self.digest = resp.digest
    return true
end

function Fetcher:_drop_topic(name)
    local ok, err = self.broker.topic_manager:delete_topic(name)
    if not ok then return nil, err end
    self.cache:forget_topic(name)
    log:info("dropped topic %s to match the leader", name)
    return true
end

function Fetcher:_sync_config(name, topic, config)
    local tm = self.broker.topic_manager
    local current = tm:config(name) or {}
    if canonical(current) == canonical(config) then return true end
    local ok, err = tm:set_config(name, config)
    if not ok then return nil, err end
    for key, value in pairs(config) do
        if LIVE_FIELDS[key] then
            for _, p in ipairs(topic.partitions) do
                if p[key] ~= nil then p[key] = value end
            end
        end
    end
    return true
end

function Fetcher:_apply_manifest(resp)
    local tm = self.broker.topic_manager
    local wanted = {}
    for _, t in ipairs(type(resp.topics) == "table" and resp.topics or {}) do
        if type(t) == "table" and type(t.name) == "string"
           and type(t.partitions) == "number" and t.partitions >= 1 then
            wanted[t.name] = t
        end
    end

    local names = {}
    for name in pairs(tm.topics) do names[#names + 1] = name end
    for _, name in ipairs(names) do
        if not wanted[name] and not is_internal(name) then
            local ok, err = self:_drop_topic(name)
            if not ok then return nil, err end
        end
    end

    for name, t in pairs(wanted) do
        local topic = tm.topics[name]
        local config = type(t.config) == "table" and t.config or {}
        local local_uid = self.cache:uid(name)
        if topic and #topic.partitions ~= t.partitions then
            if is_internal(name) then
                return nil, string.format(
                    "internal topic %s has %d partition(s) here and %d on the leader",
                    name, #topic.partitions, t.partitions)
            end
            local ok, err = self:_drop_topic(name)
            if not ok then return nil, err end
            topic = nil
        elseif topic and local_uid and type(t.uid) == "string" and local_uid ~= t.uid
               and not is_internal(name) then
            local ok, err = self:_drop_topic(name)
            if not ok then return nil, err end
            topic = nil
        end

        if not topic then
            local created, cerr = self.broker:create_topic(name, t.partitions,
                next(config) ~= nil and config or nil)
            if not created then
                return nil, string.format("create topic %s: %s", name, tostring(cerr))
            end
            log:info("created topic %s (%d partitions) to match the leader", name, t.partitions)
        else
            local ok, err = self:_sync_config(name, topic, config)
            if not ok then return nil, err end
        end

        if type(t.uid) == "string" then
            local ok, err = self.cache:set_uid(name, t.uid)
            if not ok then return nil, err end
        end
    end

    local aborts = self.broker.transactions and self.broker.transactions.aborts
    if aborts and type(resp.aborts) == "table" then
        local ok, err = aborts:replace(resp.aborts)
        if not ok then return nil, err end
    end
    return true
end

function Fetcher:_truncate_partition(r)
    local p = self:_partition(r.t, r.p)
    if not p then return false end

    if not self.cache:has(r.t, r.p) then
        if p.offset > p:oldest_offset() and type(r.start) == "number" then
            local ok, err = p:reset_to(r.start)
            if not ok then return nil, err end
            log:warn("%s/partition-%d had records without leader epochs; "
                .. "discarded them to copy the leader's log from %d", r.t, r.p, r.start)
        end
        return false
    end

    local latest = self.cache:latest(r.t, r.p)
    local target, again
    if r.e == latest then
        target = math.min(r["end"], p.offset)
        again = false
    else
        local _, own_end = self.cache:end_offset_for(r.t, r.p, r.e, p.offset)
        target = math.min(r["end"], own_end)
        again = true
    end

    if target < p.offset then
        log:info("truncating %s/partition-%d from %d to %d (leader epoch %d ends at %d)",
            r.t, r.p, p.offset, target, r.e, r["end"])
        local ok, err = p:truncate_to(target)
        if not ok then return nil, err end
        metrics.inc("moonmq_replication_truncations_total")
    end
    local ok, err = self.cache:truncate_from(r.t, r.p, target)
    if not ok then return nil, err end
    if again then
        local e = self.cache:latest(r.t, r.p)
        again = self.cache:has(r.t, r.p) and e > r.e
    end
    return again
end

function Fetcher:_truncate()
    local epoch = self.epoch
    for _ = 1, MAX_TRUNCATION_ROUNDS do
        local queries = {}
        self.group:each_partition(function(name, id)
            queries[#queries + 1] = { t = name, p = id, e = (self.cache:latest(name, id)) }
        end)

        local resp, err = self:_post("/replication/epochs",
            { epoch = epoch, partitions = queries })
        if not resp then
            log:debug("leader epochs from %s: %s", tostring(self.leader), tostring(err))
            return false
        end
        if not self:_current() or resp.epoch ~= epoch then return false end

        self.busy = true
        local again = false
        for _, r in ipairs(type(resp.partitions) == "table" and resp.partitions or {}) do
            if type(r) == "table" and not r.missing and type(r.t) == "string"
               and type(r.p) == "number" and type(r.e) == "number"
               and type(r["end"]) == "number" then
                local more, terr = self:_truncate_partition(r)
                if more == nil then
                    self.busy = false
                    log:error("truncating %s/partition-%d: %s", r.t, r.p, tostring(terr))
                    return false
                end
                if more then again = true end
            end
        end
        self.busy = false

        if not again then
            self.truncated = true
            return true
        end
    end
    log:error("log truncation against %s did not converge", tostring(self.leader))
    return false
end

function Fetcher:_fetch()
    local g = self.group
    local epoch = self.epoch
    local parts = {}
    g:each_partition(function(name, id, p)
        parts[#parts + 1] = { t = name, p = id, o = p.offset }
    end)

    local body, err, code = self:_post("/replication/fetch", {
        replica_id = g.id,
        epoch      = epoch,
        max_wait   = self.max_wait,
        max_bytes  = self.max_bytes,
        partitions = parts,
    }, true)
    if not body then
        if code ~= 409 then
            log:debug("fetch from %s: %s", tostring(self.leader), tostring(err))
        end
        return false
    end

    local resp, derr = wire.decode_fetch(body)
    if not resp then
        log:error("fetch response from %s: %s", tostring(self.leader), tostring(derr))
        return false
    end
    if not self:_current() or resp.epoch ~= epoch then return true end

    self.busy = true
    local ok = self:_apply_fetch(resp)
    self.busy = false
    if resp.digest ~= self.digest then self.digest = nil end
    return ok
end

function Fetcher:_apply_fetch(resp)
    for _, r in ipairs(resp.resets) do
        local p = self:_partition(r.topic, r.partition)
        if p then
            local ok, err = p:reset_to(r.start)
            if not ok then
                log:error("resetting %s/partition-%d to %d: %s",
                    r.topic, r.partition, r.start, tostring(err))
                self.truncated = false
                return false
            end
            self.cache:clear(r.topic, r.partition)
            log:warn("%s/partition-%d fell behind the leader's retention; restarting it at %d",
                r.topic, r.partition, r.start)
        end
    end

    local touched, broken, applied = {}, {}, 0
    for _, r in ipairs(resp.records) do
        local k = r.topic .. "\0" .. tostring(r.partition)
        local p = self:_partition(r.topic, r.partition)
        if p and not broken[k] then
            local msg, err = decode_record(r.bytes)
            local ok = msg ~= nil
            if ok and (not self.cache:has(r.topic, r.partition)
                       or r.epoch > self.cache:latest(r.topic, r.partition)) then
                ok, err = self.cache:assign(r.topic, r.partition, r.epoch, r.offset)
            end
            if ok then
                ok, err = p:append_raw(r.offset, r.bytes, msg.timestamp)
            end
            if ok then
                touched[k] = p
                applied = applied + 1
            else
                broken[k] = true
                self.truncated = false
                log:error("replica append %s/partition-%d at %d: %s",
                    r.topic, r.partition, r.offset, tostring(err))
            end
        end
    end

    for _, p in pairs(touched) do
        local sok, serr = p:request_sync()
        if not sok then
            log:error("fsync after replica append on %s/partition-%d: %s",
                p.topic and p.topic.name or "?", p.id or -1, tostring(serr))
            self.truncated = false
        end
    end

    for _, e in ipairs(resp.errors) do
        if e.reason == "ahead" then
            self.truncated = false
        else
            log:warn("leader could not serve %s/partition-%d: %s",
                e.topic, e.partition, e.reason)
        end
    end

    if applied > 0 then
        metrics.inc("moonmq_replication_fetched_records_total", applied)
    end
    return next(broken) == nil
end

return Fetcher
