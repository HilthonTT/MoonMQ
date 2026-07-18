-- BalanceLoop — the periodic glue between the cluster and the autobalancer:
-- feed the ClusterModel with real loads (local partitions + every peer's
-- /cluster/loads report), run one detection pass, and hand the plan to the
-- Reassigner. Run it on ONE broker per cluster; with a fence configured a
-- second loop is refused by peers instead of executing competing plans.
--
-- Load signals fed: DISK (bytes on disk per partition), PARTITION_COUNT
-- (implicit), and NW_IN/NW_OUT byte rates — computed by differencing the
-- cumulative per-partition byte counters each loads report carries against the
-- previous pass (so network samples start flowing from the second pass; a
-- counter reset after a broker restart is skipped, not fed as a negative
-- rate).

local socket       = require("socket")
local Autobalancer = require("src.autobalancer")
local Resource     = require("src.autobalancer.common.resource")
local Topic        = require("src.storage.topic")
local log          = require("src.log.logger").get("balance_loop")

local DEFAULT_INTERVAL_S = 60

local BalanceLoop = {}
BalanceLoop.__index = BalanceLoop

-- opts:
--   broker, assignments, peers, self_id   the cluster pieces
--   reassigner   executes the plan (its execute() is the detector hook)
--   interval_s   seconds between passes (default 60)
--   dry_run      when true, plans are logged but never executed
--   fence        a ControllerFence. When present, the loop claims a controller
--                epoch before its first pass, re-announces it to peers every
--                pass, and STOPS acting the moment any peer reports a newer
--                claim — the fix for "two brokers both running the loop".
--                nil = legacy unfenced behaviour.
--   Plus any Autobalancer.new opts (goals, window, percentile, ...).
function BalanceLoop.new(opts)
    local self = setmetatable({
        broker      = assert(opts.broker, "broker required"),
        assignments = assert(opts.assignments, "assignments required"),
        peers       = assert(opts.peers, "peers required"),
        self_id     = assert(opts.self_id, "self_id required"),
        reassigner  = opts.reassigner,
        interval_s  = opts.interval_s or DEFAULT_INTERVAL_S,
        dry_run     = opts.dry_run or false,
        fence       = opts.fence,
        controller_epoch = nil,
        fenced      = false,
        -- Placeholder Topic objects for replicas that only exist on peers.
        -- Action construction needs a real Topic; the reassigner only reads
        -- its name.
        topic_refs  = {},
        -- Previous pass's cumulative byte counters per replica, for the
        -- NW_IN/NW_OUT rate computation: skey -> { at, bin, bout }.
        prev_totals = {},
    }, BalanceLoop)

    local goals = opts.goals or {}

    self.ab = Autobalancer.new({
        goals                  = goals,
        window                 = opts.window,
        min_valid              = opts.min_valid or 1,
        percentile             = opts.percentile,
        max_actions_per_detect = opts.max_actions_per_detect,
        emit_metrics           = opts.emit_metrics,
        execute                = (not self.dry_run and self.reassigner) and function(actions)
            return self.reassigner:execute(actions)
        end or nil,
    })
    return self
end

function BalanceLoop:_topic_ref(name)
    local t = self.topic_refs[name]
    if not t then
        t = Topic.new(name)
        self.topic_refs[name] = t
    end
    return t
end

-- Feed one broker's load report into the model, updating DISK samples,
-- registering replicas, and — when the report carries the cumulative byte
-- counters — turning them into NW_IN/NW_OUT rates against the previous pass.
-- `loads` is an array of { topic, partition, disk_bytes[, bytes_in_total,
-- bytes_out_total] }. Returns the set of replica keys seen, for stale-replica
-- cleanup.
function BalanceLoop:_feed_broker(broker_id, loads, seen)
    self.ab.model:register_broker(broker_id)
    local now = socket.gettime()
    for _, l in ipairs(loads) do
        if type(l.topic) == "string" and type(l.partition) == "number" then
            local topic = self:_topic_ref(l.topic)
            self.ab.model:register_replica(broker_id, topic, l.partition)
            self.ab.model:update_replica_load(broker_id, l.topic, l.partition,
                Resource.DISK, tonumber(l.disk_bytes) or 0)
            local skey = broker_id .. "\0" .. l.topic .. "\0" .. l.partition
            seen[skey] = true

            -- Byte rates: difference the cumulative counters against the last
            -- pass. First sighting (or a counter reset — restart, or the
            -- partition moved brokers) primes the baseline without feeding a
            -- sample.
            local bin, bout = tonumber(l.bytes_in_total), tonumber(l.bytes_out_total)
            if bin ~= nil and bout ~= nil then
                local prev = self.prev_totals[skey]
                if prev and now > prev.at and bin >= prev.bin and bout >= prev.bout then
                    local dt = now - prev.at
                    self.ab.model:update_replica_load(broker_id, l.topic, l.partition,
                        Resource.NW_IN, (bin - prev.bin) / dt)
                    self.ab.model:update_replica_load(broker_id, l.topic, l.partition,
                        Resource.NW_OUT, (bout - prev.bout) / dt)
                end
                self.prev_totals[skey] = { at = now, bin = bin, bout = bout }
            end
        end
    end
end

-- Local equivalent of a peer's /cluster/loads report.
function BalanceLoop:_local_loads()
    local loads = {}
    local traffic = self.broker.traffic
    for name, topic in pairs(self.broker.topic_manager.topics) do
        if name:sub(1, 2) ~= "__" then
            for _, p in ipairs(topic.partitions) do
                if self.assignments:owned_by_self(name, p.id) then
                    local bin, bout = 0, 0
                    if traffic then bin, bout = traffic:totals(name, p.id) end
                    loads[#loads + 1] = {
                        topic = name, partition = p.id, disk_bytes = p.offset or 0,
                        bytes_in_total = bin, bytes_out_total = bout,
                    }
                end
            end
        end
    end
    return loads
end

-- Drop replicas the model knows but no report mentioned this pass (moved or
-- deleted since the last tick), so the snapshot doesn't balance ghosts.
function BalanceLoop:_prune(seen)
    local model = self.ab.model
    for broker_id, bucket in pairs(model.replicas) do
        for key, updater in pairs(bucket) do
            local skey = broker_id .. "\0" .. updater.topic.name .. "\0" .. updater.partition
            if not seen[skey] then
                bucket[key] = nil
            end
        end
    end
    -- Forget rate baselines for replicas that vanished too, so a partition
    -- that later reappears (or moves back) re-primes instead of producing a
    -- bogus delta.
    for skey in pairs(self.prev_totals) do
        if not seen[skey] then self.prev_totals[skey] = nil end
    end
end

-- Claim (first pass) and re-announce (every pass) this broker's controller
-- epoch. Returns true when this loop may act; (nil, err) when superseded —
-- permanently: a fenced controller stays quiet until an operator restarts it,
-- rather than reclaiming a higher epoch and duelling forever. An unreachable
-- peer doesn't block the claim (it is inactive this pass; the fence catches up
-- the moment it answers a mutating request or a later claim).
function BalanceLoop:_ensure_controller()
    if not self.fence then return true end
    if self.fenced then return nil, "controller superseded; not acting" end

    if not self.controller_epoch then
        local epoch, cerr = self.fence:claim(self.self_id)
        if not epoch then return nil, cerr end
        self.controller_epoch = epoch
        for _, peer in pairs(self.peers) do
            if peer.set_controller then
                peer:set_controller(epoch, self.self_id)
            end
        end
        log:info("claimed controller epoch %d", epoch)
    end

    for id, peer in pairs(self.peers) do
        if peer.claim_controller then
            local accepted, highest, reason =
                peer:claim_controller(self.controller_epoch, self.self_id)
            if accepted == false then
                self.fenced = true
                log:error("fenced: peer %s holds controller epoch %s (%s); "
                    .. "this balance loop stops acting",
                    id, tostring(highest), tostring(reason))
                return nil, "controller superseded; not acting"
            end
        end
    end
    return true
end

-- One full pass: feed → detect → execute. Peer-report failures degrade to
-- marking that broker inactive for this pass (a balancer must never move data
-- TOWARD a broker it can't reach — and with the broker inactive, goals won't).
function BalanceLoop:tick()
    local cok, cerr = self:_ensure_controller()
    if not cok then return {}, cerr end

    local seen = {}
    self:_feed_broker(self.self_id, self:_local_loads(), seen)

    for id, peer in pairs(self.peers) do
        local loads, err = peer:loads()
        if loads then
            self.ab.model:register_broker(id, { active = true })
            self:_feed_broker(id, loads, seen)
        else
            log:warn("peer %s unreachable, excluded from this pass: %s", id, err)
            self.ab.model:register_broker(id)
            self.ab.model:set_broker_active(id, false)
        end
    end

    self:_prune(seen)

    local actions, err = self.ab:run_once()
    if self.dry_run and #actions > 0 then
        for _, a in ipairs(actions) do log:info("dry-run plan: %s", a:pretty()) end
    end
    return actions, err
end

-- Long-running loop for the reactor. `running` is polled so the Server can
-- stop it at shutdown the same way it stops the reaper/cleaner loops.
function BalanceLoop:run(reactor, running)
    while running() do
        reactor:sleep(self.interval_s)
        if not running() then return end
        local ok, err = pcall(self.tick, self)
        if not ok then
            log:error("balance tick failed: %s", tostring(err))
        end
    end
end

return BalanceLoop
