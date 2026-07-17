-- BalanceLoop — the periodic glue between the cluster and the autobalancer:
-- feed the ClusterModel with real loads (local partitions + every peer's
-- /cluster/loads report), run one detection pass, and hand the plan to the
-- Reassigner. Run it on ONE broker per cluster (the de-facto controller);
-- running it on several would produce competing plans.
--
-- Load signals fed today: DISK (bytes on disk per partition) and
-- PARTITION_COUNT (implicit). The NW_IN/NW_OUT goals are disabled by default
-- here because no per-partition byte-rate feed exists yet — enable them via
-- opts.goals once you feed those samples yourself.

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
        -- Placeholder Topic objects for replicas that only exist on peers.
        -- Action construction needs a real Topic; the reassigner only reads
        -- its name.
        topic_refs  = {},
    }, BalanceLoop)

    local goals = opts.goals or {}
    -- Default the throughput goals OFF (no byte-rate feed yet) unless the
    -- caller configured them explicitly.
    if goals.network_in == nil then goals.network_in = false end
    if goals.network_out == nil then goals.network_out = false end

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

-- Feed one broker's load report into the model, updating DISK samples and
-- registering replicas. `loads` is an array of { topic, partition, disk_bytes }.
-- Returns the set of replica keys seen, for stale-replica cleanup.
function BalanceLoop:_feed_broker(broker_id, loads, seen)
    self.ab.model:register_broker(broker_id)
    for _, l in ipairs(loads) do
        if type(l.topic) == "string" and type(l.partition) == "number" then
            local topic = self:_topic_ref(l.topic)
            self.ab.model:register_replica(broker_id, topic, l.partition)
            self.ab.model:update_replica_load(broker_id, l.topic, l.partition,
                Resource.DISK, tonumber(l.disk_bytes) or 0)
            seen[broker_id .. "\0" .. l.topic .. "\0" .. l.partition] = true
        end
    end
end

-- Local equivalent of a peer's /cluster/loads report.
function BalanceLoop:_local_loads()
    local loads = {}
    for name, topic in pairs(self.broker.topic_manager.topics) do
        if name:sub(1, 2) ~= "__" then
            for _, p in ipairs(topic.partitions) do
                if self.assignments:owned_by_self(name, p.id) then
                    loads[#loads + 1] = {
                        topic = name, partition = p.id, disk_bytes = p.offset or 0,
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
end

-- One full pass: feed → detect → execute. Peer-report failures degrade to
-- marking that broker inactive for this pass (a balancer must never move data
-- TOWARD a broker it can't reach — and with the broker inactive, goals won't).
function BalanceLoop:tick()
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
