local constants = require("src.autobalancer.common.constants")
local Resource  = require("src.autobalancer.common.resource")
local Log       = require("src.log.logger")

-- The AnomalyDetector runs one detection pass: freeze the live ClusterModel,
-- run each goal in priority order against the shared snapshot (so a later goal
-- sees the moves an earlier one made), collect the resulting Actions into a
-- rebalance plan, and optionally hand that plan to an executor. Mirrors
-- AutoMQ's AnomalyDetector, minus the built-in scheduler: the host drives
-- run_once() from its own loop (e.g. a reactor coroutine), keeping this module
-- free of any I/O or timing assumptions.
local AnomalyDetector = {}
AnomalyDetector.__index = AnomalyDetector

-- opts:
--   goals                   ordered goal list (required; see config.build_goals)
--   max_actions_per_detect  hard cap on plan size (default: unlimited)
--   emit_metrics            publish gauges/counters to src/metrics (default true)
--   execute                 optional fn(actions) -> ok, err; run per pass with
--                           the plan. Absent = dry-run (plan only).
--   metrics                 metrics module override (mostly for tests)
function AnomalyDetector.new(cluster_model, opts)
    assert(cluster_model, "cluster_model required")
    opts = opts or {}
    assert(type(opts.goals) == "table" and #opts.goals > 0,
        "at least one goal is required")

    local emit = opts.emit_metrics
    if emit == nil then emit = true end

    return setmetatable({
        cluster_model          = cluster_model,
        goals                  = opts.goals,
        max_actions_per_detect = opts.max_actions_per_detect,
        emit_metrics           = emit,
        execute                = opts.execute,
        metrics                = opts.metrics,   -- lazily required if nil
        log                    = Log.get(constants.AUTO_BALANCER_LOGGER_CLAZZ),
        last_plan              = {},
    }, AnomalyDetector)
end

function AnomalyDetector:_metrics()
    if not self.emit_metrics then return nil end
    if self.metrics == nil then self.metrics = require("src.metrics") end
    return self.metrics
end

-- Compute the rebalance plan without executing it. Returns (actions, snapshot)
-- so callers can inspect the post-plan cluster state the goals converged on.
--
-- Goals apply their accepted actions to the shared snapshot as they generate
-- them, so when the plan cap truncates a goal's output the EXCESS actions must
-- be undone in the snapshot — otherwise later goals (and the published
-- metrics) would reason from placements the executed plan never creates.
function AnomalyDetector:detect()
    local snapshot = self.cluster_model:snapshot()
    local actions = {}
    local cap = self.max_actions_per_detect

    for _, goal in ipairs(self.goals) do
        local produced = goal:optimize(snapshot, self.goals)
        local room = cap and (cap - #actions) or #produced
        for i = 1, math.min(#produced, room) do
            actions[#actions + 1] = produced[i]
        end
        if #produced > room then
            -- Revert the dropped tail, newest first, so the snapshot matches
            -- exactly the actions that made it into the plan.
            for i = #produced, room + 1, -1 do
                local undone, uerr = snapshot:apply_action(produced[i]:undo())
                if not undone then
                    self.log:error("failed to revert over-cap action %s: %s",
                        produced[i]:pretty(), tostring(uerr))
                end
            end
            self.log:warn("plan hit max_actions_per_detect=%d; %d action(s) dropped",
                cap, #produced - room)
            break
        end
    end

    return actions, snapshot
end

local function count_by_type(actions)
    local moves, swaps = 0, 0
    for _, a in ipairs(actions) do
        if a.action_type == 1 then moves = moves + 1 else swaps = swaps + 1 end
    end
    return moves, swaps
end

function AnomalyDetector:_publish(snapshot, actions)
    local m = self:_metrics()
    if not m then return end

    for _, id in ipairs(snapshot:active_broker_ids()) do
        for _, r in ipairs(Resource.VALUES) do
            m.set("moonmq_autobalancer_broker_load",
                snapshot:broker_load(id, r),
                { broker = id, resource = Resource.name(r) })
        end
    end
    for _, goal in ipairs(self.goals) do
        if goal.distribution_stats then
            local mean, stddev = goal:distribution_stats(snapshot)
            m.set("moonmq_autobalancer_load_mean", mean, { goal = goal.name })
            m.set("moonmq_autobalancer_load_stddev", stddev, { goal = goal.name })
        end
    end
    local moves, swaps = count_by_type(actions)
    m.set("moonmq_autobalancer_last_plan_size", #actions)
    if moves > 0 then m.inc("moonmq_autobalancer_actions_total", moves, { type = "MOVE" }) end
    if swaps > 0 then m.inc("moonmq_autobalancer_actions_total", swaps, { type = "SWAP" }) end
end

-- One full pass: detect -> publish metrics -> execute (if wired). Returns
-- (actions, err). A dry-run (no execute hook) returns the plan with err = nil.
function AnomalyDetector:run_once()
    local actions, snapshot = self:detect()
    self.last_plan = actions

    -- Note: metrics reflect the POST-plan snapshot (goals mutate it in place),
    -- i.e. the balanced state the plan is designed to reach.
    self:_publish(snapshot, actions)

    if #actions == 0 then
        self.log:debug("detection pass: cluster balanced, no actions")
        return actions, nil
    end

    local moves, swaps = count_by_type(actions)
    self.log:info("detection pass produced %d action(s): %d MOVE, %d SWAP",
        #actions, moves, swaps)
    for _, a in ipairs(actions) do
        self.log:debug("  plan: %s", a:pretty())
    end

    if self.execute then
        local ok, err = self.execute(actions)
        if not ok then
            self.log:error("plan execution failed: %s", tostring(err))
            return actions, err
        end
        self.log:info("plan executed: %d action(s) applied", #actions)
    end

    return actions, nil
end

return AnomalyDetector
