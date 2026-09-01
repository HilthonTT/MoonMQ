local constants = require("src.autobalancer.common.constants")
local Resource  = require("src.autobalancer.common.resource")
local Log       = require("src.log.logger")

local AnomalyDetector = {}
AnomalyDetector.__index = AnomalyDetector

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
        metrics                = opts.metrics,
        log                    = Log.get(constants.AUTO_BALANCER_LOGGER_CLAZZ),
        last_plan              = {},
    }, AnomalyDetector)
end

function AnomalyDetector:_metrics()
    if not self.emit_metrics then return nil end
    if self.metrics == nil then self.metrics = require("src.metrics") end
    return self.metrics
end

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

function AnomalyDetector:run_once()
    local actions, snapshot = self:detect()
    self.last_plan = actions

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
