local config          = require("src.autobalancer.config")
local ClusterModel    = require("src.autobalancer.model.cluster_model")
local AnomalyDetector = require("src.autobalancer.detector.anomaly_detector")

-- Public entry point for MoonMQ's autobalancer, an AutoMQ-style, partition-level
-- rebalancer. Feed it metrics through the ClusterModel, then call
-- detector:run_once() on whatever cadence the host prefers to get (and
-- optionally execute) a rebalance plan.
--
--   local Autobalancer = require("src.autobalancer")
--   local ab = Autobalancer.new({
--       window = 60, percentile = 0.9,
--       goals  = { disk = false },                 -- disable a goal
--       execute = function(actions) ... end,       -- optional; dry-run if omitted
--   })
--   ab.model:register_broker("b1")
--   ab.model:register_replica("b1", topic, 1)
--   ab.model:update_replica_load("b1", topic.name, 1, Resource.NW_IN, 2e6)
--   local plan = ab.detector:run_once()
local Autobalancer = {}
Autobalancer.__index = Autobalancer

-- opts merges over config.defaults. Recognized keys: percentile, window,
-- min_valid, max_actions_per_detect, emit_metrics, execute, goals (per-goal
-- overrides / disables), metrics (override).
function Autobalancer.new(opts)
    opts = opts or {}
    local d = config.defaults

    local model = ClusterModel.new({
        percentile = opts.percentile or d.percentile,
        window     = opts.window or d.window,
        min_valid  = opts.min_valid or d.min_valid,
    })

    local goals = config.build_goals(opts.goals)

    local emit_metrics = opts.emit_metrics
    if emit_metrics == nil then emit_metrics = d.emit_metrics end

    local detector = AnomalyDetector.new(model, {
        goals                  = goals,
        max_actions_per_detect = opts.max_actions_per_detect or d.max_actions_per_detect,
        emit_metrics           = emit_metrics,
        execute                = opts.execute,
        metrics                = opts.metrics,
    })

    return setmetatable({
        model    = model,
        goals    = goals,
        detector = detector,
    }, Autobalancer)
end

-- Convenience pass-through so callers can hold just the Autobalancer handle.
function Autobalancer:run_once()
    return self.detector:run_once()
end

function Autobalancer:detect()
    return self.detector:detect()
end

-- Re-export the pieces so callers can build things by hand or type-check.
Autobalancer.config          = config
Autobalancer.ClusterModel    = ClusterModel
Autobalancer.AnomalyDetector = AnomalyDetector
Autobalancer.Resource        = require("src.autobalancer.common.resource")
Autobalancer.Action          = require("src.autobalancer.common.action")

return Autobalancer
