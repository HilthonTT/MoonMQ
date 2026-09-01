local config          = require("src.autobalancer.config")
local ClusterModel    = require("src.autobalancer.model.cluster_model")
local AnomalyDetector = require("src.autobalancer.detector.anomaly_detector")

local Autobalancer = {}
Autobalancer.__index = Autobalancer

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

function Autobalancer:run_once()
    return self.detector:run_once()
end

function Autobalancer:detect()
    return self.detector:detect()
end

Autobalancer.config          = config
Autobalancer.ClusterModel    = ClusterModel
Autobalancer.AnomalyDetector = AnomalyDetector
Autobalancer.Resource        = require("src.autobalancer.common.resource")
Autobalancer.Action          = require("src.autobalancer.common.action")

return Autobalancer
