local Base     = require("src.autobalancer.goals.abstract_resource_distribution_goal")
local Resource = require("src.autobalancer.common.resource")

-- Balances inbound (produce) byte-rate across brokers. AutoMQ's
-- NetworkInUsageDistributionGoal; soft by default.
local M = {}

function M.new(opts)
    opts = opts or {}
    return Base.new({
        name             = "NetworkInDistributionGoal",
        resource         = Resource.NW_IN,
        priority         = opts.priority or 10,
        weight           = opts.weight or 1.0,
        is_hard          = false,
        detect_threshold = opts.detect_threshold or (1024 * 1024), -- 1 MiB/s
        avg_deviation    = opts.avg_deviation or 0.2,
        trusted_only     = opts.trusted_only ~= false,           -- default true
        max_actions      = opts.max_actions,
    })
end

return M
