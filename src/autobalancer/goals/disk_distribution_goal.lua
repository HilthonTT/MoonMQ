local Base     = require("src.autobalancer.goals.abstract_resource_distribution_goal")
local Resource = require("src.autobalancer.common.resource")

-- Balances on-disk bytes across brokers, for storage-bound clusters. Soft by
-- default. Disk is slower-moving than network, so its band is wider.
local M = {}

function M.new(opts)
    opts = opts or {}
    return Base.new({
        name             = "DiskDistributionGoal",
        resource         = Resource.DISK,
        priority         = opts.priority or 30,
        weight           = opts.weight or 1.0,
        is_hard          = false,
        detect_threshold = opts.detect_threshold or (100 * 1024 * 1024), -- 100 MiB
        avg_deviation    = opts.avg_deviation or 0.25,
        trusted_only     = opts.trusted_only ~= false,                   -- default true
        max_actions      = opts.max_actions,
    })
end

return M
