local Base     = require("src.autobalancer.goals.abstract_resource_distribution_goal")
local Resource = require("src.autobalancer.common.resource")

-- Balances the number of replicas per broker. Unlike the throughput goals this
-- needs no metrics feed (each replica counts as 1, always trusted), so it's the
-- robust baseline that works even when the cluster is idle or metrics are
-- missing. Lowest priority so it only smooths out what the load-aware goals
-- leave behind.
local M = {}

function M.new(opts)
    opts = opts or {}
    return Base.new({
        name             = "PartitionCountDistributionGoal",
        resource         = Resource.PARTITION_COUNT,
        priority         = opts.priority or 40,
        weight           = opts.weight or 1.0,
        is_hard          = false,
        detect_threshold = opts.detect_threshold or 0,   -- act on any imbalance
        avg_deviation    = opts.avg_deviation or 0.2,
        trusted_only     = false,                        -- counts are always trusted
        max_actions      = opts.max_actions,
    })
end

return M
