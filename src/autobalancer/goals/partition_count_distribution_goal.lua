local Base     = require("src.autobalancer.goals.abstract_resource_distribution_goal")
local Resource = require("src.autobalancer.common.resource")

return Base.define({
    name             = "PartitionCountDistributionGoal",
    resource         = Resource.PARTITION_COUNT,
    priority         = 40,
    detect_threshold = 0,
    avg_deviation    = 0.2,
    trusted_only     = false,
})
