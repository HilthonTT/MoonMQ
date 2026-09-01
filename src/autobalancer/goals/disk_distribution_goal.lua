local Base     = require("src.autobalancer.goals.abstract_resource_distribution_goal")
local Resource = require("src.autobalancer.common.resource")

return Base.define({
    name             = "DiskDistributionGoal",
    resource         = Resource.DISK,
    priority         = 30,
    detect_threshold = 100 * 1024 * 1024,
    avg_deviation    = 0.25,
    trusted_only     = true,
})
