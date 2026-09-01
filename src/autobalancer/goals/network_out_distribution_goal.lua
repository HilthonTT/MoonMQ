local Base     = require("src.autobalancer.goals.abstract_resource_distribution_goal")
local Resource = require("src.autobalancer.common.resource")

return Base.define({
    name             = "NetworkOutDistributionGoal",
    resource         = Resource.NW_OUT,
    priority         = 20,
    detect_threshold = 1024 * 1024,
    avg_deviation    = 0.2,
    trusted_only     = true,
})
