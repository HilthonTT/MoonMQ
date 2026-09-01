local NetworkInGoal      = require("src.autobalancer.goals.network_in_distribution_goal")
local NetworkOutGoal     = require("src.autobalancer.goals.network_out_distribution_goal")
local DiskGoal           = require("src.autobalancer.goals.disk_distribution_goal")
local PartitionCountGoal = require("src.autobalancer.goals.partition_count_distribution_goal")

local M = {}

local GOAL_SPECS = {
    { key = "network_in",      module = NetworkInGoal,      default = true },
    { key = "network_out",     module = NetworkOutGoal,     default = true },
    { key = "disk",            module = DiskGoal,           default = true },
    { key = "partition_count", module = PartitionCountGoal, default = true },
}

M.defaults = {
    percentile = 0.9,
    window     = 60,
    min_valid  = 3,

    max_actions_per_detect = 50,
    emit_metrics           = true,

    goals = {},
}

function M.build_goals(goals_cfg)
    goals_cfg = goals_cfg or {}
    local goals = {}
    for _, spec in ipairs(GOAL_SPECS) do
        local cfg = goals_cfg[spec.key]
        local enabled = spec.default
        if cfg == false then
            enabled = false
        elseif cfg ~= nil then
            enabled = true
        end
        if enabled then
            goals[#goals + 1] = spec.module.new(type(cfg) == "table" and cfg or nil)
        end
    end
    table.sort(goals, function(a, b) return a.priority < b.priority end)
    return goals
end

M.GOAL_SPECS = GOAL_SPECS
return M
