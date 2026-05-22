local M = {}

local ChaosBehavior = {
    BehaviorNetworkDelay = 0,
    BehaviorMessageLoss = 1,
    BehaviorDuplication = 2,
    BehaviorCorruption = 3,
    BehaviorReordering = 4,
    BehaviorServiceRestart = 5,
}
M.ChaosBehavior = ChaosBehavior

local ChaosProducer = {}
ChaosProducer.__index = ChaosProducer

M.ChaosBehavior = ChaosBehavior

return M