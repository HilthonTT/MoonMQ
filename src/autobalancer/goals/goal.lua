local Goal = {}
Goal.__index = Goal

function Goal.new(fields)
    local self = setmetatable(fields or {}, Goal)
    self.name     = self.name or "goal"
    self.priority = self.priority or 100
    self.weight   = self.weight or 1.0
    if self.is_hard == nil then self.is_hard = false end
    return self
end

function Goal.derive(mt)
    mt.__index = mt
    setmetatable(mt, { __index = Goal })
    return mt
end

function Goal:optimize(_cluster, _goals_by_priority)
    error("optimize not implemented for goal " .. tostring(self.name))
end

function Goal:action_acceptance_score(_action, _cluster)
    return 1.0
end

function Goal.hard_goals_accept(action, cluster, goals_by_priority, proposer)
    local ok, err = cluster:apply_action(action)
    if not ok then return false, err end

    local accepted = true
    for _, g in ipairs(goals_by_priority) do
        if g.is_hard and g ~= proposer then
            if g:action_acceptance_score(action, cluster) <= 0 then
                accepted = false
                break
            end
        end
    end

    local undone, uerr = cluster:apply_action(action:undo())
    assert(undone, "hard-goal probe could not be reverted: " .. tostring(uerr))
    return accepted
end

return Goal
