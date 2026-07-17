-- Base for all balancing goals, mirroring AutoMQ's Goal interface. A goal has:
--   name      human/metric label
--   priority  lower runs first (goals mutate a shared snapshot in order)
--   is_hard   hard goals may VETO another goal's action; soft goals only nudge
--   weight    relative importance (informational / for future weighting)
--
-- Subclasses override:
--   optimize(cluster, goals_by_priority) -> array of Actions (applied in place)
--   action_acceptance_score(action, cluster) -> number in [0, 1]
--       0 means "reject" (only meaningful for hard goals); the score is read
--       AFTER the action has been applied to the snapshot.
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

-- Subclasses that use their own metatable call this to inherit Goal's methods.
function Goal.derive(mt)
    mt.__index = mt
    setmetatable(mt, { __index = Goal })
    return mt
end

function Goal:optimize(_cluster, _goals_by_priority)
    error("optimize not implemented for goal " .. tostring(self.name))
end

-- Default: soft goals never veto (score 1). Hard goals override this.
function Goal:action_acceptance_score(_action, _cluster)
    return 1.0
end

-- Would every hard goal accept this action? Simulates by applying the action to
-- the snapshot, scoring it against each hard goal, then undoing — so the caller
-- can probe a candidate without committing to it. The proposing goal is skipped
-- (a goal doesn't veto its own move).
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

    -- The undo must apply cleanly — a failed revert would leave the probe's
    -- mutation in the snapshot and every later goal would balance against a
    -- placement that doesn't exist. That's a programming error, not a
    -- recoverable condition, so fail loudly.
    local undone, uerr = cluster:apply_action(action:undo())
    assert(undone, "hard-goal probe could not be reverted: " .. tostring(uerr))
    return accepted
end

return Goal
