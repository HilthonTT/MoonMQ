local Goal   = require("src.autobalancer.goals.goal")
local Action = require("src.autobalancer.common.action")

local ActionType = Action.ActionType

local G = Goal.derive({})

function G.new(fields)
    local self = Goal.new(fields)
    assert(self.resource, "resource distribution goal requires a resource")
    self.detect_threshold = self.detect_threshold or 0
    self.avg_deviation    = self.avg_deviation or 0.2
    if self.trusted_only == nil then self.trusted_only = false end
    return setmetatable(self, G)
end

function G:distribution_stats(cluster)
    local ids = cluster:active_broker_ids()
    if #ids == 0 then return 0, 0 end
    local sum = 0
    for _, id in ipairs(ids) do sum = sum + cluster:broker_load(id, self.resource) end
    local mean = sum / #ids
    local sq = 0
    for _, id in ipairs(ids) do
        local d = cluster:broker_load(id, self.resource) - mean
        sq = sq + d * d
    end
    return mean, math.sqrt(sq / #ids)
end

function G:_candidate_dests(cluster, ids, src, rep_load, mean, upper)
    local cands = {}
    for _, id in ipairs(ids) do
        if id ~= src then
            local dl = cluster:broker_load(id, self.resource)
            if dl < mean and (dl + rep_load) <= upper then
                cands[#cands + 1] = { id = id, load = dl }
            end
        end
    end
    table.sort(cands, function(a, b) return a.load < b.load end)
    return cands
end

function G:optimize(cluster, goals_by_priority)
    local actions = {}
    local ids = cluster:active_broker_ids()
    if #ids < 2 then return actions end

    local function bload(id) return cluster:broker_load(id, self.resource) end

    local mean = select(1, self:distribution_stats(cluster))
    if mean < self.detect_threshold then return actions end

    local upper = mean * (1 + self.avg_deviation)

    local over = {}
    for _, id in ipairs(ids) do
        if bload(id) > upper then over[#over + 1] = id end
    end
    table.sort(over, function(a, b) return bload(a) > bload(b) end)

    for _, src in ipairs(over) do
        local reps = cluster:replicas_of(src)
        table.sort(reps, function(a, b)
            return a:get_load(self.resource) > b:get_load(self.resource)
        end)

        for _, rep in ipairs(reps) do
            if bload(src) <= upper then break end
            if self.max_actions and #actions >= self.max_actions then return actions end

            local rep_load = rep:get_load(self.resource)
            local movable = (rep_load > 0)
                and (not self.trusted_only or rep:is_trusted(self.resource))

            if movable then
                local cands = self:_candidate_dests(cluster, ids, src, rep_load, mean, upper)
                for _, cand in ipairs(cands) do
                    local action = Action.new(ActionType.MOVE, rep.topic, src,
                        cand.id, nil, rep.partition)
                    if Goal.hard_goals_accept(action, cluster, goals_by_priority, self) then
                        assert(cluster:apply_action(action))
                        actions[#actions + 1] = action
                        break
                    end
                end
            end
        end
    end

    return actions
end

function G.define(spec)
    return {
        new = function(opts)
            opts = opts or {}
            return G.new({
                name             = spec.name,
                resource         = spec.resource,
                priority         = opts.priority or spec.priority,
                weight           = opts.weight or 1.0,
                is_hard          = false,
                detect_threshold = opts.detect_threshold or spec.detect_threshold,
                avg_deviation    = opts.avg_deviation or spec.avg_deviation,
                trusted_only     = spec.trusted_only and (opts.trusted_only ~= false) or false,
                max_actions      = opts.max_actions,
            })
        end,
    }
end

return G
