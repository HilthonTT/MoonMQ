-- Access control lists: which principal may do what to which resource.
--
-- Until this module existed, authentication was the whole authorization
-- story — any client that completed AUTH could delete any topic, read any
-- other tenant's records, and reset any consumer group's offsets. An ACL
-- turns "who are you" into "what may you touch".
--
-- The model is deliberately Kafka-shaped, because operators arriving here
-- already know it:
--
--     resource   := topic | group | cluster
--     operation  := read | write | create | delete | alter | describe
--     effect     := allow | deny        (deny wins, evaluated first)
--     name       := "orders" | "orders.*" | "*"
--
-- Two rules that matter:
--
--   * **Default deny.** A principal with no matching allow rule is refused.
--     A user list that forgets to grant something fails closed, loudly, at
--     the first request — not silently open.
--   * **Deny wins.** An explicit deny always beats an allow, whatever the
--     order or specificity. "Everything under orders.* except orders.audit"
--     is then two rules and no ordering subtleties.
--
-- Rules are validated at CONFIG LOAD, not at request time: a mistyped
-- operation or resource stops the broker at boot with the offending rule
-- quoted, rather than quietly granting nothing.

local M = {}

M.ALLOW = "allow"
M.DENY  = "deny"

-- Which operations are meaningful on which resource. Enforced when a rule is
-- built, so `group:write` (which nothing would ever check) is a boot error
-- rather than a rule that silently never fires.
local VALID_OPS = {
    topic   = { read = true, write = true, create = true,
                delete = true, alter = true, describe = true },
    group   = { read = true, delete = true, describe = true },
    cluster = { describe = true, alter = true, create = true },
}

M.VALID_OPS = VALID_OPS

-- Everything a handler is allowed to ask about, in one place, so the
-- authorization call sites can be checked against it by grep.
M.OP_READ     = "read"
M.OP_WRITE    = "write"
M.OP_CREATE   = "create"
M.OP_DELETE   = "delete"
M.OP_ALTER    = "alter"
M.OP_DESCRIBE = "describe"

M.RES_TOPIC   = "topic"
M.RES_GROUP   = "group"
M.RES_CLUSTER = "cluster"

local function is_list(t)
    return type(t) == "table" and (t[1] ~= nil or next(t) == nil)
end

-- Build one normalized rule from config shape. Returns (rule, nil) or
-- (nil, err) — err names the problem precisely enough to fix the JSON
-- without reading this file.
--
-- spec = { Resource = "topic", Name = "orders.*",
--          Operations = { "read", "write" } | "*",
--          Effect = "allow" | "deny" }
function M.rule(spec)
    if type(spec) ~= "table" then
        return nil, "acl rule must be an object"
    end

    local resource = spec.Resource or spec.resource
    if type(resource) ~= "string" then
        return nil, "acl rule: Resource is required (topic|group|cluster)"
    end
    resource = resource:lower()
    if not VALID_OPS[resource] then
        return nil, string.format("acl rule: unknown resource %q "
            .. "(expected topic, group or cluster)", resource)
    end

    -- A cluster is a single unnamed resource; accepting a name there would
    -- imply a scoping that does not exist.
    local name = spec.Name or spec.name or "*"
    if resource == "cluster" then
        name = "*"
    elseif type(name) ~= "string" or name == "" then
        return nil, "acl rule: Name must be a non-empty string"
    end

    local effect = (spec.Effect or spec.effect or M.ALLOW):lower()
    if effect ~= M.ALLOW and effect ~= M.DENY then
        return nil, string.format("acl rule: Effect must be allow or deny, got %q", effect)
    end

    local ops_spec = spec.Operations or spec.operations or spec.Ops
    local ops = {}
    if type(ops_spec) == "string" then
        ops_spec = { ops_spec }
    end

    if type(ops_spec) == "table" then
        if not is_list(ops_spec) then
            return nil, "acl rule: Operations must be a list or \"*\""
        end
        for _, op in ipairs(ops_spec) do
            if type(op) ~= "string" then
                return nil, "acl rule: each operation must be a string"
            end
            op = op:lower()
            if op == "*" then
                for valid in pairs(VALID_OPS[resource]) do ops[valid] = true end
            elseif VALID_OPS[resource][op] then
                ops[op] = true
            else
                local allowed = {}
                for valid in pairs(VALID_OPS[resource]) do allowed[#allowed + 1] = valid end
                table.sort(allowed)
                return nil, string.format(
                    "acl rule: operation %q is not valid on a %s (valid: %s)",
                    op, resource, table.concat(allowed, ", "))
            end
        end
    else
        return nil, "acl rule: Operations is required (a list, or \"*\")"
    end

    if next(ops) == nil then
        return nil, "acl rule: Operations is empty"
    end

    -- Pre-compute the match shape once. A rule is consulted on every gated
    -- request; deciding "is this a prefix pattern" per request would be a
    -- string scan per rule per op.
    local prefix
    if name ~= "*" and name:sub(-1) == "*" then
        prefix = name:sub(1, -2)
    end

    return {
        resource = resource,
        name     = name,
        prefix   = prefix,
        any_name = name == "*",
        effect   = effect,
        ops      = ops,
    }, nil
end

local function matches(rule, resource, name, operation)
    if rule.resource ~= resource then return false end
    if not rule.ops[operation] then return false end
    if rule.any_name then return true end
    if rule.prefix then
        return name:sub(1, #rule.prefix) == rule.prefix
    end
    return rule.name == name
end

local Acl = {}
Acl.__index = Acl

-- specs: list of config-shaped rules. Returns (acl, nil) or (nil, err); the
-- error carries the 1-based index of the offending rule.
function M.new(specs)
    specs = specs or {}
    if not is_list(specs) then
        return nil, "acls must be a list"
    end

    local rules = {}
    for i, spec in ipairs(specs) do
        local rule, err = M.rule(spec)
        if not rule then
            return nil, string.format("rule #%d: %s", i, err)
        end
        rules[#rules + 1] = rule
    end

    return setmetatable({ rules = rules }, Acl)
end

-- The single question every gated handler asks. `name` is ignored for
-- cluster. Returns true when at least one allow matches and no deny does.
function Acl:authorized(resource, name, operation)
    if type(resource) ~= "string" or type(operation) ~= "string" then
        return false
    end
    name = name or "*"

    local allowed = false
    for i = 1, #self.rules do
        local rule = self.rules[i]
        if matches(rule, resource, name, operation) then
            -- Deny short-circuits: no later allow can rescue it, so there is
            -- no reason to keep scanning.
            if rule.effect == M.DENY then return false end
            allowed = true
        end
    end
    return allowed
end

function Acl:is_empty()
    return #self.rules == 0
end

-- Human-readable rendering, for the boot log and admin tooling.
function Acl:describe()
    local out = {}
    for i, rule in ipairs(self.rules) do
        local ops = {}
        for op in pairs(rule.ops) do ops[#ops + 1] = op end
        table.sort(ops)
        out[i] = string.format("%s %s:%s=%s", rule.effect, rule.resource,
            rule.name, table.concat(ops, ","))
    end
    return table.concat(out, "; ")
end

-- An ACL that permits everything, for superusers and for the compatibility
-- path where a single-user config carries no rules at all.
M.ALLOW_ALL = setmetatable({ rules = {} }, {
    __index = {
        authorized = function(_self, _resource, _name, _operation) return true end,
        is_empty   = function(_self) return false end,
        describe   = function(_self) return "allow *:*=*" end,
    },
})

return M
