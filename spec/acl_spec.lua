-- ACL evaluation and rule validation.
--
-- Two properties carry the whole model and both are easy to break by
-- accident: default-deny (a principal with no matching rule is refused, so a
-- forgotten grant fails closed) and deny-wins (an explicit deny beats any
-- allow regardless of order, so "everything under orders.* except
-- orders.audit" needs no ordering rules). Everything else here pins the
-- config-time validation, which is what stops a mistyped rule from silently
-- granting nothing — or, worse, more than intended.

local acl_m = require("src.server.acl")

describe("acl rules", function()

    it("validates the resource", function()
        local rule, err = acl_m.rule({ Resource = "queue", Operations = { "read" } })
        assert.is_nil(rule)
        assert.is_truthy(err:find("unknown resource"))

        assert.is_nil((acl_m.rule({ Operations = { "read" } })))
    end)

    it("rejects operations that are meaningless on the resource", function()
        -- group:write is never checked by any handler; a rule granting it
        -- would look effective and do nothing.
        local rule, err = acl_m.rule({
            Resource = "group", Name = "g", Operations = { "write" } })
        assert.is_nil(rule)
        assert.is_truthy(err:find("not valid on a group"))
        -- The error names the alternatives, so the fix does not require
        -- reading the source.
        assert.is_truthy(err:find("describe"))
    end)

    it("requires operations", function()
        assert.is_nil((acl_m.rule({ Resource = "topic", Name = "t" })))
        assert.is_nil((acl_m.rule({
            Resource = "topic", Name = "t", Operations = {} })))
    end)

    it("expands \"*\" to every operation valid on the resource", function()
        local rule = assert(acl_m.rule({
            Resource = "topic", Name = "t", Operations = "*" }))
        for op in pairs(acl_m.VALID_OPS.topic) do
            assert.is_true(rule.ops[op])
        end
    end)

    it("validates the effect", function()
        local rule, err = acl_m.rule({
            Resource = "topic", Name = "t", Operations = { "read" }, Effect = "maybe" })
        assert.is_nil(rule)
        assert.is_truthy(err:find("allow or deny"))
    end)

    it("ignores a name on cluster, which has no instances", function()
        local rule = assert(acl_m.rule({
            Resource = "cluster", Name = "ignored", Operations = { "describe" } }))
        assert.are.equal("*", rule.name)
    end)

    it("reports the index of a bad rule in a list", function()
        local a, err = acl_m.new({
            { Resource = "topic", Name = "a", Operations = { "read" } },
            { Resource = "topic", Name = "b", Operations = { "fly" } },
        })
        assert.is_nil(a)
        assert.is_truthy(err:find("rule #2"))
    end)
end)

describe("acl evaluation", function()

    local function build(rules) return assert(acl_m.new(rules)) end

    it("denies by default", function()
        local a = build({})
        assert.is_false(a:authorized("topic", "orders", "read"))
        assert.is_false(a:authorized("cluster", nil, "describe"))
    end)

    it("matches literal names exactly", function()
        local a = build({
            { Resource = "topic", Name = "orders", Operations = { "read" } },
        })
        assert.is_true(a:authorized("topic", "orders", "read"))
        assert.is_false(a:authorized("topic", "orders2", "read"))
        assert.is_false(a:authorized("topic", "order", "read"))
        -- The grant is per-operation, not per-resource.
        assert.is_false(a:authorized("topic", "orders", "write"))
        -- ...and per-resource-type: a topic grant is not a group grant.
        assert.is_false(a:authorized("group", "orders", "read"))
    end)

    it("matches a trailing * as a prefix", function()
        local a = build({
            { Resource = "topic", Name = "orders.*", Operations = { "read" } },
        })
        assert.is_true(a:authorized("topic", "orders.eu", "read"))
        assert.is_true(a:authorized("topic", "orders.", "read"))
        assert.is_false(a:authorized("topic", "orders", "read"))
        assert.is_false(a:authorized("topic", "billing.eu", "read"))
    end)

    it("matches a bare * as any name", function()
        local a = build({
            { Resource = "group", Name = "*", Operations = { "read", "describe" } },
        })
        assert.is_true(a:authorized("group", "anything", "read"))
        assert.is_true(a:authorized("group", "", "describe"))
    end)

    it("lets deny beat allow whatever the order", function()
        local deny_first = build({
            { Resource = "topic", Name = "orders.audit",
              Operations = "*", Effect = "deny" },
            { Resource = "topic", Name = "orders.*", Operations = { "read" } },
        })
        local allow_first = build({
            { Resource = "topic", Name = "orders.*", Operations = { "read" } },
            { Resource = "topic", Name = "orders.audit",
              Operations = "*", Effect = "deny" },
        })
        for _, a in ipairs({ deny_first, allow_first }) do
            assert.is_true(a:authorized("topic", "orders.eu", "read"))
            assert.is_false(a:authorized("topic", "orders.audit", "read"))
        end
    end)

    it("scopes a deny to the operations it names", function()
        local a = build({
            { Resource = "topic", Name = "*", Operations = "*" },
            { Resource = "topic", Name = "*", Operations = { "delete" },
              Effect = "deny" },
        })
        assert.is_true(a:authorized("topic", "orders", "write"))
        assert.is_false(a:authorized("topic", "orders", "delete"))
    end)

    it("rejects malformed queries rather than guessing", function()
        local a = build({ { Resource = "topic", Name = "*", Operations = "*" } })
        assert.is_false(a:authorized(nil, "orders", "read"))
        assert.is_false(a:authorized("topic", "orders", nil))
    end)

    it("has an allow-all for superusers", function()
        assert.is_true(acl_m.ALLOW_ALL:authorized("topic", "anything", "delete"))
        assert.is_true(acl_m.ALLOW_ALL:authorized("cluster", nil, "alter"))
        assert.is_false(acl_m.ALLOW_ALL:is_empty())
    end)

    it("renders itself for the boot log", function()
        local a = build({
            { Resource = "topic", Name = "orders.*", Operations = { "write", "read" } },
        })
        assert.are.equal("allow topic:orders.*=read,write", a:describe())
    end)
end)
