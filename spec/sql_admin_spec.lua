local Parser  = require("src.repl.sql.parser")
local Session = require("src.repl.sql.executor")

local function one(src)
    local stmts, err = Parser.parse(src)
    assert(stmts, err and err.message)
    assert.equals(1, #stmts)
    return stmts[1]
end

local function fake_client(calls)
    return {
        closed = false,
        delete_topic = function(_, name) calls[#calls + 1] = { "delete_topic", name }; return true end,
        delete_group = function(_, name)
            calls[#calls + 1] = { "delete_group", name }
            if name == "busy" then return nil, "group has live members" end
            return true
        end,
        alter_topic_config = function(_, name, cfg)
            calls[#calls + 1] = { "alter_topic_config", name, cfg }; return true
        end,
        create_partitions = function(_, name, n)
            calls[#calls + 1] = { "create_partitions", name, n }; return true
        end,
        describe_topic = function(_, name)
            return { name = name, num_partitions = 2, config = { retention = "60" } }
        end,
        list_offsets = function()
            return {
                { partition = 1, earliest = 0, latest = 7, high_watermark = 7, lso = 7, lso_known = true, leader = true },
                { partition = 0, earliest = 0, latest = 3, high_watermark = 3, lso = 0, lso_known = false, leader = true },
            }
        end,
        list_groups = function()
            return {
                { group_id = "zeta", state = "empty", member_count = 0 },
                { group_id = "billing", state = "stable", member_count = 2 },
            }
        end,
        describe_group = function(_, gid)
            return {
                group_id = gid, state = "stable",
                members = { { member_id = "m1", assignment = { orders = { 0, 1 } } } },
                offsets = {
                    { topic = "orders", partition = 1, offset = 5 },
                    { topic = "orders", partition = 0, offset = 3 },
                },
            }
        end,
        close = function(self) self.closed = true end,
    }
end

local function connected_session(calls)
    local s = Session.new({ client_factory = function() return fake_client(calls) end })
    assert.is_true(s:execute({ type = "connect" }).ok)
    return s
end

describe("MQL admin statements", function()
    describe("parser", function()
        it("parses DROP TOPIC and DROP GROUP", function()
            assert.same({ type = "drop_topic", name = "orders" }, one("DROP TOPIC orders;"))
            assert.same({ type = "drop_group", name = "billing" }, one("drop group billing;"))
        end)

        it("parses DESCRIBE TOPIC and DESCRIBE GROUP", function()
            assert.same({ type = "describe_topic", name = "orders" }, one("DESCRIBE TOPIC orders;"))
            assert.same({ type = "describe_group", name = "billing" }, one("DESCRIBE GROUP billing;"))
        end)

        it("parses ALTER TOPIC ... SET with several keys and value kinds", function()
            local n = one("ALTER TOPIC orders SET retention = 3600, cleanup_policy = 'compact', backend = segmented;")
            assert.equals("alter_config", n.type)
            assert.equals("orders", n.name)
            assert.same({ retention = "3600", cleanup_policy = "compact", backend = "segmented" }, n.config)
            assert.equals(3, n.count)
        end)

        it("parses ALTER TOPIC ... PARTITIONS", function()
            assert.same({ type = "alter_partitions", name = "orders", partitions = 8 },
                one("ALTER TOPIC orders PARTITIONS 8;"))
        end)

        it("requires '=' in ALTER ... SET", function()
            local stmts, err = Parser.parse("ALTER TOPIC orders SET retention 10;")
            assert.is_nil(stmts)
            assert.matches("'='", err.message)
        end)

        it("requires SET or PARTITIONS after ALTER TOPIC <name>", function()
            local stmts, err = Parser.parse("ALTER TOPIC orders;")
            assert.is_nil(stmts)
            assert.matches("SET", err.message)
        end)

        it("distinguishes SHOW GROUPS from SHOW GROUP", function()
            assert.equals("list_groups", one("SHOW GROUPS;").type)
            assert.equals("list_groups", one("LIST GROUPS;").type)
            assert.equals("show_group", one("SHOW GROUP;").type)
        end)

        it("rejects DROP without an object kind", function()
            local stmts, err = Parser.parse("DROP orders;")
            assert.is_nil(stmts)
            assert.matches("TOPIC or GROUP", err.message)
        end)
    end)

    describe("executor", function()
        it("routes DROP, ALTER and partitions to the client", function()
            local calls = {}
            local s = connected_session(calls)
            assert.is_true(s:execute(one("DROP TOPIC orders;")).ok)
            assert.is_true(s:execute(one("ALTER TOPIC orders SET retention = 60;")).ok)
            assert.is_true(s:execute(one("ALTER TOPIC orders PARTITIONS 4;")).ok)
            assert.same({ "delete_topic", "orders" }, calls[1])
            assert.same({ "alter_topic_config", "orders", { retention = "60" } }, calls[2])
            assert.same({ "create_partitions", "orders", 4 }, calls[3])
        end)

        it("surfaces a broker refusal from DROP GROUP", function()
            local s = connected_session({})
            local res = s:execute(one("DROP GROUP busy;"))
            assert.is_false(res.ok)
            assert.matches("live members", res.error)
        end)

        it("DESCRIBE TOPIC returns partition offsets sorted and the config", function()
            local s = connected_session({})
            local res = s:execute(one("DESCRIBE TOPIC orders;"))
            assert.equals("multi", res.kind)
            local parts, cfg = res.results[1], res.results[2]
            assert.same({ 0, 0, 3, 3, "", "yes" }, parts.rows[1])
            assert.same({ 1, 0, 7, 7, 7, "yes" }, parts.rows[2])
            assert.same({ { "retention", "60" } }, cfg.rows)
        end)

        it("LIST GROUPS returns every group sorted by id", function()
            local s = connected_session({})
            local res = s:execute(one("LIST GROUPS;"))
            assert.same({ { "billing", "stable", 2 }, { "zeta", "empty", 0 } }, res.rows)
        end)

        it("DESCRIBE GROUP computes lag against the high watermark", function()
            local s = connected_session({})
            local res = s:execute(one("DESCRIBE GROUP billing;"))
            assert.same({ { "m1", "orders", "0,1" } }, res.results[1].rows)
            assert.same({
                { "orders", 0, 3, 3, 0 },
                { "orders", 1, 5, 7, 2 },
            }, res.results[2].rows)
        end)

        it("requires a connection", function()
            local s = Session.new({})
            local res = s:execute(one("DROP TOPIC orders;"))
            assert.is_false(res.ok)
            assert.matches("not connected", res.error)
        end)
    end)
end)
