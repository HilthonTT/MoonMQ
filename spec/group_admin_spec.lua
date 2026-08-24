-- LIST_GROUPS / DESCRIBE_GROUP / DELETE_GROUP.
--
-- The coordinator has always held members and assignments, and OffsetManager
-- has always held committed offsets; none of it was reachable over the wire,
-- which is why lag had to be reconstructed out-of-band. The case that drives
-- the design here is the group with committed offsets but NO live members --
-- an abandoned or between-deploys consumer -- because it is invisible to the
-- coordinator registry (reap() drops emptied groups) and is exactly the thing
-- an operator goes looking for.

local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local handlers   = require("src.server.handlers")
local GroupCoordinator = require("src.server.group_coordinator")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_group_admin_test"
                                      or "/tmp/moonmq_group_admin_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function unframe(frame) return proto.parse_frame(frame:sub(5)) end

local function fake_conn()
    return {
        id_short = "test",
        sent     = {},
        send = function(self, frame)
            self.sent[#self.sent + 1] = frame
            return true
        end,
    }
end

local function only_reply(conn)
    assert(#conn.sent == 1,
        string.format("expected exactly 1 frame, got %d", #conn.sent))
    local op, _, payload = unframe(conn.sent[1])
    return op, payload
end

local function call(handler, server, frame)
    local conn = fake_conn()
    local _, _, payload = unframe(frame)
    handler(server, conn, uuid.bytes(), payload)
    return conn
end

local function expect_error(conn)
    local op, payload = only_reply(conn)
    assert(op == proto.OP_ERROR, string.format("expected ERROR, got 0x%02x", op))
    return assert(proto.decode_error(payload))
end

describe("group admin wire formats", function()
    local correl = uuid.bytes()

    it("round-trips a group list", function()
        local frame = proto.encode_group_list(correl, {
            { group_id = "billing", state = "stable", member_count = 2 },
            { group_id = "reporting", state = "empty", member_count = 0 },
        })
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_GROUP_LIST, op)
        assert.are.equal(correl, c)

        local got = assert(proto.decode_group_list(payload))
        assert.are.equal(2, #got)
        assert.are.equal("billing", got[1].group_id)
        assert.are.equal("stable", got[1].state)
        assert.are.equal(2, got[1].member_count)
        assert.are.equal(0, got[2].member_count)
    end)

    it("round-trips a full group description", function()
        local frame = proto.encode_group_description(correl, {
            group_id = "billing",
            state    = "stable",
            members  = {
                { member_id = "m1", assignment = { orders = { 1, 2 }, events = { 1 } } },
                { member_id = "m2", assignment = {} },
            },
            offsets = {
                { topic = "orders", partition = 1, offset = 42 },
                { topic = "orders", partition = 2, offset = 7 },
            },
        })
        local op, _, payload = unframe(frame)
        assert.are.equal(proto.OP_GROUP_DESCRIPTION, op)

        local d = assert(proto.decode_group_description(payload))
        assert.are.equal("billing", d.group_id)
        assert.are.equal("stable", d.state)

        assert.are.equal(2, #d.members)
        assert.are.equal("m1", d.members[1].member_id)
        assert.are.same({ 1, 2 }, d.members[1].assignment.orders)
        assert.are.same({ 1 }, d.members[1].assignment.events)
        -- A member with no assignment round-trips as an empty table, not nil.
        assert.are.same({}, d.members[2].assignment)

        assert.are.equal(2, #d.offsets)
        assert.are.equal("orders", d.offsets[1].topic)
        assert.are.equal(1, d.offsets[1].partition)
        assert.are.equal(42, d.offsets[1].offset)
    end)

    it("round-trips a description with no members and no offsets", function()
        local frame = proto.encode_group_description(correl, {
            group_id = "ghost", state = "empty",
        })
        local _, _, payload = unframe(frame)
        local d = assert(proto.decode_group_description(payload))
        assert.are.equal("ghost", d.group_id)
        assert.are.same({}, d.members)
        assert.are.same({}, d.offsets)
    end)

    it("round-trips DESCRIBE_GROUP and DELETE_GROUP requests", function()
        local _, _, p1 = unframe(proto.encode_describe_group(correl, "billing"))
        assert.are.equal("billing", assert(proto.decode_describe_group(p1)).group_id)

        local _, _, p2 = unframe(proto.encode_delete_group(correl, "billing"))
        assert.are.equal("billing", assert(proto.decode_delete_group(p2)).group_id)
    end)
end)

describe("group admin handlers", function()
    local broker, coordinator, server

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        coordinator = GroupCoordinator.new(broker, { max_groups = 32 })
        broker.group_coordinator = coordinator
        server = { broker = broker, coordinator = coordinator }
    end)

    after_each(function() rmdir(BASE_DIR) end)

    local function list_groups()
        return call(handlers.list_groups, server,
            proto.encode_list_groups(uuid.bytes()))
    end
    local function describe_group(id)
        return call(handlers.describe_group, server,
            proto.encode_describe_group(uuid.bytes(), id))
    end
    local function delete_group(id)
        return call(handlers.delete_group, server,
            proto.encode_delete_group(uuid.bytes(), id))
    end

    local function listed()
        local op, payload = only_reply(list_groups())
        assert(op == proto.OP_GROUP_LIST, string.format("got 0x%02x", op))
        return assert(proto.decode_group_list(payload))
    end

    local function described(id)
        local op, payload = only_reply(describe_group(id))
        assert(op == proto.OP_GROUP_DESCRIPTION, string.format("got 0x%02x", op))
        return assert(proto.decode_group_description(payload))
    end

    describe("LIST_GROUPS", function()
        it("is empty on a fresh broker", function()
            assert.are.same({}, listed())
        end)

        it("reports a live group with its state and member count", function()
            assert(broker:create_topic("orders", 2))
            assert(coordinator:join("billing", "m1", { "orders" }))

            local got = listed()
            assert.are.equal(1, #got)
            assert.are.equal("billing", got[1].group_id)
            assert.are.equal("stable", got[1].state)
            assert.are.equal(1, got[1].member_count)
        end)

        it("reports an offsets-only group as empty with no members", function()
            -- No JOIN_GROUP ever happened: this group exists only because it
            -- committed an offset. It is invisible to the coordinator registry
            -- and is precisely what an abandoned consumer looks like.
            assert(broker:create_topic("orders", 1))
            assert(broker:commit_offset("ghost", "orders", 1, 5))

            local got = listed()
            assert.are.equal(1, #got)
            assert.are.equal("ghost", got[1].group_id)
            assert.are.equal("empty", got[1].state)
            assert.are.equal(0, got[1].member_count)
        end)

        it("does not double-count a group that is both live and committed", function()
            assert(broker:create_topic("orders", 1))
            assert(coordinator:join("billing", "m1", { "orders" }))
            assert(broker:commit_offset("billing", "orders", 1, 5))

            local got = listed()
            assert.are.equal(1, #got)
            assert.are.equal(1, got[1].member_count)
        end)

        it("sorts by group id", function()
            assert(broker:create_topic("orders", 1))
            assert(broker:commit_offset("zulu", "orders", 1, 1))
            assert(broker:commit_offset("alpha", "orders", 1, 1))
            assert(coordinator:join("mike", "m1", { "orders" }))

            local got = listed()
            assert.are.equal("alpha", got[1].group_id)
            assert.are.equal("mike",  got[2].group_id)
            assert.are.equal("zulu",  got[3].group_id)
        end)
    end)

    describe("DESCRIBE_GROUP", function()
        it("reports members with their assignments", function()
            assert(broker:create_topic("orders", 2))
            assert(coordinator:join("billing", "m1", { "orders" }))

            local d = described("billing")
            assert.are.equal("billing", d.group_id)
            assert.are.equal("stable", d.state)
            assert.are.equal(1, #d.members)
            assert.are.equal("m1", d.members[1].member_id)
            assert.are.same({ 1, 2 }, d.members[1].assignment.orders)
        end)

        it("reports durable committed offsets", function()
            assert(broker:create_topic("orders", 2))
            assert(coordinator:join("billing", "m1", { "orders" }))
            assert(broker:commit_offset("billing", "orders", 1, 42))
            assert(broker:commit_offset("billing", "orders", 2, 7))

            local d = described("billing")
            assert.are.equal(2, #d.offsets)
            assert.are.equal("orders", d.offsets[1].topic)
            assert.are.equal(1,  d.offsets[1].partition)
            assert.are.equal(42, d.offsets[1].offset)
            assert.are.equal(2,  d.offsets[2].partition)
            assert.are.equal(7,  d.offsets[2].offset)
        end)

        it("describes an offsets-only group rather than erroring", function()
            assert(broker:create_topic("orders", 1))
            assert(broker:commit_offset("ghost", "orders", 1, 9))

            local d = described("ghost")
            assert.are.equal("empty", d.state)
            assert.are.same({}, d.members)
            -- Offsets without members is the single most useful thing this
            -- call reports, so it must not be an error.
            assert.are.equal(1, #d.offsets)
            assert.are.equal(9, d.offsets[1].offset)
        end)

        it("errors on a group nothing knows about", function()
            assert.are.equal(proto.ERR_GROUP_MISSING,
                expect_error(describe_group("nope")).code)
        end)
    end)

    describe("DELETE_GROUP", function()
        it("removes an offsets-only group and tombstones its offsets", function()
            assert(broker:create_topic("orders", 1))
            assert(broker:commit_offset("ghost", "orders", 1, 9))

            local op = only_reply(delete_group("ghost"))
            assert.are.equal(proto.OP_OK, op)

            assert.is_nil(broker:fetch_offset("ghost", "orders", 1))
            assert.are.same({}, listed())
        end)

        it("keeps the group deleted across a restart", function()
            assert(broker:create_topic("orders", 1))
            assert(broker:commit_offset("ghost", "orders", 1, 9))
            only_reply(delete_group("ghost"))

            -- The tombstone is a zero-length-value record in the offsets log
            -- (what compact_cleaner already recognises), so replay must erase
            -- the earlier commit rather than resurrect it.
            local reopened = assert(brk_m.Broker.new(BASE_DIR))
            assert.is_nil(reopened:fetch_offset("ghost", "orders", 1))
        end)

        it("refuses while the group still has live members", function()
            assert(broker:create_topic("orders", 1))
            assert(coordinator:join("billing", "m1", { "orders" }))

            local e = expect_error(delete_group("billing"))
            assert.are.equal(proto.ERR_GROUP_NOT_EMPTY, e.code)
            -- And nothing was torn down.
            assert.is_truthy(coordinator:get("billing"))
        end)

        it("succeeds once the last member leaves", function()
            assert(broker:create_topic("orders", 1))
            assert(coordinator:join("billing", "m1", { "orders" }))
            assert(broker:commit_offset("billing", "orders", 1, 3))
            assert(coordinator:leave("billing", "m1"))

            local op = only_reply(delete_group("billing"))
            assert.are.equal(proto.OP_OK, op)
            assert.is_nil(broker:fetch_offset("billing", "orders", 1))
        end)

        it("leaves other groups' offsets alone", function()
            assert(broker:create_topic("orders", 1))
            assert(broker:commit_offset("ghost", "orders", 1, 9))
            assert(broker:commit_offset("keeper", "orders", 1, 4))

            only_reply(delete_group("ghost"))

            assert.is_nil(broker:fetch_offset("ghost", "orders", 1))
            assert.are.equal(4, broker:fetch_offset("keeper", "orders", 1))
        end)

        it("errors on a group nothing knows about", function()
            assert.are.equal(proto.ERR_GROUP_MISSING,
                expect_error(delete_group("nope")).code)
        end)
    end)
end)

describe("OffsetManager tombstones", function()
    local broker

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
    end)

    after_each(function() rmdir(BASE_DIR) end)

    it("a commit after a tombstone re-establishes the offset", function()
        assert(broker.offsets:commit("g", "orders", 1, 5))
        assert(broker.offsets:delete("g", "orders", 1))
        assert(broker.offsets:commit("g", "orders", 1, 11))

        assert.are.equal(11, broker:fetch_offset("g", "orders", 1))

        -- Records replay in append order, so the trailing commit must win over
        -- the tombstone that precedes it.
        local reopened = assert(brk_m.Broker.new(BASE_DIR))
        assert.are.equal(11, reopened:fetch_offset("g", "orders", 1))
    end)

    it("prunes a group out of the durable listing when its last offset goes", function()
        assert(broker.offsets:commit("g", "orders", 1, 5))
        assert.are.same({ "g" }, broker.offsets:groups())

        assert(broker.offsets:delete("g", "orders", 1))
        assert.are.same({}, broker.offsets:groups())
    end)
end)
