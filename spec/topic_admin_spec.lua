-- DELETE_TOPIC / DESCRIBE_TOPIC / ALTER_TOPIC_CONFIG.
--
-- Before these, topic config was write-once at creation: topic_config.lua
-- persisted retention and friends to a sidecar and the cleaners acted on it,
-- but nothing could read it back, change it, or remove a topic. The
-- interesting cases here are the ones where deletion has to reach past the
-- log: live consumer groups, durable committed offsets, and DLQ counters all
-- reference a topic by name and would otherwise dangle.

local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local handlers   = require("src.server.handlers")
local groups_m   = require("src.broker.groups")
local message    = require("src.record.message")
local topic_config = require("src.storage.topic_config")
local fs_m       = require("src.io.fs")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_topic_admin_test"
                                      or "/tmp/moonmq_topic_admin_test"

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

-- Fake coordinator standing in for the Server's real one where a test only
-- needs delete_topic's group-eviction call to be observable.
local function fake_coordinator()
    return {
        forgotten = {},
        forget_topic = function(self, name)
            self.forgotten[#self.forgotten + 1] = name
            return 0
        end,
    }
end

local function call(handler, server, frame)
    local conn = fake_conn()
    local _, _, payload = unframe(frame)
    handler(server, conn, uuid.bytes(), payload)
    return conn
end

local function expect_ok(conn)
    local op, payload = only_reply(conn)
    if op == proto.OP_ERROR then
        error("expected OK, got ERROR: " .. proto.decode_error(payload).message)
    end
    assert(op == proto.OP_OK, string.format("expected OK, got 0x%02x", op))
end

local function expect_error(conn)
    local op, payload = only_reply(conn)
    assert(op == proto.OP_ERROR, string.format("expected ERROR, got 0x%02x", op))
    return assert(proto.decode_error(payload))
end

describe("topic admin wire formats", function()
    local correl = uuid.bytes()

    it("round-trips DELETE_TOPIC and DESCRIBE_TOPIC", function()
        local _, _, p1 = unframe(proto.encode_delete_topic(correl, "orders"))
        assert.are.equal("orders", assert(proto.decode_delete_topic(p1)).name)

        local _, _, p2 = unframe(proto.encode_describe_topic(correl, "orders"))
        assert.are.equal("orders", assert(proto.decode_describe_topic(p2)).name)
    end)

    it("round-trips a topic description", function()
        local frame = proto.encode_topic_description(correl, "orders", 4, {
            retention = 604800, backend = "commitlog",
        })
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_TOPIC_DESCRIPTION, op)
        assert.are.equal(correl, c)

        local d = assert(proto.decode_topic_description(payload))
        assert.are.equal("orders", d.name)
        assert.are.equal(4, d.num_partitions)
        -- Values come back as strings even for the numeric keys: the sidecar
        -- mixes types and a client that prints or re-submits them should not
        -- need a per-key type table.
        assert.are.equal("604800", d.config.retention)
        assert.are.equal("commitlog", d.config.backend)
    end)

    it("round-trips ALTER_TOPIC_CONFIG", function()
        local frame = proto.encode_alter_topic_config(correl, "orders", {
            retention = 3600, cleanup_policy = "compact",
        })
        local _, _, payload = unframe(frame)
        local c = assert(proto.decode_alter_topic_config(payload))
        assert.are.equal("orders", c.name)
        assert.are.equal("3600", c.config.retention)
        assert.are.equal("compact", c.config.cleanup_policy)
    end)

    it("caps how many config entries one frame can ask us to decode", function()
        local body = proto.encode_string("orders") ..
            string.pack(">I4", proto.MAX_CONFIG_ENTRIES + 1)
        local c, err = proto.decode_alter_topic_config(body)
        assert.is_nil(c)
        assert.is_truthy(err:match("too many config entries"))
    end)
end)

describe("topic admin handlers", function()
    local broker, server

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        server = { broker = broker, max_topics = 100, max_list_topics = 100 }
    end)

    after_each(function() rmdir(BASE_DIR) end)

    local function describe_topic(name)
        return call(handlers.describe_topic, server,
            proto.encode_describe_topic(uuid.bytes(), name))
    end
    local function delete_topic(name)
        return call(handlers.delete_topic, server,
            proto.encode_delete_topic(uuid.bytes(), name))
    end
    local function alter_config(name, cfg)
        return call(handlers.alter_topic_config, server,
            proto.encode_alter_topic_config(uuid.bytes(), name, cfg))
    end

    describe("DESCRIBE_TOPIC", function()
        it("reports the partition count", function()
            assert(broker:create_topic("orders", 3))
            local _, payload = only_reply(describe_topic("orders"))
            local d = assert(proto.decode_topic_description(payload))
            assert.are.equal("orders", d.name)
            assert.are.equal(3, d.num_partitions)
        end)

        it("reports only the keys actually set", function()
            assert(broker:create_topic("tuned", 1, { retention = 60 }))
            local _, payload = only_reply(describe_topic("tuned"))
            local d = assert(proto.decode_topic_description(payload))

            assert.are.equal("60", d.config.retention)
            -- An unset key is absent rather than filled with the current
            -- default: "unset" and "set to today's default" diverge the day
            -- the default moves, and the sidecar records that difference.
            assert.is_nil(d.config.max_segment_size)
        end)

        it("returns an empty config for a topic created with none", function()
            assert(broker:create_topic("plain", 1))
            local _, payload = only_reply(describe_topic("plain"))
            local d = assert(proto.decode_topic_description(payload))
            assert.are.same({}, d.config)
        end)

        it("errors on a missing topic", function()
            assert.are.equal(proto.ERR_TOPIC_MISSING,
                expect_error(describe_topic("nope")).code)
        end)
    end)

    describe("ALTER_TOPIC_CONFIG", function()
        it("persists a change to the sidecar", function()
            assert(broker:create_topic("orders", 1))
            expect_ok(alter_config("orders", { retention = 120 }))

            -- Read the sidecar directly: it is the durable source of truth
            -- Broker:load_topics restores from, so a change that only touched
            -- memory would be lost on the next restart.
            local dir = fs_m.join_path(BASE_DIR, "orders")
            local saved = assert(topic_config.load(dir))
            assert.are.equal(120, saved.retention)
        end)

        it("applies live to open partitions", function()
            local topic = assert(broker:create_topic("orders", 2))
            expect_ok(alter_config("orders", { retention = 99 }))

            for _, p in ipairs(topic.partitions) do
                assert.are.equal(99, p.retention)
            end
        end)

        it("merges rather than replacing existing config", function()
            assert(broker:create_topic("orders", 1,
                { retention = 60, max_segment_size = 4096 }))
            expect_ok(alter_config("orders", { retention = 120 }))

            local saved = assert(topic_config.load(fs_m.join_path(BASE_DIR, "orders")))
            assert.are.equal(120, saved.retention)
            assert.are.equal(4096, saved.max_segment_size)
        end)

        it("survives a broker restart", function()
            assert(broker:create_topic("orders", 1))
            expect_ok(alter_config("orders", { retention = 77 }))

            local reopened = assert(brk_m.Broker.new(BASE_DIR))
            local topic = assert(reopened:get_topic("orders"))
            assert.are.equal(77, topic.partitions[1].retention)
        end)

        it("rejects an unknown key", function()
            assert(broker:create_topic("orders", 1))
            local e = expect_error(alter_config("orders", { nonsense = 1 }))
            assert.are.equal(proto.ERR_INVALID_CONFIG, e.code)
            assert.is_truthy(e.message:match("nonsense"))
        end)

        it("rejects backend as immutable", function()
            assert(broker:create_topic("orders", 1))
            -- backend decides the on-disk format; changing it on a topic with
            -- data would mean reinterpreting existing segments.
            local e = expect_error(alter_config("orders", { backend = "commitlog" }))
            assert.are.equal(proto.ERR_INVALID_CONFIG, e.code)
        end)

        it("rejects a non-numeric value for a numeric key", function()
            assert(broker:create_topic("orders", 1))
            local e = expect_error(alter_config("orders", { retention = "soon" }))
            assert.are.equal(proto.ERR_INVALID_CONFIG, e.code)
        end)

        it("rejects a non-positive numeric value", function()
            assert(broker:create_topic("orders", 1))
            assert.are.equal(proto.ERR_INVALID_CONFIG,
                expect_error(alter_config("orders", { retention = 0 })).code)
        end)

        it("refuses to reconfigure an internal topic", function()
            local e = expect_error(alter_config("__consumer_offsets", { retention = 60 }))
            assert.are.equal(proto.ERR_TOPIC_FORBIDDEN, e.code)
        end)

        it("errors on a missing topic", function()
            assert.are.equal(proto.ERR_TOPIC_MISSING,
                expect_error(alter_config("nope", { retention = 60 })).code)
        end)
    end)

    describe("DELETE_TOPIC", function()
        it("removes the topic from the listing and from disk", function()
            assert(broker:create_topic("orders", 2))
            assert.is_truthy(fs_m.is_dir(fs_m.join_path(BASE_DIR, "orders")))

            expect_ok(delete_topic("orders"))

            assert.are.same({}, broker:list_topics())
            assert.is_nil(broker.topic_manager.topics["orders"])
            assert.is_false(fs_m.is_dir(fs_m.join_path(BASE_DIR, "orders")))
        end)

        it("stays deleted across a restart", function()
            assert(broker:create_topic("orders", 1))
            expect_ok(delete_topic("orders"))

            -- The directory is what load_topics scans, so a delete that only
            -- forgot the topic in memory would resurrect it here.
            local reopened = assert(brk_m.Broker.new(BASE_DIR))
            assert.are.same({}, reopened:list_topics())
        end)

        it("tombstones the committed offsets that referenced it", function()
            assert(broker:create_topic("orders", 1))
            assert(broker:commit_offset("billing", "orders", 1, 42))
            assert.are.equal(42, broker:fetch_offset("billing", "orders", 1))

            expect_ok(delete_topic("orders"))
            assert.is_nil(broker:fetch_offset("billing", "orders", 1))

            -- Durably, not just in memory: a recreated topic of the same name
            -- must not hand the group a stale offset into an unrelated log.
            local reopened = assert(brk_m.Broker.new(BASE_DIR))
            assert.is_nil(reopened:fetch_offset("billing", "orders", 1))
        end)

        it("leaves other topics' offsets alone", function()
            assert(broker:create_topic("orders", 1))
            assert(broker:create_topic("events", 1))
            assert(broker:commit_offset("billing", "orders", 1, 42))
            assert(broker:commit_offset("billing", "events", 1, 7))

            expect_ok(delete_topic("orders"))

            assert.is_nil(broker:fetch_offset("billing", "orders", 1))
            assert.are.equal(7, broker:fetch_offset("billing", "events", 1))
        end)

        it("evicts the topic from live consumer groups", function()
            assert(broker:create_topic("orders", 2))
            local coord = fake_coordinator()
            broker.group_coordinator = coord

            expect_ok(delete_topic("orders"))
            assert.are.same({ "orders" }, coord.forgotten)
        end)

        it("drops the topic's DLQ attempt counters", function()
            -- record_failure reads the record before counting (it validates
            -- the offset and needs the bytes for the dead-letter path), so
            -- each topic needs a real record to NACK against.
            local orders = assert(broker:create_topic("orders", 1))
            local events = assert(broker:create_topic("events", 1))
            local o_off = orders.partitions[1]:write_message(
                message.Message.new("k", "v", 1000))
            local e_off = events.partitions[1]:write_message(
                message.Message.new("k", "v", 1000))

            assert(broker.dlq:record_failure("g", "orders", 1, o_off, "boom"))
            assert(broker.dlq:record_failure("g", "events", 1, e_off, "boom"))
            assert.are.equal(1, broker.dlq:attempts_for("g", "orders", 1, o_off))

            expect_ok(delete_topic("orders"))

            assert.are.equal(0, broker.dlq:attempts_for("g", "orders", 1, o_off))
            -- The other topic's counter is keyed by the same packed format and
            -- must survive: forget_topic unpacks the key rather than
            -- pattern-matching the packed bytes.
            assert.are.equal(1, broker.dlq:attempts_for("g", "events", 1, e_off))
        end)

        it("refuses to delete an internal topic", function()
            local e = expect_error(delete_topic("__consumer_offsets"))
            assert.are.equal(proto.ERR_TOPIC_FORBIDDEN, e.code)
            -- And the broker's own state is untouched.
            assert.is_truthy(broker.topic_manager.topics["__consumer_offsets"])
        end)

        it("errors on a missing topic", function()
            assert.are.equal(proto.ERR_TOPIC_MISSING,
                expect_error(delete_topic("nope")).code)
        end)

        it("lets the same name be created again afterwards", function()
            assert(broker:create_topic("orders", 2))
            local part = broker:get_topic("orders").partitions[1]
            part:write_message(message.Message.new("k", "v", 1000))

            expect_ok(delete_topic("orders"))

            local fresh = assert(broker:create_topic("orders", 1))
            assert.are.equal(1, #fresh.partitions)
            -- A brand new log, not the old one reopened.
            assert.are.equal(fresh.partitions[1]:oldest_offset(),
                             fresh.partitions[1].offset)
        end)
    end)
end)

describe("ConsumerGroup:forget_topic", function()
    local broker

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
    end)

    after_each(function() rmdir(BASE_DIR) end)

    it("removes the topic from the group and reassigns the rest", function()
        assert(broker:create_topic("orders", 2))
        assert(broker:create_topic("events", 2))

        local group = groups_m.ConsumerGroup.new(broker, "billing")
        assert(group:join("m1", { "orders", "events" }))

        assert.is_true(group:forget_topic("orders"))

        assert.is_nil(group.topics["orders"])
        assert.is_truthy(group.topics["events"])
        -- The member keeps its events partitions and loses only the orders
        -- ones: a rebalance after deletion must not hand out partitions of a
        -- log that no longer exists.
        local m = group.members["m1"]
        assert.is_nil(m.partitions["orders"])
        assert.are.equal(2, #m.partitions["events"])
        assert.are.same({ "events" }, m.topics)
    end)

    it("collapses to empty when the last subscription goes", function()
        assert(broker:create_topic("orders", 1))
        local group = groups_m.ConsumerGroup.new(broker, "billing")
        assert(group:join("m1", { "orders" }))

        assert.is_true(group:forget_topic("orders"))
        assert.are.equal("empty", group:state())
    end)

    it("reports false for a topic the group never had", function()
        local group = groups_m.ConsumerGroup.new(broker, "billing")
        assert.is_false(group:forget_topic("orders"))
    end)
end)
