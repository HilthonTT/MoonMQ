local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local handlers   = require("src.server.handlers")
local groups_m   = require("src.broker.groups")
local consumer_m = require("src.broker.consumer")
local message    = require("src.record.message")
local CommitLogPartition = require("src.storage.commitlog_partition")
local EpochCache = require("src.replication.epoch_cache")
local Fetcher    = require("src.replication.fetcher")
local fs_m       = require("src.io.fs")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_create_partitions_test"
                                      or "/tmp/moonmq_create_partitions_test"

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

describe("CREATE_PARTITIONS wire format", function()
    it("round-trips the topic name and target count", function()
        local correl = uuid.bytes()
        local op, c, payload = unframe(proto.encode_create_partitions(correl, "orders", 6))
        assert.are.equal(proto.OP_CREATE_PARTITIONS, op)
        assert.are.equal(correl, c)
        local d = assert(proto.decode_create_partitions(payload))
        assert.are.equal("orders", d.name)
        assert.are.equal(6, d.total)
    end)

    it("rejects a payload without the count", function()
        local d, err = proto.decode_create_partitions(proto.encode_string("orders"))
        assert.is_nil(d)
        assert.is_truthy(err:match("short"))
    end)
end)

describe("CREATE_PARTITIONS handler", function()
    local broker, server

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        server = { broker = broker, max_topics = 100, max_list_topics = 100 }
    end)

    after_each(function() rmdir(BASE_DIR) end)

    local function create_partitions(name, total)
        local conn = fake_conn()
        local _, _, payload = unframe(proto.encode_create_partitions(uuid.bytes(), name, total))
        handlers.create_partitions(server, conn, uuid.bytes(), payload)
        return conn
    end

    it("grows the topic and leaves existing records in place", function()
        local topic = assert(broker:create_topic("orders", 2))
        local first = topic.partitions[1]
        local off = first:write_message(message.Message.new("k", "v", 1000))

        expect_ok(create_partitions("orders", 5))

        assert.are.equal(5, #topic.partitions)
        assert.are.equal(first, topic.partitions[1])
        for i = 3, 5 do
            assert.are.equal(i, topic.partitions[i].id)
            assert.is_truthy(fs_m.is_dir(
                fs_m.join_path(BASE_DIR, "orders", string.format("partition-%d", i))))
        end
        assert.are.equal("v", first:read_message(off).value)
    end)

    it("survives a broker restart", function()
        assert(broker:create_topic("orders", 1))
        expect_ok(create_partitions("orders", 3))

        local reopened = assert(brk_m.Broker.new(BASE_DIR))
        assert.are.equal(3, #assert(reopened:get_topic("orders")).partitions)
    end)

    it("keeps a commitlog topic on the commitlog backend", function()
        local topic = assert(broker:create_topic("events", 1, { backend = "commitlog" }))
        expect_ok(create_partitions("events", 2))
        assert.are.equal(CommitLogPartition, getmetatable(topic.partitions[2]))
    end)

    it("applies the topic's config to the new partitions", function()
        local topic = assert(broker:create_topic("orders", 1))
        assert(broker:alter_topic_config("orders", { retention = 77 }))
        expect_ok(create_partitions("orders", 2))
        assert.are.equal(77, topic.partitions[2].retention)
    end)

    it("refuses to shrink or keep the same count", function()
        assert(broker:create_topic("orders", 3))
        assert.are.equal(proto.ERR_INVALID_PARTITIONS,
            expect_error(create_partitions("orders", 3)).code)
        assert.are.equal(proto.ERR_INVALID_PARTITIONS,
            expect_error(create_partitions("orders", 2)).code)
        assert.are.equal(3, #broker:get_topic("orders").partitions)
    end)

    it("caps the count at 1024", function()
        assert(broker:create_topic("orders", 1))
        assert.are.equal(proto.ERR_INVALID_PARTITIONS,
            expect_error(create_partitions("orders", 1025)).code)
    end)

    it("refuses an internal topic", function()
        assert.are.equal(proto.ERR_TOPIC_FORBIDDEN,
            expect_error(create_partitions("__consumer_offsets", 100)).code)
    end)

    it("errors on a missing topic", function()
        assert.are.equal(proto.ERR_TOPIC_MISSING,
            expect_error(create_partitions("nope", 2)).code)
    end)

    it("tells the group coordinator the new count", function()
        assert(broker:create_topic("orders", 2))
        local grown = {}
        broker.group_coordinator = {
            grow_topic = function(_, name, n) grown[#grown + 1] = { name, n } end,
        }
        expect_ok(create_partitions("orders", 4))
        assert.are.same({ { "orders", 4 } }, grown)
    end)
end)

describe("growing a topic under live readers", function()
    local broker

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
    end)

    after_each(function() rmdir(BASE_DIR) end)

    it("a subscribed consumer reads the new partitions without resubscribing", function()
        local topic = assert(broker:create_topic("orders", 1))
        local consumer = consumer_m.Consumer.new(broker, "g")
        assert(consumer:subscribe("orders"))

        assert(broker:add_partitions("orders", 2))
        topic.partitions[2]:write_message(message.Message.new("k", "fresh", 1000))

        local records = assert(consumer:poll({ max_records = 10 }))
        assert.are.equal(1, #records)
        assert.are.equal("fresh", records[1].value)
    end)

    it("a consumer group rebalances the new partitions across members", function()
        assert(broker:create_topic("orders", 2))
        local group = groups_m.ConsumerGroup.new(broker, "billing")
        assert(group:join("m1", { "orders" }))
        assert(group:join("m2", { "orders" }))

        assert.is_true(group:grow_topic("orders", 4))

        assert.are.same({ 1, 2, 3, 4 }, group.topics["orders"])
        local total = #group.members["m1"].partitions["orders"]
                    + #group.members["m2"].partitions["orders"]
        assert.are.equal(4, total)
        assert.are.equal(2, #group.members["m1"].partitions["orders"])
        assert.are.equal("stable", group:state())
    end)

    it("a follower grows a topic the leader grew instead of recopying it", function()
        local topic = assert(broker:create_topic("orders", 1))
        local off = topic.partitions[1]:write_message(message.Message.new("k", "kept", 1000))
        local cache = assert(EpochCache.new(BASE_DIR))
        assert(cache:set_uid("orders", "uid-1"))
        local fetcher = Fetcher.new({ broker = broker, cache = cache })

        assert(fetcher:_apply_manifest({ topics = {
            { name = "orders", partitions = 3, uid = "uid-1", config = {} },
        } }))

        local grown = broker:get_topic("orders")
        assert.are.equal(topic, grown)
        assert.are.equal(3, #grown.partitions)
        assert.are.equal("kept", grown.partitions[1]:read_message(off).value)
    end)

    it("a group ignores growth of a topic it does not read or that did not grow", function()
        assert(broker:create_topic("orders", 2))
        local group = groups_m.ConsumerGroup.new(broker, "billing")
        assert(group:join("m1", { "orders" }))

        assert.is_false(group:grow_topic("events", 4))
        assert.is_false(group:grow_topic("orders", 2))
    end)
end)
