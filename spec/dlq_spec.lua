local brk_m    = require("src.broker")
local msg_m    = require("src.record.message")
local envelope = require("src.record.dlq_envelope")
local proto    = require("src.wire.protocol")
local os_utils = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_dlq_test"
                                      or "/tmp/moonmq_dlq_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function new_broker_with_topic(topic, parts, dlq_opts)
    local broker = assert(brk_m.Broker.new(BASE_DIR, { dlq = dlq_opts }))
    assert(broker:create_topic(topic, parts))
    return broker
end

local function append(broker, topic_name, partition_id, key, value)
    local topic = assert(broker:get_topic(topic_name))
    local part = topic.partitions[partition_id]
    local before = part.offset
    assert(part:write_message(msg_m.Message.new(key, value, 42)))
    return before
end

describe("dlq envelope", function()
    it("round-trips all fields", function()
        local meta = {
            topic = "orders", partition = 3, offset = 1234,
            timestamp = 987654321, group = "g1", attempts = 5,
            reason = "boom", value = "payload \0 with binary",
        }
        local decoded = assert(envelope.decode(envelope.encode(meta)))
        assert.are.same(meta, decoded)
    end)

    it("rejects unknown versions and truncated blobs", function()
        local blob = envelope.encode({
            topic = "t", partition = 1, offset = 0, timestamp = 0,
            group = "g", attempts = 1, reason = "", value = "v",
        })
        local bad = string.char(99) .. blob:sub(2)
        assert.is_nil((envelope.decode(bad)))
        assert.is_nil((envelope.decode(blob:sub(1, #blob - 3))))
        assert.is_nil((envelope.decode("")))
    end)
end)

describe("dlq wire protocol", function()
    local correl = string.rep("\0", 16)

    it("round-trips NACK", function()
        local frame = proto.encode_nack(correl, "orders", 2, 777, "cannot parse")
        local op, _, payload = proto.parse_frame(frame:sub(5))
        assert.are.equal(proto.OP_NACK, op)
        local n = assert(proto.decode_nack(payload))
        assert.are.same(
            { topic = "orders", partition = 2, offset = 777, reason = "cannot parse" },
            n)
    end)

    it("round-trips NACK_ACK for both verdicts", function()
        local frame = proto.encode_nack_ack(correl, true, 3, "orders.dlq")
        local op, _, payload = proto.parse_frame(frame:sub(5))
        assert.are.equal(proto.OP_NACK_ACK, op)
        local a = assert(proto.decode_nack_ack(payload))
        assert.is_true(a.dead_lettered)
        assert.are.equal(3, a.attempts)
        assert.are.equal("orders.dlq", a.dlq_topic)

        local _, _, payload2 = proto.parse_frame(
            proto.encode_nack_ack(correl, false, 1, nil):sub(5))
        local a2 = assert(proto.decode_nack_ack(payload2))
        assert.is_false(a2.dead_lettered)
        assert.are.equal(1, a2.attempts)
        assert.is_nil(a2.dlq_topic)
    end)
end)

describe("DlqManager", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("is built by Broker.new", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert.is_table(broker.dlq)
        assert.are.equal(".dlq", broker.dlq.suffix)
        assert.are.equal(3, broker.dlq.max_deliveries)
    end)

    it("counts attempts and dead-letters at max_deliveries", function()
        local broker = new_broker_with_topic("orders", 2)
        local off = append(broker, "orders", 1, "k1", "v1")

        local r1 = assert(broker.dlq:record_failure("g1", "orders", 1, off, "boom"))
        assert.is_false(r1.dead_lettered)
        assert.are.equal(1, r1.attempts)
        local r2 = assert(broker.dlq:record_failure("g1", "orders", 1, off, "boom"))
        assert.is_false(r2.dead_lettered)
        assert.are.equal(2, r2.attempts)
        assert.is_nil((broker:get_topic("orders.dlq")))

        local r3 = assert(broker.dlq:record_failure("g1", "orders", 1, off, "boom"))
        assert.is_true(r3.dead_lettered)
        assert.are.equal(3, r3.attempts)
        assert.are.equal("orders.dlq", r3.dlq_topic)
        assert.are.equal(1, r3.dlq_partition)
        assert.is_number(r3.next_offset)

        local dlq_topic = assert(broker:get_topic("orders.dlq"))
        assert.are.equal(2, #dlq_topic.partitions)
        local listed = false
        for _, name in ipairs(broker:list_topics()) do
            if name == "orders.dlq" then listed = true end
        end
        assert.is_true(listed)

        local msg = assert(dlq_topic.partitions[1]:read_message(r3.dlq_offset))
        assert.are.equal("k1", msg.key)
        local env = assert(envelope.decode(msg.value))
        assert.are.equal("orders", env.topic)
        assert.are.equal(1, env.partition)
        assert.are.equal(off, env.offset)
        assert.are.equal(42, env.timestamp)
        assert.are.equal("g1", env.group)
        assert.are.equal(3, env.attempts)
        assert.are.equal("boom", env.reason)
        assert.are.equal("v1", env.value)
    end)

    it("clears the counter after dead-lettering", function()
        local broker = new_broker_with_topic("orders", 1, { max_deliveries = 2 })
        local off = append(broker, "orders", 1, "k", "v")

        assert(broker.dlq:record_failure("g1", "orders", 1, off, ""))
        local r = assert(broker.dlq:record_failure("g1", "orders", 1, off, ""))
        assert.is_true(r.dead_lettered)
        assert.are.equal(0, broker.dlq:attempts_for("g1", "orders", 1, off))

        local again = assert(broker.dlq:record_failure("g1", "orders", 1, off, ""))
        assert.is_false(again.dead_lettered)
        assert.are.equal(1, again.attempts)
    end)

    it("dead-letters on the first nack when max_deliveries is 1", function()
        local broker = new_broker_with_topic("orders", 1, { max_deliveries = 1 })
        local off = append(broker, "orders", 1, "k", "v")
        local r = assert(broker.dlq:record_failure("g1", "orders", 1, off, "fatal"))
        assert.is_true(r.dead_lettered)
        assert.are.equal(1, r.attempts)
    end)

    it("tracks groups independently", function()
        local broker = new_broker_with_topic("orders", 1)
        local off = append(broker, "orders", 1, "k", "v")
        assert(broker.dlq:record_failure("g1", "orders", 1, off, ""))
        assert(broker.dlq:record_failure("g1", "orders", 1, off, ""))
        local other = assert(broker.dlq:record_failure("g2", "orders", 1, off, ""))
        assert.are.equal(1, other.attempts)
        assert.are.equal(2, broker.dlq:attempts_for("g1", "orders", 1, off))
    end)

    it("honours a custom suffix", function()
        local broker = new_broker_with_topic("orders", 1,
            { suffix = ".dead", max_deliveries = 1 })
        local off = append(broker, "orders", 1, "k", "v")
        local r = assert(broker.dlq:record_failure("g1", "orders", 1, off, ""))
        assert.are.equal("orders.dead", r.dlq_topic)
        assert(broker:get_topic("orders.dead"))
    end)

    it("rejects a control record", function()
        local broker = new_broker_with_topic("orders", 1)
        local topic = assert(broker:get_topic("orders"))
        local part = topic.partitions[1]
        local before = part.offset
        assert(part:write_message(msg_m.Message.new("", "marker", 0, msg_m.ATTR_CONTROL)))

        local r, err = broker.dlq:record_failure("g1", "orders", 1, before, "")
        assert.is_nil(r)
        assert.matches("control record", err)
    end)

    it("rejects unknown topics, partitions, and unreadable offsets", function()
        local broker = new_broker_with_topic("orders", 1)
        assert.is_nil((broker.dlq:record_failure("g1", "nope", 1, 0, "")))
        assert.is_nil((broker.dlq:record_failure("g1", "orders", 9, 0, "")))
        local topic = assert(broker:get_topic("orders"))
        assert.is_nil((broker.dlq:record_failure(
            "g1", "orders", 1, topic.partitions[1].offset + 100, "")))
        assert.are.equal(0, broker.dlq.tracked)
    end)

    it("evicts the stalest counter when full", function()
        local broker = new_broker_with_topic("orders", 1)
        local clock = 0
        broker.dlq.max_tracked = 2
        broker.dlq.now_ms = function() clock = clock + 1; return clock end

        local o1 = append(broker, "orders", 1, "k1", "v1")
        local o2 = append(broker, "orders", 1, "k2", "v2")
        local o3 = append(broker, "orders", 1, "k3", "v3")

        assert(broker.dlq:record_failure("g1", "orders", 1, o1, ""))
        assert(broker.dlq:record_failure("g1", "orders", 1, o2, ""))
        assert(broker.dlq:record_failure("g1", "orders", 1, o3, ""))

        assert.are.equal(2, broker.dlq.tracked)
        assert.are.equal(0, broker.dlq:attempts_for("g1", "orders", 1, o1))
        assert.are.equal(1, broker.dlq:attempts_for("g1", "orders", 1, o2))
    end)
end)
