local brk_m      = require("src.broker")
local txn_m      = require("src.broker.txn_coordinator")
local consumer_m = require("src.broker.consumer")
local message    = require("src.record.message")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_read_committed_test"
                                      or "/tmp/moonmq_read_committed_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function txn_produce(broker, txn_id, pid, epoch, topic_name, partition_id, key, value)
    local topic = assert(broker:get_topic(topic_name))
    assert(broker.transactions:add_partition(txn_id, pid, epoch, topic_name, partition_id))
    return assert(topic.partitions[partition_id]:write_message(
        message.Message.new(key, value, 1, message.ATTR_TXN, pid, epoch)))
end

local function plain_produce(broker, topic_name, partition_id, key, value)
    local topic = assert(broker:get_topic(topic_name))
    return assert(topic.partitions[partition_id]:write_message(
        message.Message.new(key, value, 1)))
end

local function poll_values(consumer)
    local records = assert(consumer:poll())
    local out = {}
    for _, r in ipairs(records) do out[#out + 1] = r.value end
    return out
end

local function drain(consumer, max_polls)
    local all = {}
    for _ = 1, max_polls or 20 do
        local vals = poll_values(consumer)
        if #vals == 0 then break end
        for _, v in ipairs(vals) do all[#all + 1] = v end
    end
    return all
end

describe("record format: transactional attr", function()
    it("round-trips pid/epoch through serialize/decode", function()
        local m = message.Message.new("k", "v", 42, message.ATTR_TXN, 77, 3)
        local bytes = assert(message.serialize_message(m))
        local decoded = assert(message.decode_body(bytes:sub(9)))
        assert.is_true(decoded:is_txn())
        assert.are.equal(77, decoded.pid)
        assert.are.equal(3, decoded.epoch)
        assert.are.equal("k", decoded.key)
        assert.are.equal("v", decoded.value)
    end)

    it("rejects a transactional record without a producer session", function()
        local m = message.Message.new("k", "v", 42, message.ATTR_TXN)
        local bytes, err = message.serialize_message(m)
        assert.is_nil(bytes)
        assert.matches("pid", err)
    end)

    it("non-transactional records keep the 13-byte header", function()
        local m = message.Message.new("k", "v", 42)
        local bytes = assert(message.serialize_message(m))
        local decoded = assert(message.decode_body(bytes:sub(9)))
        assert.is_false(decoded:is_txn())
        assert.is_nil(decoded.pid)
    end)
end)

describe("read_committed isolation", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("filters aborted records; read_uncommitted still sees them", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 1))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("t-abort"))

        plain_produce(broker, "out", 1, "k", "before")
        assert(broker.transactions:begin("t-abort", pid, epoch))
        txn_produce(broker, "t-abort", pid, epoch, "out", 1, "k", "doomed-1")
        txn_produce(broker, "t-abort", pid, epoch, "out", 1, "k", "doomed-2")
        assert(broker.transactions:end_txn("t-abort", pid, epoch, false))
        plain_produce(broker, "out", 1, "k", "after")

        local rc = consumer_m.Consumer.new(broker, "g-rc", { isolation = "read_committed" })
        assert(rc:subscribe("out"))
        assert.are.same({ "before", "after" }, drain(rc))

        local ru = consumer_m.Consumer.new(broker, "g-ru")
        assert(ru:subscribe("out"))
        assert.are.same({ "before", "doomed-1", "doomed-2", "after" }, drain(ru))
    end)

    it("delivers committed transactional records under read_committed", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 1))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("t-commit"))

        assert(broker.transactions:begin("t-commit", pid, epoch))
        txn_produce(broker, "t-commit", pid, epoch, "out", 1, "k", "kept")
        assert(broker.transactions:end_txn("t-commit", pid, epoch, true))

        local rc = consumer_m.Consumer.new(broker, "g", { isolation = "read_committed" })
        assert(rc:subscribe("out"))
        assert.are.same({ "kept" }, drain(rc))
    end)

    it("stops at the LSO while a transaction is unresolved", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 1))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("t-open"))

        plain_produce(broker, "out", 1, "k", "stable")
        assert(broker.transactions:begin("t-open", pid, epoch))
        txn_produce(broker, "t-open", pid, epoch, "out", 1, "k", "pending")
        plain_produce(broker, "out", 1, "k", "later")

        local rc = consumer_m.Consumer.new(broker, "g", { isolation = "read_committed" })
        assert(rc:subscribe("out"))
        assert.are.same({ "stable" }, drain(rc))

        assert(broker.transactions:end_txn("t-open", pid, epoch, true))
        assert.are.same({ "pending", "later" }, drain(rc))
    end)

    it("keeps filtering aborted records after a broker restart", function()
        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            assert(broker:create_topic("out", 1))
            local pid, epoch = assert(broker.producer_state:get_or_create_producer("t-crash"))
            assert(broker.transactions:begin("t-crash", pid, epoch))
            txn_produce(broker, "t-crash", pid, epoch, "out", 1, "k", "doomed")
            assert(broker.transactions:end_txn("t-crash", pid, epoch, false))
            plain_produce(broker, "out", 1, "k", "kept")
            for _, p in ipairs(assert(broker:get_topic("out")).partitions) do
                if p.sync then p:sync() end
            end
            for _, p in ipairs(broker.topic_manager.topics[txn_m.STATE_TOPIC].partitions) do
                if p.sync then p:sync() end
            end
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        local rc = consumer_m.Consumer.new(broker2, "g", { isolation = "read_committed" })
        assert(rc:subscribe("out"))
        assert.are.same({ "kept" }, drain(rc))
    end)

    it("aborts an in-flight txn on recovery and hides its records", function()
        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            assert(broker:create_topic("out", 1))
            local pid, epoch = assert(broker.producer_state:get_or_create_producer("t-orphan"))
            assert(broker.transactions:begin("t-orphan", pid, epoch))
            txn_produce(broker, "t-orphan", pid, epoch, "out", 1, "k", "orphan")
            for _, p in ipairs(assert(broker:get_topic("out")).partitions) do
                if p.sync then p:sync() end
            end
            for _, p in ipairs(broker.topic_manager.topics[txn_m.STATE_TOPIC].partitions) do
                if p.sync then p:sync() end
            end
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        plain_produce(broker2, "out", 1, "k", "fresh")
        local rc = consumer_m.Consumer.new(broker2, "g", { isolation = "read_committed" })
        assert(rc:subscribe("out"))
        assert.are.same({ "fresh" }, drain(rc))
    end)

    it("interleaved transactions: aborting one never hides the other", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 1))
        local pid_a, epoch_a = assert(broker.producer_state:get_or_create_producer("t-A"))
        local pid_b, epoch_b = assert(broker.producer_state:get_or_create_producer("t-B"))

        assert(broker.transactions:begin("t-A", pid_a, epoch_a))
        assert(broker.transactions:begin("t-B", pid_b, epoch_b))
        txn_produce(broker, "t-A", pid_a, epoch_a, "out", 1, "k", "a1")
        txn_produce(broker, "t-B", pid_b, epoch_b, "out", 1, "k", "b1")
        txn_produce(broker, "t-A", pid_a, epoch_a, "out", 1, "k", "a2")
        assert(broker.transactions:end_txn("t-A", pid_a, epoch_a, false))
        assert(broker.transactions:end_txn("t-B", pid_b, epoch_b, true))

        local rc = consumer_m.Consumer.new(broker, "g", { isolation = "read_committed" })
        assert(rc:subscribe("out"))
        assert.are.same({ "b1" }, drain(rc))
    end)

    it("lso() reports the open txn's first offset and clears on resolve", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 1))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("t-lso"))

        plain_produce(broker, "out", 1, "k", "v0")
        local part = assert(broker:get_topic("out")).partitions[1]
        local leo_before = part.offset

        assert(broker.transactions:begin("t-lso", pid, epoch))
        assert.is_nil(broker.transactions:lso("out", 1))
        txn_produce(broker, "t-lso", pid, epoch, "out", 1, "k", "v1")
        assert.are.equal(leo_before, broker.transactions:lso("out", 1))

        assert(broker.transactions:end_txn("t-lso", pid, epoch, true))
        assert.is_nil(broker.transactions:lso("out", 1))
    end)
end)
