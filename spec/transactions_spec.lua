local brk_m   = require("src.broker")
local txn_m   = require("src.broker.txn_coordinator")
local message = require("src.record.message")
local os_utils = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_txn_test"
                                      or "/tmp/moonmq_txn_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

-- Count COMMIT / ABORT control markers in a partition.
local function markers(part)
    local commits, aborts = 0, 0
    part:scan(function(_off, m)
        if m:is_control() then
            local mk = string.unpack(">B", m.value)
            if mk == txn_m.MARKER_COMMIT then commits = commits + 1
            elseif mk == txn_m.MARKER_ABORT then aborts = aborts + 1 end
        end
    end)
    return commits, aborts
end

-- Simulate a transactional produce the way the produce handler does: enrol the
-- partition BEFORE the append (so the coordinator captures the pre-append LEO
-- as the txn's first offset there), then write a record carrying the
-- transactional attr + producer session.
local function txn_produce(broker, txn_id, pid, epoch, topic_name, partition_id, value)
    local topic = assert(broker:get_topic(topic_name))
    assert(broker.transactions:add_partition(txn_id, pid, epoch, topic_name, partition_id))
    assert(topic.partitions[partition_id]:write_message(
        message.Message.new("k", value, 1, message.ATTR_TXN, pid, epoch)))
end

local function flush_txn_state(broker)
    for _, p in ipairs(broker.topic_manager.topics[txn_m.STATE_TOPIC].partitions) do
        if p.sync then p:sync() end
    end
end

describe("transactions (atomic multi-partition)", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("creates the internal __transaction_state topic, hidden from list", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert.is_not_nil(broker.topic_manager.topics[txn_m.STATE_TOPIC])
        for _, name in ipairs(broker:list_topics()) do
            assert.are_not.equal(txn_m.STATE_TOPIC, name)
        end
    end)

    it("commit writes COMMIT markers to every participant and applies offsets", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 2))
        assert(broker:create_topic("in", 1))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("txn-1"))

        assert(broker.transactions:begin("txn-1", pid, epoch))
        txn_produce(broker, "txn-1", pid, epoch, "out", 1, "a")
        txn_produce(broker, "txn-1", pid, epoch, "out", 2, "b")
        assert(broker.transactions:add_offsets("txn-1", pid, epoch, "grp",
            { { topic = "in", partition = 1, offset = 99 } }))
        assert(broker.transactions:end_txn("txn-1", pid, epoch, true))

        local out = assert(broker:get_topic("out"))
        local c1 = markers(out.partitions[1])
        local c2 = markers(out.partitions[2])
        assert.are.equal(1, c1, "partition 1 should have one COMMIT marker")
        assert.are.equal(1, c2, "partition 2 should have one COMMIT marker")
        -- Buffered offset was applied on commit.
        assert.are.equal(99, broker:fetch_offset("grp", "in", 1))
        assert.are.equal(txn_m.STATES.COMPLETE_COMMIT,
            broker.transactions:current("txn-1").state)
    end)

    it("abort writes ABORT markers and does NOT apply offsets", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("out", 1))
        assert(broker:create_topic("in", 1))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("txn-2"))

        assert(broker.transactions:begin("txn-2", pid, epoch))
        txn_produce(broker, "txn-2", pid, epoch, "out", 1, "x")
        assert(broker.transactions:add_offsets("txn-2", pid, epoch, "grp",
            { { topic = "in", partition = 1, offset = 5 } }))
        assert(broker.transactions:end_txn("txn-2", pid, epoch, false))

        local _, aborts = markers(assert(broker:get_topic("out")).partitions[1])
        assert.are.equal(1, aborts, "should have one ABORT marker")
        assert.is_nil(broker:fetch_offset("grp", "in", 1), "aborted offset must not apply")
        assert.are.equal(txn_m.STATES.COMPLETE_ABORT,
            broker.transactions:current("txn-2").state)
    end)

    it("fences a producer with a stale epoch", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local pid = assert(broker.producer_state:get_or_create_producer("txn-3"))
        -- A new session bumps the epoch to 1; epoch 0 is now a zombie.
        local _, epoch1 = assert(broker.producer_state:get_or_create_producer("txn-3"))
        assert.are.equal(1, epoch1)
        local ok, err, code = broker.transactions:begin("txn-3", pid, 0)
        assert.is_nil(ok)
        assert.are.equal("fenced", code)
        assert.is_string(err)
    end)

    it("rejects begin when a txn is already ongoing", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local pid, epoch = assert(broker.producer_state:get_or_create_producer("txn-4"))
        assert(broker.transactions:begin("txn-4", pid, epoch))
        local ok, _, code = broker.transactions:begin("txn-4", pid, epoch)
        assert.is_nil(ok)
        assert.are.equal("state", code)
    end)

    it("aborts an ONGOING transaction on crash recovery", function()
        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            assert(broker:create_topic("out", 1))
            local pid, epoch = assert(broker.producer_state:get_or_create_producer("txn-5"))
            assert(broker.transactions:begin("txn-5", pid, epoch))
            txn_produce(broker, "txn-5", pid, epoch, "out", 1, "orphan")
            -- No end_txn: simulate a crash mid-transaction.
            flush_txn_state(broker)
            for _, p in ipairs(assert(broker:get_topic("out")).partitions) do
                if p.sync then p:sync() end
            end
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        assert.are.equal(txn_m.STATES.COMPLETE_ABORT,
            broker2.transactions:current("txn-5").state)
        local _, aborts = markers(assert(broker2:get_topic("out")).partitions[1])
        assert.are.equal(1, aborts, "recovery should have written an ABORT marker")
    end)

    it("rolls a PREPARE_COMMIT forward on crash recovery", function()
        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            assert(broker:create_topic("out", 1))
            assert(broker:create_topic("in", 1))
            local pid, epoch = assert(broker.producer_state:get_or_create_producer("txn-6"))
            assert(broker.transactions:begin("txn-6", pid, epoch))
            txn_produce(broker, "txn-6", pid, epoch, "out", 1, "z")
            assert(broker.transactions:add_offsets("txn-6", pid, epoch, "grp",
                { { topic = "in", partition = 1, offset = 77 } }))
            -- Simulate a crash AFTER the commit decision was made durable
            -- (PREPARE_COMMIT) but BEFORE markers/offsets were finished.
            local t = broker.transactions:current("txn-6")
            t.state = txn_m.STATES.PREPARE_COMMIT
            assert(broker.transactions:_persist("txn-6"))
            flush_txn_state(broker)
            for _, p in ipairs(assert(broker:get_topic("out")).partitions) do
                if p.sync then p:sync() end
            end
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        assert.are.equal(txn_m.STATES.COMPLETE_COMMIT,
            broker2.transactions:current("txn-6").state)
        local commits = markers(assert(broker2:get_topic("out")).partitions[1])
        assert.are.equal(1, commits, "recovery should have written a COMMIT marker")
        assert.are.equal(77, broker2:fetch_offset("grp", "in", 1),
            "recovery should have applied the buffered offset")
    end)
end)
