local brk_m       = require("src.broker")
local prodstate_m = require("src.storage.producer_state")
local os_utils    = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_prodstate_test"
                                      or "/tmp/moonmq_prodstate_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

-- Flush the internal producer-state topic so a rebuilt broker sees the writes.
local function flush(broker)
    for _, p in ipairs(broker.topic_manager.topics[prodstate_m.STATE_TOPIC].partitions) do
        if p.sync then p:sync() end
    end
end

describe("durable producer state", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("creates the internal __producer_state topic, hidden from list_topics", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert.is_not_nil(broker.topic_manager.topics[prodstate_m.STATE_TOPIC])
        for _, name in ipairs(broker:list_topics()) do
            assert.are_not.equal(prodstate_m.STATE_TOPIC, name)
        end
    end)

    it("assigns a stable pid and bumps epoch per session for a named producer", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local ps = broker.producer_state

        local pid1, epoch1 = assert(ps:get_or_create_producer("orders-writer"))
        assert.are.equal(0, epoch1)
        local pid2, epoch2 = assert(ps:get_or_create_producer("orders-writer"))
        assert.are.equal(pid1, pid2, "same name keeps its pid")
        assert.are.equal(1, epoch2, "each session bumps the epoch")
        assert.are.equal(1, ps:current_epoch(pid1))
    end)

    it("hands ephemeral pids that don't collide with named ones", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local ps = broker.producer_state
        local a = ps:allocate_ephemeral()
        local b = ps:allocate_ephemeral()
        assert.are_not.equal(a, b)
        local named = assert(ps:get_or_create_producer("n"))
        assert.are_not.equal(a, named)
        assert.are_not.equal(b, named)
        -- Ephemeral pids are not durable identities.
        assert.is_nil(ps:current_epoch(a))
    end)

    it("survives a restart: pid, epoch, and seq memo replay", function()
        local pid_before
        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            local ps = broker.producer_state
            local pid, epoch = assert(ps:get_or_create_producer("payments"))
            pid_before = pid
            assert.are.equal(0, epoch)
            -- Record a produced sequence memo (pid, topic) -> seq/offset/partition.
            assert(ps:record_produce(pid, "payments", 2, 4096, 3))
            flush(broker)
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        local ps2 = broker2.producer_state

        -- Reconnect with the same name: same pid, epoch bumped to 1.
        local pid2, epoch2 = assert(ps2:get_or_create_producer("payments"))
        assert.are.equal(pid_before, pid2)
        assert.are.equal(1, epoch2)

        -- The seq memo replayed, so an idempotent retry of seq 2 replays the
        -- original ack instead of appending a duplicate.
        local memo = ps2:lookup_memo(pid_before, "payments")
        assert.is_not_nil(memo)
        assert.are.equal(2, memo.last_seq)
        assert.are.equal(4096, memo.last_offset)
        assert.are.equal(3, memo.last_partition)

        -- next_pid advanced past the persisted max so a new name gets a fresh pid.
        local other = assert(ps2:get_or_create_producer("other"))
        assert.are_not.equal(pid_before, other)
    end)

    it("keeps the latest memo per (pid, topic) across many records", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local ps = broker.producer_state
        local pid = assert(ps:get_or_create_producer("p"))
        assert(ps:record_produce(pid, "t", 0, 10, 1))
        assert(ps:record_produce(pid, "t", 1, 20, 1))
        assert(ps:record_produce(pid, "t", 2, 30, 1))
        flush(broker)

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        local memo = broker2.producer_state:lookup_memo(pid, "t")
        assert.are.equal(2, memo.last_seq)
        assert.are.equal(30, memo.last_offset)
    end)
end)
