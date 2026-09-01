local brk_m       = require("src.broker")
local prodstate_m = require("src.storage.producer_state")
local os_utils    = require("src.core.os")
local time_m      = require("src.core.time")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_prodstate_test"
                                      or "/tmp/moonmq_prodstate_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

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
            assert(ps:record_produce(pid, "payments", 2, 4096, 3))
            flush(broker)
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        local ps2 = broker2.producer_state

        local pid2, epoch2 = assert(ps2:get_or_create_producer("payments"))
        assert.are.equal(pid_before, pid2)
        assert.are.equal(1, epoch2)

        local memo = ps2:lookup_memo(pid_before, "payments")
        assert.is_not_nil(memo)
        assert.are.equal(2, memo.last_seq)
        assert.are.equal(4096, memo.last_offset)
        assert.are.equal(3, memo.last_partition)

        local other = assert(ps2:get_or_create_producer("other"))
        assert.are_not.equal(pid_before, other)
    end)

    it("expires an idle producer: identity and memos gone, survives restart", function()
        local pid
        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            local ps = broker.producer_state
            pid = assert(ps:get_or_create_producer("stale-writer"))
            assert(ps:record_produce(pid, "t", 0, 10, 1))

            local future = time_m.now_ms() + 86400 * 1000 + 1
            local n = assert(ps:expire_idle(86400 * 1000, { now_ms = future }))
            assert.are.equal(1, n)
            assert.is_nil(ps:pid_for("stale-writer"))
            assert.is_nil(ps:current_epoch(pid))
            assert.is_nil(ps:lookup_memo(pid, "t"))
            flush(broker)
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        local ps2 = broker2.producer_state
        assert.is_nil(ps2:pid_for("stale-writer"))
        assert.is_nil(ps2:lookup_memo(pid, "t"))

        local fresh = assert(ps2:get_or_create_producer("new-writer"))
        assert.is_true(fresh > pid)
    end)

    it("does not expire a producer that is still active or recently used", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local ps = broker.producer_state
        local pid = assert(ps:get_or_create_producer("busy"))

        local soon = time_m.now_ms() + 1000
        assert.are.equal(0, (assert(ps:expire_idle(86400 * 1000, { now_ms = soon }))))
        assert.are.equal(pid, ps:pid_for("busy"))

        local future = time_m.now_ms() + 2 * 86400 * 1000
        local n = assert(ps:expire_idle(86400 * 1000, {
            now_ms = future,
            is_active = function(name) return name == "busy" end,
        }))
        assert.are.equal(0, n)
        assert.are.equal(pid, ps:pid_for("busy"))
    end)

    it("broker sweep vetoes producers with an unresolved transaction", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local ps = broker.producer_state
        local pid, epoch = assert(ps:get_or_create_producer("txn-writer"))
        assert(broker.transactions:begin("txn-writer", pid, epoch))

        local future = time_m.now_ms() + 2 * 86400 * 1000
        local n = assert(broker:expire_idle_producers(86400 * 1000, { now_ms = future }))
        assert.are.equal(0, n, "ongoing transaction must veto expiry")

        assert(broker.transactions:end_txn("txn-writer", pid, epoch, true))
        n = assert(broker:expire_idle_producers(86400 * 1000, { now_ms = future }))
        assert.are.equal(1, n)
        assert.is_nil(ps:pid_for("txn-writer"))
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
