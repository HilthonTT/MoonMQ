-- Regression tests for the 2026-07-17 review findings (see git history).
-- Each describe names the defect it pins down.

local brk_m        = require("src.broker")
local prd_m        = require("src.broker.producer")
local consumer_m   = require("src.broker.consumer")
local prodstate_m  = require("src.storage.producer_state")
local tpm_m        = require("src.storage.topic_manager")
local msg_m        = require("src.record.message")
local proto        = require("src.wire.protocol")
local handlers     = require("src.server.handlers")
local uuid         = require("src.core.uuid")
local os_utils     = require("src.core.os")

local Autobalancer = require("src.autobalancer")
local Action       = require("src.autobalancer.common.action")
local Resource     = require("src.autobalancer.common.resource")
local ClusterModel = require("src.autobalancer.model.cluster_model")
local Linear       = require("src.autobalancer.common.normalizer.linear_normalizer")
local Step         = require("src.autobalancer.common.normalizer.step_normalizer")
local Topic        = require("src.storage.topic")

local BASE = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_fixes_test"
    or "/tmp/moonmq_fixes_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

describe("SWAP undo restores the pre-swap placement (B4)", function()
    it("round-trips through apply_action", function()
        local m = ClusterModel.new({ min_valid = 1 })
        m:register_broker("A"); m:register_broker("B")
        local t1, t2 = Topic.new("t1"), Topic.new("t2")
        m:register_replica("A", t1, 1)
        m:register_replica("B", t2, 1)
        local snap = m:snapshot()

        local swap = Action.new(Action.ActionType.SWAP, t1, "A", "B", t2, 1, 1)
        assert.is_true(snap:apply_action(swap))
        assert.is_not_nil(snap:replica_at("B", "t1", 1))
        assert.is_not_nil(snap:replica_at("A", "t2", 1))

        -- The undo must apply cleanly and restore the original placement.
        local ok, err = snap:apply_action(swap:undo())
        assert.is_nil(err)
        assert.is_true(ok)
        assert.is_not_nil(snap:replica_at("A", "t1", 1))
        assert.is_not_nil(snap:replica_at("B", "t2", 1))
    end)
end)

describe("detector cap keeps snapshot consistent with the plan (B3)", function()
    it("reverts dropped actions from the shared snapshot", function()
        local ab = Autobalancer.new({
            emit_metrics = false,
            max_actions_per_detect = 1,
            goals = { network_in = false, network_out = false, disk = false },
        })
        ab.model:register_broker("A"); ab.model:register_broker("B")
        for i = 1, 4 do ab.model:register_replica("A", Topic.new("t" .. i), 1) end

        local plan, snap = ab:detect()
        assert.are.equal(1, #plan)
        -- Only ONE move may be reflected in the snapshot: A keeps 3.
        assert.are.equal(3, snap:broker_load("A", Resource.PARTITION_COUNT))
        assert.are.equal(1, snap:broker_load("B", Resource.PARTITION_COUNT))
    end)
end)

describe("durable idempotence is epoch-scoped (B1)", function()
    before_each(function() rmdir(BASE) end)

    it("persists the memo epoch through write and recovery", function()
        local tm = tpm_m.new(BASE .. "/ps")
        local ps = assert(prodstate_m.ProducerStateManager.new(tm))
        local pid, epoch = ps:get_or_create_producer("app-1")
        assert.are.equal(0, epoch)

        assert(ps:record_produce(pid, "orders", 0, 123, 1, epoch))
        local m = ps:lookup_memo(pid, "orders")
        assert.are.equal(0, m.epoch)
        assert.are.equal(0, m.last_seq)

        -- Fresh manager over the same topic re-reads the memo with its epoch.
        local ps2 = assert(prodstate_m.ProducerStateManager.new(tm))
        local m2 = ps2:lookup_memo(pid, "orders")
        assert.are.equal(0, m2.epoch)
        assert.are.equal(123, m2.last_offset)
    end)

    it("reads pre-epoch (v1) memos as epoch 0", function()
        local tm = tpm_m.new(BASE .. "/ps_v1")
        local ps = assert(prodstate_m.ProducerStateManager.new(tm))
        local pid = ps:get_or_create_producer("app-legacy")
        -- Write a v1-format memo record directly (no epoch field).
        assert(ps:_write("\2" .. string.pack(">I8", pid) .. "orders",
            string.pack(">I4I8I4", 5, 999, 2)))

        local ps2 = assert(prodstate_m.ProducerStateManager.new(tm))
        local m = ps2:lookup_memo(pid, "orders")
        assert.are.equal(0, m.epoch)
        assert.are.equal(5, m.last_seq)
    end)

    it("a reconnected session's seq 0 appends instead of replaying (handler)", function()
        local broker = assert(brk_m.Broker.new(BASE .. "/b1"))
        assert(broker:create_topic("orders", 1))
        local server = {
            broker = broker,
            producer = prd_m.Producer.new(broker, prd_m.AckMode.AckLeader),
        }
        local sent
        local function conn_for(epoch, pid)
            return {
                id_short = "test", pid = pid, epoch = epoch,
                producer_name = "app-1", seq_state = {},
                send = function(_, frame) sent = frame; return true end,
            }
        end
        local ps = broker.producer_state
        local pid, epoch0 = ps:get_or_create_producer("app-1")

        -- Session 1 (epoch 0): produce seq 0.
        -- Wire header: pid(8) seq(4) epoch(2) codec(1), then topic/key/value.
        local payload = string.pack(">I8I4I2B", pid, 0, epoch0, 0)
            .. proto.encode_string("orders")
            .. proto.encode_string("k") .. proto.encode_string("v-session1")
        handlers.produce_idempotent(server, conn_for(epoch0, pid), uuid.bytes(), payload)
        local topic = assert(broker:get_topic("orders"))
        local leo_after_s1 = topic.partitions[1].offset
        assert.is_true(leo_after_s1 > 0)

        -- Session 2: reconnect bumps the epoch; client restarts at seq 0.
        local pid2, epoch1 = ps:get_or_create_producer("app-1")
        assert.are.equal(pid, pid2)
        assert.are.equal(epoch0 + 1, epoch1)
        local payload2 = string.pack(">I8I4I2B", pid, 0, epoch1, 0)
            .. proto.encode_string("orders")
            .. proto.encode_string("k") .. proto.encode_string("v-session2")
        handlers.produce_idempotent(server, conn_for(epoch1, pid), uuid.bytes(), payload2)

        -- The record must have been APPENDED (not swallowed as a replay of
        -- session 1's memo).
        assert.is_true(topic.partitions[1].offset > leo_after_s1)
        assert.is_not_nil(sent)
    end)
end)

describe("pull FETCH commits only what it delivers (B2)", function()
    before_each(function() rmdir(BASE) end)

    it("rewinds records dropped by max_records instead of committing them", function()
        local broker = assert(brk_m.Broker.new(BASE .. "/fetch"))
        assert(broker:create_topic("orders", 3))
        -- One record in each of the 3 partitions (written directly per
        -- partition for determinism).
        local topic = assert(broker:get_topic("orders"))
        for i = 1, 3 do
            assert(topic.partitions[i]:write_message(
                msg_m.Message.new("k" .. i, "v" .. i, 1)))
        end

        local frames = {}
        local conn = {
            id_short = "test", mode = nil, subscriptions = {},
            send = function(_, frame) frames[#frames + 1] = frame; return true end,
        }
        local server = {
            broker = broker,
            coordinator = { apply_assignment = function() end },
        }

        -- FETCH with max_records = 1: poll reads one record from each of the
        -- 3 partitions, but only 1 may be delivered — and only that one
        -- committed.
        local payload = proto.encode_string("orders")
            .. proto.encode_string("g1") .. string.pack(">I4", 1)
        handlers.fetch(server, conn, uuid.bytes(), payload)

        -- 1 record frame + 1 OK frame.
        assert.are.equal(2, #frames)

        -- Exactly one partition has a committed offset; the other two were
        -- rewound (nothing durably committed).
        local committed = 0
        for p = 1, 3 do
            if broker:fetch_offset("g1", "orders", p) then
                committed = committed + 1
            end
        end
        assert.are.equal(1, committed)

        -- A second consumer session must see the 2 unsent records again.
        local c2 = consumer_m.Consumer.new(broker, "g1")
        assert(c2:subscribe("orders"))
        local again = assert(c2:poll())
        assert.are.equal(2, #again)
    end)
end)

describe("consumer resumes at the oldest retained offset (ST4)", function()
    before_each(function() rmdir(BASE) end)

    it("does not skip to the tail when its offset aged out", function()
        local broker = assert(brk_m.Broker.new(BASE .. "/aged"))
        assert(broker:create_topic("orders", 1))
        local c = consumer_m.Consumer.new(broker, "g1")
        assert(c:subscribe("orders"))

        -- Swap in a stub partition emulating a retention-cleaned log:
        -- oldest retained = 50, tail = 100, committed cursor below both.
        local topic = assert(broker:get_topic("orders"))
        topic.partitions[1] = {
            id = 1, offset = 100,
            oldest_offset = function() return 50 end,
            read_message = function(_, off)
                return nil, off, "failed to read message: unexpected EOF"
            end,
        }
        c.offsets["orders"][1] = 10   -- aged out (below oldest)

        local records = assert(c:poll())
        assert.are.equal(0, #records)
        -- Cursor resumed at the OLDEST retained offset, not the tail.
        assert.are.equal(50, c.offsets["orders"][1])
    end)
end)

describe("append-position guards (ST1/ST2)", function()
    before_each(function() rmdir(BASE) end)

    it("segmented: partial bytes from a failed write don't corrupt later records", function()
        local broker = assert(brk_m.Broker.new(BASE .. "/seg"))
        assert(broker:create_topic("orders", 1))
        local part = assert(broker:get_topic("orders")).partitions[1]

        local off1 = assert(part:write_message(msg_m.Message.new("k1", "v1", 1)))

        -- Simulate a failed write that left partial bytes at physical EOF.
        part.active_segment.file:seek("end")
        part.active_segment.file:write("GARBAGE")
        part.active_segment.file:flush()

        -- Next write must detect the divergence and still produce a readable
        -- record at its advertised offset.
        local off2, werr = part:write_message(msg_m.Message.new("k2", "v2", 2))
        assert.is_nil(werr)
        local m1 = assert(part:read_message(off1))
        local m2 = assert(part:read_message(off2))
        assert.are.equal("v1", m1.value)
        assert.are.equal("v2", m2.value)
    end)

    it("commitlog: partial bytes are truncated before the next append", function()
        local broker = assert(brk_m.Broker.new(BASE .. "/cl"))
        assert(broker:create_topic("orders", 1, { backend = "commitlog" }))
        local part = assert(broker:get_topic("orders")).partitions[1]

        local off1 = assert(part:write_message(msg_m.Message.new("k1", "v1", 1)))

        local seg = part.commitlog.active_segment
        seg.file:seek("end")
        seg.file:write("GARBAGE")
        seg.file:flush()

        local off2, werr = part:write_message(msg_m.Message.new("k2", "v2", 2))
        assert.is_nil(werr)
        local m1 = assert(part:read_message(off1))
        local m2 = assert(part:read_message(off2))
        assert.are.equal("v1", m1.value)
        assert.are.equal("v2", m2.value)
    end)
end)

describe("txn completion failures are surfaced, not swallowed (B5)", function()
    before_each(function() rmdir(BASE) end)

    it("end_txn fails on offset-commit failure and succeeds on retry", function()
        local broker = assert(brk_m.Broker.new(BASE .. "/txn"))
        assert(broker:create_topic("orders", 1))
        local ps = broker.producer_state
        local pid, epoch = ps:get_or_create_producer("txn-app")
        local txns = broker.transactions

        assert(txns:begin("txn-app", pid, epoch))
        assert(txns:add_partition("txn-app", pid, epoch, "orders", 1))
        assert(txns:add_offsets("txn-app", pid, epoch, "g1",
            { { topic = "orders", partition = 1, offset = 42 } }))

        -- Make the offsets manager fail once.
        local real_commit = broker.offsets.commit
        broker.offsets.commit = function() return nil, "injected failure" end

        local ok, err = txns:end_txn("txn-app", pid, epoch, true)
        assert.is_nil(ok)
        assert.matches("offset commit failed", err)
        -- Not COMPLETE: the decision is durable, the completion pending.
        assert.are.equal("PREPARE_COMMIT",
            require("src.broker.txn_coordinator").STATE_NAMES[
                txns:current("txn-app").state])

        -- Retry with the offsets manager healthy again.
        broker.offsets.commit = real_commit
        assert.is_true(txns:end_txn("txn-app", pid, epoch, true))
        assert.are.equal(42, broker:fetch_offset("g1", "orders", 1))
    end)
end)

describe("normalizer guards (B7)", function()
    it("LinearNormalizer rejects a degenerate range", function()
        local n, err = Linear.new(5, 5)
        assert.is_nil(n)
        assert.is_not_nil(err)
    end)

    it("StepNormalizer rejects a log-degenerate step config", function()
        local n, err = Step.new(0, 0.5, 0.5, 0.5)   -- step + offset == 1
        assert.is_nil(n)
        assert.is_not_nil(err)
    end)
end)
