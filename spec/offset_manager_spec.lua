local brk_m    = require("src.broker")
local offmgr_m = require("src.storage.offset_manager")
local consumer_m = require("src.broker.consumer")
local clp_m    = require("src.storage.commitlog_partition")
local os_utils = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_offset_test"
                                      or "/tmp/moonmq_offset_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

describe("durable consumer offsets", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("creates the internal __consumer_offsets topic, hidden from list_topics", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert.is_not_nil(broker.topic_manager.topics[offmgr_m.OFFSETS_TOPIC])
        for _, name in ipairs(broker:list_topics()) do
            assert.are_not.equal(offmgr_m.OFFSETS_TOPIC, name)
        end
    end)

    it("backs __consumer_offsets with the compacting commitlog engine", function()
        -- The segmented backend ignores cleanup_policy and only does time/byte
        -- *retention*, which would eventually delete a quiet group's latest
        -- commit (losing its offset) and never bound a busy one. The offsets
        -- topic must therefore run on the commitlog backend, which compacts.
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local topic  = broker.topic_manager.topics[offmgr_m.OFFSETS_TOPIC]
        for _, p in ipairs(topic.partitions) do
            assert.are.equal(clp_m, getmetatable(p),
                "offsets partition must be a CommitLogPartition")
        end
        -- And the backend is persisted, so a restart restores it rather than
        -- silently reverting to the default segmented engine.
        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        local topic2  = broker2.topic_manager.topics[offmgr_m.OFFSETS_TOPIC]
        for _, p in ipairs(topic2.partitions) do
            assert.are.equal(clp_m, getmetatable(p))
        end
    end)

    it("round-trips a committed offset in memory", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))

        assert(broker:commit_offset("g1", "orders", 2, 4096))
        assert.are.equal(4096, broker:fetch_offset("g1", "orders", 2))
        -- Unknown keys read as nil, not error.
        assert.is_nil(broker:fetch_offset("g1", "orders", 3))
        assert.is_nil(broker:fetch_offset("other", "orders", 2))
    end)

    it("keeps the latest offset per key (latest wins)", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        assert(broker:commit_offset("g1", "orders", 1, 10))
        assert(broker:commit_offset("g1", "orders", 1, 20))
        assert(broker:commit_offset("g1", "orders", 1, 35))
        assert.are.equal(35, broker:fetch_offset("g1", "orders", 1))
    end)

    it("survives a broker restart by replaying the offsets topic", function()
        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            assert(broker:create_topic("orders", 4))
            assert(broker:commit_offset("g1", "orders", 1, 100))
            assert(broker:commit_offset("g1", "orders", 2, 200))
            assert(broker:commit_offset("g2", "orders", 1, 999))
            -- flush partition files before we drop the broker
            for _, p in ipairs(broker.topic_manager.topics[offmgr_m.OFFSETS_TOPIC].partitions) do
                if p.sync then p:sync() end
            end
        end

        local broker2 = assert(brk_m.Broker.new(BASE_DIR))
        assert.are.equal(100, broker2:fetch_offset("g1", "orders", 1))
        assert.are.equal(200, broker2:fetch_offset("g1", "orders", 2))
        assert.are.equal(999, broker2:fetch_offset("g2", "orders", 1))
    end)

    it("a Consumer loads its durable offset on subscribe", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("events", 2))
        assert(broker:commit_offset("grp", "events", 1, 512))

        local c = consumer_m.Consumer.new(broker, "grp")
        assert(c:subscribe("events"))
        -- partition 1 resumes from the committed offset; partition 2 from 0.
        assert.are.equal(512, c.offsets["events"][1])
        assert.are.equal(0, c.offsets["events"][2])
    end)
end)
