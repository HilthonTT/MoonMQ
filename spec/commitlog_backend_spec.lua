-- Integration test: a topic backed by the commitlog storage backend, exercised
-- through the same Broker / Producer / Consumer path the server uses.

local broker_m = require("src.broker")
local prd_m    = require("src.broker.producer")
local cons_m   = require("src.broker.consumer")
local message  = require("src.record.message")
local os_utils = require("src.core.os")
local fs_m     = require("src.io.fs")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\lua_cl_backend_test"
                                      or "/tmp/lua_cl_backend_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function close_broker(b)
    for _, t in pairs(b.topic_manager.topics) do
        for _, p in ipairs(t.partitions) do p:close() end
    end
end

local function drain(consumer)
    local all = {}
    while true do
        local recs, err = consumer:poll()
        assert(not err, err)
        if not recs or #recs == 0 then break end
        for _, r in ipairs(recs) do all[#all + 1] = r end
    end
    return all
end

describe("commitlog storage backend", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("creates a commitlog-backed topic when requested per-topic", function()
        local b = assert(broker_m.Broker.new(BASE_DIR))
        local _, err = b:create_topic("ct", 1, { backend = "commitlog" })
        assert.is_nil(err)

        local p = b.topic_manager.topics["ct"].partitions[1]
        -- The adapter exposes the live CommitLog; the default backend would not.
        assert.is_not_nil(p.commitlog)
        assert.are.equal(0, p.offset)
        close_broker(b)
    end)

    it("round-trips produce + consume with message-count offsets", function()
        local b = assert(broker_m.Broker.new(BASE_DIR))
        assert(b:create_topic("ct", 1, { backend = "commitlog" }))

        local producer = prd_m.Producer.new(b, prd_m.AckMode.AckLeader)
        for i = 1, 3 do
            local part_id, offset, err =
                producer:produce("ct", message.Message.new("k", "v" .. i, 1000 + i))
            assert.is_nil(err)
            assert.are.equal(1, part_id)
            assert.are.equal(i - 1, offset)  -- 0,1,2
        end

        local consumer = cons_m.Consumer.new(b, "g1")
        assert(consumer:subscribe("ct"))
        local records = drain(consumer)

        assert.are.equal(3, #records)
        for i = 1, 3 do
            assert.are.equal("v" .. i, records[i].value)
            assert.are.equal(i - 1, records[i].offset)
            assert.are.equal(1000 + i, records[i].timestamp)
        end
        close_broker(b)
    end)

    it("persists the backend choice and recovers after restart", function()
        local b1 = assert(broker_m.Broker.new(BASE_DIR))
        assert(b1:create_topic("ct", 1, { backend = "commitlog" }))

        local producer = prd_m.Producer.new(b1, prd_m.AckMode.AckLeader)
        for i = 1, 3 do
            local _, _, err =
                producer:produce("ct", message.Message.new("k", "msg" .. i, 2000 + i))
            assert.is_nil(err)
        end
        close_broker(b1)

        -- Sidecar recorded the backend.
        local cfg = io.open(fs_m.join_path(BASE_DIR, "ct", "topic.config"), "rb")
        assert.is_not_nil(cfg)
        local body = cfg:read("*a"); cfg:close()
        assert.is_not_nil(body:find("backend=commitlog", 1, true))

        -- Reopen: the topic comes back as a commitlog backend with its data.
        local b2 = assert(broker_m.Broker.new(BASE_DIR))
        local p = b2.topic_manager.topics["ct"].partitions[1]
        assert.is_not_nil(p.commitlog)
        assert.are.equal(3, p.offset)

        local consumer = cons_m.Consumer.new(b2, "g2")
        assert(consumer:subscribe("ct"))
        local records = drain(consumer)
        assert.are.equal(3, #records)
        assert.are.equal("msg1", records[1].value)
        assert.are.equal("msg3", records[3].value)
        close_broker(b2)
    end)

    it("honours a broker-wide default_backend", function()
        local b = assert(broker_m.Broker.new(BASE_DIR, { default_backend = "commitlog" }))
        assert(b:create_topic("ct", 2))  -- no per-topic backend opt
        for _, p in ipairs(b.topic_manager.topics["ct"].partitions) do
            assert.is_not_nil(p.commitlog)
        end
        close_broker(b)
    end)

    it("still defaults to the segmented backend", function()
        local b = assert(broker_m.Broker.new(BASE_DIR))
        assert(b:create_topic("seg", 1))
        local p = b.topic_manager.topics["seg"].partitions[1]
        assert.is_nil(p.commitlog)              -- not a commitlog adapter
        assert.is_not_nil(p.active_segment)     -- it's a SegmentedPartition
        close_broker(b)
    end)
end)
