local message    = require("src.record.message")
local os_utils   = require("src.core.os")
local fs_m       = require("src.io.fs")
local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local producer_m = require("src.broker.producer")
local handlers   = require("src.server.handlers")
local Reactor    = require("src.server.reactor")
local Connection = require("src.server.connection")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_durability_test"
                                      or "/tmp/moonmq_durability_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function msg(k, v, ts)
    return message.Message.new(k, v, ts or 1)
end

local function close_broker(broker)
    for _, topic in pairs(broker.topic_manager.topics) do
        for _, partition in ipairs(topic.partitions) do
            if partition.close then partition:close() end
        end
    end
end

local function read_file(path)
    local f = io.open(path, "rb")
    if not f then return nil end
    local body = f:read("*a")
    f:close()
    return body
end

describe("fs.atomic_write", function()
    before_each(function() rmdir(BASE_DIR); fs_m.mkdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("replaces the target and leaves no temp file behind", function()
        local path = fs_m.join_path(BASE_DIR, "state.json")
        assert.is_true(fs_m.atomic_write(path, "one"))
        assert.is_true(fs_m.atomic_write(path, "two"))
        assert.are.equal("two", read_file(path))
        assert.is_false(fs_m.exists(path .. ".tmp"))
    end)

    it("reports a failure instead of pretending the write happened", function()
        local path = fs_m.join_path(BASE_DIR, "missing-dir", "state.json")
        local ok, err = fs_m.atomic_write(path, "data")
        assert.is_nil(ok)
        assert.is_not_nil(err)
    end)
end)

describe("push delivery commits", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    local function setup()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local producer = producer_m.Producer.new(broker, 0)
        for i = 1, 3 do
            local _, _, err = producer:produce("orders", msg("k" .. i, "v" .. i, i))
            assert.is_nil(err)
        end

        local reactor = Reactor.new()
        local server = {
            broker            = broker,
            reactor           = reactor,
            push_batch        = 10,
            push_interval     = 0.02,
            max_pending_bytes = 1 << 20,
            coordinator       = { apply_assignment = function() end },
            _unregister_conn  = function() end,
        }
        local sock = {
            settimeout = function() end,
            close      = function() end,
            send       = function(_, d, _, j) return j or #d end,
        }
        local conn = Connection.new(server, sock, "127.0.0.1:1", "127.0.0.1")
        conn.state = Connection.STATE_AUTHENTICATED
        return broker, reactor, server, conn
    end

    local function subscribe(server, conn)
        local frame = proto.encode_subscribe(uuid.bytes(), "orders", "g1")
        local _, correl, payload = proto.parse_frame(frame:sub(5))
        handlers.subscribe(server, conn, correl, payload)
    end

    it("does not commit records that are still queued", function()
        local broker, reactor, server, conn = setup()
        local end_offset = broker:get_topic("orders").partitions[1].offset
        local before_flush, after_flush

        reactor:spawn(function()
            subscribe(server, conn)
            reactor:sleep(0.1)
            before_flush = broker:fetch_offset("g1", "orders", 1)
            reactor:spawn(function() conn:run_sender() end)
            reactor:sleep(0.1)
            after_flush = broker:fetch_offset("g1", "orders", 1)
            conn:close("test_done")
            reactor:stop()
        end)
        reactor:run()

        assert.is_true(before_flush == nil or before_flush == 0,
            "offset committed before the frames reached the socket")
        assert.are.equal(end_offset, after_flush)
        close_broker(broker)
    end)

    it("wakes a waiter with false when the connection closes", function()
        local broker, reactor, _, conn = setup()
        local result

        reactor:spawn(function()
            conn:send("frame")
            result = conn:wait_flushed()
        end)
        reactor:spawn(function()
            reactor:sleep(0.05)
            conn:close("test_done")
            reactor:sleep(0.05)
            reactor:stop()
        end)
        reactor:run()

        assert.is_false(result)
        close_broker(broker)
    end)
end)
