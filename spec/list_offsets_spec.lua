local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local producer_m = require("src.broker.producer")
local handlers   = require("src.server.handlers")
local message    = require("src.record.message")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_list_offsets_test"
                                      or "/tmp/moonmq_list_offsets_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function unframe(frame)
    return proto.parse_frame(frame:sub(5))
end

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

local function fake_server(broker, opts)
    opts = opts or {}
    return {
        broker     = broker,
        replicator = opts.replicator,
    }
end

local function only_reply(conn)
    assert(#conn.sent == 1,
        string.format("expected exactly 1 frame, got %d", #conn.sent))
    local op, _, payload = unframe(conn.sent[1])
    return op, payload
end

local function list_offsets(server, topic)
    local conn = fake_conn()
    local _, _, payload = unframe(proto.encode_list_offsets(uuid.bytes(), topic))
    handlers.list_offsets(server, conn, uuid.bytes(), payload)
    return conn
end

local function offsets_reply(conn)
    local op, payload = only_reply(conn)
    assert(op == proto.OP_OFFSETS,
        string.format("expected OFFSETS, got 0x%02x", op))
    return assert(proto.decode_offsets(payload))
end

local function reply_error(conn)
    local op, payload = only_reply(conn)
    assert(op == proto.OP_ERROR, string.format("expected ERROR, got 0x%02x", op))
    return assert(proto.decode_error(payload))
end

describe("LIST_OFFSETS wire format", function()
    local correl = uuid.bytes()

    it("round-trips the request", function()
        local frame = proto.encode_list_offsets(correl, "orders")
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_LIST_OFFSETS, op)
        assert.are.equal(correl, c)

        local q = assert(proto.decode_list_offsets(payload))
        assert.are.equal("orders", q.topic)
    end)

    it("round-trips a reply with every bound known", function()
        local entries = {
            { partition = 1, earliest = 0,  latest = 42,
              high_watermark = 40, lso = 38, local_leader = true },
            { partition = 2, earliest = 17, latest = 17,
              high_watermark = 17, lso = 17, local_leader = true },
        }
        local frame = proto.encode_offsets(correl, entries)
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_OFFSETS, op)
        assert.are.equal(correl, c)

        local got = assert(proto.decode_offsets(payload))
        assert.are.equal(2, #got)

        assert.are.equal(1,  got[1].partition)
        assert.are.equal(0,  got[1].earliest)
        assert.are.equal(42, got[1].latest)
        assert.are.equal(40, got[1].high_watermark)
        assert.are.equal(38, got[1].lso)
        assert.is_true(got[1].hwm_exact)
        assert.is_true(got[1].lso_known)
        assert.is_true(got[1].leader)

        assert.are.equal(17, got[2].earliest)
        assert.are.equal(17, got[2].latest)
    end)

    it("substitutes latest for an unknown bound and clears its flag", function()
        local frame = proto.encode_offsets(correl, {
            { partition = 1, earliest = 5, latest = 99,
              high_watermark = nil, lso = nil, local_leader = false },
        })
        local _, _, payload = unframe(frame)
        local got = assert(proto.decode_offsets(payload))

        assert.are.equal(99, got[1].high_watermark)
        assert.are.equal(99, got[1].lso)
        assert.is_false(got[1].hwm_exact)
        assert.is_false(got[1].lso_known)
        assert.is_false(got[1].leader)
    end)

    it("round-trips a topic with no partitions as an empty list", function()
        local _, _, payload = unframe(proto.encode_offsets(correl, {}))
        assert.are.same({}, assert(proto.decode_offsets(payload)))
    end)

    it("rejects a truncated reply and one that overstates its count", function()
        assert.is_nil(proto.decode_offsets("\0\0"))

        local lying = string.pack(">I4", 1)
        local got, err = proto.decode_offsets(lying)
        assert.is_nil(got)
        assert.is_truthy(err:find("short offsets entry"))
    end)

    it("caps partitions per reply", function()
        local huge = string.pack(">I4", proto.MAX_PARTITIONS + 1)
        local got, err = proto.decode_offsets(huge)
        assert.is_nil(got)
        assert.is_truthy(err:find("too many partitions"))
    end)
end)

describe("LIST_OFFSETS handler", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("reports one entry per partition, in partition order", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        assert.are.equal(4, #got)
        for i = 1, 4 do assert.are.equal(i, got[i].partition) end
    end)

    it("reports earliest == latest on a fresh topic", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 2))

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        for i = 1, #got do
            assert.are.equal(got[i].earliest, got[i].latest)
        end
    end)

    it("advances latest as records are produced", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        local producer = producer_m.Producer.new(broker, 0)

        local before = offsets_reply(list_offsets(fake_server(broker), "orders"))

        local batch = {}
        for i = 1, 5 do batch[i] = message.Message.new("same-key", "v" .. i, 1) end
        local acks = assert(producer:produce_batch("orders", batch))
        local written = acks[1].partition

        local after = offsets_reply(list_offsets(fake_server(broker), "orders"))
        for i = 1, #after do
            if after[i].partition == written then
                assert.is_true(after[i].latest > before[i].latest)
                assert.are.equal(0, after[i].earliest)
            else
                assert.are.equal(before[i].latest, after[i].latest)
            end
        end
    end)

    it("agrees with the offset a produce acked", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local producer = producer_m.Producer.new(broker, 0)

        local acks = assert(producer:produce_batch("orders",
            { message.Message.new("k", "v", 1) }))

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        assert.is_true(got[1].latest > acks[1].offset)
    end)

    it("reports the log end as the watermark when replication is off", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        assert.is_true(got[1].hwm_exact)
        assert.are.equal(got[1].latest, got[1].high_watermark)
    end)

    it("clears hwm_exact when replication is on but nothing is in sync", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))

        local server = fake_server(broker, {
            replicator = {
                enabled        = function() return true end,
                high_watermark = function() return nil end,
            },
        })

        local got = offsets_reply(list_offsets(server, "orders"))
        assert.is_false(got[1].hwm_exact)
        assert.are.equal(got[1].latest, got[1].high_watermark)
    end)

    it("reports a real watermark below the log end when a follower lags", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local producer = producer_m.Producer.new(broker, 0)
        assert(producer:produce_batch("orders", { message.Message.new("k", "v", 1) }))

        local server = fake_server(broker, {
            replicator = {
                enabled        = function() return true end,
                high_watermark = function() return 0 end,
            },
        })

        local got = offsets_reply(list_offsets(server, "orders"))
        assert.is_true(got[1].hwm_exact)
        assert.are.equal(0, got[1].high_watermark)
        assert.is_true(got[1].latest > got[1].high_watermark)
    end)

    it("reports a known LSO when no transaction is in flight", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local producer = producer_m.Producer.new(broker, 0)
        assert(producer:produce_batch("orders", { message.Message.new("k", "v", 1) }))

        assert.is_nil(broker.transactions:lso("orders", 1))

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        assert.is_true(got[1].lso_known)
        assert.are.equal(got[1].latest, got[1].lso)
    end)

    it("clears lso_known only when there is no coordinator at all", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        broker.transactions = nil

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        assert.is_false(got[1].lso_known)
        assert.are.equal(got[1].latest, got[1].lso)
    end)

    it("reports an LSO below the log end while a transaction is open", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local producer = producer_m.Producer.new(broker, 0)
        assert(producer:produce_batch("orders", { message.Message.new("k", "v", 1) }))

        local real_lso = broker.transactions.lso
        broker.transactions.lso = function(_, _, id) return id == 1 and 0 or nil end

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        broker.transactions.lso = real_lso

        assert.is_true(got[1].lso_known)
        assert.are.equal(0, got[1].lso)
        assert.is_true(got[1].latest > got[1].lso)
    end)

    it("marks partitions this broker serves", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 2))

        local got = offsets_reply(list_offsets(fake_server(broker), "orders"))
        for i = 1, #got do assert.is_true(got[i].leader) end

        broker.cluster_assignments = {
            owned_by_self = function(_self, _topic, id) return id == 1 end,
        }
        local split = offsets_reply(list_offsets(fake_server(broker), "orders"))
        assert.is_true(split[1].leader)
        assert.is_false(split[2].leader)
    end)

    it("answers TOPIC_MISSING for an unknown topic", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local err = reply_error(list_offsets(fake_server(broker), "nope"))
        assert.are.equal(proto.ERR_TOPIC_MISSING, err.code)
    end)

    it("answers BAD_FRAME for a truncated request", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local conn = fake_conn()
        handlers.list_offsets(fake_server(broker), conn, uuid.bytes(), "\0\0")
        assert.are.equal(proto.ERR_BAD_FRAME, reply_error(conn).code)
    end)
end)
