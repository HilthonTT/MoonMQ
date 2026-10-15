local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local producer_m = require("src.broker.producer")
local consumer_m = require("src.broker.consumer")
local handlers   = require("src.server.handlers")
local message    = require("src.record.message")
local headers_m  = require("src.record.headers")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_headers_test"
                                      or "/tmp/moonmq_headers_test"

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
        id_short      = "test",
        sent          = {},
        seq_state     = {},
        subscriptions = {},
        send = function(self, frame)
            self.sent[#self.sent + 1] = frame
            return true
        end,
    }
end

local function fake_server(broker)
    return {
        broker      = broker,
        producer    = producer_m.Producer.new(broker, 0),
        coordinator = { apply_assignment = function() end },
        max_topics  = 64,
    }
end

local function close_broker(broker)
    for _, topic in pairs(broker.topic_manager.topics) do
        for _, partition in ipairs(topic.partitions) do
            if partition.close then partition:close() end
        end
    end
end

local function body_of(bytes)
    return bytes:sub(9)
end

local TRACE = { { name = "trace-id", value = "abc123" }, { name = "retry", value = "2" } }

describe("headers codec", function()
    it("normalizes a map into a list sorted by name", function()
        local list = assert(headers_m.normalize({ b = "2", a = "1" }))
        assert.are.same({ { name = "a", value = "1" }, { name = "b", value = "2" } }, list)
    end)

    it("keeps list order and duplicate names", function()
        local list = assert(headers_m.normalize({ { "x", "1" }, { name = "x", value = "2" } }))
        assert.are.same({ { name = "x", value = "1" }, { name = "x", value = "2" } }, list)
    end)

    it("treats nil as no headers", function()
        assert.are.equal(0, #assert(headers_m.normalize(nil)))
    end)

    it("rejects empty names, non-string values and too many headers", function()
        assert.is_nil(headers_m.normalize({ { "", "v" } }))
        assert.is_nil(headers_m.normalize({ { "n", 5 } }))
        local many = {}
        for i = 1, headers_m.MAX_HEADERS + 1 do many[i] = { "h" .. i, "" } end
        assert.is_nil(headers_m.normalize(many))
    end)

    it("round-trips through encode/decode", function()
        local blob = headers_m.encode(TRACE)
        local list, next_pos = headers_m.decode(blob, 1)
        assert.are.same(TRACE, list)
        assert.are.equal(#blob + 1, next_pos)
    end)

    it("refuses a truncated block", function()
        local blob = headers_m.encode(TRACE)
        for cut = 1, #blob - 1 do
            assert.is_nil((headers_m.decode(blob:sub(1, cut), 1)))
        end
    end)
end)

describe("record format with headers", function()
    it("round-trips headers, key and value", function()
        local msg = message.Message.new("k", "v", 42, 0, nil, nil, TRACE)
        local bytes = assert(message.serialize_message(msg))
        local back = assert(message.decode_body(body_of(bytes)))
        assert.are.equal("k", back.key)
        assert.are.equal("v", back.value)
        assert.are.equal(42, back.timestamp)
        assert.are.same(TRACE, back.headers)
        assert.is_true((back.attrs & message.ATTR_HEADERS) ~= 0)
    end)

    it("combines with the transactional header", function()
        local attrs = message.ATTR_TXN | message.CODEC_GZIP
        local msg = message.Message.new("k", "v", 1, attrs, 77, 3, TRACE)
        local back = assert(message.decode_body(body_of(assert(message.serialize_message(msg)))))
        assert.are.equal(77, back.pid)
        assert.are.equal(3, back.epoch)
        assert.are.equal(message.CODEC_GZIP, back:codec())
        assert.is_true(back:is_txn())
        assert.are.same(TRACE, back.headers)
    end)

    it("writes a record without headers exactly as before", function()
        local plain = assert(message.serialize_message(message.Message.new("k", "v", 9)))
        local empty = assert(message.serialize_message(message.Message.new("k", "v", 9, 0,
            nil, nil, {})))
        assert.are.equal(plain, empty)
        assert.are.equal(8 + message.HEADER_LEN + 4 + 2 + 4, #plain)
        assert.is_nil(assert(message.decode_body(body_of(plain))).headers)
    end)

    it("re-serializes a decoded record byte for byte", function()
        local bytes = assert(message.serialize_message(
            message.Message.new("k", "v", 5, 0, nil, nil, TRACE)))
        local back = assert(message.decode_body(body_of(bytes)))
        assert.are.equal(bytes, assert(message.serialize_message(back)))
    end)

    it("detects a corrupted header block through the payload checksum", function()
        local bytes = assert(message.serialize_message(
            message.Message.new("k", "v", 5, 0, nil, nil, TRACE)))
        local pos = 8 + message.HEADER_LEN + 4 + 4 + 3
        local bad = bytes:sub(1, pos - 1) .. "Z" .. bytes:sub(pos + 1)
        assert.is_nil((message.decode_body(body_of(bad))))
    end)
end)

describe("headers on the wire", function()
    local correl = uuid.bytes()

    it("adds nothing to a PRODUCE frame without headers", function()
        local a = proto.encode_produce(correl, "t", "k", "v", 0)
        local b = proto.encode_produce(correl, "t", "k", "v", 0, {})
        assert.are.equal(a, b)
        local _, _, payload = unframe(a)
        assert.is_nil(assert(proto.decode_produce(payload)).headers)
    end)

    it("carries headers as a trailing block on PRODUCE and PRODUCE_IDEMPOTENT", function()
        local _, _, p1 = unframe(proto.encode_produce(correl, "t", "k", "v", 0, TRACE))
        assert.are.same(TRACE, assert(proto.decode_produce(p1)).headers)

        local _, _, p2 = unframe(proto.encode_produce_idempotent(
            correl, 1, 0, "t", "k", "v", 0, 0, TRACE))
        assert.are.same(TRACE, assert(proto.decode_produce_idempotent(p2)).headers)
    end)

    it("flags PRODUCE_BATCH only when a record has headers", function()
        local _, _, plain = unframe(proto.encode_produce_batch(correl, "t",
            { { key = "a", value = "1" } }))
        assert.are.equal(0, plain:byte(1) & proto.BATCH_FLAG_HEADERS)

        local _, _, payload = unframe(proto.encode_produce_batch(correl, "t", {
            { key = "a", value = "1", headers = TRACE },
            { key = "b", value = "2" },
        }))
        local b = assert(proto.decode_produce_batch(payload))
        assert.are.same(TRACE, b.records[1].headers)
        assert.are.same({}, b.records[2].headers)
        assert.are.equal("2", b.records[2].value)
    end)

    it("includes headers in RECORD_BATCH only when asked", function()
        local recs = { { topic = "t", partition = 1, offset = 0, timestamp = 1,
                         key = "k", value = "v", headers = TRACE } }
        local _, _, without = unframe(proto.encode_record_batch(correl, recs))
        local r0 = assert(proto.decode_record_batch(without))[1]
        assert.are.equal("v", r0.value)
        assert.is_nil(r0.headers)

        local _, _, with = unframe(proto.encode_record_batch(correl, recs, true))
        assert.are.same(TRACE, assert(proto.decode_record_batch(with, true))[1].headers)
    end)

    it("round-trips headers on a RECORD frame", function()
        local _, _, payload = unframe(proto.encode_record(correl, "t", 1, 5, 9, "k", "v", TRACE))
        local r = assert(proto.decode_record(payload))
        assert.are.equal(5, r.offset)
        assert.are.same(TRACE, r.headers)
    end)
end)

describe("headers end to end", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function produce(server, frame, handler)
        local conn = fake_conn()
        local _, _, payload = unframe(frame)
        handlers[handler](server, conn, uuid.bytes(), payload)
        local op, _, reply = unframe(conn.sent[1])
        assert(op ~= proto.OP_ERROR, op == proto.OP_ERROR
            and assert(proto.decode_error(reply)).message or nil)
    end

    local function fetch(server, flags)
        local conn = fake_conn()
        local _, _, payload = unframe(proto.encode_fetch(uuid.bytes(), "orders", "g",
            10, proto.ISOLATION_READ_UNCOMMITTED, flags))
        handlers.fetch(server, conn, uuid.bytes(), payload)
        return conn
    end

    it("stores headers from PRODUCE and PRODUCE_BATCH and returns them on FETCH", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local server = fake_server(broker)

        produce(server, proto.encode_produce(uuid.bytes(), "orders", "k1", "v1", 0, TRACE),
            "produce")
        produce(server, proto.encode_produce_batch(uuid.bytes(), "orders", {
            { key = "k2", value = "v2", headers = { { name = "n", value = "x" } } },
            { key = "k3", value = "v3" },
        }), "produce_batch")

        local conn = fetch(server, proto.FETCH_FLAG_BATCHED | proto.FETCH_FLAG_HEADERS)
        local _, _, payload = unframe(conn.sent[1])
        local recs = assert(proto.decode_record_batch(payload, true))
        assert.are.equal(3, #recs)
        assert.are.same(TRACE, recs[1].headers)
        assert.are.same({ { name = "n", value = "x" } }, recs[2].headers)
        assert.are.same({}, recs[3].headers)
    end)

    it("leaves a client that did not ask for headers with the old batch layout", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local server = fake_server(broker)
        produce(server, proto.encode_produce(uuid.bytes(), "orders", "k", "v", 0, TRACE),
            "produce")

        local conn = fetch(server, proto.FETCH_FLAG_BATCHED)
        local _, _, payload = unframe(conn.sent[1])
        local recs = assert(proto.decode_record_batch(payload))
        assert.are.equal(1, #recs)
        assert.are.equal("v", recs[1].value)
    end)

    it("sends headers on unbatched RECORD frames", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local server = fake_server(broker)
        produce(server, proto.encode_produce(uuid.bytes(), "orders", "k", "v", 0, TRACE),
            "produce")

        local conn = fetch(server, 0)
        local op, _, payload = unframe(conn.sent[1])
        assert.are.equal(proto.OP_RECORD, op)
        assert.are.same(TRACE, assert(proto.decode_record(payload)).headers)
    end)

    it("keeps headers across a broker restart", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local part = assert(broker:get_topic("orders")).partitions[1]
        assert(part:write_message(message.Message.new("k", "v", 1, 0, nil, nil, TRACE)))
        close_broker(broker)

        local reopened = assert(brk_m.Broker.new(BASE_DIR))
        local consumer = consumer_m.Consumer.new(reopened, "g")
        assert(consumer:subscribe("orders"))
        local recs = assert(consumer:poll({ max_records = 10 }))
        assert.are.equal(1, #recs)
        assert.are.same(TRACE, recs[1].headers)
        close_broker(reopened)
    end)
end)
