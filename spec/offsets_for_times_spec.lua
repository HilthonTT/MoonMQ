local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local handlers   = require("src.server.handlers")
local message    = require("src.record.message")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_for_times_test"
                                      or "/tmp/moonmq_for_times_test"

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

local function only_reply(conn)
    assert(#conn.sent == 1,
        string.format("expected exactly 1 frame, got %d", #conn.sent))
    local op, _, payload = unframe(conn.sent[1])
    return op, payload
end

local function list_offsets(server, topic, ts)
    local conn = fake_conn()
    local _, _, payload = unframe(proto.encode_list_offsets(uuid.bytes(), topic, ts))
    handlers.list_offsets(server, conn, uuid.bytes(), payload)
    return conn
end

local function offsets_reply(conn)
    local op, payload = only_reply(conn)
    assert(op == proto.OP_OFFSETS,
        string.format("expected OFFSETS, got 0x%02x", op))
    return assert(proto.decode_offsets(payload))
end

describe("LIST_OFFSETS timestamp mode: wire format", function()
    local correl = uuid.bytes()

    it("omits the mode byte entirely when no timestamp is asked for", function()
        local frame = proto.encode_list_offsets(correl, "orders")
        local _, _, payload = unframe(frame)

        assert.are.equal(4 + #"orders", #payload)

        local q = assert(proto.decode_list_offsets(payload))
        assert.are.equal("orders", q.topic)
        assert.are.equal(proto.LIST_OFFSETS_MODE_BOUNDS, q.mode)
        assert.is_nil(q.timestamp)
    end)

    it("round-trips a timestamp query", function()
        local frame = proto.encode_list_offsets(correl, "orders", 1700000000123)
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_LIST_OFFSETS, op)
        assert.are.equal(correl, c)

        local q = assert(proto.decode_list_offsets(payload))
        assert.are.equal("orders", q.topic)
        assert.are.equal(proto.LIST_OFFSETS_MODE_TIMESTAMP, q.mode)
        assert.are.equal(1700000000123, q.timestamp)
    end)

    it("rejects a timestamp mode byte with no timestamp behind it", function()
        local truncated = proto.encode_string("orders") ..
            string.pack(">B", proto.LIST_OFFSETS_MODE_TIMESTAMP)
        local q, err = proto.decode_list_offsets(truncated)
        assert.is_nil(q)
        assert.is_truthy(err:match("timestamp"))
    end)

    it("rejects an unknown mode", function()
        local bogus = proto.encode_string("orders") .. string.pack(">B", 77)
        local q, err = proto.decode_list_offsets(bogus)
        assert.is_nil(q)
        assert.is_truthy(err:match("unknown mode"))
    end)

    it("round-trips the for_times reply section", function()
        local entries = {
            { partition = 1, earliest = 0, latest = 50, local_leader = true },
            { partition = 2, earliest = 0, latest = 10, local_leader = true },
        }
        local times = {
            { partition = 1, offset = 17, found = true },
            { partition = 2, offset = 10, found = false },
        }
        local frame = proto.encode_offsets(correl, entries, times)
        local _, _, payload = unframe(frame)

        local got = assert(proto.decode_offsets(payload))
        assert.are.equal(2, #got)
        assert.are.equal(50, got[1].latest)

        assert.are.equal(2, #got.for_times)
        assert.are.equal(1,  got.for_times[1].partition)
        assert.are.equal(17, got.for_times[1].offset)
        assert.is_true(got.for_times[1].found)
        assert.are.equal(10, got.for_times[2].offset)
        assert.is_false(got.for_times[2].found)
    end)

    it("leaves for_times absent on a bounds-only reply", function()
        local frame = proto.encode_offsets(correl, {
            { partition = 1, earliest = 0, latest = 3, local_leader = true },
        })
        local _, _, payload = unframe(frame)
        local got = assert(proto.decode_offsets(payload))
        assert.are.equal(1, #got)
        assert.is_nil(got.for_times)
    end)
end)

describe("LIST_OFFSETS timestamp mode: handler", function()
    local broker, server

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        server = { broker = broker }
    end)

    after_each(function()
        rmdir(BASE_DIR)
    end)

    local function seed(topic_name, timestamps)
        local topic = assert(broker:create_topic(topic_name, 1))
        local part  = topic.partitions[1]
        local offs  = {}
        for i, ts in ipairs(timestamps) do
            local m = message.Message.new("k" .. i, "v" .. i, ts)
            offs[i] = (part:write_message(m))
        end
        return offs
    end

    it("finds the earliest record at or after the timestamp", function()
        local offs = seed("orders", { 1000, 2000, 3000, 4000 })

        local ft = offsets_reply(list_offsets(server, "orders", 3000)).for_times
        assert.are.equal(1, #ft)
        assert.is_true(ft[1].found)
        assert.are.equal(offs[3], ft[1].offset)
    end)

    it("rounds a between-records timestamp forward, never backward", function()
        local offs = seed("orders", { 1000, 2000, 3000, 4000 })

        local ft = offsets_reply(list_offsets(server, "orders", 2500)).for_times
        assert.is_true(ft[1].found)
        assert.are.equal(offs[3], ft[1].offset)
    end)

    it("returns the first record for a timestamp before the whole log", function()
        local offs = seed("orders", { 1000, 2000, 3000 })

        local ft = offsets_reply(list_offsets(server, "orders", 1)).for_times
        assert.is_true(ft[1].found)
        assert.are.equal(offs[1], ft[1].offset)
    end)

    it("reports latest with found clear when the query is past every record", function()
        seed("orders", { 1000, 2000 })
        local reply = offsets_reply(list_offsets(server, "orders", 9999))

        assert.is_false(reply.for_times[1].found)
        assert.are.equal(reply[1].latest, reply.for_times[1].offset)
    end)

    it("reports latest with found clear on an empty partition", function()
        assert(broker:create_topic("empty", 1))
        local reply = offsets_reply(list_offsets(server, "empty", 1000))

        assert.is_false(reply.for_times[1].found)
        assert.are.equal(reply[1].latest, reply.for_times[1].offset)
        assert.are.equal(reply[1].earliest, reply.for_times[1].offset)
    end)

    it("answers every partition, in partition order", function()
        local topic = assert(broker:create_topic("multi", 3))
        topic.partitions[1]:write_message(message.Message.new("a", "1", 1000))
        local want = topic.partitions[2]:write_message(
            message.Message.new("b", "2", 6000))

        local ft = offsets_reply(list_offsets(server, "multi", 5000)).for_times
        assert.are.equal(3, #ft)
        assert.are.equal(1, ft[1].partition)
        assert.are.equal(2, ft[2].partition)
        assert.are.equal(3, ft[3].partition)

        assert.is_false(ft[1].found)
        assert.is_true(ft[2].found)
        assert.are.equal(want, ft[2].offset)
        assert.is_false(ft[3].found)
    end)

    it("still answers bounds, and only bounds, without a timestamp", function()
        seed("orders", { 1000, 2000 })
        local reply = offsets_reply(list_offsets(server, "orders", nil))

        assert.are.equal(1, #reply)
        assert.is_true(reply[1].latest > 0)
        assert.is_nil(reply.for_times)
    end)

    it("errors on a missing topic", function()
        local conn = list_offsets(server, "nope", 1000)
        local op, payload = only_reply(conn)
        assert.are.equal(proto.OP_ERROR, op)
        assert.are.equal(proto.ERR_TOPIC_MISSING, proto.decode_error(payload).code)
    end)
end)
