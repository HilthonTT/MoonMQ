-- Record batching: PRODUCE_BATCH / PRODUCE_BATCH_ACK, batched FETCH replies
-- (RECORD_BATCH), and the multi-record-per-partition poll that feeds them.
-- See docs/batching.md.

local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local producer_m = require("src.broker.producer")
local consumer_m = require("src.broker.consumer")
local handlers   = require("src.server.handlers")
local message    = require("src.record.message")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_batching_test"
                                      or "/tmp/moonmq_batching_test"

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

-- A Connection stand-in that records every frame the handler sends. Carries
-- the same fields the real one exposes to handlers (producer identity,
-- consumer, subscriptions, mode).
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

-- A Server stand-in: the pieces handlers actually reach through.
local function fake_server(broker, acks)
    return {
        broker      = broker,
        producer    = producer_m.Producer.new(broker, acks or 0),
        coordinator = { apply_assignment = function() end },
        max_topics  = 64,
    }
end

-- Decode the single frame a handler is expected to have sent.
local function only_reply(conn)
    assert(#conn.sent == 1,
        string.format("expected exactly 1 frame, got %d", #conn.sent))
    local op, _, payload = unframe(conn.sent[1])
    return op, payload
end

local function reply_error(conn)
    local op, payload = only_reply(conn)
    assert(op == proto.OP_ERROR, string.format("expected ERROR, got 0x%02x", op))
    return assert(proto.decode_error(payload))
end

local function batch_ack(conn)
    local op, payload = only_reply(conn)
    assert(op == proto.OP_PRODUCE_BATCH_ACK,
        string.format("expected PRODUCE_BATCH_ACK, got 0x%02x", op))
    return assert(proto.decode_produce_batch_ack(payload))
end

-- Build the PRODUCE_BATCH payload a handler consumes, straight from the
-- encoder (so the test exercises the real wire format, not a hand-rolled one).
local function batch_payload(topic, records, opts)
    local _, _, payload =
        unframe(proto.encode_produce_batch(uuid.bytes(), topic, records, opts))
    return payload
end

local function values(records)
    local out = {}
    for i = 1, #records do out[i] = records[i].value end
    return out
end

describe("batching wire format", function()
    local correl = uuid.bytes()

    it("round-trips a plain PRODUCE_BATCH", function()
        local frame = proto.encode_produce_batch(correl, "orders", {
            { key = "a", value = "1" },
            { key = "",  value = "2" },
        }, { codec = 0 })
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_PRODUCE_BATCH, op)
        assert.are.equal(correl, c)

        local b = assert(proto.decode_produce_batch(payload))
        assert.is_false(b.idempotent)
        assert.are.equal("orders", b.topic)
        assert.are.equal(0, b.codec)
        assert.are.same({ { key = "a", value = "1" }, { key = "", value = "2" } },
            b.records)
    end)

    it("round-trips an idempotent PRODUCE_BATCH with its producer header", function()
        local frame = proto.encode_produce_batch(correl, "orders",
            { { key = "k", value = "v" } },
            { pid = 7, base_seq = 41, epoch = 3, codec = 1 })
        local _, _, payload = unframe(frame)

        local b = assert(proto.decode_produce_batch(payload))
        assert.is_true(b.idempotent)
        assert.are.equal(7, b.pid)
        assert.are.equal(41, b.base_seq)
        assert.are.equal(3, b.epoch)
        assert.are.equal(1, b.codec)
    end)

    it("round-trips PRODUCE_BATCH_ACK, error status included", function()
        local acks = { { partition = 2, offset = 0 }, { partition = 1, offset = 33 } }
        local _, _, payload =
            unframe(proto.encode_produce_batch_ack(correl, acks, 0, nil))
        local res = assert(proto.decode_produce_batch_ack(payload))
        assert.are.same(acks, res.acks)
        assert.are.equal(0, res.code)

        local _, _, epayload = unframe(
            proto.encode_produce_batch_ack(correl, { acks[1] }, proto.ERR_INTERNAL, "disk full"))
        local eres = assert(proto.decode_produce_batch_ack(epayload))
        assert.are.equal(1, #eres.acks)
        assert.are.equal(proto.ERR_INTERNAL, eres.code)
        assert.are.equal("disk full", eres.message)
    end)

    it("round-trips RECORD_BATCH", function()
        local records = {
            { topic = "orders", partition = 1, offset = 0,  timestamp = 11,
              key = "k1", value = "v1" },
            { topic = "orders", partition = 2, offset = 42, timestamp = 12,
              key = "",   value = "v2" },
        }
        local frame = proto.encode_record_batch(correl, records)
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_RECORD_BATCH, op)
        assert.are.equal(correl, c)
        assert.are.same(records, assert(proto.decode_record_batch(payload)))
    end)

    it("rejects a batch with no records and one that overruns the payload", function()
        local empty = string.pack(">BB", 0, 0) .. proto.encode_string("t")
            .. string.pack(">I4", 0)
        assert.is_nil(proto.decode_produce_batch(empty))

        local lying = string.pack(">BB", 0, 0) .. proto.encode_string("t")
            .. string.pack(">I4", 100000)
        assert.is_nil(proto.decode_produce_batch(lying))
    end)

    it("caps records per batch", function()
        local huge = string.pack(">BB", 0, 0) .. proto.encode_string("t")
            .. string.pack(">I4", proto.MAX_BATCH_RECORDS + 1)
        local b, err = proto.decode_produce_batch(huge)
        assert.is_nil(b)
        assert.is_truthy(err:find("too many records"))
    end)

    it("keeps FETCH backward-compatible: isolation and flags are both optional", function()
        -- A pre-batching client sends topic|group|max_records|isolation.
        local legacy = proto.encode_string("orders") .. proto.encode_string("g")
            .. string.pack(">I4", 10) .. string.pack(">B", proto.ISOLATION_READ_COMMITTED)
        local f = assert(proto.decode_fetch(legacy))
        assert.are.equal(proto.ISOLATION_READ_COMMITTED, f.isolation)
        assert.is_false(f.batched)

        -- An older one still sends no isolation byte at all.
        local ancient = proto.encode_string("orders") .. proto.encode_string("g")
            .. string.pack(">I4", 10)
        local f2 = assert(proto.decode_fetch(ancient))
        assert.are.equal(proto.ISOLATION_READ_UNCOMMITTED, f2.isolation)
        assert.is_false(f2.batched)

        -- A batching client sets the flag after isolation.
        local _, _, payload = unframe(proto.encode_fetch(uuid.bytes(), "orders", "g",
            10, proto.ISOLATION_READ_COMMITTED, proto.FETCH_FLAG_BATCHED))
        local f3 = assert(proto.decode_fetch(payload))
        assert.are.equal(proto.ISOLATION_READ_COMMITTED, f3.isolation)
        assert.is_true(f3.batched)
    end)
end)

describe("Producer:produce_batch", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function msgs(n, key)
        local out = {}
        for i = 1, n do
            out[i] = message.Message.new(key or "k", "v" .. i, 1)
        end
        return out
    end

    it("appends every record in order and acks one per record", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        local producer = producer_m.Producer.new(broker, 0)

        local acks, err = producer:produce_batch("orders", msgs(5, "same-key"))
        assert.is_nil(err)
        assert.are.equal(5, #acks)

        -- A fixed key hashes to one partition, so all five land together and
        -- read back in the order they were produced.
        local partition_id = acks[1].partition
        for i = 2, 5 do assert.are.equal(partition_id, acks[i].partition) end

        local part = assert(broker:get_topic("orders")).partitions[partition_id]
        local seen = {}
        part:scan(function(_off, m) seen[#seen + 1] = m.value end)
        assert.are.same({ "v1", "v2", "v3", "v4", "v5" }, seen)
    end)

    it("fans out across partitions when keys differ", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 8))
        local producer = producer_m.Producer.new(broker, 0)

        local batch = {}
        for i = 1, 24 do
            batch[i] = message.Message.new("key-" .. i, "v" .. i, 1)
        end
        local acks = assert(producer:produce_batch("orders", batch))
        assert.are.equal(24, #acks)

        local distinct = {}
        for _, a in ipairs(acks) do distinct[a.partition] = true end
        local n = 0
        for _ in pairs(distinct) do n = n + 1 end
        assert.is_true(n > 1, "24 distinct keys should reach more than one partition")
    end)

    it("syncs each touched partition exactly once for the whole batch", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        local topic = assert(broker:get_topic("orders"))

        -- Count request_sync calls per partition. acks=1 (AckLeader) is the
        -- mode that pays for durability, and the point of batching is that it
        -- pays once per partition rather than once per record.
        local syncs = {}
        for _, p in ipairs(topic.partitions) do
            local real = p.request_sync
            p.request_sync = function(self)
                syncs[self.id] = (syncs[self.id] or 0) + 1
                return real(self)
            end
        end

        local producer = producer_m.Producer.new(broker, producer_m.AckMode.AckLeader)
        local acks = assert(producer:produce_batch("orders", msgs(20, "same-key")))
        assert.are.equal(20, #acks)

        local total = 0
        for _, n in pairs(syncs) do total = total + n end
        assert.are.equal(1, total,
            "20 records on one partition should cost exactly one fsync")
    end)

    it("returns the durable prefix and the error when a record fails mid-batch", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local topic = assert(broker:get_topic("orders"))
        local part  = topic.partitions[1]

        local real, writes = part.write_message, 0
        part.write_message = function(self, m)
            writes = writes + 1
            if writes > 3 then return nil, "injected write failure" end
            return real(self, m)
        end

        local producer = producer_m.Producer.new(broker, 0)
        local acks, err = producer:produce_batch("orders", msgs(6, "k"))
        assert.are.equal(3, #acks)
        assert.is_truthy(err:find("injected write failure"))

        part.write_message = real
        local seen = 0
        part:scan(function() seen = seen + 1 end)
        assert.are.equal(3, seen, "the prefix must actually be on disk")
    end)

    it("rejects a batch for a topic that does not exist", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local producer = producer_m.Producer.new(broker, 0)
        local acks, err = producer:produce_batch("nope", msgs(2))
        assert.are.equal(0, #acks)
        assert.is_truthy(err)
    end)
end)

describe("PRODUCE_BATCH handler", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function records(n, prefix)
        local out = {}
        for i = 1, n do
            out[i] = { key = "k", value = (prefix or "v") .. i }
        end
        return out
    end

    it("acks a plain batch with one (partition, offset) per record", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 2))
        local server, conn = fake_server(broker), fake_conn()

        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(4)))

        local res = batch_ack(conn)
        assert.are.equal(0, res.code)
        assert.are.equal(4, #res.acks)
    end)

    it("errors when the topic is missing", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local server, conn = fake_server(broker), fake_conn()

        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("ghost", records(2)))

        assert.are.equal(proto.ERR_TOPIC_MISSING, reply_error(conn).code)
    end)

    it("charges the rate limiter one token per record", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local server, conn = fake_server(broker), fake_conn()

        local charged
        conn.rate_limiter = { take = function(_, n) charged = n; return false end }
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(7)))

        assert.are.equal(7, charged)
        assert.are.equal(proto.ERR_RATE_LIMITED, reply_error(conn).code)
    end)

    it("requires INIT_PRODUCER_ID before an idempotent batch", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local server, conn = fake_server(broker), fake_conn()

        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(2), { pid = 1, base_seq = 0 }))

        assert.are.equal(proto.ERR_NO_PRODUCER_ID, reply_error(conn).code)
    end)

    it("replays the memoized acks for an exact duplicate batch, appending nothing",
    function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 4))
        local server = fake_server(broker)

        local conn = fake_conn()
        conn.pid, conn.epoch = broker.producer_state:allocate_ephemeral(), 0

        local batch = records(5)
        local opts  = { pid = conn.pid, base_seq = 0, epoch = 0 }
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", batch, opts))
        local first = batch_ack(conn)
        assert.are.equal(0, first.code)
        assert.are.equal(5, #first.acks)

        -- Same batch, same base sequence: a retry after a lost ack.
        conn.sent = {}
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", batch, opts))
        local replay = batch_ack(conn)
        assert.are.equal(0, replay.code)
        assert.are.same(first.acks, replay.acks)

        -- Nothing was appended the second time.
        local total = 0
        for _, p in ipairs(assert(broker:get_topic("orders")).partitions) do
            p:scan(function() total = total + 1 end)
        end
        assert.are.equal(5, total)
    end)

    it("advances the sequence space by the batch size", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 2))
        local server = fake_server(broker)

        local conn = fake_conn()
        conn.pid, conn.epoch = broker.producer_state:allocate_ephemeral(), 0

        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(3), { pid = conn.pid, base_seq = 0 }))
        assert.are.equal(0, batch_ack(conn).code)

        -- The next batch must start at 3; a gap or an overlap is rejected.
        conn.sent = {}
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(2), { pid = conn.pid, base_seq = 5 }))
        assert.are.equal(proto.ERR_OUT_OF_ORDER_SEQUENCE, reply_error(conn).code)

        conn.sent = {}
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(2), { pid = conn.pid, base_seq = 1 }))
        assert.are.equal(proto.ERR_OUT_OF_ORDER_SEQUENCE, reply_error(conn).code)

        conn.sent = {}
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(2), { pid = conn.pid, base_seq = 3 }))
        assert.are.equal(0, batch_ack(conn).code)
    end)

    it("interleaves with single-record idempotent produce on one sequence counter",
    function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 2))
        local server = fake_server(broker)

        local conn = fake_conn()
        conn.pid, conn.epoch = broker.producer_state:allocate_ephemeral(), 0

        -- Batch takes sequences 0..2 ...
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(3), { pid = conn.pid, base_seq = 0 }))
        assert.are.equal(0, batch_ack(conn).code)

        -- ... so a following single record must be sequence 3.
        conn.sent = {}
        local _, _, single = unframe(proto.encode_produce_idempotent(
            uuid.bytes(), conn.pid, 3, "orders", "k", "after-batch", 0, 0))
        handlers.produce_idempotent(server, conn, uuid.bytes(), single)
        local op = only_reply(conn)
        assert.are.equal(proto.OP_PRODUCE_ACK, op)

        -- And re-sending sequence 3 replays that single record's ack. Compare
        -- payloads, not frames — each call gets a fresh correlation id.
        local _, first_ack = only_reply(conn)
        conn.sent = {}
        handlers.produce_idempotent(server, conn, uuid.bytes(), single)
        local replay_op, replay_ack = only_reply(conn)
        assert.are.equal(proto.OP_PRODUCE_ACK, replay_op)
        assert.are.equal(first_ack, replay_ack)
    end)

    it("rejects an idempotent batch larger than the memo limit before appending",
    function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local server = fake_server(broker)

        local conn = fake_conn()
        conn.pid, conn.epoch = broker.producer_state:allocate_ephemeral(), 0

        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(proto.MAX_IDEMPOTENT_BATCH + 1),
                { pid = conn.pid, base_seq = 0 }))

        assert.are.equal(proto.ERR_BATCH_TOO_LARGE, reply_error(conn).code)
        local total = 0
        for _, p in ipairs(assert(broker:get_topic("orders")).partitions) do
            p:scan(function() total = total + 1 end)
        end
        assert.are.equal(0, total, "an over-sized batch must not partially apply")
    end)

    it("fences a stale-epoch batch from a durable producer", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        local server = fake_server(broker)

        local pid, epoch = assert(broker.producer_state:get_or_create_producer("app"))
        local conn = fake_conn()
        conn.pid, conn.epoch, conn.producer_name = pid, epoch, "app"

        -- A newer session takes over the identity, bumping the epoch.
        assert(broker.producer_state:get_or_create_producer("app"))

        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", records(2),
                { pid = pid, base_seq = 0, epoch = epoch }))

        assert.are.equal(proto.ERR_PRODUCER_FENCED, reply_error(conn).code)
    end)

    it("replays a durable producer's batch acks after a broker restart", function()
        local batch = records(4)
        local acks_before

        do
            local broker = assert(brk_m.Broker.new(BASE_DIR))
            assert(broker:create_topic("orders", 4))
            local server = fake_server(broker)
            local pid, epoch = assert(broker.producer_state:get_or_create_producer("app"))

            local conn = fake_conn()
            conn.pid, conn.epoch, conn.producer_name = pid, epoch, "app"
            handlers.produce_batch(server, conn, uuid.bytes(),
                batch_payload("orders", batch, { pid = pid, base_seq = 0, epoch = epoch }))
            acks_before = batch_ack(conn).acks
            assert.are.equal(4, #acks_before)

            for _, p in ipairs(broker.topic_manager.topics["__producer_state"].partitions) do
                if p.sync then p:sync() end
            end
        end

        -- Reopen: the memo (with its per-record acks) is replayed off disk.
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local server = fake_server(broker)
        local pid = assert(broker.producer_state:pid_for("app"))
        local memo = assert(broker.producer_state:lookup_memo(pid, "orders"))
        assert.are.equal(0, memo.base_seq)
        assert.are.same(acks_before, memo.acks)

        -- The original session (epoch 0) retrying its batch gets the same acks
        -- back rather than a second copy of the records.
        local conn = fake_conn()
        conn.pid, conn.epoch, conn.producer_name = pid, memo.epoch, "app"
        handlers.produce_batch(server, conn, uuid.bytes(),
            batch_payload("orders", batch,
                { pid = pid, base_seq = 0, epoch = memo.epoch }))
        assert.are.same(acks_before, batch_ack(conn).acks)

        local total = 0
        for _, p in ipairs(assert(broker:get_topic("orders")).partitions) do
            p:scan(function() total = total + 1 end)
        end
        assert.are.equal(4, total)
    end)
end)

describe("Consumer:poll batching", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function seed(broker, topic_name, partitions, per_partition)
        assert(broker:create_topic(topic_name, partitions))
        local topic = assert(broker:get_topic(topic_name))
        for _, part in ipairs(topic.partitions) do
            for i = 1, per_partition do
                assert(part:write_message(
                    message.Message.new("k", string.format("p%d-%d", part.id, i), 1)))
            end
        end
        return topic
    end

    it("still takes one record per partition by default", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, "orders", 3, 5)

        local consumer = consumer_m.Consumer.new(broker, "g")
        assert(consumer:subscribe("orders"))
        assert.are.equal(3, #assert(consumer:poll()))
    end)

    it("takes up to max_per_partition records from each partition", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, "orders", 3, 5)

        local consumer = consumer_m.Consumer.new(broker, "g")
        assert(consumer:subscribe("orders"))
        local records = assert(consumer:poll({ max_per_partition = 4 }))
        assert.are.equal(12, #records)

        -- Per partition the records come back in log order.
        local by_partition = {}
        for _, r in ipairs(records) do
            by_partition[r.partition] = by_partition[r.partition] or {}
            local list = by_partition[r.partition]
            list[#list + 1] = r.value
        end
        for id, list in pairs(by_partition) do
            assert.are.same({
                string.format("p%d-1", id), string.format("p%d-2", id),
                string.format("p%d-3", id), string.format("p%d-4", id),
            }, list)
        end
    end)

    it("honours max_records as a hard total and shares it across partitions", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, "orders", 4, 20)

        local consumer = consumer_m.Consumer.new(broker, "g")
        assert(consumer:subscribe("orders"))
        local records = assert(consumer:poll({ max_records = 12 }))
        assert.are.equal(12, #records)

        -- 12 across 4 partitions is 3 each: no partition monopolises the batch.
        local counts = {}
        for _, r in ipairs(records) do
            counts[r.partition] = (counts[r.partition] or 0) + 1
        end
        for _, n in pairs(counts) do
            assert.is_true(n <= 3, "a partition took more than its even share")
        end
    end)

    it("resumes exactly where the previous batch stopped", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, "orders", 1, 10)

        local consumer = consumer_m.Consumer.new(broker, "g")
        assert(consumer:subscribe("orders"))

        local first  = assert(consumer:poll({ max_per_partition = 4 }))
        local second = assert(consumer:poll({ max_per_partition = 4 }))
        local third  = assert(consumer:poll({ max_per_partition = 4 }))

        local all = {}
        for _, set in ipairs({ first, second, third }) do
            for _, r in ipairs(set) do all[#all + 1] = r.value end
        end
        assert.are.equal(10, #all)
        for i = 1, 10 do
            assert.are.equal("p1-" .. i, all[i])
        end
    end)

    it("only reads partitions the member owns when computing the share", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, "orders", 4, 10)

        local consumer = consumer_m.Consumer.new(broker, "g")
        assert(consumer:subscribe("orders"))
        consumer:set_assignment({ orders = { 2 } })

        -- One owned partition out of four: the whole allowance goes to it,
        -- rather than a quarter of it.
        local records = assert(consumer:poll({ max_records = 6 }))
        assert.are.equal(6, #records)
        for _, r in ipairs(records) do
            assert.are.equal(2, r.partition)
        end
    end)
end)

describe("FETCH batching", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function seed(broker, partitions, per_partition)
        assert(broker:create_topic("orders", partitions))
        local topic = assert(broker:get_topic("orders"))
        for _, part in ipairs(topic.partitions) do
            for i = 1, per_partition do
                assert(part:write_message(
                    message.Message.new("k", string.format("p%d-%d", part.id, i), 1)))
            end
        end
    end

    local function fetch(server, conn, max_records, flags)
        local _, _, payload = unframe(proto.encode_fetch(uuid.bytes(), "orders", "g",
            max_records, proto.ISOLATION_READ_UNCOMMITTED, flags))
        handlers.fetch(server, conn, uuid.bytes(), payload)
    end

    -- Pull the records out of a reply, whichever shape it took, and assert the
    -- reply ends with the OK that closes a FETCH.
    local function collect(conn)
        local out, saw_ok = {}, false
        for _, frame in ipairs(conn.sent) do
            local op, _, payload = unframe(frame)
            if op == proto.OP_RECORD_BATCH then
                for _, r in ipairs(assert(proto.decode_record_batch(payload))) do
                    out[#out + 1] = r
                end
            elseif op == proto.OP_RECORD then
                out[#out + 1] = { value = "record-frame" }
            elseif op == proto.OP_OK then
                saw_ok = true
            end
        end
        assert.is_true(saw_ok, "FETCH must end with OK")
        return out
    end

    it("answers a batched FETCH with a single RECORD_BATCH frame", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, 2, 5)
        local server, conn = fake_server(broker), fake_conn()

        fetch(server, conn, 10, proto.FETCH_FLAG_BATCHED)

        -- Exactly two frames: the batch and the closing OK.
        assert.are.equal(2, #conn.sent)
        local op = select(1, unframe(conn.sent[1]))
        assert.are.equal(proto.OP_RECORD_BATCH, op)
        assert.are.equal(10, #collect(conn))
    end)

    it("answers an unbatched FETCH with one RECORD frame per record", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, 2, 5)
        local server, conn = fake_server(broker), fake_conn()

        fetch(server, conn, 10, 0)

        -- 10 RECORD frames + OK.
        assert.are.equal(11, #conn.sent)
        for i = 1, 10 do
            assert.are.equal(proto.OP_RECORD, select(1, unframe(conn.sent[i])))
        end
    end)

    it("delivers more than one record per partition in one FETCH", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, 2, 8)
        local server, conn = fake_server(broker), fake_conn()

        fetch(server, conn, 8, proto.FETCH_FLAG_BATCHED)

        local records = collect(conn)
        assert.are.equal(8, #records)
        local counts = {}
        for _, r in ipairs(records) do
            counts[r.partition] = (counts[r.partition] or 0) + 1
        end
        for _, n in pairs(counts) do
            assert.is_true(n > 1, "a batched fetch should drain several per partition")
        end
    end)

    it("commits once per partition and resumes there on the next FETCH", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, 2, 6)
        local server, conn = fake_server(broker), fake_conn()

        local commits = 0
        local real = broker.commit_offset
        broker.commit_offset = function(self, ...)
            commits = commits + 1
            return real(self, ...)
        end

        fetch(server, conn, 6, proto.FETCH_FLAG_BATCHED)
        local first = collect(conn)
        assert.are.equal(6, #first)
        assert.are.equal(2, commits,
            "6 records over 2 partitions should cost 2 offset commits")

        broker.commit_offset = real
        conn.sent = {}
        fetch(server, conn, 6, proto.FETCH_FLAG_BATCHED)
        local second = collect(conn)
        assert.are.equal(6, #second)

        -- The two fetches together cover all 12 records exactly once.
        local seen = {}
        for _, set in ipairs({ first, second }) do
            for _, r in ipairs(set) do
                assert.is_nil(seen[r.value], "record delivered twice: " .. r.value)
                seen[r.value] = true
            end
        end
    end)

    it("rewinds to the first undelivered record when max_records truncates", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        seed(broker, 1, 10)
        local server, conn = fake_server(broker), fake_conn()

        -- max_records=3 on a single partition: the poll may read more, and
        -- everything past the limit must come back on the next fetch.
        fetch(server, conn, 3, proto.FETCH_FLAG_BATCHED)
        assert.are.same({ "p1-1", "p1-2", "p1-3" }, values(collect(conn)))

        conn.sent = {}
        fetch(server, conn, 3, proto.FETCH_FLAG_BATCHED)
        assert.are.same({ "p1-4", "p1-5", "p1-6" }, values(collect(conn)))
    end)
end)
