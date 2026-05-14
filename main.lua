-- main.lua — Nuntius end-to-end example.
--
-- Nuntius is a Kafka-style message broker. This file is the project's
-- runnable example: it wires the modules under src/ together into one
-- narrative — a small "order-events" pipeline — and narrates each stage.
--
-- Run from the project root:
--     lua5.4 main.lua
--
-- Where each module is exercised:
--   message, crc32, io_sync, fs, util, time  — transitively, throughout
--   broker, topic, topic_manager             — [1]  bootstrap
--   producer, partition                      — [2]  produce (synchronous)
--   future                                   — [3]  produce (async, via produce_async)
--   groups                                   — [4]  consumer-group rebalance
--   consumer                                 — [5]  consume
--   batch_writer                             — [6]  synchronous batching
--   partition.PartitionWriter                — [7]  asynchronous batching
--   partition.write_with_resilience          — [8]  retrying transient I/O errors
--   buffer                                   — [9]  accumulating byte buffer
--   segmentation                             — [10] rolling log segments
--   compression, snappy                      — [11] compression (optional dep)
--   replica                                  — [12] replication (optional dep)
--   crash_recovery                           — [13] recovery on restart

local message      = require("src.message")
local broker_m     = require("src.broker")
local producer_m   = require("src.producer")
local consumer_m   = require("src.consumer")
local groups_m     = require("src.groups")
local batch_writer = require("src.batch_writer")
local partition_m  = require("src.partition")
local segmentation = require("src.segmentation")
local Buffer       = require("src.buffer")

local DATA_DIR = "./data_test"

-- ---------------------------------------------------------------------------
-- Narration helpers
-- ---------------------------------------------------------------------------
local function banner(text)
    print("============================================================")
    print(" " .. text)
    print("============================================================")
end
local function step(n, title) print(string.format("\n[%d] %s", n, title)) end
local function note(s)         print("    " .. s) end
local function fail(msg)
    io.stderr:write("\nDEMO FAILED: " .. tostring(msg) .. "\n")
    os.exit(1)
end
local function check(cond, msg) if not cond then fail(msg) end end

-- A trivial synchronous scheduler: `resume` just forwards to
-- coroutine.resume. It is enough to drive the Future / PartitionWriter /
-- ReplicatedPartition APIs in a single-threaded example.
local scheduler = {
    resume = function(co, ...) return coroutine.resume(co, ...) end,
}

math.randomseed(os.time())
local suffix     = math.random(100000, 999999)
local topic_name = string.format("orders-%d", suffix)

banner("Nuntius — a Kafka-style message broker in Lua")
print(" End-to-end demo: an order-events pipeline")

-- ---------------------------------------------------------------------------
-- [1] Bootstrap the broker and create a topic
-- ---------------------------------------------------------------------------
step(1, "Bootstrapping the broker at " .. DATA_DIR)

local broker, berr = broker_m.Broker.new(DATA_DIR)
check(broker, berr)

local _, cerr = broker:create_topic(topic_name, 4)
check(not cerr, cerr)

local topic, gerr = broker:get_topic(topic_name)
check(topic, gerr)
note(string.format("created topic '%s' with %d partitions",
    topic_name, #topic.partitions))

-- ---------------------------------------------------------------------------
-- [2] Produce a stream of order events (synchronous, acks=1)
-- ---------------------------------------------------------------------------
step(2, "Producing order events (synchronous, acks=1)")

local producer = producer_m.Producer.new(broker, producer_m.AckMode.AckLeader)

local ORDERS = {
    { key = "alice", value = "order#1001 widget x2"   },
    { key = "bob",   value = "order#1002 gadget x1"   },
    { key = "carol", value = "order#1003 widget x5"   },
    { key = "alice", value = "order#1004 sprocket x3" },
    { key = "dave",  value = "order#1005 gadget x2"   },
    { key = "bob",   value = "order#1006 widget x1"   },
    { key = "carol", value = "order#1007 sprocket x1" },
    { key = "erin",  value = "order#1008 gadget x4"   },
}

local produced = 0
for _, o in ipairs(ORDERS) do
    local msg = message.Message.new(o.key, o.value, os.time())
    local pid, off, perr = producer:produce(topic_name, msg)
    check(not perr, perr)
    produced = produced + 1
    note(string.format("%-6s -> partition %d  offset %d", o.key, pid, off))
end
note(string.format("%d events produced synchronously", produced))

-- ---------------------------------------------------------------------------
-- [3] Produce a couple more events asynchronously (Future)
-- ---------------------------------------------------------------------------
step(3, "Producing late events asynchronously (Future)")

local LATE_ORDERS = {
    { key = "frank", value = "order#1009 widget x2" },
    { key = "alice", value = "order#1010 gadget x1" },
}

for _, o in ipairs(LATE_ORDERS) do
    local msg    = message.Message.new(o.key, o.value, os.time())
    local future = producer:produce_async(scheduler, topic_name, msg)
    -- With the synchronous scheduler the produce coroutine runs to
    -- completion inside produce_async, so the Future is already resolved
    -- by the time it returns; await() hands back its value immediately.
    local result = future:await()
    check(not result.error, result.error)
    produced = produced + 1
    note(string.format("%-6s -> partition %d  offset %d  (async)",
        o.key, result.partition, result.offset))
end
note(string.format("%d events produced in total", produced))

-- ---------------------------------------------------------------------------
-- [4] A consumer group rebalances partitions across its members
-- ---------------------------------------------------------------------------
step(4, "Consumer group: assigning partitions across members")

local function fmt_assignment(a)
    local out = {}
    for t, pids in pairs(a) do
        out[#out + 1] = string.format("%s=[%s]", t, table.concat(pids, ","))
    end
    return table.concat(out, "  ")
end

local cg = groups_m.ConsumerGroup.new(broker, "orders-cg")

local a1, jerr1 = cg:join("worker-A", { topic_name })
check(a1, jerr1)
note("worker-A joined  -> " .. fmt_assignment(a1))

local a2, jerr2 = cg:join("worker-B", { topic_name })
check(a2, jerr2)
note("worker-B joined  -> group rebalanced:")
note("  worker-A now " .. fmt_assignment(cg.members["worker-A"].partitions))
note("  worker-B now " .. fmt_assignment(cg.members["worker-B"].partitions))

cg:leave("worker-B")
note("worker-B left    -> worker-A now "
    .. fmt_assignment(cg.members["worker-A"].partitions))

-- ---------------------------------------------------------------------------
-- [5] Consume the topic and verify nothing was lost
-- ---------------------------------------------------------------------------
step(5, "Consuming the topic")

local consumer = consumer_m.Consumer.new(broker, "orders-cg")
consumer.auto_commit = false   -- commit once at the end instead of per record

local sok, serr = consumer:subscribe(topic_name)
check(sok, serr)

local consumed     = 0
local empty_polls  = 0
while empty_polls < 3 do
    local records, perr = consumer:poll()
    check(records, perr)
    if #records == 0 then
        empty_polls = empty_polls + 1
    else
        empty_polls = 0
        for _, r in ipairs(records) do
            consumed = consumed + 1
            note(string.format("partition %d  offset %-3d  %-6s  %s",
                r.partition, r.offset, r.key, r.value))
        end
    end
end

check(consumed == produced,
    string.format("consumed %d but produced %d", consumed, produced))
note(string.format("consumed %d / %d events — nothing lost", consumed, produced))

note("committing consumer-group offsets:")
local ccok, ccerr = consumer:commit_offsets()
check(ccok, ccerr)

-- ---------------------------------------------------------------------------
-- [6] BatchWriter: synchronous batched append + fsync
-- ---------------------------------------------------------------------------
step(6, "BatchWriter: batching writes to a single partition")

local p1        = topic.partitions[1]
local p1_before = p1.offset

local bw = batch_writer.new(p1, 4096, 100)
for i = 1, 5 do
    local m = message.Message.new(
        string.format("audit-%d", i),
        string.format("audit log entry %d", i),
        os.time())
    local aok, aerr = bw:add(m)
    check(aok, aerr)
end
local fok, ferr = bw:flush()
check(fok, ferr)
note(string.format("flushed 5 records to partition 1 (%d bytes, fsynced)",
    p1.offset - p1_before))

-- ---------------------------------------------------------------------------
-- [7] PartitionWriter: asynchronous batching via coroutines
-- ---------------------------------------------------------------------------
step(7, "PartitionWriter: async batching via a coroutine scheduler")

local p2        = topic.partitions[2]
local p2_before = p2.offset

local pw = partition_m.PartitionWriter.new(p2, {
    max_size     = 64 * 1024,
    max_messages = 10,
    flush_every  = 0.01,
})

local writer_co = coroutine.create(function() pw:run(scheduler) end)
local wok, werr = coroutine.resume(writer_co)
check(wok, werr)

local pw_results = {}
local function submit(label)
    local co = coroutine.create(function()
        local m = message.Message.new(label, label .. " payload", os.time())
        local off, errs = pw:submit(scheduler, m)
        pw_results[#pw_results + 1] = { label = label, offset = off, err = errs }
    end)
    local ok, e = coroutine.resume(co)
    check(ok, e)
end

for i = 1, 4 do submit(string.format("metric-%d", i)) end
pw:stop(scheduler)   -- drains the batch, resumes every queued submitter

check(#pw_results == 4,
    string.format("expected 4 async results, got %d", #pw_results))
for _, r in ipairs(pw_results) do
    check(not r.err, r.err)
    note(string.format("%-9s -> offset %d", r.label, r.offset))
end
note(string.format("partition 2 advanced %d bytes", p2.offset - p2_before))

-- ---------------------------------------------------------------------------
-- [8] Partition:write_with_resilience — retry transient I/O errors
-- ---------------------------------------------------------------------------
step(8, "Resilient writes: retrying transient I/O errors")

local p4        = topic.partitions[4]
local p4_before = p4.offset

for i = 1, 3 do
    local m = message.Message.new(
        string.format("resilient-%d", i),
        string.format("resilient payload %d", i),
        os.time())
    -- write_with_resilience retries write_message up to 3x on transient
    -- errors (EAGAIN / EINTR / EBUSY / ...), backing off 0.1s then 0.2s
    -- between tries; a non-retryable error is returned immediately. The
    -- happy path here succeeds on the first attempt.
    local off, rerr = p4:write_with_resilience(m)
    check(off and off >= 0, rerr)
    note(string.format("resilient-%d -> partition 4 offset %d", i, off))
end
note(string.format("partition 4 advanced %d bytes (transient errors retried)",
    p4.offset - p4_before))

-- ---------------------------------------------------------------------------
-- [9] Buffer: a simple accumulating byte buffer
-- ---------------------------------------------------------------------------
step(9, "Buffer: accumulating serialized records in memory")

local buf = Buffer.new()
for i = 1, 4 do
    local m = message.Message.new("buf-" .. i, "buffered value " .. i, os.time())
    buf:write(message.serialize_message(m))
end
note(string.format("buffer holds %d bytes across 4 records", buf:len()))
check(#buf:bytes() == buf:len(), "buffer length disagrees with its bytes")
buf:reset()
note(string.format("after reset: %d bytes", buf:len()))

-- ---------------------------------------------------------------------------
-- [10] SegmentedPartition: a log that rolls into multiple segments
-- ---------------------------------------------------------------------------
step(10, "SegmentedPartition: rolling log segments")

local seg_dir   = string.format("%s/segmented-%d", DATA_DIR, suffix)
local sp, sperr = segmentation.SegmentedPartition.new(topic, 1, seg_dir)
check(sp, sperr)

-- Shrink the segment cap so a handful of writes forces several rolls.
sp.max_segment_size = 256

for i = 1, 12 do
    local m = message.Message.new(
        string.format("seg-%d", i),
        string.format("segmented payload %d", i),
        os.time())
    local off, werr2 = sp:write_message(m)
    check(off, werr2)
end
note(string.format("wrote 12 records -> %d segment file(s)", #sp.segments))
check(#sp.segments > 1, "expected the log to roll into multiple segments")

sp:tick_cleaner()         -- drive the retention-cleaner coroutine once
sp:clean_old_segments()   -- run a retention sweep (nothing old enough yet)
sp:stop_cleaner()
for _, s in ipairs(sp.segments) do s:close() end
note("segments closed, retention cleaner stopped")

-- ---------------------------------------------------------------------------
-- [11] Compression (optional — needs the 'zlib' rock; 'snappy' needs LuaJIT)
-- ---------------------------------------------------------------------------
step(11, "Compression: gzip round-trip")

local comp_ok, comp = pcall(require, "src.compression")
if comp_ok then
    local original = string.rep("the quick brown fox ", 8)
    local m        = message.Message.new("c-key", original, os.time())
    local cm, ce   = comp.CompressedMessage.new(m, comp.CompressionType.CompressionGzip)
    check(cm, ce)
    note(string.format("gzip: %d bytes -> %d bytes", #original, #cm.value))
    local restored, de = cm:decompress()
    check(restored, de)
    check(restored.value == original, "decompressed value does not match original")
    note("decompressed value matches the original")
else
    note("skipped — compression module unavailable (install the 'zlib' rock)")
end

-- ---------------------------------------------------------------------------
-- [12] Replication (optional — leader-side write path, single process)
-- ---------------------------------------------------------------------------
step(12, "Replication: leader-side write path")

local repl_ok, repl = pcall(require, "src.replica")
if repl_ok then
    local p3        = topic.partitions[3]
    local p3_before = p3.offset
    -- No peer addresses configured: this exercises the leader's local-write
    -- path. Real replication ships each record to a peer's /replicate HTTP
    -- endpoint, which needs a server process Nuntius does not yet include.
    local rp = repl.ReplicatedPartition.new(p3, 1, true, {}, {})
    rp:start()
    for i = 1, 2 do
        local m = message.Message.new(
            string.format("repl-%d", i),
            string.format("replicated payload %d", i),
            os.time())
        local off, rerr = rp:write(scheduler, m)
        check(off and off >= 0, rerr)
        note(string.format("repl-%d -> partition 3 offset %d", i, off))
    end
    rp:close(scheduler)
    note(string.format("partition 3 advanced %d bytes (0 peers configured)",
        p3.offset - p3_before))
else
    note("skipped — replica module unavailable")
end

-- ---------------------------------------------------------------------------
-- [13] Crash recovery: reopen the broker and re-scan the logs
-- ---------------------------------------------------------------------------
step(13, "Crash recovery: reopening the broker on the same data dir")

local pre_offsets = {}
for i, p in ipairs(topic.partitions) do
    pre_offsets[i] = p.offset
    p:close()
end
note("closed all partitions of the original broker")

local broker2, b2err = broker_m.Broker.new(DATA_DIR)
check(broker2, b2err)
-- Broker.new -> load_topics ran crash_recovery on every partition file
-- before reopening it: each log's tail was scanned and CRC-validated.
local topic2, t2err = broker2:get_topic(topic_name)
check(topic2, t2err)

for i, p in ipairs(topic2.partitions) do
    check(p.offset == pre_offsets[i], string.format(
        "partition %d offset changed across restart: %d -> %d",
        i, pre_offsets[i], p.offset))
end
note(string.format("topic '%s' recovered — all %d partition offsets intact",
    topic_name, #topic2.partitions))

-- ---------------------------------------------------------------------------
-- [14] Done
-- ---------------------------------------------------------------------------
step(14, "Done")
for _, p in ipairs(topic2.partitions) do p:close() end
note(string.format(
    "pipeline complete: %d events produced, %d consumed, logs recovered cleanly",
    produced, consumed))

print()
banner("Nuntius demo finished — all checks passed")
