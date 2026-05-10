-- Smoke test for the existing Nuntius code.
-- Exercises: Broker, Producer, Consumer, BatchWriter, PartitionWriter.
-- Run from the project root:  lua main.lua

local message      = require("src.message")
local broker_m     = require("src.broker")
local producer_m   = require("src.producer")
local consumer_m   = require("src.consumer")
local batch_writer = require("src.batch_writer")
local partition_m  = require("src.partition")

local DATA_DIR = "./data_test"

local function header(s) print(("\n=== %s ==="):format(s)) end
local function fail(msg) print("FAIL:", msg); os.exit(1) end

math.randomseed(os.time())

-- ---------------------------------------------------------------------------
-- 1. Broker bootstrap
-- ---------------------------------------------------------------------------
header("Bootstrapping broker at " .. DATA_DIR)

local broker, berr = broker_m.Broker.new(DATA_DIR)
if not broker then
    fail(berr)
    return
end

local topic_name = ("smoke-%d"):format(math.random(100000, 999999))
header("Creating topic '" .. topic_name .. "' with 3 partitions")

local _, cerr = broker:create_topic(topic_name, 3)
if cerr then fail(cerr) end

local topic, gerr = broker:get_topic(topic_name)
if not topic then fail(gerr) end
print("partitions:", #topic.partitions)

-- ---------------------------------------------------------------------------
-- 2. Producer: send N messages, fan out across partitions via FNV-1a hash
-- ---------------------------------------------------------------------------
header("Producing 6 messages")

local producer = producer_m.Producer.new(broker, 1)   -- acks=1: flush after write

local sent = {}
for i = 1, 6 do
    local key   = ("key-%d"):format(i)
    local value = ("payload-%d"):format(i)
    local msg   = message.Message.new(key, value, os.time())
    local pid, off, perr = producer:produce(topic_name, msg)
    if perr then fail(perr) end
    sent[#sent+1] = { key = key, value = value, partition = pid, offset = off }
    print(("  -> partition=%d offset=%d key=%s"):format(pid, off, key))
end

-- ---------------------------------------------------------------------------
-- 3. Consumer: subscribe and drain. poll() returns at most one msg/partition.
-- ---------------------------------------------------------------------------
header("Consuming")

local consumer = consumer_m.Consumer.new(broker, "smoke-group")
local sok, serr = consumer:subscribe(topic_name)
if not sok then fail(serr) end

local seen = {}
local total = 0
local empty_polls = 0
while empty_polls < 3 do
    local records, perr = consumer:poll()
    if perr then fail(perr) end
    if #records == 0 then
        empty_polls = empty_polls + 1
    else
        empty_polls = 0
        for _, r in ipairs(records) do
            print(("  <- partition=%d offset=%d key=%s value=%s"):format(
                r.partition, r.offset, r.key, r.value))
            seen[r.key] = r.value
            total = total + 1
        end
    end
end

local missing = 0
for _, s in ipairs(sent) do
    if seen[s.key] ~= s.value then
        print(("  MISSING: %s -> %s"):format(s.key, s.value))
        missing = missing + 1
    end
end
if missing > 0 then fail(("missing %d message(s)"):format(missing)) end
print(("OK: produced=%d consumed=%d"):format(#sent, total))

-- ---------------------------------------------------------------------------
-- 4. BatchWriter: synchronous batched append
-- ---------------------------------------------------------------------------
header("BatchWriter")

local p1 = topic.partitions[1]
local off_before_batch = p1.offset

local bw = batch_writer.new(p1, 4096, 100)
for i = 1, 5 do
    local m = message.Message.new(
        ("bk-%d"):format(i),
        ("bv-%d"):format(i),
        os.time())
    local aok, aerr = bw:add(m)
    if not aok then fail(aerr) end
end
local fok, ferr = bw:flush()
if not fok then fail(ferr) end

local bytes_written = p1.offset - off_before_batch
print(("flushed 5 messages to partition 1 (%d bytes)"):format(bytes_written))
if bytes_written <= 0 then fail("BatchWriter:flush did not advance partition offset") end

-- ---------------------------------------------------------------------------
-- 5. PartitionWriter: async batching via tiny coroutine scheduler
-- ---------------------------------------------------------------------------
header("PartitionWriter (async batching)")

-- Minimal scheduler: synchronous, just forwards to coroutine.resume.
local scheduler = {
    resume = function(co, ...) return coroutine.resume(co, ...) end,
}

local p2 = topic.partitions[2]
local off_before_pw = p2.offset

local pw = partition_m.PartitionWriter.new(p2, {
    max_size     = 64 * 1024,
    max_messages = 10,
    flush_every  = 0.01,
})

local writer_co = coroutine.create(function() pw:run(scheduler) end)
local wok, werr = coroutine.resume(writer_co)
if not wok then fail("writer coroutine: " .. tostring(werr)) end

-- Submitter helper: runs the producer side in its own coroutine so it can yield.
local results = {}
local function submit(label)
    local co = coroutine.create(function()
        local m = message.Message.new(label, label .. "-value", os.time())
        local off, errs = pw:submit(scheduler, m)
        results[#results+1] = { label = label, offset = off, err = errs }
    end)
    local sok, se = coroutine.resume(co)
    if not sok then fail("submit coroutine: " .. tostring(se)) end
end

for i = 1, 4 do
    submit(("pw-%d"):format(i))
end

-- Stop drains the in-memory batch and resumes every queued submitter
-- with its real on-disk offset (or the write error).
pw:stop(scheduler)

if #results ~= 4 then fail(("expected 4 results, got %d"):format(#results)) end
for _, r in ipairs(results) do
    if r.err then fail(("submit '%s' failed: %s"):format(r.label, r.err)) end
    print(("  pw label=%s offset=%d"):format(r.label, r.offset))
end

local pw_bytes = p2.offset - off_before_pw
print(("partition 2 advanced %d bytes"):format(pw_bytes))
if pw_bytes <= 0 then fail("PartitionWriter did not advance partition offset") end

-- ---------------------------------------------------------------------------
-- 6. Cleanup
-- ---------------------------------------------------------------------------
header("Closing partitions")
for _, p in ipairs(topic.partitions) do
    p:close()
end

print("\nALL SMOKE TESTS PASSED")
