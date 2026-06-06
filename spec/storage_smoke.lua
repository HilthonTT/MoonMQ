-- Standalone Lua 5.4 smoke test for the persisted-config + timestamp-index
-- storage slice. Bypasses busted (the launcher on this box only ships for
-- 5.1; our code uses 5.3+ features) so we can verify against the real
-- runtime the broker uses.
--
-- Run with:  lua5.4 spec/storage_smoke.lua
--
-- Exit code 0 on all-pass, 1 on first failure (with a short trace).

package.path = "./?.lua;./?/init.lua;" .. package.path

local fs_m              = require("src.fs")
local Topic             = require("src.topic")
local seg_m             = require("src.segmentation")
local SegmentedPartition = seg_m.SegmentedPartition
local message           = require("src.message")
local topic_config      = require("src.topic_config")
local TopicManager      = require("src.topic_manager")
local broker_m          = require("src.broker")
local os_utils          = require("src.utils.os")

local BASE = os_utils.IS_WINDOWS
    and "C:\\Temp\\moonmq_storage_smoke"
    or  "/tmp/moonmq_storage_smoke"

local function rmtree(p)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', p:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", p))
    end
end

-- Tiny test harness so output reads sensibly.
local failures = 0
local function check(cond, msg)
    if not cond then
        failures = failures + 1
        io.stderr:write("FAIL: " .. msg .. "\n")
    else
        io.stdout:write("  ok  " .. msg .. "\n")
    end
end

local function section(name)
    io.stdout:write("\n== " .. name .. " ==\n")
end

-- ---------------------------------------------------------------------------
section("topic_config round-trip")

rmtree(BASE)
fs_m.mkdir(BASE)
local dir1 = fs_m.join_path(BASE, "t1")
fs_m.mkdir(dir1)

local ok, err = topic_config.save(dir1, {
    max_segment_size = 4096,
    retention = 60,
    cleaner_interval = 5,
})
check(ok, "save returns ok; err=" .. tostring(err))

local loaded, lerr = topic_config.load(dir1)
check(loaded ~= nil, "load returns table; err=" .. tostring(lerr))
if loaded then
    check(loaded.max_segment_size == 4096, "max_segment_size round-trips")
    check(loaded.retention == 60, "retention round-trips")
    check(loaded.cleaner_interval == 5, "cleaner_interval round-trips")
end

-- Missing sidecar → empty opts, no error.
local dir2 = fs_m.join_path(BASE, "t2")
fs_m.mkdir(dir2)
local empty, eerr = topic_config.load(dir2)
check(empty ~= nil and next(empty) == nil and eerr == nil,
    "missing sidecar yields empty opts, no err")

-- Malformed sidecar → error.
local dir3 = fs_m.join_path(BASE, "t3")
fs_m.mkdir(dir3)
local mf = io.open(fs_m.join_path(dir3, "topic.config"), "wb")
mf:write("max_segment_size=not_a_number\n")
mf:close()
local bad, berr = topic_config.load(dir3)
check(bad == nil and berr ~= nil, "malformed sidecar surfaces err")

-- Unknown keys ignored.
local dir4 = fs_m.join_path(BASE, "t4")
fs_m.mkdir(dir4)
local uf = io.open(fs_m.join_path(dir4, "topic.config"), "wb")
uf:write("max_segment_size=4096\nfuture_key=99\n")
uf:close()
local fwd, fwd_err = topic_config.load(dir4)
check(fwd ~= nil and fwd.max_segment_size == 4096 and fwd.future_key == nil,
    "unknown key ignored; err=" .. tostring(fwd_err))

-- ---------------------------------------------------------------------------
section("Broker restart preserves per-topic config")

rmtree(BASE)
local b1, b1err = broker_m.Broker.new(BASE)
check(b1 ~= nil, "broker created; err=" .. tostring(b1err))

local _, cerr = b1:create_topic("orders", 2, {
    max_segment_size = 256,   -- tiny — forces roll on a few messages
    retention        = 3600,
    cleaner_interval = 600,
})
check(cerr == nil, "create_topic with opts; err=" .. tostring(cerr))

-- Sidecar landed on disk.
local sidecar = io.open(fs_m.join_path(BASE, "orders/topic.config"), "rb")
check(sidecar ~= nil, "topic.config exists on disk")
if sidecar then sidecar:close() end

-- Write enough to roll segment 0.
local topic_obj = b1.topic_manager.topics["orders"]
local p1 = topic_obj.partitions[1]
for i = 1, 10 do
    local m = message.Message.new("k" .. i, string.rep("x", 50), 1000 + i)
    p1:write_message(m)
end
check(#p1.segments >= 2,
    "tiny max_segment_size rolled active segment (segments=" .. #p1.segments .. ")")

-- Close the broker properly so .clean-shutdown gets written.
for _, t in pairs(b1.topic_manager.topics) do
    for _, p in ipairs(t.partitions) do p:close() end
end

-- Reopen and verify the config came back.
local b2, b2err = broker_m.Broker.new(BASE)
check(b2 ~= nil, "broker reopened; err=" .. tostring(b2err))
local p_reloaded = b2.topic_manager.topics["orders"].partitions[1]
check(p_reloaded.max_segment_size == 256,
    "max_segment_size persisted: got " .. tostring(p_reloaded.max_segment_size))
check(p_reloaded.retention == 3600, "retention persisted")
check(p_reloaded.cleaner_interval == 600, "cleaner_interval persisted")

for _, t in pairs(b2.topic_manager.topics) do
    for _, p in ipairs(t.partitions) do p:close() end
end

-- ---------------------------------------------------------------------------
section("Broker handles a topic with NO sidecar (upgrade path)")

rmtree(BASE)
local b3 = broker_m.Broker.new(BASE)
b3:create_topic("legacy", 1)  -- no opts → no sidecar written
local no_sidecar = io.open(fs_m.join_path(BASE, "legacy/topic.config"), "rb")
check(no_sidecar == nil, "no sidecar written when opts omitted")

-- Write a message so the partition has on-disk state.
local lp = b3.topic_manager.topics["legacy"].partitions[1]
lp:write_message(message.Message.new("k", "v", 42))
for _, t in pairs(b3.topic_manager.topics) do
    for _, p in ipairs(t.partitions) do p:close() end
end

local b4, b4err = broker_m.Broker.new(BASE)
check(b4 ~= nil, "broker reopens with sidecar-less topic; err=" .. tostring(b4err))
local lp2 = b4.topic_manager.topics["legacy"].partitions[1]
check(lp2.max_segment_size == 1024 * 1024 * 1024,
    "sidecar-less topic gets default max_segment_size")

for _, t in pairs(b4.topic_manager.topics) do
    for _, p in ipairs(t.partitions) do p:close() end
end

-- ---------------------------------------------------------------------------
section("Timestamp index writes entries")

rmtree(BASE)
fs_m.mkdir(BASE)
local td = fs_m.join_path(BASE, "tsidx")
fs_m.mkdir(td)
local topic = Topic.new("tsidx")
local p, perr = SegmentedPartition.new(topic, 1, td, {
    -- Force an index entry on every message: interval = 1 byte.
    index_interval_bytes = 1,
})
check(p ~= nil, "partition opened; err=" .. tostring(perr))

local offsets = {}
for i = 1, 5 do
    local m = message.Message.new("k", "v" .. i, 1000 + i * 10)
    local off, werr = p:write_message(m)
    check(werr == nil and off ~= nil, "write_message " .. i .. " ok")
    offsets[i] = off
end

local idx_path = fs_m.join_path(td, "partition-1/00000000000000000000.timeindex")
local f = io.open(idx_path, "rb")
check(f ~= nil, "timeindex file exists")
if f then
    local body = f:read("*a")
    f:close()
    check(#body == 5 * 12,
        "5 entries x 12 bytes = " .. (5*12) .. ", got " .. #body)
end

-- ---------------------------------------------------------------------------
section("offset_for_timestamp edge cases")

-- Use the same partition. Timestamps are 1010, 1020, 1030, 1040, 1050.

-- Exact match.
local hit_exact = p:offset_for_timestamp(1030)
check(hit_exact == offsets[3],
    "exact match: expected offsets[3]=" .. offsets[3] ..
    " got " .. tostring(hit_exact))

-- Between entries — should return the earliest message with ts >= target.
local hit_between = p:offset_for_timestamp(1025)
check(hit_between == offsets[3],
    "between 1020 and 1030: expected offsets[3]=" .. offsets[3] ..
    " got " .. tostring(hit_between))

-- Before first → first offset.
local hit_before = p:offset_for_timestamp(0)
check(hit_before == offsets[1],
    "before-first: expected offsets[1]=" .. offsets[1] ..
    " got " .. tostring(hit_before))

-- After last → nil.
local hit_after = p:offset_for_timestamp(9999)
check(hit_after == nil,
    "after-last: expected nil, got " .. tostring(hit_after))

p:close()

-- ---------------------------------------------------------------------------
section("Index survives segment roll")

rmtree(BASE)
fs_m.mkdir(BASE)
local td2 = fs_m.join_path(BASE, "roll")
fs_m.mkdir(td2)
local topic2 = Topic.new("roll")
local pr, prerr = SegmentedPartition.new(topic2, 1, td2, {
    max_segment_size     = 80,   -- forces roll every ~1 message
    index_interval_bytes = 1,
})
check(pr ~= nil, "partition opened; err=" .. tostring(prerr))

local roll_offsets = {}
for i = 1, 4 do
    local m = message.Message.new("k", "vvvv" .. i, 2000 + i * 10)
    roll_offsets[i] = (pr:write_message(m))
end
check(#pr.segments >= 2,
    "rolled into multiple segments: " .. #pr.segments)

-- Lookup across segments.
local across = pr:offset_for_timestamp(2030)
check(across == roll_offsets[3],
    "cross-segment lookup: expected " .. roll_offsets[3] ..
    " got " .. tostring(across))

pr:close()

-- ---------------------------------------------------------------------------
io.stdout:write(string.format("\nfailures: %d\n", failures))
os.exit(failures == 0 and 0 or 1)
