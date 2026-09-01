local brk_m   = require("src.broker")
local group_m = require("src.broker.groups")

local function die(fmt, ...)
    io.stderr:write(string.format("consumer_group: " .. fmt .. "\n", ...))
    os.exit(1)
end

local DATA_DIR = os.getenv("MOONMQ_DEMO_DIR")
    or (package.config:sub(1, 1) == "\\" and os.getenv("TEMP") .. "\\moonmq_group_demo"
                                          or "/tmp/moonmq_group_demo")

local TOPIC, PARTITIONS = "orders", 4

local function show(group, label, assignment)
    local parts = assignment and assignment[TOPIC]
    local owned = parts and ("partitions {" .. table.concat(parts, ", ") .. "}") or ""
    print(string.format("  %-22s state=%-20s %s", label, group:state(), owned))
end

print(string.format("opening broker at %s ...", DATA_DIR))
local broker, berr = brk_m.Broker.new(DATA_DIR)
if not broker then die("broker: %s", berr) end

print(string.format("create_topic(%q, %d) ...", TOPIC, PARTITIONS))
local _, terr = broker:create_topic(TOPIC, PARTITIONS)
if terr then
    print(string.format("  note: %s (continuing — likely already exists)", terr))
end

local group = group_m.ConsumerGroup.new(broker, "example-group")
print("\nlifecycle:")
show(group, "new group", nil)

local a1, e1 = group:join("m1", { TOPIC })
if not a1 then die("join m1: %s", e1) end
show(group, "join m1", a1)

local a2, e2 = group:join("m2", { TOPIC })
if not a2 then die("join m2: %s", e2) end
show(group, "join m2", { [TOPIC] = group.members["m1"].partitions[TOPIC] })
show(group, "  -> m2 assignment", a2)

group.members["m1"].last_heartbeat = group.members["m1"].last_heartbeat - 31
local evicted = group:check_heartbeats()
print(string.format("  reaped stale member(s): %s", table.concat(evicted, ", ")))
show(group, "after eviction", { [TOPIC] = group.members["m2"].partitions[TOPIC] })

local ok, lerr = group:leave("m2")
if not ok then die("leave m2: %s", lerr) end
show(group, "leave m2", nil)

group:close()
show(group, "close", nil)

local _, derr = group:join("m3", { TOPIC })
print(string.format("  join after close rejected: %s", derr))

print("\ndone.")
