local Client = require("src.client")

local function getenv(name, default)
    local v = os.getenv(name)
    if v == nil or v == "" then return default end
    return v
end

local function die(fmt, ...)
    io.stderr:write(string.format("tcp_client: " .. fmt .. "\n", ...))
    os.exit(1)
end

local host       = getenv("MOONMQ_HOST", "127.0.0.1")
local port       = tonumber(getenv("MOONMQ_PORT", "9092"))
local username   = getenv("MOONMQ_USER", "admin")
local password   = getenv("MOONMQ_PASS", "admin")
local topic      = getenv("MOONMQ_TOPIC", "demo")
local partitions = tonumber(getenv("MOONMQ_PARTITIONS", "3"))
local group      = "example-group"

print(string.format("connecting to %s:%d ...", host, port))
local client, cerr = Client.new{
    host           = host,
    port           = port,
    client_name    = "tcp_client_example",
    client_version = "0.1.0",
    username       = username,
    password       = password,
}
if not client then die("%s", cerr) end
print("connected.")

print(string.format("create_topic(%q, %d) ...", topic, partitions))
local ok, err = client:create_topic(topic, partitions)
if not ok then
    print(string.format("  note: create_topic returned: %s", err))
    print("  (continuing — the topic likely already exists)")
else
    print("  ok.")
end

local key   = "k1"
local value = string.format("hello at %d", os.time())
print(string.format("produce(%q, %q, %q) ...", topic, key, value))
local ack, perr = client:produce(topic, key, value)
if not ack then die("produce: %s", perr) end
print(string.format("  ack partition=%d offset=%d", ack.partition, ack.offset))

print("list_topics() ...")
local names, lerr = client:list_topics()
if not names then die("list_topics: %s", lerr) end
table.sort(names)
print(string.format("  %d topic(s): %s", #names, table.concat(names, ", ")))

print(string.format("fetch(%q, %q, 10) ...", topic, group))
local records, ferr = client:fetch(topic, group, 10)
if not records then die("fetch: %s", ferr) end
print(string.format("  got %d record(s):", #records))
for i, r in ipairs(records) do
    print(string.format("    [%d] partition=%d offset=%d key=%q value=%q",
        i, r.partition, r.offset, r.key, r.value))
end

print(string.format("commit(%q, %d, %d) ...", topic, ack.partition, ack.offset + 1))
local cok, cerr2 = client:commit(topic, ack.partition, ack.offset + 1)
if not cok then die("commit: %s", cerr2) end
print("  ok.")

print("opening idempotent producer ...")
local iclient, ierr = Client.new{
    host           = host,
    port           = port,
    client_name    = "tcp_client_example_idempotent",
    client_version = "0.1.0",
    username       = username,
    password       = password,
    idempotent     = true,
}
if not iclient then die("idempotent client: %s", ierr) end
print(string.format("  assigned producer_id=%d", iclient.pid))

local itopic = topic
local ikey   = "idem-key"
local ivalue = string.format("idempotent at %d", os.time())

print(string.format("produce(%q, %q, %q) [seq=0] ...", itopic, ikey, ivalue))
local iack1, ie1 = iclient:produce(itopic, ikey, ivalue)
if not iack1 then die("idempotent produce 1: %s", ie1) end
print(string.format("  ack partition=%d offset=%d seq=%d",
    iack1.partition, iack1.offset, iack1.seq))

print(string.format("retry at seq=0 (expect same offset back) ..."))
local iack2, ie2 = iclient:produce_at_seq(itopic, ikey, ivalue, 0)
if not iack2 then die("idempotent retry: %s", ie2) end
if iack2.partition == iack1.partition and iack2.offset == iack1.offset then
    print(string.format("  dedup OK — broker returned original offset %d",
        iack2.offset))
else
    die("dedup FAILED: got partition=%d offset=%d, expected %d/%d",
        iack2.partition, iack2.offset, iack1.partition, iack1.offset)
end

print(string.format("send seq=42 (gap; expect error) ..."))
local _, ie3 = iclient:produce_at_seq(itopic, ikey, ivalue, 42)
if ie3 then
    print(string.format("  expected error: %s", ie3))
else
    die("out-of-order seq was accepted; broker dedup is broken")
end

iclient:close()
print("idempotent demo done.")

print(string.format("join_group(%q, {%q}) ...", group, topic))
local jres, jerr = client:join_group(group, { topic })
if not jres then die("join_group: %s", jerr) end
local owned = jres.assignment[topic] or {}
print(string.format("  member_id=%s owns partitions {%s}",
    jres.member_id, table.concat(owned, ", ")))

print("group_heartbeat() ...")
local hok, herr = client:group_heartbeat()
if not hok then die("group_heartbeat: %s", herr) end
print("  lease renewed.")

print("leave_group() ...")
local lgok, lgerr = client:leave_group()
if not lgok then die("leave_group: %s", lgerr) end
print("  left group.")

client:close()
print("done.")
