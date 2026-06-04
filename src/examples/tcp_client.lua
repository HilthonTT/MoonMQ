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
-- Defaults match appsettings.json's dev credential (Username "admin",
-- PasswordHash for plaintext "admin"). Override either via env in real
-- deployments. Leaving MOONMQ_USER blank skips AUTH entirely — only
-- useful if the broker is in OPEN mode.
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

-- 1. Create the topic. Re-running the example is fine: we surface
--    the error but keep going if it already exists.
print(string.format("create_topic(%q, %d) ...", topic, partitions))
local ok, err = client:create_topic(topic, partitions)
if not ok then
    print(string.format("  note: create_topic returned: %s", err))
    print("  (continuing — the topic likely already exists)")
else
    print("  ok.")
end

-- 2. Produce a single record.
local key   = "k1"
local value = string.format("hello at %d", os.time())
print(string.format("produce(%q, %q, %q) ...", topic, key, value))
local ack, perr = client:produce(topic, key, value)
if not ack then die("produce: %s", perr) end
print(string.format("  ack partition=%d offset=%d", ack.partition, ack.offset))

-- 3. List topics — the freshly-created one must appear.
print("list_topics() ...")
local names, lerr = client:list_topics()
if not names then die("list_topics: %s", lerr) end
table.sort(names)
print(string.format("  %d topic(s): %s", #names, table.concat(names, ", ")))

-- 4. Fetch records from the group's current position.
print(string.format("fetch(%q, %q, 10) ...", topic, group))
local records, ferr = client:fetch(topic, group, 10)
if not records then die("fetch: %s", ferr) end
print(string.format("  got %d record(s):", #records))
for i, r in ipairs(records) do
    print(string.format("    [%d] partition=%d offset=%d key=%q value=%q",
        i, r.partition, r.offset, r.key, r.value))
end

-- 5. Commit the offset just past the produced record so the group
--    won't re-deliver it on the next fetch.
print(string.format("commit(%q, %d, %d) ...", topic, ack.partition, ack.offset + 1))
local cok, cerr2 = client:commit(topic, ack.partition, ack.offset + 1)
if not cok then die("commit: %s", cerr2) end
print("  ok.")

-- 6. Clean shutdown — sends GOODBYE and closes the socket.
client:close()
print("done.")
