# Client API

`src/client` is the reference Lua client. Runnable examples:
`src/examples/tcp_client.lua`, `src/examples/consumer_group.lua`.

```lua
local Client = require("src.client")
local c = assert(Client.new{ host = "127.0.0.1", port = 9092,
                             username = "admin", password = "admin",
                             idempotent = true })

local ack   = assert(c:produce("orders", "key-1", "payload"))  -- {partition, offset, seq}
local acks, berr = c:produce_batch("orders", {               -- one frame, one fsync/partition
    { key = "k1", value = "v1" },
    { key = "k2", value = "v2" },
})                                                             -- berr set: only #acks landed
local res   = assert(c:join_group("billing", { "orders" }))    -- {member_id, assignment}
local ends  = assert(c:list_offsets("orders"))                 -- per partition {earliest, latest, …}
```

`produce` takes optional headers as a 4th argument, and `produce_batch` records
take a `headers` field. Fetched records carry `headers`. See
[headers.md](headers.md).

For a replicated cluster pass `hosts = { … }` instead of `host`/`port`; the
client follows `leader=` hints across the list. For TLS and SCRAM options see
[security.md](security.md#clients).

## Offsets and lag

`list_offsets` reports where each partition starts and ends, which is what
makes lag (`latest - committed`) computable and what lets a consumer seek to
either end. Under `read_committed` measure against `lso` instead — that is the
offset the broker will actually deliver up to.

## Seeking by time

To seek by wall-clock rather than by offset, `offsets_for_times` resolves the
earliest offset per partition whose record timestamp is at or after a given
millisecond timestamp — Kafka's `offsetForTimes`. It reads the sparse
`.timeindex` every partition already writes, so it is a short binary search
plus a bounded scan rather than a walk of the log:

```lua
local at = assert(c:offsets_for_times("orders", 1700000000000))
-- { { partition = 1, offset = 8134, found = true }, ... }
```

`found = false` means nothing at or after that time exists yet; `offset` is
then the partition's `latest`, i.e. where the first such record will land, so
"seek to T and follow" works on a partition that has not reached T yet.

## Admin

Topics can be described, reconfigured and deleted at runtime, and consumer
groups inspected:

```lua
assert(c:alter_topic_config("orders", { retention = 86400 }))  -- seconds
assert(c:create_partitions("orders", 8))                       -- grow to 8 in total
local d = assert(c:describe_group("billing"))
-- d.members[i].assignment, d.offsets[i] = { topic, partition, offset }
```

Pairing `describe_group`'s committed offsets with `list_offsets` is what makes
lag a broker-side fact rather than something a sidecar has to reconstruct. A
group holding committed offsets but no live members — an abandoned or
between-deploys consumer — shows up in `list_groups` with state `empty`, which
is usually the thing you went looking for.

`create_partitions` takes the new **total**, not the number to add, and only
grows. Existing records stay where they are, and consumer groups rebalance
right away so the new partitions get a reader. A key's partition is
`hash(key) % count`, so after growing, new records for a key may land on a
different partition than its older ones. Ordering per key holds only from
that point on, as in Kafka.

## Reference

Every method returns `nil, err` on failure. `produce_batch` is the one
exception: on a partial failure it returns the acks that did land **and** the
error, so check the second value rather than wrapping it in `assert`.

### `Client.new` options

| Option | Default | Meaning |
| --- | --- | --- |
| `host`, `port` | `127.0.0.1`, `9092` | Broker to connect to |
| `hosts` | — | List of `"host:port"`; replaces `host`/`port` and follows `leader=` hints across it |
| `username`, `password` | — | Credentials; omit for an open broker |
| `mechanism` | `plain` | `plain`, `scram-sha-256` or `external` (certificate login) — see [security.md](security.md) |
| `tls` | — | `true` or a table (`cafile`, `certfile`, `keyfile`, `server_name`, `insecure`) |
| `channel_binding` | on with TLS | `false` disables it; `"tls-server-end-point"` pins the older binding type |
| `timeout` | 30 | Socket timeout in seconds |
| `compression` | `none` | `none`, `gzip` or `snappy` for produced records |
| `isolation` | `read_uncommitted` | `read_committed` hides aborted and open transactions — see [transactions.md](transactions.md) |
| `idempotent` | `false` | Ask for a producer id so retries are deduplicated |
| `producer_name` | — | Durable producer identity; required for transactions |
| `client_name`, `client_version` | — | Reported to the broker with `IDENTIFY_CLIENT` |
| `reactor` | — | Run on a MoonMQ reactor instead of blocking sockets |

### Methods

| Method | Returns |
| --- | --- |
| `produce(topic, key, value[, headers])` | `{partition, offset, seq}` |
| `produce_batch(topic, records)` | list of acks, plus an error on partial failure |
| `produce_at_seq(topic, key, value, seq[, headers])` | ack; idempotent clients only, for retrying a known sequence |
| `fetch(topic[, group[, max_records[, batched]]])` | list of records (`max_records` defaults to 100) |
| `subscribe(topic[, group])` · `next_record([timeout])` | `true` · the next pushed record, or `nil, "timeout"` |
| `commit(topic, partition, offset)` | `true` |
| `nack(topic, partition, offset[, reason])` | NACK result — see [dlq.md](dlq.md) |
| `create_topic(name, partitions)` · `delete_topic(name)` | `true` |
| `list_topics()` · `describe_topic(name)` | topic list · topic description |
| `alter_topic_config(name, config)` · `create_partitions(name, total)` | `true` |
| `list_offsets(topic)` · `offsets_for_times(topic, ms)` | per-partition offsets |
| `join_group(group, topics[, member_id])` | `{member_id, assignment}`; remembered for the calls below |
| `group_heartbeat([group, member_id])` · `leave_group([group, member_id])` | `true` |
| `list_groups()` · `describe_group(group)` · `delete_group(group)` | group list · description · `true` |
| `begin_transaction()` · `commit_transaction()` · `abort_transaction()` | `true`; needs `producer_name` |
| `send_offsets_to_transaction(group, {{topic, partition, offset}, …})` | `true` |
| `close()` | — |

`Client.decode_dlq_value(value)` unpacks the envelope of a dead-lettered record.
