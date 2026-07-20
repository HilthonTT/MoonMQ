# Dead-letter queue

MoonMQ's baseline redelivery is implicit at-least-once: an offset is only
committed after a record is handed to the send layer, so an unprocessed record
simply comes back on the next poll. That loops forever on a *poison record* —
one the consumer can never process. The dead-letter queue (DLQ) breaks the
loop.

## How it works

1. A consumer that fails to process a delivered record sends **`OP_NACK`**
   (`topic | partition | offset | reason`), where `offset` is the record's own
   offset from the `OP_RECORD` frame.
2. The broker counts attempts per `(group, topic, partition, offset)`
   (`src/broker/dlq.lua`):
   * **Below `MaxDeliveries`** — the group's committed offset is rewound to
     the record so it is redelivered. The `NACK_ACK` reply carries
     `dead_lettered = false` and the attempt count.
   * **At `MaxDeliveries`** — the record is appended to the topic's
     dead-letter topic (`<topic>.dlq` by default, created lazily with the same
     partition count, same partition id), the counter is cleared, and the
     group's offset advances past the record. The reply carries
     `dead_lettered = true` and the DLQ topic name.
3. The DLQ record keeps the **original key** (partitioning/compaction still
   work) and wraps the **original value in an envelope**
   (`src/record/dlq_envelope.lua`) carrying its provenance: source topic,
   partition, offset, timestamp, the group that gave up, the attempt count,
   and the client-supplied reason. The value is stored decompressed. The v2
   record format has no named headers, which is why provenance lives in the
   value.

A dead-letter topic is an ordinary, visible topic — subscribe to it like any
other to build reprocessing or alerting.

## Client API

```lua
local Client = require("src.client")
local c = Client.new({ port = 9092 })

local records = c:fetch("orders", "billing", 10)
for _, r in ipairs(records) do
    local ok, why = process(r)
    if not ok then
        local res = c:nack(r.topic, r.partition, r.offset, why)
        -- res.dead_lettered, res.attempts, res.dlq_topic
    end
end

-- Consuming the DLQ:
for _, r in ipairs(c:fetch("orders.dlq", "dlq-inspector", 10)) do
    local env = Client.decode_dlq_value(r.value)
    -- env.topic/partition/offset/timestamp, env.group, env.attempts,
    -- env.reason, env.value (the original payload)
end
```

## Configuration

`appsettings.json`:

```json
"Server": {
  "Dlq": { "Suffix": ".dlq", "MaxDeliveries": 3 }
}
```

`MaxDeliveries = 1` dead-letters on the first NACK. Metrics:
`moonmq_nack_total` and `moonmq_dlq_records_total` (both labelled by topic).

## Semantics and limits

* **Attempt counters are in-memory.** A broker restart (or eviction from the
  bounded counter map) forgets them; the record is then redelivered up to
  `MaxDeliveries` more times before dead-lettering. At-least-once, never lost.
* A NACK is validated like a COMMIT: it must come from a live group member on
  the broker that serves the partition, and the offset must point at a
  readable, non-control record.
* The dead-letter append is durable (fsynced) before the `NACK_ACK` is sent,
  and the group only advances after the append — a crash can duplicate a DLQ
  record, never lose one.
* Cluster: NACKs go to the broker serving the source partition; the DLQ record
  is written on that same broker.
