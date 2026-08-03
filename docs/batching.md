# Record batching

Before batching, one record cost one frame, one dispatch, one round trip, and
— under `acks=1` — one fsync. Everything about that scales linearly with record
count, and the fsync dominates: a client producing sequentially can't benefit
from the group committer, which only coalesces fsyncs across *concurrent*
producers.

Batching amortises all four. `PRODUCE_BATCH` carries N records in one frame and
is answered by one `PRODUCE_BATCH_ACK`; `Producer:produce_batch` issues **one
fsync per partition the batch touched**, not one per record. On the read side a
`FETCH` can ask for its records in a single `RECORD_BATCH` frame, and the
consumer now drains several records per partition per poll instead of exactly
one.

Measured on the reference dev box (4-partition topic, `acks=leader`, loopback
TCP, `lua5.4`), 400 records one at a time vs. one batch:

| | 400 records |
| --- | --- |
| `produce()` in a loop | ~2.2 s |
| `produce_batch()` | ~0.03 s |

That ratio is mostly fsync amortisation, so it narrows with `acks=0` and widens
on slower storage. Treat it as an order-of-magnitude claim, not a benchmark.

## What a batch is and isn't

A batch is **N ordinary appends that share a frame, a dispatch, and a
fsync**. It is deliberately *not* an atomic unit:

- Records are partitioned individually, by the same key-hash / sticky
  partitioner `PRODUCE` uses. One batch may fan out across every partition of
  the topic. Records sharing a key still land together, in order.
- A failure part-way through leaves the prefix **durably appended** and
  reports the error alongside the acks for that prefix. The client resends the
  tail.
- Ordering within a partition is preserved: records are appended in list order.

If you need all-or-nothing, wrap the batch in a transaction — a batch produced
inside `BEGIN_TXN`/`END_TXN` enrols each partition it touches exactly once, and
the whole transaction commits or aborts together.

## Wire format

### `PRODUCE_BATCH` (0x17)

```
u8  flags          bit0 = IDEMPOTENT
u8  codec          compression codec applied to every value (0 none, 1 gzip, 2 snappy)
str topic
u64 pid            ┐
u32 base_seq       ├ present only when flags has IDEMPOTENT
u16 epoch          ┘
u32 count
(str key | str value) * count
```

### `PRODUCE_BATCH_ACK` (0x8B)

```
u32 count
(u32 partition | u64 offset) * count
u16 err_code       0 = every record landed
str err_message    empty when err_code is 0
```

`count` is always a **prefix** of the request: records past it were not
written. A non-zero code with `count > 0` is a partial append.

### `RECORD_BATCH` (0x8C)

```
u32 count
(str topic | u32 partition | u64 offset | u64 timestamp | str key | str value) * count
```

Same per-record fields as `RECORD`, minus N-1 frame headers.

### `FETCH` flags

`FETCH` gained an optional `u8 flags` byte *after* the optional isolation byte.
Set `FETCH_FLAG_BATCHED` (0x01) to ask for a `RECORD_BATCH` reply. A broker
that predates batching stops decoding at isolation and answers with the legacy
one-`RECORD`-frame-per-record stream, so setting the bit is safe against any
broker version — and the client handles both shapes.

## Limits

| Bound | Value | Why |
| --- | --- | --- |
| Frame size | `Server.MaxFrameSize` (1 MiB default) | The framer rejects the frame before decoding; this is the real limit on batch bytes. |
| Records per batch | 10 000 | Count bound checked before any allocation, so a malformed frame can't ask the broker to size a table for an attacker-chosen count. |
| Records per **idempotent** batch | 1 024 (`ERR_BATCH_TOO_LARGE`, code 14) | An idempotent batch's per-record acks are persisted (below); 1 024 records is a ~12 KB memo record, the most we'll write per batch. Nothing is appended when this fires — split and resend. |

## Idempotent batches

An idempotent batch extends the single-record contract rather than replacing
it: record *i* carries sequence `base_seq + i`, so the per-`(pid, topic)`
counter advances by `count`. Batches and single-record produces interleave
freely on one counter.

Dedup is at **batch granularity**:

| Incoming batch `base..base+N-1` | Result |
| --- | --- |
| `base == last_seq + 1` | Fresh — append all. |
| Exact resend of the last batch (same `base_seq`, same length) | Replay the memoized acks. Nothing is appended. |
| Anything else overlapping consumed sequence space | `ERR_OUT_OF_ORDER_SEQUENCE` |

Replaying a duplicate needs the batch's per-record acks, and they cannot be
reconstructed from a base offset: a batch spans partitions, and offsets are
opaque per-backend cursors (on the segmented backend they are byte positions,
so `offset + 1` is meaningless). The acks are therefore stored verbatim.

For a **durable** (named) producer they go into the `__producer_state` memo as
a v3 tail appended to the existing value:

```
<v2 value: last_seq | last_offset | last_partition | epoch> | u32 base_seq | u32 count | (u32 partition | u64 offset)*
```

The tail is strictly additive — `string.unpack` ignores trailing bytes, so a
reader that only knows the v2 layout parses a v3 value exactly as before, and a
single-record produce keeps writing plain v2 values. That is what lets batch
and single produces interleave on one `(pid, topic)` with no special cases, and
what makes the memo survive a broker restart: reopen the broker and an
in-flight batch retry still replays its original acks.

A **partial** append memoizes the prefix, so the client's resend of the tail
starts at `last_seq + 1` and is treated as fresh rather than as a duplicate.

Ephemeral (unnamed) idempotent producers keep the same state on the connection
instead of on disk, exactly as they do for single records.

## Consumer side

`Consumer:poll(opts)` takes:

- `max_per_partition` — records to take from one partition before moving on.
  **Defaults to 1**, which is the historical behaviour every caller that passes
  no options keeps.
- `max_records` — hard total for the poll. Given without `max_per_partition`,
  the per-partition allowance is derived as an even share across the partitions
  this poll would actually read (subscribed, owned under the current group
  assignment, still served locally), so a busy partition can't monopolise a
  fixed-size batch.

`FETCH` passes its `max_records` straight through, which is why a fetch now
returns a real batch: previously `max_records=100` on a 4-partition topic
returned at most 4 records.

Push mode (`SUBSCRIBE`) drains up to `push_batch` (256) records per pass. Its
frames are unchanged — one `RECORD` per record, which is what every subscriber
already expects — but a backlogged partition no longer drains at one record per
`push_interval`.

Offsets are committed **once per partition** per fetch instead of once per
record. The contract is unchanged (commit strictly after the send layer
accepted the record, at-least-once); the committed value is read after every
rewind has been applied, so it always names the first record the group has not
been handed.

## Client API

```lua
local acks, err = c:produce_batch("orders", {
    { key = "k1", value = "v1" },
    { key = "k2", value = "v2" },
})
-- acks[i] = { partition, offset, seq? }, aligned with the first #acks records.
```

A plain list of strings is also accepted and treated as values with empty keys.
On an idempotent client the sequence counter advances by `#acks` — so after a
partial failure, resending the tail lands as fresh records.

`Client:fetch(topic, group, max_records)` asks for a batched reply by default.
Pass a fourth argument of `false` to force the legacy per-record frame shape.
