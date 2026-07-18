# Transactions in MoonMQ

Status: **read_committed isolation + Last Stable Offset shipped** (2026-07-18),
completing the exactly-once story on top of atomic multi-partition transactions
(2026-07-10) and the session-scoped idempotent producer (2026-06-21).

What now ships:

- **Durable producers** (`src/storage/producer_state.lua`): a producer opened
  with a stable `producer_name` (Kafka's `transactional_id`) gets a PID + epoch
  that survive reconnects and broker restarts. Each new session bumps the epoch,
  fencing the old one (`ERR_PRODUCER_FENCED`). Idempotent-produce sequence memos
  are persisted, so a retry replays the original ack across a restart.
- **Atomic multi-partition transactions** (`src/broker/txn_coordinator.lua`):
  `BEGIN_TXN` / transactional `PRODUCE_IDEMPOTENT` / `TXN_OFFSET_COMMIT` /
  `END_TXN`. On commit the coordinator writes COMMIT control records to every
  participant partition and applies buffered offset commits; on abort it writes
  ABORT markers and drops the offsets. State lives in the internal
  `__transaction_state` topic and is re-driven idempotently on crash recovery
  (ONGOING → abort, PREPARE_COMMIT → roll forward).
- **`read_committed` isolation + Last Stable Offset** (2026-07-18). How it fits
  together:
  * Transactional data records carry the producer session in the record header
    (`ATTR_TXN` bit + pid/epoch — see `src/record/message.lua`), so a reader
    can attribute any record to its transaction.
  * The produce path enrols a partition with the coordinator BEFORE the
    transaction's first append there, capturing the partition's LEO as the
    txn's `first_offset` — the lower bound of everything it writes there.
  * **LSO** = min `first_offset` over unresolved transactions touching the
    partition (`Coordinator:lso`); a `read_committed` consumer never reads at
    or past it, so every record it does read belongs to a resolved txn.
  * On abort, the coordinator durably records the range
    `[first_offset, abort_marker)` per participant in the **abort index**
    (`src/broker/abort_index.lua`, `<data_dir>/txn-aborts.json`); below the
    LSO, records matching an aborted `(pid, epoch, range)` are filtered out.
    Entries are pruned once retention deletes their whole range.
  * On the wire, `FETCH`/`SUBSCRIBE` take an optional trailing isolation byte
    (0 = read_uncommitted — the default, and what old clients send; 1 =
    read_committed). The Lua client: `Client.new{ isolation = "read_committed" }`.
  * Boundary: transactional produce to a partition owned by a peer broker is
    refused (`Producer:produce`) — markers could not be written on the owner.
  * Boundary: abort-index ranges are offset-based, so they assume offsets are
    stable — true on the segmented backend (the default for user topics; its
    retention deletes whole segments without renumbering). Don't run
    transactions against a `commitlog`-backend topic with
    `cleanup_policy=compact`: compaction renumbers surviving records, which
    would break the aborted-range filter.

This document records:

1. What ships today.
2. What is intentionally NOT in scope today.
3. The original design surface, retained for context.

---

## 1. Today: idempotent producer (session-scoped)

### Guarantees

Within the lifetime of a single TCP connection that called
`INIT_PRODUCER_ID`, the broker guarantees:

- A `PRODUCE_IDEMPOTENT` retry with the same `(PID, topic, seq)` returns
  the original `(partition, offset)` instead of appending a duplicate.
- An out-of-order `seq` (gap or regression) is rejected with
  `ERR_OUT_OF_ORDER_SEQUENCE` so the client can detect lost messages.

### Non-guarantees

- **No durability across the broker process.** PIDs and sequence memos
  live in broker memory. A restart wipes them; clients must call
  `INIT_PRODUCER_ID` again on reconnect.
- **No cross-connection idempotence.** Two connections with the same
  client identity get two distinct PIDs; the broker can't dedup
  retries that move across connections.
- **No transaction boundary.** Multiple `PRODUCE_IDEMPOTENT` calls do
  NOT form an atomic group; a crash mid-batch leaves a partial write.

### Wire format

Opcodes (additive on PROTOCOL_VERSION 1):

| Op                          | Direction | Payload                                                |
|----------------------------|-----------|--------------------------------------------------------|
| `OP_INIT_PRODUCER_ID` 0x0E | C → S     | empty                                                  |
| `OP_PRODUCER_ID` 0x88      | S → C     | `u64 pid`                                              |
| `OP_PRODUCE_IDEMPOTENT` 0x0F | C → S   | `u64 pid \| u32 seq \| str topic \| str key \| str value` |

Acks reuse `OP_PRODUCE_ACK` (0x82) with the original `(partition, offset)`.
Errors use `OP_ERROR` with codes:

| Code | Name                       | Meaning                                                |
|-----:|----------------------------|--------------------------------------------------------|
| 10   | `ERR_NO_PRODUCER_ID`       | `PRODUCE_IDEMPOTENT` sent before `INIT_PRODUCER_ID`.   |
| 11   | `ERR_OUT_OF_ORDER_SEQUENCE` | seq not equal to last_seq or last_seq + 1.            |

### Why per-(PID, topic) and not per-(PID, partition)?

Real Kafka tracks sequences per `(PID, topic, partition)`. We track per
`(PID, topic)` because:

- TCP guarantees per-connection in-order delivery, so a single monotonic
  counter across partitions is internally consistent.
- The client doesn't have to know the broker's partition count or
  hashing function to pick the right seq slot.
- It costs: idempotent producers can't batch across partitions in
  parallel (we don't anyway).

The full Kafka layout is the natural upgrade path when consumer groups
+ offset commits land (see §3).

### Client API

```lua
local Client = require("src.client")

-- Open a connection AND ask the broker for a producer ID.
local c = Client.new{
    host       = "127.0.0.1",
    port       = 9092,
    username   = "admin",
    password   = "admin",
    idempotent = true,
}

-- produce() auto-uses the idempotent path when pid is set.
local ack = c:produce("orders", "key-1", "value-1")
-- ack.seq is the sequence number the broker dedup-keyed on.

-- For testing: send a specific seq (e.g. simulate a retry).
local replay = c:produce_at_seq("orders", "key-1", "value-1", 0)
-- replay.partition == ack.partition, replay.offset == ack.offset
```

---

## 2. Out of scope (today)

Still deferred:

- PID expiry / garbage-collecting idle producer state.
- Transactional produce to a partition owned by a peer broker (cluster mode).

Now implemented (were previously out of scope):

- COMMIT / ABORT control records in the log (v2 record format, control bit).
- Cross-partition atomic commit via the transaction coordinator.
- Transactional offset-commit (`TXN_OFFSET_COMMIT`, applied on commit).
- `__transaction_state` coordinator topic + crash recovery.
- PID generation epochs and producer fencing across reconnects.
- `read_committed` consumer isolation level (2026-07-18).
- Last Stable Offset (LSO) tracking on partitions (2026-07-18).
- Filtering aborted records out at read time via the durable abort index
  (2026-07-18).

---

## 3. Original design: full Kafka-style transactions

Everything below has since landed (including `read_committed` + LSO as of
2026-07-18); the section is retained as the design rationale.

- **Consumer subsystem.** `commit_offset` / `load_offset` are now durable
  (`src/broker/consumer.lua` → `OffsetManager`).
- **`__consumer_offsets` topic.** Implemented (`src/storage/offset_manager.lua`);
  `__producer_state` and `__transaction_state` follow the same internal-topic
  pattern.

### Components

1. **Transaction coordinator.** A new broker subsystem that owns
   `__transaction_state`. Producers route `INIT_PRODUCER_ID` to the
   coordinator (today: the server assigns inline). The coordinator
   tracks `(transactional_id → (PID, epoch, state))` durably.

2. **Producer epochs.** PIDs become 2-field: `(pid: u64, epoch: u16)`.
   On reconnect with a known `transactional_id`, the coordinator bumps
   epoch and fences older sessions (rejects writes with stale epoch
   via `ERR_PRODUCER_FENCED`).

3. **Control records.** Every transactional log append carries a
   marker bit. The coordinator writes `COMMIT` / `ABORT` control
   records to every affected partition at txn boundary. The records
   are written through the same log path so consumers see them.

4. **Read-committed isolation.** Consumers in `read_committed` mode
   stop at the **Last Stable Offset (LSO)** — the smallest offset
   above which any partition has uncommitted transactional records.
   Aborted records below LSO are filtered out at read time using the
   ABORT markers.

5. **Transactional offset commit.** `OFFSET_COMMIT_REQUEST` carries the
   PID + epoch + sequence; the offset commit becomes part of the txn
   and only takes effect when the COMMIT control record lands.

### Opcodes (proposed)

| Op                              | Notes                                |
|---------------------------------|--------------------------------------|
| `INIT_TRANSACTIONAL_ID`         | Negotiates PID + epoch for txn-id   |
| `BEGIN_TXN`                     | Starts a transaction                |
| `ADD_PARTITIONS_TO_TXN`         | Coordinator-tracked participation   |
| `END_TXN`                       | COMMIT / ABORT                      |
| `TXN_OFFSET_COMMIT`             | Transactional offset commit         |
| `WRITE_TXN_MARKERS` (internal)  | Coordinator → partition leaders     |

### Error codes (proposed)

| Code  | Name                              |
|------:|-----------------------------------|
| 50    | `ERR_PRODUCER_FENCED`             |
| 51    | `ERR_INVALID_TXN_STATE`           |
| 52    | `ERR_TRANSACTION_TIMED_OUT`       |
| 53    | `ERR_OFFSETS_OUT_OF_RANGE_FOR_TXN`|

### Storage shape

- `__transaction_state` partition-of-N stores
  `(transactional_id, pid, epoch, state, participating_partitions[],
  last_update_ms, timeout_ms)`.
- Per-partition log adds a 1-bit transactional flag in the record
  header (claim one of the high bits of `key_size`, or add a v2
  record format with a leading control byte).

### Migration

Plan:

1. Land consumer groups + durable offset commit (blocking).
2. Add `__consumer_offsets` and `__transaction_state` as internal
   topics (broker-managed, not user-creatable).
3. Add v2 record format with the control flag.
4. Wire coordinator + new opcodes.
5. Extend the consumer to honour `read_committed` + LSO.
6. Once all above land, the in-memory PID table from §1 becomes a
   short-circuit: producers with `transactional_id == nil` keep the
   session-scoped behaviour; producers with a txn-id go through the
   coordinator.

---

## References

- Kafka KIP-98: Exactly Once Delivery and Transactional Messaging
- Kafka KIP-129: Streams EOS
- Confluent's `Transactions in Apache Kafka` blog post (2017)
