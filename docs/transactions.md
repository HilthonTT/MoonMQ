# Transactions in MoonMQ

Status: **partial**. Session-scoped idempotent producer landed 2026-06-21.
Full Kafka-style transactions (`read_committed` isolation, cross-partition
atomicity, exactly-once semantics across consumer + producer) are
deferred — they require a consumer-group coordinator and durable offset
storage, neither of which exists yet.

This document records:

1. What ships today: session-scoped idempotent producer.
2. What is intentionally NOT in scope today.
3. The design surface for the deferred full-transaction system, so the
   next pass can pick it up without re-discovering the constraints.

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

- `read_committed` consumer isolation level.
- Last Stable Offset (LSO) tracking on partitions.
- COMMIT / ABORT control records in the log.
- Cross-partition atomic commit.
- Transactional offset-commit (offsets written as part of the same txn
  as the records they cover).
- `__transaction_state` coordinator topic.
- PID expiry & PID generation epochs.
- Producer fencing across reconnects (require last-known epoch).

---

## 3. Deferred design: full Kafka-style transactions

The work below is blocked on two prerequisites:

- **Consumer subsystem.** Today `commit_offset` and `load_offset` are
  no-ops (see `src/broker/consumer.lua`). Until offsets are durable, the
  transactional offset commit can't be implemented.
- **`__consumer_offsets` topic.** Real Kafka stores committed offsets
  in an internal topic with the same replication and storage path as
  user topics. We need that internal-topic mechanism in place.

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
