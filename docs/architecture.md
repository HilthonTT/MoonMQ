# MoonMQ architecture

A guided tour for new contributors: how a record travels from a producer's
socket to disk and back out to a consumer, and which module owns each step.
Read this next to the [Code layout](../README.md#code-layout) table.

## The one rule

**Dependencies point downward.** `server` → `broker` → `storage`/`commitlog`
→ `record`/`io`, with `core`/`log`/`metrics` as cross-cutting utilities.
Nothing in a lower layer requires an upper one. If your change needs an
upward reference, pass a callback or attach a factory (see
`Broker:attach_committer_factory` for the pattern).

## Process model

One process, one OS thread, one **reactor** (`src/server/reactor.lua`) —
a cooperative coroutine scheduler over `luasocket`'s select loop. Every
connection gets reader/sender/heartbeat coroutines; background jobs (group
reaper, retention cleaner tick, balance loop) are just more coroutines.
There are no locks: atomicity between yield points is the concurrency model.
Anything that blocks the loop for long must either yield periodically (see
auth's PBKDF2 slicing) or be tolerated as a design decision (inter-broker
HTTP calls are blocking, documented as LAN-appropriate).

## Produce path

```
client ──frame──> server/connection.lua   (framing, state machine, deadlines)
                  server/handlers.lua     (opcode → handler; PRODUCE decodes,
                                           compresses value if requested)
                  broker/producer.lua     (partition pick: keyed FNV-1a or
                                           sticky; acks handling)
                  cluster/router.lua      (only in a cluster: forward to the
                                           partition's owner if not local)
                  storage/segmentation.lua  or  storage/commitlog_partition.lua
                                          (append + fsync policy)
```

* `acks=none` — ack after the in-memory append.
* `acks=leader` — ack after fsync. Concurrent producers coalesce into ONE
  fsync via the group committer (`storage/group_committer.lua`).
* `acks=all` — additionally wait until every configured follower's LEO
  covers the record (`server/replicator.lua`).

## Storage

Two interchangeable backends behind one duck-typed partition interface
(`write_message` / `read_message` / `oldest_offset` / `offset` / `sync` /
`scan` / `close`), selected per topic:

* **segmented** (`storage/segmentation.lua`, default): byte-offset log.
  Segment files roll at a size threshold; a sparse timeindex maps timestamps
  to file positions; retention deletes whole aged-out segments; recovery
  CRC-verifies the unclean tail and truncates torn records.
* **commitlog** (`src/commitlog/`, jocko-style): message-offset log with a
  dense offset index per segment, byte-budget retention, and key compaction
  (latest record per key survives; offsets renumber — see the caveat in
  `compact_cleaner.lua`).

The on-disk record format (`src/record/message.lua`) is shared:
`len(8) | header(13) | hdr_crc(4) | key | value | payload_crc(4)`, with an
attrs byte carrying the compression codec and the transaction-control flag.

Internal topics (name prefix `__`) reuse the same machinery: consumer
offsets (`__consumer_offsets`), producer state (`__producer_state`),
transaction state (`__transaction_state`). They run on the commitlog backend
because compaction — not time retention — is the right bound for
latest-value-per-key state.

## Fetch / subscribe path

Pull (`FETCH`) polls up to one record per partition per call; push
(`SUBSCRIBE`) runs a per-connection delivery coroutine. Both commit an
offset only **after** the record is handed to the send layer — at-least-once
delivery. Consumer groups (`broker/groups.lua`, an FSM) partition topics
across members; the coordinator reaps members that stop heartbeating.

## Durable producer identities & transactions

`INIT_PRODUCER_ID` with a `producer_name` gives a producer a stable PID
whose epoch bumps each session (stale sessions are fenced). Idempotent
produce dedups on (PID, topic, seq) *within an epoch* — a reconnect restarts
sequences at 0, KIP-360 style. Transactions
(`broker/txn_coordinator.lua`) buffer offset commits and write COMMIT/ABORT
control markers to participants; recovery rolls prepared decisions forward
and aborts abandoned ones. There is no `read_committed` isolation — aborted
data records remain visible; only markers are hidden.

## Replication vs clustering

Two orthogonal, both static-config features:

* **Replication** (`server/replicator.lua` + `replica_server.lua`):
  single-leader full-copy for durability. The leader ships every record to
  followers over `POST /replicate`; `acks=all` blocks on follower LEOs.
  No election.
* **Clustering** (`src/cluster/` + `src/autobalancer/`): partition
  *placement* for load distribution — ownership table, live partition
  migration, produce forwarding, autobalancer. See [cluster.md](cluster.md).

## Where to start reading

| If you want to understand… | Start at |
| --- | --- |
| the event loop | `src/server/reactor.lua` (260 lines, self-contained) |
| a request's lifecycle | `src/server/connection.lua`, then `handlers.lua` |
| the wire protocol | `src/wire/protocol.lua` (every frame documented) |
| the disk format | `src/record/message.lua`, then `storage/segmentation.lua` |
| crash recovery | `storage/segment_verify.lua` + `Broker:load_topics` |
| the balancer | `src/autobalancer/init.lua`, then `docs/cluster.md` |
