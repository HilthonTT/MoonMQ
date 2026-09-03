# Clustering, partition reassignment & the autobalancer

MoonMQ's cluster layer (`src/cluster/`) turns a set of independent brokers
into a statically-configured cluster that can **move partitions between
brokers at runtime** — either by hand (drive the `Reassigner` yourself) or
automatically via the AutoMQ-style autobalancer (`src/autobalancer/`).

It follows the same philosophy as MoonMQ's replication: static membership
from config, HTTP for inter-broker transport, no consensus protocol, and
every boundary documented rather than hidden.

## The pieces

```
src/autobalancer/          the DECISION engine (what should move where)
  model/                   live cluster model: brokers, replicas, windowed
                           load samples (p90 over a ring buffer)
  goals/                   distribution goals, run in priority order against
                           a mutable snapshot: NW_IN, NW_OUT, DISK,
                           PARTITION_COUNT (mean ± deviation banding)
  detector/                AnomalyDetector: freeze model → run goals →
                           emit a plan (list of MOVE/SWAP Actions)

src/cluster/               the EXECUTION layer (actually moving them)
  assignments.lua          durable topic-partition → broker-id ownership
                           table (sparse JSON sidecar in data_dir; absent
                           entry = owned locally)
  peer.lua                 HTTP client for one peer broker
  cluster_server.lua       the inter-broker endpoint each broker serves
  reassigner.lua           executes a plan: per MOVE, migrate the data +
                           committed offsets and flip ownership
  router.lua               produce-path routing: forward records for
                           partitions owned elsewhere
  balance_loop.lua         periodic feed-model → detect → execute glue,
                           fenced by a claimed controller epoch
  controller_fence.lua     durable highest-controller-epoch tracking; what
                           stops two balance loops from duelling
```

## What a MOVE does

`Reassigner:_move(topic, partition, dest)` runs six steps:

1. **ensure** — dest creates the topic if missing (same partition count).
2. **copy** — records stream to dest in ~256 KiB batches over
   `POST /cluster/append`. The loop yields between batches, so produce and
   fetch traffic interleave with a long migration.
3. **cutover** — the local ownership entry flips to dest and is persisted.
   From this instant the produce router forwards new records for this
   partition to dest. Under the cooperative reactor the flip is exact: no
   local write can begin after it.
4. **drain** — records that landed locally *during* the copy are streamed
   across. Because of step 3 the tail is stable; the drain terminates. If
   the drain fails, ownership is rolled back and the partition stays local.
5. **offsets** — every group's committed offset for the partition is pushed
   to dest over `POST /cluster/offsets` (higher-wins per group on dest), so
   consumers resume where they left off instead of restarting. The local
   COMMIT handler refuses commits for a partition this broker no longer
   serves, so the snapshot taken here is complete. A failed push degrades to
   the old start-over behaviour and is logged loudly, but does not roll back
   the move — the data is already safe on dest.
6. **confirm** — dest records that it owns the partition (clears any stale
   entry on its side).

The destination partition must be **empty**: records keep their byte offsets
only when dest appends from zero. A non-empty destination is refused.

A SWAP is executed as the local half only (a one-way MOVE) — the
counterpart's data lives on the other broker, which this broker can't push.

## Produce routing

`Producer:produce` consults the ownership table after picking a partition.
Owned locally → normal local append. Owned by a peer → the serialized record
is forwarded to that peer's cluster endpoint, and the ack carries the offset
on the *owner's* log. Clients keep talking to whichever broker they
connected to; no client changes are needed.

Consumers on the source broker stop being served a moved partition the
moment the cutover flips (`Broker:serves_partition`); consume it from the
new owner.

## The autobalancer loop

`balance_loop.lua` runs on **one** broker per cluster. That used to be pure
convention; it is now **fenced** (`src/cluster/controller_fence.lua`): the
loop claims a controller epoch (persisted at `<data_dir>/controller-epoch.json`)
and re-announces it to every peer each pass over
`POST /cluster/controller/claim`. Every control-plane request the reassigner
sends (`/cluster/ensure`, `/cluster/owner`, `/cluster/offsets`) carries the
epoch (`X-Controller-Epoch`/`X-Controller-Id` headers), and a broker that has
seen a newer claim refuses it with 409 — so if a second broker
starts the loop, exactly one survives: the elder claimant's next pass is
rejected and it stops acting (permanently, until an operator restarts it).
Data-plane forwards (`/cluster/append`, group and txn routes) never carry the
epoch: a broker whose balance loop was superseded must keep forwarding
produces to partition owners.
This is fencing, not consensus — two brokers claiming simultaneously against
disjoint reachable peers can both act until their claims meet; requests
without epoch headers (hand-driven reassignment, old peers) bypass the fence.

Each pass:

1. Verify/announce controllership (above).
2. Feed the model: local partitions' disk bytes + every peer's
   `GET /cluster/loads` report. An unreachable peer is marked inactive for
   the pass (the balancer will never move data *toward* a broker it can't
   reach). Loads now carry cumulative per-partition produce/consume byte
   counters (`src/metrics/traffic.lua`); the loop differences successive
   reports into **NW_IN / NW_OUT byte rates**, so the network goals act on
   real signal. Rates start flowing from the second pass; a counter reset
   (broker restart) primes a fresh baseline instead of feeding a negative.
3. Run the detector: goals execute in priority order against a shared
   snapshot; the result is a plan of MOVE actions.
4. Execute the plan through the Reassigner (or just log it with
   `DryRun: true`).

All four goals (NW_IN, NW_OUT, DISK, PARTITION_COUNT) are active by default.
The network goals' `detect_threshold` (1 MiB/s mean) keeps an idle cluster
still. Forwarded produces count as NW_IN on the partition's *owner*;
migration copies are deliberately not counted.

## Configuration

Under `Server` in `appsettings.json`:

```json
"Cluster": {
  "BrokerId": "b1",                 // required; stable cluster identity
  "Host": "127.0.0.1",              // cluster endpoint bind (default loopback)
  "Port": 9095,                     // omit → client-only node (no endpoint)
  "Token": "shared-secret",         // X-Cluster-Token auth; SET THIS if the
                                    // port is reachable beyond loopback
  "Peers": [
    { "Id": "b2", "Address": "10.0.0.2:9095" }
  ]
},
"Autobalance": {
  "Enabled": true,                  // needs Cluster; run on ONE broker
  "IntervalSeconds": 60,
  "DryRun": true,                   // start here: log plans, move nothing
  "MaxActionsPerDetect": 50
}
```

Start with `DryRun: true` and watch the `balance_loop` log lines and the
`moonmq_autobalancer_*` metrics until the plans look sane.

## Metrics

| Metric | Meaning |
| ------ | ------- |
| `moonmq_autobalancer_broker_load{broker,resource}` | per-broker load in the post-plan snapshot |
| `moonmq_autobalancer_load_mean/stddev{goal}` | distribution stats per goal |
| `moonmq_autobalancer_last_plan_size` | actions in the most recent plan |
| `moonmq_reassign_partitions_total{dest}` | completed migrations |
| `moonmq_reassign_bytes_total{dest}` | bytes migrated |
| `moonmq_reassign_duration_seconds{topic}` | migration latency histogram |

## Boundaries (read before enabling)

* **Consumer groups span the cluster (2026-07-19), with caveats.** Each group
  hashes to ONE coordinator broker (deterministic over the sorted member ids);
  JOIN/HEARTBEAT/LEAVE arriving elsewhere are forwarded to it over
  `/cluster/group/*`. Assignment is ownership-aware: a member only receives
  partitions owned by the broker its connection lives on (fetches are local —
  there is no cross-broker fetch forwarding), so a partition whose owner has
  no connected member goes unassigned until one connects there. Rebalances
  propagate to remote members via their next heartbeat (assignment rides the
  response), so remote assignment — and commit fencing — can be up to one
  heartbeat interval stale. If a group's coordinator broker is unreachable,
  joins/heartbeats for that group fail until it returns; membership state is
  in-memory on the coordinator and re-forms after it restarts. The subscribed
  topics must exist on the coordinator broker.
* **Offset migration assumes both brokers run the same storage backend for
  the topic.** Offsets are backend-native cursors (byte offsets vs record
  counts); they only mean the same thing on dest because the data was copied
  byte-for-byte from zero.
* **Local data is left in place after a move.** The ownership table routes
  around it; reclaim disk by deleting the partition directory once you're
  satisfied.
* **Controller fencing is not consensus.** A superseded balance loop is
  refused wherever the newer claim has propagated, and requests without
  epoch headers bypass the fence entirely. Two controllers claiming at the
  same instant against disjoint reachable peers can both act until their
  claims meet.
* **Transactional produce crosses brokers (2026-07-19), with one window.**
  A transactional record routed to a peer-owned partition enrols the owner
  first (`/cluster/txn/enroll` floors its LSO), forwards the record, and on
  END_TXN writes the marker on the owner and resolves there (aborted ranges
  land in the owner's abort index). The owner's LSO floor for a REMOTE
  transaction is in-memory: if the owner restarts mid-transaction,
  read_committed readers there may see the txn's records until the markers
  arrive. Reassigning a partition away mid-transaction is unsupported.
* **Internal topics (`__*`) never move.**
* **Inter-broker HTTP is plaintext.** Bind to loopback/private networks,
  set `Token`, firewall the port — same posture as `/replicate` and
  `/metrics`.
