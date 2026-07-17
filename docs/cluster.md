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
  reassigner.lua           executes a plan: per MOVE, migrate the data and
                           flip ownership
  router.lua               produce-path routing: forward records for
                           partitions owned elsewhere
  balance_loop.lua         periodic feed-model → detect → execute glue
```

## What a MOVE does

`Reassigner:_move(topic, partition, dest)` runs five steps:

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
5. **confirm** — dest records that it owns the partition (clears any stale
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

`balance_loop.lua` runs on **one** broker per cluster (the de-facto
controller — running it on several would produce competing plans):

1. Feed the model: local partitions' disk bytes + every peer's
   `GET /cluster/loads` report. An unreachable peer is marked inactive for
   the pass (the balancer will never move data *toward* a broker it can't
   reach).
2. Run the detector: goals execute in priority order against a shared
   snapshot; the result is a plan of MOVE actions.
3. Execute the plan through the Reassigner (or just log it with
   `DryRun: true`).

Only the DISK and PARTITION_COUNT goals are active by default — there is no
per-partition byte-rate feed yet, so the NW_IN/NW_OUT goals would act on
noise. Enable them only if you feed those samples yourself.

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

* **Consumer groups are per-broker.** Group membership, heartbeats, and
  rebalancing do not span brokers. After a partition moves, consume it from
  its new owner.
* **Committed consumer offsets do not migrate.** They live in the source
  broker's `__consumer_offsets`. A group consuming from the new owner starts
  from its configured start position.
* **Local data is left in place after a move.** The ownership table routes
  around it; reclaim disk by deleting the partition directory once you're
  satisfied.
* **The balance loop is a single controller by convention, not election.**
  There is no fencing against two brokers both running it — configure it on
  exactly one.
* **Internal topics (`__*`) never move.**
* **Inter-broker HTTP is plaintext.** Bind to loopback/private networks,
  set `Token`, firewall the port — same posture as `/replicate` and
  `/metrics`.
