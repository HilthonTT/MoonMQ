# Replication and automatic failover

MoonMQ has two replication modes, chosen by `Server.Replication.Failover`:

| | Static (no `Failover` block) | Failover (`Failover.Enabled`) |
| --- | --- | --- |
| Leader | fixed by `Role` | elected by a majority, re-elected when it dies |
| What is copied | records produced through the producer path | every partition log, internal topics included |
| Transport | leader pushes `POST /replicate` | followers pull `POST /replication/fetch` |
| `acks=all` waits for | every configured follower | the in-sync replica set (ISR) |
| Offsets on followers | assigned locally (may drift after an error) | identical to the leader's |
| Consumers read up to | log end | high watermark |

The rest of this page is about failover mode. Static mode is unchanged and is
what an existing `Replication` block keeps getting.

## Topology

A replication group is this broker plus `Replication.Peers`: three or five
brokers, each a full copy of the others. Exactly one of them, the **leader**,
accepts client traffic; the others are **followers** that copy its logs and
refuse clients with `ERR_NOT_LEADER` (code 20) and a hint
`leader=<host:port>`.

Automatic failover needs a majority alive, so **use at least three replicas**.
With two, losing either one stops elections (the survivor cannot tell a dead
peer from a network split, and acting alone would risk two leaders).

Failover mode cannot be combined with `Server.Cluster`: a replication group is
a set of standbys holding the same data, not brokers splitting partitions
between them.

## Configuration

Every replica gets the same block with its own `ReplicaId`, `ReplicatePort`
and `ClientAddress`, and lists the other two as peers:

```json
"Server": {
  "Acks": "all",
  "Replication": {
    "Enabled": true,
    "ReplicaId": 1,
    "Role": "leader",
    "ReplicateHost": "0.0.0.0",
    "ReplicatePort": 9095,
    "ClientAddress": "10.0.0.1:9092",
    "Token": "shared-secret",
    "AckTimeout": 5,
    "Peers": [
      { "Id": 2, "Address": "10.0.0.2:9095", "ClientAddress": "10.0.0.2:9092" },
      { "Id": 3, "Address": "10.0.0.3:9095", "ClientAddress": "10.0.0.3:9092" }
    ],
    "Failover": { "Enabled": true }
  }
}
```

| Key | Default | Meaning |
| --- | --- | --- |
| `Role` | `leader` | Only `leader`/`both` replicas may win the very first election (before any ISR exists). Set it on exactly one replica. |
| `ClientAddress`, `Peers[].ClientAddress` | – | What followers put in the `leader=` hint. Without them clients only get "not the leader". |
| `Token` | none | Shared secret sent as `X-Cluster-Token` on every replication request and required on the listener. |
| `Tls` | none | Same block as before; covers the Raft and fetch traffic. |
| `AckTimeout` | 5 s | How long `acks=all` waits for the ISR. |
| `Failover.IsrLagSeconds` | 10 | A follower that has not caught up to the leader's log end for this long leaves the ISR. |
| `Failover.MinInsyncReplicas` | 1 | `acks=all` fails fast when the ISR (leader included) is smaller than this. |
| `Failover.ElectionTimeoutMs` | 2500 | Raft election timeout lower bound; the upper bound is randomised from it (`ElectionTimeoutMaxMs` overrides). |
| `Failover.HeartbeatMs` | 500 | Leader heartbeat interval. |
| `Failover.RpcTimeoutMs` | 1000 | Per-request timeout for Raft RPCs. |
| `Failover.CommitTimeoutSeconds` | 10 | How long an ISR change may take to commit. |
| `Failover.MaxLogEntries` | 512 | Raft log length before it is compacted into a snapshot. |
| `Failover.MaxFetchWaitMs` | 500 | Long-poll time for an idle follower fetch. |
| `Failover.MaxFetchBytes` | 1 MiB | Upper bound on one fetch response. |

Clients should list every replica so they can find the leader after a failover:

```lua
local c = Client.new{ hosts = { "10.0.0.1:9092", "10.0.0.2:9092", "10.0.0.3:9092" },
                      username = "app", password = "..." }
```

`Client.new` tries each host in turn and follows up to three `leader=` hints.
It does not reconnect an established client: when the leader it was talking to
dies or steps down the connection closes, and the application creates a new
client the same way.

## How it works

### Leadership (`src/replication/group.lua`)

The replicas run a second instance of the Raft code the cluster controller
uses (`src/cluster/raft/`), over `/replication/raft/*` on the replication
port. Its log holds two kinds of entry:

* the leadership claim a new Raft leader commits at the start of its term — the
  term becomes the **leader epoch**;
* ISR changes, each stamped with the epoch that proposed it.

A replica only stands for election if it is in the ISR as recorded in its own
Raft log (or, before any ISR exists, if its `Role` is `leader`/`both`). Raft
already guarantees the winner's log holds every committed entry, so the
winner is a member of the last committed ISR and therefore holds every record
that was acknowledged with `acks=all`.

A leader only starts serving clients after its claim has committed. It then
rebuilds the broker's in-memory state (committed offsets, producer ids,
transactions) from the replicated internal topics, resolves in-flight
transactions, and starts accepting connections. A leader that loses contact
with a majority steps down, closes every client connection, and becomes a
follower.

### The in-sync set

The leader tracks each follower's position from its fetch requests:

* **shrink** — a follower that has not been at the leader's log end for
  `IsrLagSeconds` is proposed out of the ISR. It stays in the set `acks=all`
  waits for until that change commits.
* **expand** — a follower whose every partition has reached the high
  watermark is proposed back in, and `acks=all` starts waiting for it
  immediately, before the change commits.

Waiting for the larger of the two sets while a change is pending is what keeps
"every ISR member has every acknowledged record" true across the change.

The **high watermark** of a partition is the lowest position across the ISR.
Consumers are only served records below it, so they never see a record that a
failover could still take back.

### Log shipping (`src/replication/fetcher.lua`)

Followers pull. Each loop a follower:

1. **manifest** — on a new leader, or when the leader's metadata digest
   changes, it fetches the topic list (names, partition counts, configs, a
   per-topic incarnation id) and the transaction abort index, then creates,
   drops or reconfigures local topics to match. A topic that was deleted and
   recreated on the leader has a new id and is dropped and recopied.
2. **truncation** — once per epoch it asks, for every partition, where the
   leader's copy of its latest epoch ends, and truncates anything past that.
   This is what removes an old leader's unreplicated tail when it comes back.
3. **fetch** — it reports its log end for every partition and receives raw
   record bytes tagged with offset and epoch, appending each at exactly that
   offset. The leader long-polls when there is nothing new.

Each replica keeps a per-partition **leader epoch history** in
`<DataDir>/replication-epochs.json`: the first offset written under each
epoch. It answers the truncation question and survives restarts. Raft state
lives in `<DataDir>/replication-raft.json`.

If a follower is behind the leader's retention (its log end is below the
leader's oldest offset), the leader tells it to reset that partition to the
leader's log start.

## Guarantees and limits

* A record acknowledged with `acks=all` survives the loss of any minority of
  replicas. `acks=leader` and `acks=none` records written after the last
  follower fetch are lost if the leader dies, as in Kafka.
* There is no unclean election: if every ISR member is lost, no replica takes
  over until one of them returns.
* Membership is static. Adding or replacing a replica needs a config change and
  a restart; there is no joint consensus.
* A follower should start with an empty `DataDir` or a copy of the leader's.
  A partition that holds records but no epoch history is discarded and
  recopied the first time the replica follows a leader, which covers data left
  over from static mode. Data that has epoch history is trusted.
* A replica that rejoins after a partition can trigger one extra election
  (there is no Raft pre-vote). No acknowledged data is at risk; clients see one
  more reconnect.
* Consumer offsets, producer ids and transaction state are replicated, but
  consumer-group membership is not: after a failover, members rejoin their
  groups on the new leader.

## Operations

`GET /replication/status` on the replication port (with the token, if set)
returns this replica's view: `role`, `leader`, `epoch`, `isr`, Raft term. The
same object appears under `replication` in the metrics port's `/stats`.

| Metric | Meaning |
| --- | --- |
| `moonmq_replication_is_leader` | 1 on the replica serving clients |
| `moonmq_replication_epoch` | current leader epoch |
| `moonmq_replication_isr_size` | committed ISR size, leader included |
| `moonmq_replication_isr_shrinks_total` / `_expands_total` | ISR changes proposed by this replica |
| `moonmq_replication_leader_changes_total` | times this replica took over |
| `moonmq_replication_truncations_total` | partition logs cut back to match a new leader |
| `moonmq_replication_fetched_records_total` | records copied as a follower |
| `moonmq_replication_raft_term`, `_is_controller`, `_commit_index`, `_elections_total` | the leadership Raft |
