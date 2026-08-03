<p align="center">
  <img src="assets/logo.jpg" alt="MoonMQ logo" width="180" style="border-radius: 16px;" />
</p>

<h1 align="center">MoonMQ</h1>

<p align="center">
  A log-structured, partitioned message broker written in <b>pure Lua</b>.
</p>

<p align="center">
  <a href="https://github.com/HilthonTT/Nuntius/actions/workflows/ci.yml"><img src="https://github.com/HilthonTT/Nuntius/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/Lua-5.4-2C2D72?logo=lua&logoColor=white" alt="Lua 5.4" />
  <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT" />
</p>

---

Clients connect over TCP and speak a compact binary protocol to produce
records, fetch or subscribe to topics, create topics, and commit consumer
offsets. Topics are partitioned append-only logs with CRC-checked on-disk
records.

| | |
| --- | --- |
| **Storage** | Segmented log with time + offset indexes, group-commit fsync, retention, crash recovery. Alternative jocko-style commitlog backend with key compaction. |
| **Delivery** | Pull (`FETCH`) or push (`SUBSCRIBE`), consumer groups with a range assignor and durable offsets, DLQ with NACK. |
| **Correctness** | Idempotent producer (PID + sequence dedupe), multi-partition transactions, `read_committed` isolation with LSO. |
| **Cluster** | Static-membership peers, AutoMQ-style autobalancer, live partition migration, cluster-wide consumer groups, single-leader replication with `acks=all`. |
| **Ops** | Prometheus `/metrics`, JSON `/stats`, PBKDF2 auth with per-IP lockout, an interactive SQL-like console (MQL). |

**New here?** Read [docs/architecture.md](docs/architecture.md) (module map +
data flow), then [CONTRIBUTING.md](CONTRIBUTING.md). Deep dives:
[cluster](docs/cluster.md) · [transactions](docs/transactions.md) ·
[DLQ](docs/dlq.md) · [roadmap](docs/roadmap-security-consensus.md).

## Quick start

```bash
sudo apt install lua5.4 lua-socket    # Debian/Ubuntu
make deps                             # dkjson, busted, luasocket via luarocks-5.4
lua5.4 main.lua                       # or: make run
```

```
[main] env=Development host=0.0.0.0 port=9092 data_dir=./data_server
[server] listening on 0.0.0.0:9092 (proto v1, MoonMQ/v0.01)
```

Ctrl+C (SIGINT) shuts down cleanly. `lua5.4 main.lua --repl` opens the MQL
console instead.

### Requirements

- **Lua 5.4** — uses native bitwise operators and `goto`/labels
- **LuaSocket** — TCP networking
- **dkjson** — pure-Lua JSON, reads `appsettings.json`
- **luaposix** (Linux) — real `fsync`/`ftruncate`, so `acks=1` actually hits disk

SHA-256/HMAC is vendored (`src/vendor/sha2.lua`) — nothing to install.
Optional: `zlib` for gzip, LuaJIT FFI + `libsnappy` for Snappy. Both codecs
load lazily; a broker without them boots fine and only rejects produce
requests asking for the missing codec.

<details>
<summary><b>Building luaposix for Lua 5.4</b> (Ubuntu's package only ships 5.1–5.3)</summary>

```bash
sudo apt install liblua5.4-dev autoconf automake libtool
git clone --depth 1 --branch v36.2.1 https://github.com/luaposix/luaposix.git /tmp/luaposix
cd /tmp/luaposix
lua5.4 build-aux/luke package=luaposix version=36.2.1 \
    PREFIX=/usr/local LUA=lua5.4 \
    LUA_INCDIR=/usr/include/lua5.4 \
    LUA_LIBDIR=/usr/lib/x86_64-linux-gnu \
    INST_LIBDIR=/usr/local/lib/lua/5.4 \
    INST_LUADIR=/usr/local/share/lua/5.4
sudo lua5.4 build-aux/luke install package=luaposix version=36.2.1 \
    PREFIX=/usr/local LUA=lua5.4 \
    LUA_INCDIR=/usr/include/lua5.4 \
    LUA_LIBDIR=/usr/lib/x86_64-linux-gnu \
    INST_LIBDIR=/usr/local/lib/lua/5.4 \
    INST_LUADIR=/usr/local/share/lua/5.4
```

Verify: `lua5.4 -e 'print(require("posix.unistd").fsync)'` should print a function.

</details>

<details>
<summary><b>Windows</b></summary>

`src/io/io_sync.lua` falls back to LuaJIT FFI (`_commit`, `_chsize_s` from the
C runtime), so Windows needs **LuaJIT** rather than stock Lua 5.4.

</details>

## Configuration

`appsettings.json` in the working directory, deep-merged with
`appsettings.<MOONMQ_ENVIRONMENT>.json` when present (default environment:
`Development`).

| Section | Keys |
| --- | --- |
| `Server` | `Host`, `Port` (9092), `DataDir`, `MaxConnections`, `MaxConnectionsPerIP`, `MaxFrameSize`, `MaxTopics` (1024), `MaxListTopics`, `IdleDeadline` (60s), `PreAuthReadDeadline` (5s), `HandshakeDeadline` (5s), `MetricsHost`, `MetricsPort` (`null` disables), `Acks`, `Replication`, `Cluster`, `Autobalance` |
| `Auth` | `Username` + either `PasswordHash` (`pbkdf2-sha256$<iter>$<salt_hex>$<hash_hex>`) or plaintext `Password`; `MaxFailures`, `FailureWindow`, `BanDuration` for per-IP lockout |
| `Logging` | `Level` (`DEBUG`/`INFO`/`WARN`/`ERROR`), `File` (empty = stderr only), `LogToStderr` (default true, tees both) |

Generate a password hash — 600 000 PBKDF2 iterations per NIST 2024 guidance
(existing hashes keep their stored iteration count):

```bash
lua5.4 -e 'print(require("src.server.auth").hash_password("yourpw"))'
```

> [!WARNING]
> With no usable credential the broker starts **open** (no authentication)
> and logs a warning.

There is no built-in log rotation — pair `Logging.File` with `logrotate(8)`
using `copytruncate` (the broker holds the FD open). If the path can't be
opened it logs a one-time WARN and falls back to stderr.

## Observability

Two HTTP endpoints on `MetricsHost:MetricsPort` (default `127.0.0.1:9090`).
**Unauthenticated** — keep it on loopback or firewall the port.

- **`GET /metrics`** — Prometheus exposition format.

  | Metric | Type | Labels |
  | --- | --- | --- |
  | `moonmq_connections_open` | gauge | — |
  | `moonmq_connections_accepted_total` | counter | — |
  | `moonmq_connections_closed_total` | counter | `reason` |
  | `moonmq_bytes_sent_total` · `moonmq_frames_sent_total` | counter | — |
  | `moonmq_frames_received_total` | counter | `op` |
  | `moonmq_send_duration_seconds` | histogram | — |
  | `moonmq_dispatch_duration_seconds` | histogram | `op` |
  | `moonmq_produce_records_total` · `moonmq_idempotent_produce_total` · `moonmq_fetch_records_total` · `moonmq_segment_rolls_total` | counter | `topic` |
  | `moonmq_fsync_duration_seconds` | histogram | `topic` |
  | `moonmq_topic_count` | gauge | — |
  | `moonmq_partition_log_bytes` | gauge | `topic`, `partition` |

  `moonmq_partition_log_bytes` is per-partition — a few thousand series at the
  default `MaxTopics=1024`, which is fine for Prometheus. Lift `MaxTopics`
  cautiously.

- **`GET /stats`** — bounded JSON snapshot (version, open connections, topic
  count, top 10 topics by bytes on disk) for `curl | jq` inspection. Use
  `/metrics` for monitoring agents.

## Idempotent producer

The broker assigns a producer ID (PID) on demand and dedupes retries of the
same `(PID, topic, seq)` by replaying the original `(partition, offset)`
instead of appending a duplicate.

```lua
local Client = require("src.client")

local c = assert(Client.new{ host = "127.0.0.1", port = 9092,
                             username = "admin", password = "admin",
                             idempotent = true })  -- INIT_PRODUCER_ID after AUTH

local ack = assert(c:produce("orders", "key-1", "payload"))  -- {partition, offset, seq}

-- A retry with the same seq returns the ORIGINAL offset; no duplicate.
local replay = assert(c:produce_at_seq("orders", "key-1", "payload", ack.seq))
assert(replay.offset == ack.offset)
```

Out-of-order or skipped sequences are rejected with `ERR_OUT_OF_ORDER_SEQUENCE`
(11), letting the client detect lost records; `ERR_NO_PRODUCER_ID` (10) fires
if `PRODUCE_IDEMPOTENT` arrives before `INIT_PRODUCER_ID`. See
[docs/transactions.md](docs/transactions.md) for the full transaction design.

## Consumer groups

A topic's partitions are shared across a group so each partition has exactly
one owner. `src/broker/groups.lua` tracks membership, assigns partitions with
a Kafka-style **range** strategy, and ages out members that stop heartbeating
(30s session deadline, reaped every 10s). Durable per-group offsets live in
the internal `__consumer_offsets` topic, so a rebalanced consumer resumes from
its committed position.

The lifecycle is an explicit FSM (`src/fsm/state_machine.lua`), mirroring
Kafka's GroupCoordinator:

```
        join (1st member)        membership settled       assignments synced
empty ──────────────────▶ preparing_rebalance ─▶ completing_rebalance ─▶ stable
  ▲                              │                                          │
  │ last member leaves / evicted │  another join or leave triggers a new    │
  └──────────────────────────────┴──────────── rebalance ◀──────────────────┘

  any state ──close()──▶ dead   (terminal: join/leave/heartbeat all reject)
```

A single broker drives joins synchronously, so one `join`/`leave` walks
straight through `preparing → completing → stable`. The FSM still pays its
way: state is inspectable (`group:state()`), illegal operations are guarded
(you can't heartbeat a `dead` group), and every transition logs in one place.

Over the wire, a connected client uses `JOIN_GROUP` / `GROUP_HEARTBEAT` /
`LEAVE_GROUP`. The broker keeps one `ConsumerGroup` per group id shared across
member connections; a connection is at most one member, and when it drops the
broker departs on its behalf and rebalances the survivors.

```lua
-- Pass no member_id on the first join; the broker assigns one and the
-- client remembers it for subsequent heartbeats/leave.
local res = assert(c:join_group("billing", { "orders" }))
-- res = { member_id = "...", assignment = { orders = { 1, 2, 3, 4 } } }

assert(c:group_heartbeat())   -- renew the lease (uses the remembered ids)
assert(c:leave_group())       -- or just close the connection
```

`FETCH`/`SUBSCRIBE` **honor the assignment**: each member reads only its
partitions. The broker re-derives the live assignment before each poll
(`GroupCoordinator:apply_assignment` → `Consumer:set_assignment`), so a
rebalance takes effect on the next fetch without pushing a new frame. An
evicted member is pinned to "own nothing" until it re-joins; a member that
leaves reverts to reading every subscribed partition. A
`GROUP_MEMBER_UNKNOWN` error means the member was reaped — re-`join_group`.
Joining a second group on one connection is rejected with `GROUP_CONFLICT`.

A runnable in-process walkthrough of the whole lifecycle lives at
`src/examples/consumer_group.lua`.

## Clustering & autobalancing (experimental)

Brokers form a static-membership cluster: each gets an id, a peer list, and an
inter-broker HTTP endpoint. On top sits an
[AutoMQ](https://github.com/AutoMQ/automq)-style autobalancer that watches
per-partition load (disk bytes, partition counts) across the cluster and
migrates partitions from hot brokers to cold ones — data copy, exact ownership
cutover, and produce forwarding included, with zero client changes.

```json
"Server": {
  "Cluster":     { "BrokerId": "b1", "Port": 9095,
                   "Peers": [ { "Id": "b2", "Address": "10.0.0.2:9095" } ] },
  "Autobalance": { "IntervalSeconds": 60, "DryRun": true }
}
```

Consumer groups span the cluster (each group hashes to one coordinator broker;
JOIN/HEARTBEAT/LEAVE forward over `/cluster/group/*` with ownership-aware
assignment), committed offsets migrate with a partition, and transactional
produce works across brokers. Full design, guarantees, and current boundaries:
**[docs/cluster.md](docs/cluster.md)**.

Replication (`src/server/replicator.lua` + `src/server/replica_server.lua`) is
single-leader with static peers: a follower serves `POST /replicate`, a leader
ships records to peers, and `Server.Acks: "all"` blocks produces until
followers ack.

## Code layout

Layers, wire down to disk. Each directory depends only on the ones below it
(plus cross-cutting `core`/`log`/`metrics`/`io`); nothing in
`storage`/`commitlog` reaches up into `server`.

```
main.lua           CLI entrypoint: config, logging, auth wiring, server boot
bin/               operational tools (HTTP gateway, password hasher)
src/
  server/          TCP front-end: reactor loop, framing, connection lifecycle,
                   opcode handlers, group coordination, auth, metrics HTTP
  client/          network client (speaks the wire protocol over TCP)
  wire/            binary protocol codec, shared by server and client
  broker/          domain layer: Broker facade, in-process Producer/Consumer,
                   ConsumerGroup FSM, DLQ, transaction coordinator
  cluster/         multi-broker: ownership table, inter-broker HTTP + peer
                   client, partition reassigner, produce router, balance loop
  autobalancer/    pure decision engine: cluster model, windowed load samples,
                   distribution goals, anomaly detector
  storage/         topic/partition management + default "segmented" backend
                   (segments, time index, group-commit fsync, retention, recovery)
  commitlog/       alternative jocko-style backend: dense offset index,
                   byte-budget retention, key compaction
  record/          on-disk record codec (v2: CRC-framed key/value + attrs byte)
                   plus gzip/snappy compression
  io/              filesystem + durability primitives (fsync, ftruncate, rename)
  metrics/ log/    Prometheus registry · leveled logger
  core/            pure utilities (crc32, uuid, rng, futures, time, validation)
  fsm/ chaos/      state-machine library · fault-injecting producer wrapper
  repl/            interactive MQL console (sql/ lexer→parser→executor)
  vendor/          vendored third-party code (sha2)
  examples/        runnable client examples
spec/              busted test suite + standalone storage smoke test
```

## Interactive console (MQL)

`lua5.4 main.lua --repl` opens a console that speaks a small **SQL-like
language** over TCP. It connects to `127.0.0.1:9092` with the shipped
`admin`/`admin` credentials (the broker requires an `AUTH` handshake even in
OPEN mode) and transparently reconnects if an idle socket drops.

Statements end with `;` and may span lines (the prompt switches to `..>`).
Command words are case-insensitive; topic and group names are not. Strings are
single- or double-quoted (`''` escapes a quote) and `-- ...` is a comment. The
language runs through a real lexer → parser → executor pipeline
(`src/repl/sql/`), so errors carry actual positions.

| Statement | Maps to (client) |
| --- | --- |
| `CONNECT ['host'] [HOST 'h'] [PORT n] [USER 'u'] [PASSWORD 'p'];` | `Client.new` |
| `DISCONNECT;` | `Client:close` |
| `CREATE TOPIC <name> [PARTITIONS n];` | `create_topic` |
| `LIST TOPICS;` (alias `SHOW TOPICS;`) | `list_topics` |
| `PRODUCE INTO <topic> [KEY '<k>'] VALUE '<payload>';` | `produce` |
| `FETCH FROM <topic> [GROUP <g>] [LIMIT n];` | `fetch` |
| `SUBSCRIBE [TO] <topic> [GROUP <g>] [TIMEOUT secs] [LIMIT n];` | `subscribe`/`next_record` |
| `COMMIT <topic> PARTITION <n> OFFSET <n>;` | `commit` |
| `CREATE GROUP <name> SUBSCRIBE <topic>[, ...];` | `join_group` |
| `JOIN GROUP <name> SUBSCRIBE <topic>[, ...];` | `join_group` |
| `SHOW GROUP;` | current assignment |
| `HEARTBEAT;` · `LEAVE [GROUP];` | `group_heartbeat` · `leave_group` |
| `HELP [<command>];` · `EXIT;` / `QUIT;` | — |

```sql
mq> CREATE TOPIC orders PARTITIONS 4;
OK topic 'orders' created (4 partitions)

mq> PRODUCE INTO orders KEY 'k1' VALUE 'hello world';
OK produced to orders → partition 2, offset 0

mq> FETCH FROM orders GROUP billing LIMIT 10;
+-----------+--------+-----+-------------+
| partition | offset | key | value       |
+-----------+--------+-----+-------------+
| 2         | 0      | k1  | hello world |
+-----------+--------+-----+-------------+
(1 record)

mq> COMMIT orders PARTITION 2 OFFSET 1;
OK committed orders[2] @ offset 1
```

## Wire protocol

Clients and the broker speak a compact binary protocol over TCP
(`src/wire/protocol.lua`). Integers are big-endian, strings are
length-prefixed (`u32`) UTF-8, every frame is length-prefixed:

```
┌──────────────┬────────┬──────────────┬──────────────┐
│ FrameLen(4B) │ Op(1B) │ CorrelID     │ Payload(var) │
└──────────────┴────────┴──────────────┴──────────────┘
```

`FrameLen` covers everything after itself. There is no application-level CRC —
TCP guarantees transport integrity, and on-disk records carry their own CRC
since the disk layer can corrupt independently.

Opcodes split into client requests (`0x01`–`0x7F`) and server replies
(`0x80`–`0xFE`):

| Client → server | Purpose |
| --- | --- |
| `HELLO` · `AUTH` | Protocol-version handshake · authentication |
| `PRODUCE` · `FETCH` · `SUBSCRIBE` | Append · pull a batch · stream on arrival |
| `COMMIT` · `NACK` | Commit a group offset · reject a record (→ DLQ) |
| `CREATE_TOPIC` · `LIST_TOPICS` | Topic admin |
| `PING` · `GOODBYE` | Liveness · clean disconnect |
| `INIT_PRODUCER_ID` · `PRODUCE_IDEMPOTENT` | Request a u64 PID · append `(PID, seq, …)` |
| `BEGIN_TXN` · `END_TXN` · `TXN_OFFSET_COMMIT` | Transaction lifecycle |
| `JOIN_GROUP` · `GROUP_HEARTBEAT` · `LEAVE_GROUP` | Consumer-group membership |

| Server → client | Purpose |
| --- | --- |
| `WELCOME` · `AUTH_OK` | Handshake / auth acceptance |
| `PRODUCE_ACK` · `PRODUCER_ID` | Partition + offset · assigned u64 PID |
| `GROUP_ASSIGNMENT` · `RECORD` | Member id + partitions · a delivered record |
| `TOPIC_LIST` · `PONG` · `OK` · `NACK_ACK` | Query results / acks |
| `ERROR` | Numeric error code + message |

A connection must `HELLO` then `AUTH` before anything else; only handshake
opcodes are accepted pre-auth. A consumer connection is either **pull**
(`FETCH`) or **push** (`SUBSCRIBE`) — mixing them on one connection is
rejected.

`src/server/server.lua` owns the listener, capacity accounting, and ban
enforcement, then delegates to `src/server/connection.lua`. Each `Connection`
runs three coroutines — reader, sender (bounded send queue), heartbeat probe —
driven by a `new → greeted → authenticated → closed` state machine, with
framing isolated in `src/server/framer.lua` and 16-byte connection/correlation
IDs from `src/core/uuid.lua`. A watchdog drops connections that fail to
authenticate within `HandshakeDeadline`.

## Testing

```bash
make deps          # busted + luasocket under Lua 5.4
make test          # equivalent to: busted
make smoke         # standalone restart/recovery + segment-roll test
```

Roughly one spec per module under `spec/`, plus pinned regression suites
(`bugfix_regression_spec.lua`, `review_fixes_spec.lua`) and a chaos suite that
drives fault injection through `ChaosProducer`.

<details>
<summary><b>Debian/Ubuntu dependency caveats</b></summary>

- The default `luarocks` launcher targets **Lua 5.1**; the Makefile invokes
  `luarocks-5.4` explicitly so deps land in `/usr/local/{share,lib}/lua/5.4`.
- LuaRocks' HTTPS fetcher can fail with `bad argument #2 to 'method' (string
  expected, got light userdata)` on boxes with broken luasec. The Makefile sets
  `fs_use_modules = false` so downloads route through `wget`/`curl` — run
  `sudo apt install wget` first.
- For a system-wide install run `make deps` with `sudo`; otherwise pass
  `LUAROCKS="luarocks-5.4 --local"`.
- `make deps` installs `busted` at `/usr/local/bin/busted` bound to Lua 5.4,
  overwriting any Lua-5.1 busted on PATH (preserved as `busted~`).

</details>

## Credits

MoonMQ stands on the shoulders of these projects and write-ups:

| Project | What it gave MoonMQ |
| --- | --- |
| [**jocko**](https://github.com/travisjeffery/jocko) — Kafka in Go | The commitlog backend design: dense offset index, byte-budget retention, key compaction |
| [**lua-state-machine**](https://github.com/kyleconroy/lua-state-machine) | The FSM library behind the consumer-group and connection lifecycles |
| [**luaprompt**](https://github.com/dpapavas/luaprompt) | Interactive-console foundations |
| [**croissant**](https://github.com/giann/croissant) | REPL structure and rendering ideas |
| [**AutoMQ**](https://github.com/AutoMQ/automq) | The autobalancer model: windowed load samples, distribution goals, anomaly detection |
| [**Building a Kafka clone in Go**](https://support.tools/post/building-kafka-clone-in-go/) | Broker architecture walkthrough |

Licensed under the [MIT License](LICENSE).
