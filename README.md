<p align="center">
  <img src="assets/logo.jpg" alt="MoonMQ logo" width="200" style="border-radius: 16px;" />
</p>

# MoonMQ

A log-structured message broker written in pure Lua. Clients connect over
TCP and speak a compact binary protocol to produce records, fetch or
subscribe to topics, create topics, and commit consumer offsets. Topics are
partitioned append-only logs with CRC-checked on-disk records.

## Requirements

- Lua 5.4 (uses native bitwise operators and `goto`/labels)
- LuaSocket (`lua-socket`) — TCP networking
- dkjson — pure-Lua JSON, used by `src/server/config.lua` to read `appsettings.json`
- luaposix (Linux only — used for real `fsync`/`ftruncate` so `acks=1` actually flushes to disk)

SHA-256/HMAC for authentication is provided by a vendored pure-Lua
`src/vendor/sha2.lua` — no installation needed.

On Debian/Ubuntu:

```bash
sudo apt install lua5.4 lua-socket
```

`dkjson` is installed by `make deps` (or directly with `luarocks-5.4 install dkjson`).

### Installing luaposix for Lua 5.4

Ubuntu's `lua-posix` apt package ships binaries only for Lua 5.1/5.2/5.3, and LuaRocks may not work in every environment. Build from source:

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

Verify with: `lua5.4 -e 'print(require("posix.unistd").fsync)'` — should print a function.

### Windows

`src/io/io_sync.lua` falls back to LuaJIT FFI on Windows (`_commit`, `_chsize_s` from the C runtime). Running on Windows therefore requires LuaJIT rather than stock Lua 5.4.

## Running the server

`main.lua` starts the TCP broker server. It reads configuration from
`appsettings.json` in the working directory, then listens for client
connections and handles each one in its own coroutine.

From the project root:

```bash
lua5.4 main.lua
# or
make run
```

The server logs the resolved config and a listening line, e.g.:

```
[main] env=Development host=0.0.0.0 port=9092 data_dir=./data_server
[server] listening on 0.0.0.0:9092 (proto v1, MoonMQ/v0.01)
```

Stop it with Ctrl+C (SIGINT) for a clean shutdown.

### Configuration

`appsettings.json` is structured into sections:

- **`Server`** — `Host`, `Port` (default `9092`), `DataDir`,
  `MaxConnections`, `MaxConnectionsPerIP`, `MaxFrameSize`, `MaxTopics`
  (broker-wide cap, default `1024`), `MaxListTopics` (cap on
  `LIST_TOPICS` response), `IdleDeadline` (per-frame timeout once
  authenticated, default `60`), `PreAuthReadDeadline` (per-frame
  timeout before AUTH completes, default `5`), `HandshakeDeadline`
  (overall handshake budget, default `5`), `MetricsHost`,
  `MetricsPort` (set to `null` to disable the metrics HTTP server).
- **`Auth`** — `Username`, plus either `PasswordHash` (a
  `pbkdf2-sha256$<iter>$<salt_hex>$<hash_hex>` string) or a plaintext
  `Password`. `MaxFailures`, `FailureWindow`, and `BanDuration` control
  per-IP lockout after repeated failures. Password hashes default to
  600 000 PBKDF2 iterations (NIST 2024 guidance); existing stored
  hashes keep their original iteration count.
- **`Logging`** — `Level` (`DEBUG`/`INFO`/`WARN`/`ERROR`), `File`
  (path; empty string = stderr only), `LogToStderr` (boolean; default
  true even when `File` is set, so logs go to both).

If `Auth` has no usable credential, the server starts **open** (no
authentication) and logs a warning.

**Environment overlays:** set `MOONMQ_ENVIRONMENT` (default `Development`).
If `appsettings.<environment>.json` exists, it is deep-merged over the base
`appsettings.json`.

### Setting the admin password

Generate a PBKDF2-SHA256 hash and paste it into `Auth.PasswordHash`:

```bash
lua5.4 -e 'print(require("src.server.auth").hash_password("yourpw"))'
```

Alternatively, set `Auth.Password` to a plaintext value — the server hashes
it at startup and logs a warning.

### Optional modules

Some modules under `src/` depend on libraries that may not be present:

- **`src/record/compression.lua`** needs the `zlib` LuaRocks module (gzip support).
- **`src/record/snappy.lua`** needs LuaJIT's FFI and `libsnappy` (Snappy support).
  Both codecs load lazily: a broker without them still boots and only rejects
  produce requests that ask for the missing codec.
- **Replication** (`src/server/replicator.lua` + `src/server/replica_server.lua`)
  loads on stock Lua. Configure it under `Server.Replication` (single-leader,
  static peers); a follower serves `POST /replicate` and a leader ships records
  to its peers. `Server.Acks: "all"` blocks produces until followers ack.

### Logging to a file

Set `Logging.File` to an absolute path; the server appends to it with
line-buffered I/O so a crash loses at most one partial line. There is no
built-in rotation — pair with `logrotate(8)` using `copytruncate` (the
broker holds the FD open; logrotate copies and zero-truncates):

```
/var/log/moonmq.log {
    daily
    rotate 14
    compress
    copytruncate
    missingok
}
```

If the file path can't be opened the broker logs a one-time WARN and
falls back to stderr-only. `LogToStderr=true` (the default) tees stderr
even when a file is configured, which is what you want under systemd
(stderr goes to the journal).

### Observability

The broker exposes two HTTP endpoints on `MetricsHost:MetricsPort`
(default `127.0.0.1:9090`):

- `GET /metrics` — Prometheus exposition format. Counters and gauges:

  | Metric                              | Type      | Labels               |
  | ----------------------------------- | --------- | -------------------- |
  | `moonmq_connections_open`           | gauge     | —                    |
  | `moonmq_connections_accepted_total` | counter   | —                    |
  | `moonmq_connections_closed_total`   | counter   | `reason`             |
  | `moonmq_bytes_sent_total`           | counter   | —                    |
  | `moonmq_frames_sent_total`          | counter   | —                    |
  | `moonmq_frames_received_total`      | counter   | `op`                 |
  | `moonmq_send_duration_seconds`      | histogram | —                    |
  | `moonmq_dispatch_duration_seconds`  | histogram | `op`                 |
  | `moonmq_produce_records_total`      | counter   | `topic`              |
  | `moonmq_idempotent_produce_total`   | counter   | `topic`              |
  | `moonmq_fetch_records_total`        | counter   | `topic`              |
  | `moonmq_segment_rolls_total`        | counter   | `topic`              |
  | `moonmq_fsync_duration_seconds`     | histogram | `topic`              |
  | `moonmq_topic_count`                | gauge     | —                    |
  | `moonmq_partition_log_bytes`        | gauge     | `topic`, `partition` |

  `moonmq_partition_log_bytes` is per-partition — at the default
  `MaxTopics=1024` with a typical partition count this stays a few
  thousand series, which is fine for Prometheus. Lift `MaxTopics`
  cautiously.

- `GET /stats` — JSON broker snapshot. Human-readable, bounded:
  summarises broker version, open connection count, topic count, and
  the top 10 topics by bytes-on-disk. Truncates the topic list at
  `MaxListTopics`. Intended for `curl | jq` operator inspection, not
  for monitoring agents — use `/metrics` for that.

The metrics server is unauthenticated; bind it to `127.0.0.1` or
firewall the port unless your network is trusted.

### Idempotent producer

MoonMQ supports a session-scoped idempotent producer: the broker
assigns a producer ID (PID) on demand, and dedupes retries of the same
`(PID, topic, seq)` by replaying the original `(partition, offset)`
instead of appending a duplicate. PIDs are not durable across the
broker process or the client's socket — see
[`docs/transactions.md`](docs/transactions.md) for the full deferred
Kafka-style transaction design.

```lua
local Client = require("src.client")

local c = assert(Client.new{
    host       = "127.0.0.1",
    port       = 9092,
    username   = "admin",
    password   = "admin",
    idempotent = true,        -- triggers INIT_PRODUCER_ID after AUTH
})

local ack = assert(c:produce("orders", "key-1", "payload"))
-- ack = { partition, offset, seq }

-- A retry with the same seq returns the ORIGINAL offset; no duplicate.
local replay = assert(c:produce_at_seq("orders", "key-1", "payload", ack.seq))
assert(replay.offset == ack.offset)
```

Out-of-order or skipped sequences are rejected with
`ERR_OUT_OF_ORDER_SEQUENCE` (code `11`), letting the client detect lost
records. `ERR_NO_PRODUCER_ID` (code `10`) fires if the client sends
`PRODUCE_IDEMPOTENT` without first calling `INIT_PRODUCER_ID`.

### Consumer groups

A topic's partitions can be shared across a group of consumers so each
partition is owned by exactly one member. `src/broker/groups.lua` provides
the server-side coordinator (`ConsumerGroup`) that tracks membership,
assigns partitions with a Kafka-style **range** strategy, and ages out
members that stop heartbeating. Durable per-group offsets are kept
separately by the `OffsetManager` (the internal `__consumer_offsets`
topic), so a rebalanced consumer resumes from the committed position.

The group's lifecycle is an explicit finite-state machine
(`src/fsm/state_machine.lua`), mirroring Kafka's GroupCoordinator:

```
        join (1st member)        membership settled       assignments synced
empty ──────────────────▶ preparing_rebalance ─▶ completing_rebalance ─▶ stable
  ▲                              │                                          │
  │ last member leaves / evicted │  another join or leave triggers a new    │
  └──────────────────────────────┴──────────── rebalance ◀──────────────────┘

  any state ──close()──▶ dead   (terminal: join/leave/heartbeat all reject)
```

A single broker drives joins synchronously, so one `join`/`leave` walks the
group straight through `preparing → completing → stable`. The FSM still pays
its way: it makes the state inspectable (`group:state()`), guards operations
that are illegal in the current state (you can't heartbeat a `dead` group),
and gives one place to log every transition.

```lua
local brk_m   = require("src.storage.broker")
local group_m = require("src.client.groups")

local broker = assert(brk_m.Broker.new("./data_server"))
assert(broker:create_topic("orders", 4))

local g = group_m.ConsumerGroup.new(broker, "billing")
assert(g:state() == group_m.STATES.EMPTY)

-- First member owns every partition; the group is now stable.
local m1 = assert(g:join("m1", { "orders" }))   -- m1.orders = {1,2,3,4}

-- Second member triggers a rebalance: range splits 4 partitions 2-and-2.
local m2 = assert(g:join("m2", { "orders" }))   -- m2.orders = {3,4}
assert(g:state() == group_m.STATES.STABLE)

-- Members renew their lease; the reaper evicts anyone past the 30s deadline.
g:heartbeat("m1")
g:check_heartbeats()

g:leave("m2")          -- survivors rebalance; an emptied group returns to EMPTY
g:close()              -- terminal: g:state() == group_m.STATES.DEAD
```

A runnable, in-process walkthrough of the whole lifecycle (with the state
printed after each step) lives at `src/examples/consumer_group.lua`:

```bash
lua5.4 src/examples/consumer_group.lua
```

#### Over the wire

The coordinator is reachable over TCP. A connected client uses
`JOIN_GROUP` / `GROUP_HEARTBEAT` / `LEAVE_GROUP`; the broker keeps one
`ConsumerGroup` per group id, shared across all member connections. A
connection is at most one member — when it drops, the broker departs the
group on its behalf and rebalances the survivors. Members that stop
heartbeating past the 30s session deadline are reaped by a background loop
(every 10s by default; see `group_reaper_interval`).

```lua
local Client = require("src.client")

local c = assert(Client.new{ host = "127.0.0.1", port = 9092,
                             username = "admin", password = "admin" })

-- Pass no member_id on the first join; the broker assigns one and the
-- client remembers it for subsequent heartbeats/leave.
local res = assert(c:join_group("billing", { "orders" }))
-- res = { member_id = "...", assignment = { orders = { 1, 2, 3, 4 } } }

assert(c:group_heartbeat())   -- renew the lease (uses the remembered ids)
assert(c:leave_group())       -- or just close the connection
```

If `group_heartbeat` (or a re-`join_group`) comes back with a
`GROUP_MEMBER_UNKNOWN` error, the member was reaped for inactivity — the
client should `join_group` again to get a fresh assignment. Joining a
second, different group on one connection is rejected with
`GROUP_CONFLICT`.

`JOIN_GROUP` hands back the partitions a member owns, and `FETCH` /
`SUBSCRIBE` **honor that assignment**: each member reads only its assigned
partitions, so a topic's partitions are divided across the group instead of
every member draining every one. The broker re-derives the live assignment
before each poll (`GroupCoordinator:apply_assignment` → `Consumer:set_assignment`),
so a rebalance — a member joining, leaving, or being reaped — takes effect on
the member's next fetch without pushing a new assignment frame. A member the
coordinator has evicted is pinned to "own nothing" until it re-joins, and a
consumer that leaves its group reverts to reading every subscribed partition.

## Code layout

Layers, from the wire down to the disk. Each directory depends only on the
ones below it (plus the cross-cutting `core`/`log`/`metrics`/`io` utilities);
nothing in `storage`/`commitlog` reaches up into `server`.

```
main.lua           CLI entrypoint: config, logging, auth wiring, server boot
bin/               operational tools (HTTP gateway, password hasher)
src/
  server/          TCP front-end: reactor event loop, framing, connection
                   lifecycle, opcode handlers (handlers.lua), consumer-group
                   coordination (group_coordinator.lua), auth, metrics HTTP
  client/          network client (speaks the wire protocol over TCP)
  wire/            binary protocol codec, shared by server and client
  broker/          broker domain layer: Broker (topics + offsets facade),
                   in-process Producer/Consumer, ConsumerGroup coordinator FSM
  storage/         topic/partition management and the default "segmented"
                   backend (segment files, time index, group-commit fsync
                   batching, retention, crash recovery)
  commitlog/       alternative "commitlog" backend (jocko-style: dense offset
                   index, byte-budget retention, key compaction)
  record/          on-disk record codec (v2: CRC-framed key/value + attrs byte
                   for compression codec + txn control flag); gzip/snappy
                   compression wired into the produce/store path
  io/              filesystem helpers and durability primitives (fsync,
                   ftruncate, atomic rename — POSIX and Windows)
  metrics/         Prometheus-style metrics registry
  log/             leveled logger
  core/            small pure utilities (crc32, uuid, rng, futures, time,
                   string helpers, topic-name validation, version info)
  fsm/             vendored finite-state-machine library (used by broker/groups)
  repl/            interactive MQL console (sql/ lexer→parser→executor)
  chaos/           fault-injecting producer wrapper (test support)
  vendor/          vendored third-party code (sha2)
  examples/        runnable client examples
spec/              busted test suite + standalone storage smoke test
```

## Interactive console (MQL)

MoonMQ ships an interactive console that speaks a small **SQL-like language**
instead of raw Lua. It connects to a running broker over TCP and lets you
create topics, produce/fetch records, and drive consumer groups from a prompt.

Launch it from the project root:

```bash
lua5.4 main.lua --repl
```

On startup it connects to the local broker (`127.0.0.1:9092`), authenticating
with the shipped default credentials (`admin`/`admin`). The broker requires an
`AUTH` handshake even in OPEN mode, so the console always authenticates; use
`CONNECT ... USER '...' PASSWORD '...'` to point it elsewhere or supply real
credentials. If the broker drops an idle socket between statements, the console
transparently reconnects and retries the command. Every statement ends with a semicolon `;` and
may span multiple lines (the prompt switches to `..>` until you close it).
Command words are case-insensitive; topic and group names are not. Strings are
single- or double-quoted (`''` escapes a quote), and `-- ...` is a comment.

The language is implemented as a proper lexer → parser → executor pipeline
under `src/repl/sql/` (`lexer.lua`, `parser.lua`, `executor.lua`), so the syntax
is validated with real error positions rather than pattern-matched.

### Command reference

| Statement                                                         | Maps to (client)          |
| ----------------------------------------------------------------- | ------------------------- |
| `CONNECT ['host'] [HOST 'h'] [PORT n] [USER 'u'] [PASSWORD 'p'];` | `Client.new`              |
| `DISCONNECT;`                                                     | `Client:close`            |
| `CREATE TOPIC <name> [PARTITIONS n];`                             | `create_topic`            |
| `LIST TOPICS;` (alias `SHOW TOPICS;`)                             | `list_topics`             |
| `PRODUCE INTO <topic> [KEY '<k>'] VALUE '<payload>';`             | `produce`                 |
| `FETCH FROM <topic> [GROUP <g>] [LIMIT n];`                       | `fetch`                   |
| `SUBSCRIBE [TO] <topic> [GROUP <g>] [TIMEOUT secs] [LIMIT n];`    | `subscribe`/`next_record` |
| `COMMIT <topic> PARTITION <n> OFFSET <n>;`                        | `commit`                  |
| `CREATE GROUP <name> SUBSCRIBE <topic>[, ...];`                   | `join_group`              |
| `JOIN GROUP <name> SUBSCRIBE <topic>[, ...];`                     | `join_group`              |
| `SHOW GROUP;`                                                     | current assignment        |
| `HEARTBEAT;`                                                      | `group_heartbeat`         |
| `LEAVE [GROUP];`                                                  | `leave_group`             |
| `HELP [<command>];` · `EXIT;` / `QUIT;`                           | —                         |

### A sample session

```sql
mq> CONNECT 'localhost' PORT 9092 USER 'admin' PASSWORD 'admin';
OK connected to localhost:9092 as admin

mq> CREATE TOPIC orders PARTITIONS 4;
OK topic 'orders' created (4 partitions)

mq> LIST TOPICS;
+--------+------------+
| topic  | partitions |
+--------+------------+
| orders | 4          |
+--------+------------+
(1 topic)

mq> PRODUCE INTO orders KEY 'k1' VALUE 'hello world';
OK produced to orders → partition 2, offset 0

mq> FETCH FROM orders GROUP billing LIMIT 10;
+-----------+--------+-----+-------------+
| partition | offset | key | value       |
+-----------+--------+-----+-------------+
| 2         | 0      | k1  | hello world |
+-----------+--------+-----+-------------+
(1 record)

mq> -- create/join a consumer group and inspect the assignment
mq> CREATE GROUP billing SUBSCRIBE orders;
+--------+------------+
| topic  | partitions |
+--------+------------+
| orders | 0,1,2,3    |
+--------+------------+
(joined group 'billing' as m-1a2b...)

mq> COMMIT orders PARTITION 2 OFFSET 1;
OK committed orders[2] @ offset 1

mq> LEAVE GROUP;
OK left group 'billing'

mq> EXIT;
```

A statement can also span several lines — handy for longer commands:

```sql
mq> CREATE GROUP analytics
..>   SUBSCRIBE orders, payments, audit;
```

## Wire protocol

Clients and the broker speak a compact binary protocol over TCP
(`src/wire/protocol.lua`). All integers are big-endian; strings are
length-prefixed (`u32`) UTF-8. Every frame is length-prefixed:

```
┌──────────────┬────────┬──────────────┬──────────────┐
│ FrameLen(4B) │ Op(1B) │ CorrelID     │ Payload(var) │
└──────────────┴────────┴──────────────┴──────────────┘
```

`FrameLen` covers everything after itself. There is no application-level
CRC — TCP guarantees transport integrity, and on-disk records carry their
own CRC since the disk layer can corrupt independently.

Opcodes are split into client requests (`0x01`–`0x7F`) and server replies
(`0x80`–`0xFE`):

| Op                           | Direction       | Purpose                                   |
| ---------------------------- | --------------- | ----------------------------------------- |
| `HELLO`                      | client → server | Protocol-version handshake                |
| `AUTH`                       | client → server | Username/password authentication          |
| `PRODUCE`                    | client → server | Append a record to a topic                |
| `FETCH`                      | client → server | Pull a batch of records (pull mode)       |
| `SUBSCRIBE`                  | client → server | Stream records as they arrive (push)      |
| `COMMIT`                     | client → server | Commit a consumer-group offset            |
| `CREATE_TOPIC`               | client → server | Create a partitioned topic                |
| `LIST_TOPICS`                | client → server | List existing topics                      |
| `PING`/`GOODBYE`             | client → server | Liveness / clean disconnect               |
| `INIT_PRODUCER_ID`           | client → server | Request a u64 PID for idempotent produce  |
| `PRODUCE_IDEMPOTENT`         | client → server | Idempotent append: `(PID, seq, ...)`      |
| `JOIN_GROUP`                 | client → server | Join a consumer group; get an assignment  |
| `LEAVE_GROUP`                | client → server | Depart a consumer group                   |
| `GROUP_HEARTBEAT`            | client → server | Renew a group member's lease              |
| `WELCOME` / `AUTH_OK`        | server → client | Handshake / auth acceptance               |
| `PRODUCE_ACK`                | server → client | Partition + offset of an appended record  |
| `PRODUCER_ID`                | server → client | Assigned u64 PID                          |
| `GROUP_ASSIGNMENT`           | server → client | Member id + assigned partitions per topic |
| `RECORD`                     | server → client | A delivered record (fetch or push)        |
| `TOPIC_LIST` / `PONG` / `OK` | server → client | Query results / acks                      |
| `ERROR`                      | server → client | Numeric error code + message              |

A connection must `HELLO` then `AUTH` before any other request; only
handshake opcodes are accepted pre-auth. A consumer connection is either
**pull** (`FETCH`) or **push** (`SUBSCRIBE`) — mixing the two on one
connection is rejected.

### Connection lifecycle

The TCP front-end (`src/server/server.lua`) owns the listener, capacity
accounting, and ban enforcement, then delegates each connection to
`src/server/connection.lua`. A `Connection` runs three coroutines — a
reader, a sender (bounded send queue), and a heartbeat probe — driven by an
explicit `new → greeted → authenticated → closed` state machine. Framing is
isolated in `src/server/framer.lua`, and 16-byte connection/correlation IDs
come from `src/core/uuid.lua`. A handshake watchdog drops connections that
fail to authenticate within `HandshakeDeadline`.

## Testing

The test suite lives under `spec/` and runs with [busted](https://lunarmodules.github.io/busted/).

### Installing test dependencies

`make deps` installs busted and luasocket under Lua 5.4. Two caveats on Debian/Ubuntu:

- The default `luarocks` launcher targets **Lua 5.1**; the Makefile invokes `luarocks-5.4` explicitly so deps land in `/usr/local/{share,lib}/lua/5.4`.
- LuaRocks' built-in HTTPS fetcher can fail with `bad argument #2 to 'method' (string expected, got light userdata)` on some boxes (broken luasec). The Makefile sets `fs_use_modules = false` in the user config, which routes downloads through `wget`/`curl` instead.

```bash
sudo apt install wget    # if not already present
make deps
```

If `make deps` runs as a non-root user and you need system-wide install, prefix with `sudo`; otherwise pass `--local` via `LUAROCKS="luarocks-5.4 --local"`.

### Running the suite

```bash
make test          # equivalent to: busted
```

The `busted` binary at `/usr/local/bin/busted` is installed by `make deps` and is bound to Lua 5.4. If you have a previous Lua-5.1 busted on PATH, `make deps` overwrites it (the old one is preserved as `busted~`).

Current coverage:

| Spec file                     | Module under test                                                    |
| ----------------------------- | -------------------------------------------------------------------- |
| `chaos_spec.lua`              | fault-injection resilience (`ChaosProducer` over Broker/Producer)    |
| `commitlog_spec.lua`          | `src/commitlog/commitlog.lua` — append, recovery, compaction         |
| `commitlog_backend_spec.lua`  | commitlog storage backend, end-to-end via Broker/Producer/Consumer   |
| `consumer_assignment_spec.lua`| `src/broker/consumer.lua` — `set_assignment` / `owns` filtering       |
| `crc32_spec.lua`              | `src/core/crc32.lua` — IEEE 802.3 CRC-32                             |
| `future_spec.lua`             | `src/core/future.lua` — one-shot coroutine future                    |
| `group_spec.lua`              | `src/broker/groups.lua` — consumer-group lifecycle FSM               |
| `group_assignment_spec.lua`   | end-to-end proof a member polls only its assigned partitions         |
| `group_protocol_spec.lua`     | `src/wire/protocol.lua` — JOIN/LEAVE/HEARTBEAT wire format         |
| `message_spec.lua`            | `src/record/message.lua` — message wire format                       |
| `offset_manager_spec.lua`     | `src/storage/offset_manager.lua` — durable group offsets             |
| `partition_spec.lua`          | `src/storage/topic_manager.lua` — partition append, read, recovery via TopicManager |
| `segmentation_spec.lua`       | `src/storage/segmentation.lua` — `SegmentedPartition` roll & lookup  |
| `sql_lexer_spec.lua`          | `src/repl/sql/lexer.lua` — MQL console tokenizer                     |
| `sql_parser_spec.lua`         | `src/repl/sql/parser.lua` — MQL statement grammar                    |
| `time_spec.lua`               | `src/core/time.lua` — duration constants                             |
| `topic_manager_spec.lua`      | `src/storage/topic_manager.lua` — topic/partition creation           |
| `util_spec.lua`               | `src/core/util.lua` — topic-name validation                          |

Credit:

https://github.com/kyleconroy/lua-state-machine/tree/master
https://github.com/travisjeffery/jocko/tree/master
https://support.tools/post/building-kafka-clone-in-go/
https://github.com/dpapavas/luaprompt
https://github.com/giann/croissant/tree/master
