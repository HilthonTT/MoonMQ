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
`sha2.lua` at the repo root — no installation needed.

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

`src/io_sync.lua` falls back to LuaJIT FFI on Windows (`_commit`, `_chsize_s` from the C runtime). Running on Windows therefore requires LuaJIT rather than stock Lua 5.4.

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

- **`src/compression.lua`** needs the `zlib` LuaRocks module (gzip support).
- **`src/snappy.lua`** needs LuaJIT's FFI and `libsnappy` (Snappy support).
- **`src/replica.lua`** loads on stock Lua, but real replication needs a peer
  broker exposing an HTTP `/replicate` endpoint.

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

  | Metric                                 | Type      | Labels                |
  | -------------------------------------- | --------- | --------------------- |
  | `moonmq_connections_open`              | gauge     | —                     |
  | `moonmq_connections_accepted_total`    | counter   | —                     |
  | `moonmq_connections_closed_total`      | counter   | `reason`              |
  | `moonmq_bytes_sent_total`              | counter   | —                     |
  | `moonmq_frames_sent_total`             | counter   | —                     |
  | `moonmq_frames_received_total`         | counter   | `op`                  |
  | `moonmq_send_duration_seconds`         | histogram | —                     |
  | `moonmq_dispatch_duration_seconds`     | histogram | `op`                  |
  | `moonmq_produce_records_total`         | counter   | `topic`               |
  | `moonmq_idempotent_produce_total`      | counter   | `topic`               |
  | `moonmq_fetch_records_total`           | counter   | `topic`               |
  | `moonmq_segment_rolls_total`           | counter   | `topic`               |
  | `moonmq_fsync_duration_seconds`        | histogram | `topic`               |
  | `moonmq_topic_count`                   | gauge     | —                     |
  | `moonmq_partition_log_bytes`           | gauge     | `topic`, `partition`  |

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

## Wire protocol

Clients and the broker speak a compact binary protocol over TCP
(`src/server/protocol.lua`). All integers are big-endian; strings are
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

| Op                           | Direction       | Purpose                                  |
| ---------------------------- | --------------- | ---------------------------------------- |
| `HELLO`                      | client → server | Protocol-version handshake               |
| `AUTH`                       | client → server | Username/password authentication         |
| `PRODUCE`                    | client → server | Append a record to a topic               |
| `FETCH`                      | client → server | Pull a batch of records (pull mode)      |
| `SUBSCRIBE`                  | client → server | Stream records as they arrive (push)     |
| `COMMIT`                     | client → server | Commit a consumer-group offset           |
| `CREATE_TOPIC`               | client → server | Create a partitioned topic               |
| `LIST_TOPICS`                | client → server | List existing topics                     |
| `PING`/`GOODBYE`             | client → server | Liveness / clean disconnect              |
| `INIT_PRODUCER_ID`           | client → server | Request a u64 PID for idempotent produce |
| `PRODUCE_IDEMPOTENT`         | client → server | Idempotent append: `(PID, seq, ...)`     |
| `WELCOME` / `AUTH_OK`        | server → client | Handshake / auth acceptance              |
| `PRODUCE_ACK`                | server → client | Partition + offset of an appended record |
| `PRODUCER_ID`                | server → client | Assigned u64 PID                         |
| `RECORD`                     | server → client | A delivered record (fetch or push)       |
| `TOPIC_LIST` / `PONG` / `OK` | server → client | Query results / acks                     |
| `ERROR`                      | server → client | Numeric error code + message             |

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
come from `src/server/uuid.lua`. A handshake watchdog drops connections that
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

| Spec file                | Module under test                                  |
| ------------------------ | -------------------------------------------------- |
| `buffer_spec.lua`        | `src/buffer.lua` — accumulating byte buffer        |
| `crc32_spec.lua`         | `src/crc32.lua` — IEEE 802.3 CRC-32                |
| `future_spec.lua`        | `src/future.lua` — one-shot coroutine future       |
| `message_spec.lua`       | `src/message.lua` — message wire format            |
| `partition_spec.lua`     | `src/partition.lua` — append, read, recovery       |
| `time_spec.lua`          | `src/time.lua` — duration constants                |
| `topic_manager_spec.lua` | `src/topic_manager.lua` — topic/partition creation |
| `util_spec.lua`          | `src/util.lua` — topic-name validation             |
