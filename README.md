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
| **Throughput** | Record batching on both sides: one frame for N records, one fsync per partition instead of per record. |
| **Correctness** | Idempotent producer (PID + sequence dedupe), multi-partition transactions, `read_committed` isolation with LSO. |
| **Cluster** | Static-membership peers, AutoMQ-style autobalancer, live partition migration, cluster-wide consumer groups, single-leader replication with `acks=all`. |
| **Ops** | Prometheus `/metrics`, JSON `/stats`, PBKDF2 auth with per-IP lockout, an interactive SQL-like console (MQL). |

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
console instead — a small SQL-like language over the wire protocol
(`CREATE TOPIC`, `PRODUCE INTO`, `FETCH FROM`, `JOIN GROUP`, …). Grammar:
[docs/mql.md](docs/mql.md), or `HELP;` in-session.

```lua
local Client = require("src.client")
local c = assert(Client.new{ host = "127.0.0.1", port = 9092,
                             username = "admin", password = "admin",
                             idempotent = true })

local ack   = assert(c:produce("orders", "key-1", "payload"))  -- {partition, offset, seq}
local acks  = assert(c:produce_batch("orders", {              -- one frame, one fsync/partition
    { key = "k1", value = "v1" },
    { key = "k2", value = "v2" },
}))
local res   = assert(c:join_group("billing", { "orders" }))    -- {member_id, assignment}
```

Runnable examples: `src/examples/tcp_client.lua`,
`src/examples/consumer_group.lua`.

## Requirements

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
make hash PASSWORD=yourpw
```

> [!WARNING]
> With no usable credential the broker starts **open** (no authentication)
> and logs a warning. The metrics endpoints are unauthenticated too — keep
> `MetricsHost` on loopback or firewall the port.

There is no built-in log rotation — pair `Logging.File` with `logrotate(8)`
using `copytruncate` (the broker holds the FD open). If the path can't be
opened it logs a one-time WARN and falls back to stderr.

Clustering is configured under `Server.Cluster` / `Server.Autobalance`:

```json
"Server": {
  "Cluster":     { "BrokerId": "b1", "Port": 9095,
                   "Peers": [ { "Id": "b2", "Address": "10.0.0.2:9095" } ] },
  "Autobalance": { "IntervalSeconds": 60, "DryRun": true }
}
```

## Testing

```bash
make check         # luacheck + busted + storage smoke test — what CI runs
make test          # busted only
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

## Documentation

**New here?** Read [DESIGN.md](DESIGN.md) — layering, module map, the produce
and fetch paths, wire protocol, and metrics — then
[CONTRIBUTING.md](CONTRIBUTING.md).

| Doc | Covers |
| --- | --- |
| [DESIGN.md](DESIGN.md) | Architecture: layering, code layout, data flow, wire protocol, observability |
| [docs/cluster.md](docs/cluster.md) | Clustering, partition reassignment, autobalancer |
| [docs/transactions.md](docs/transactions.md) | Idempotent producer, transactions, `read_committed` |
| [docs/batching.md](docs/batching.md) | Batch wire formats, guarantees, limits |
| [docs/dlq.md](docs/dlq.md) | Dead-letter queue and NACK semantics |
| [docs/mql.md](docs/mql.md) | The interactive console's SQL-like grammar |
| [docs/roadmap-security-consensus.md](docs/roadmap-security-consensus.md) | Scoped-but-unshipped: TLS, controller consensus |
| [SECURITY.md](SECURITY.md) | Reporting a vulnerability |

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
