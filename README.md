<p align="center">
  <img src="assets/logo.jpg" alt="MoonMQ logo" width="180" style="border-radius: 16px;" />
</p>

<h1 align="center">MoonMQ</h1>

<p align="center">
  A log-structured, partitioned message broker written in <b>pure Lua</b>.
</p>

<p align="center">
  <a href="https://github.com/HilthonTT/MoonMQ/actions/workflows/ci.yml"><img src="https://github.com/HilthonTT/MoonMQ/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/Lua-5.4-2C2D72?logo=lua&logoColor=white" alt="Lua 5.4" />
  <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT" />
</p>

---

Clients speak a compact binary protocol over TCP to produce, fetch or
subscribe, manage topics and commit consumer offsets. Topics are partitioned
append-only logs with CRC-checked on-disk records.

| | |
| --- | --- |
| **Storage** | Segmented log with time + offset indexes, group-commit fsync, retention, crash recovery; jocko-style backend with key compaction |
| **Delivery** | Pull (`FETCH`) or push (`SUBSCRIBE`), record headers, consumer groups with durable offsets, DLQ with NACK, batching on both sides |
| **Correctness** | Idempotent producer, multi-partition transactions, `read_committed` |
| **Replication** | Raft-elected leader, in-sync replica set for `acks=all`, high watermark, epoch-based truncation, client leader-following |
| **Cluster** | Static peers, autobalancer, live partition migration, cluster-wide groups, optional Raft metadata controller |
| **Security** | TLS/mTLS with `SIGHUP` reload, per-user ACLs, SCRAM-SHA-256 with channel binding, SASL EXTERNAL, quotas |
| **Ops** | Prometheus `/metrics`, JSON `/stats`, per-IP lockout, SQL-like console (MQL) |

## Quick start

```bash
sudo apt install lua5.4 lua-socket libssl-dev zlib1g-dev   # Debian/Ubuntu
make deps          # Lua dependencies, see docs/installation.md
make run           # or: lua5.4 main.lua  (--repl for the MQL console)
```

```lua
local Client = require("src.client")
local c = assert(Client.new{ host = "127.0.0.1", port = 9092,
                             username = "admin", password = "admin" })

assert(c:produce("orders", "key-1", "payload", { ["trace-id"] = "abc123" }))
local res = assert(c:join_group("billing", { "orders" }))
```

More in [docs/client.md](docs/client.md) and `src/examples/`.

> [!WARNING]
> With no credential configured the broker starts **open**, every listener is
> **plaintext** until a `Tls` block says otherwise, and the metrics endpoints
> are unauthenticated by default. See [docs/security.md](docs/security.md).

## Testing

```bash
make check         # luacheck + busted + storage smoke test — what CI runs
make test          # busted only
```

## Documentation

Start with [DESIGN.md](DESIGN.md), then [CONTRIBUTING.md](CONTRIBUTING.md).

| Doc | Covers |
| --- | --- |
| [installation](docs/installation.md) | Dependencies, fallbacks, luaposix build, Windows |
| [configuration](docs/configuration.md) | `appsettings.json`, connection limits, password hashes, cluster/replication blocks |
| [client](docs/client.md) | Client API, lag, seeking by time, admin calls |
| [headers](docs/headers.md) | Record headers: API, limits, on-disk and wire encoding |
| [security](docs/security.md) | TLS, users, ACLs, SCRAM, quotas, metrics auth |
| [replication](docs/replication.md) | Replication, failover, in-sync replicas |
| [cluster](docs/cluster.md) | Clustering, reassignment, autobalancer, Raft controller |
| [transactions](docs/transactions.md) | Idempotence, transactions, `read_committed` |
| [batching](docs/batching.md) · [dlq](docs/dlq.md) · [mql](docs/mql.md) | Batch formats · dead letters · console grammar |

## Credits

Built on ideas from [jocko](https://github.com/travisjeffery/jocko) (commitlog
backend), [AutoMQ](https://github.com/AutoMQ/automq) (autobalancer),
[lua-state-machine](https://github.com/kyleconroy/lua-state-machine),
[luaprompt](https://github.com/dpapavas/luaprompt),
[croissant](https://github.com/giann/croissant) and
[Building a Kafka clone in Go](https://support.tools/post/building-kafka-clone-in-go/).

[MIT License](LICENSE) · [Security policy](SECURITY.md)
