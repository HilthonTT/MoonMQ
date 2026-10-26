# Contributing to MoonMQ

Thanks for taking a look! This document covers environment setup, the
checks every change must pass, and the conventions the codebase follows.

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).
Found a security issue? Don't open an issue — follow
[SECURITY.md](SECURITY.md).

## Setup

Linux (or WSL on Windows) with Lua 5.4:

```bash
sudo apt install lua5.4 liblua5.4-dev luarocks wget libssl-dev zlib1g-dev
make deps          # busted, luasocket, dkjson, lua-zlib, luaossl, luasec, luacheck, …
```

Windows note: the test/lint toolchain assumes POSIX (luaposix for real
fsync, lua-zlib builds). Develop on Windows if you like, but **run the
checks in WSL** — that's also what CI (Ubuntu) matches. If your working
tree is on a Windows drive, prefer the Windows-side `git status` (WSL git
without `core.autocrlf` reports phantom CRLF diffs).

## The gate

```bash
make check         # luacheck + busted + storage smoke test
```

Every PR must pass it — CI runs exactly this. Individually:

```bash
make lint          # luacheck src bin main.lua spec
make test          # busted            (spec/*.lua)
make smoke         # lua5.4 spec/storage_smoke.lua   (restart/recovery path)
busted spec/cluster_spec.lua   # one file while iterating
```

## Conventions

The codebase is deliberately consistent; match what's around you.

* **Modules are metatable "classes"**: `local T = {}; T.__index = T;
  T.new(...) → setmetatable({...}, T)`. No class library.
* **Errors are return values**: `(result, nil)` on success, `(nil, err)` on
  failure, with `err` a descriptive string. `assert()` is for programmer
  errors (bad argument types), not runtime failures. Don't throw across
  module boundaries.
* **Dependencies point downward** — `server` → `broker` → `storage` →
  `record`/`io`. Never require an upper layer from a lower one; pass
  callbacks/factories instead (see `Broker:attach_committer_factory`).
* **Comment the why, not the what.** The house style explains invariants,
  crash-ordering decisions, and attack reasoning at the site that owns them
  — keep that density when you touch such code.
* **The reactor is single-threaded and cooperative.** State is consistent
  between yield points; anything long-running must yield (`reactor:sleep(0)`)
  or be justified in a comment.
* **Durability ordering is sacred.** If you touch a write path: data before
  index, fsync before ack, checkpoint/flag writes last. Say the ordering out
  loud in a comment.
* **Logging**: `local log = require("src.log.logger").get("component")`.
  Never format untrusted bytes into a message yourself — the logger escapes
  control characters, but keep messages single-line by construction.
* **Metrics**: Prometheus conventions via `src/metrics` (`moonmq_*` names,
  labels bounded — never label by anything unbounded like connection id).

## Tests

* Specs live in `spec/*_spec.lua` (busted). Use the temp-dir + `rmdir`
  pattern from `topic_manager_spec.lua`; never write into the repo.
* `spec/helper.lua` (loaded through `.busted`) closes every `TopicManager` a
  test opened once the test ends, so partition file handles don't leak across
  specs. It does not remove temp dirs — that is still the spec's job.
* Bug fixes come with a regression test that fails before the fix
  (`spec/review_fixes_spec.lua` and `spec/bugfix_regression_spec.lua` show
  the shape).
* Prefer real components over mocks (real Broker on a temp dir is cheap).
  Mock only at the network boundary — the duck-typed peer/replica clients
  exist exactly so tests can inject in-process fakes.

## PRs

* Keep refactors and behavior changes in separate commits.
* If you change the wire protocol, the on-disk format, or an inter-broker
  endpoint: document the compatibility story in the PR description (clean
  break vs. read-compat), and update `docs/`.
* New config keys: map them from `appsettings.json` in
  `src/server/options.lua`, add a case to `spec/options_spec.lua`, document
  them in [docs/configuration.md](docs/configuration.md) (or the feature's
  own doc), and give them safe defaults
  (features off, loopback binds). Builders there return `nil, err` instead
  of exiting; `main.lua` owns the one exit path.

## Docs map

* [README](README.md) — feature overview and quick start
* [DESIGN.md](DESIGN.md) — layering, module map, data flow, wire protocol
* [docs/installation.md](docs/installation.md) — dependencies, fallbacks, luaposix build, Windows
* [docs/configuration.md](docs/configuration.md) — every `appsettings.json` section
* [docs/client.md](docs/client.md) — the Lua client API
* [docs/headers.md](docs/headers.md) — record headers and their encoding
* [docs/security.md](docs/security.md) — TLS, users, ACLs, SCRAM, quotas, metrics auth
* [docs/replication.md](docs/replication.md) — replication, failover, in-sync replicas
* [docs/cluster.md](docs/cluster.md) — clustering, reassignment, autobalancer, Raft controller
* [docs/transactions.md](docs/transactions.md) — transaction semantics
* [docs/batching.md](docs/batching.md) — batch wire formats and guarantees
* [docs/dlq.md](docs/dlq.md) — dead-letter queue and NACK semantics
* [docs/mql.md](docs/mql.md) — the interactive console's grammar
* [docs/roadmap-security-consensus.md](docs/roadmap-security-consensus.md) — design notes behind TLS and controller consensus
