# Contributing to MoonMQ

Thanks for taking a look! This document covers environment setup, the
checks every change must pass, and the conventions the codebase follows.

## Setup

Linux (or WSL on Windows) with Lua 5.4:

```bash
sudo apt install lua5.4 liblua5.4-dev luarocks wget
make deps          # busted, luasocket, dkjson, lua-zlib, luacheck, …
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
* New config keys: wire them through `main.lua` from `appsettings.json`,
  document them in the README, and give them safe defaults (features off,
  loopback binds).

## Docs map

* [README](README.md) — running, configuring, protocol overview
* [docs/architecture.md](docs/architecture.md) — module map and data flow
* [docs/cluster.md](docs/cluster.md) — clustering, reassignment, autobalancer
* [docs/transactions.md](docs/transactions.md) — transaction semantics
