# Security policy

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Use GitHub's private vulnerability reporting:
[**Report a vulnerability**](https://github.com/HilthonTT/MoonMQ/security/advisories/new).
That opens a private advisory visible only to the maintainers.

Useful things to include:

- affected version or commit
- the config that triggers it (redact credentials)
- a reproducer — a spec under `spec/`, a raw frame, or a `curl` against the
  metrics port is ideal
- what an attacker gains (crash, data loss, auth bypass, disclosure)

Expect an acknowledgement within about a week. Fixes land on `main`; the
advisory is published once a fix is available.

## Supported versions

MoonMQ is pre-1.0 and has no release branches. Only the current `main` is
supported — fixes are not backported.

## Known-by-design exposure

These are documented behaviours, not vulnerabilities. Report them only if you
have found a way around the documented boundary.

- **The broker starts open when no usable credential is configured.** It logs
  a warning and accepts any `AUTH`. Set `Auth.PasswordHash` before exposing a
  port. See [docs/configuration.md](docs/configuration.md).
- **The metrics/stats HTTP endpoints are unauthenticated unless
  `Server.MetricsAuth` is set** (a bearer token, also readable from
  `MOONMQ_METRICS_TOKEN`, or HTTP Basic against `Auth.Users`). They bind
  `127.0.0.1:9090` by default; `/health` is always open. See
  [docs/security.md](docs/security.md).
- **Every listener is plaintext until its `Tls` block is set.** Client
  (`Server.Tls`), metrics (`Server.MetricsTls`), cluster (`Cluster.Tls`) and
  replication (`Replication.Tls`) traffic is plaintext TCP/HTTP by default.
  Configure TLS (optionally mTLS) per [docs/security.md](docs/security.md), or
  run MoonMQ on a trusted network.
- **Inter-broker endpoints trust the peer list.** Cluster membership is static
  config. Peers are authenticated only by the shared `Token`
  (`X-Cluster-Token`) and, when configured, mTLS on `Cluster.Tls` /
  `Replication.Tls`; with neither set, anything that can reach a broker's
  cluster or replication port is treated as a peer.

## Scope

In scope: the broker, client, wire protocol codec, storage/commitlog backends,
auth, and the cluster/replication endpoints in this repository.

Out of scope: vulnerabilities in Lua itself, LuaRocks packages, or the OS;
misconfiguration that contradicts the documented defaults above; and findings
that require an attacker to already have local filesystem access to `DataDir`.
