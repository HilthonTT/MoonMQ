# Configuration

`appsettings.json` in the working directory, deep-merged with
`appsettings.<MOONMQ_ENVIRONMENT>.json` when present (default environment:
`Development`).

| Section | Keys |
| --- | --- |
| `Server` | `Host`, `Port` (9092), `DataDir`, `StorageBackend` (`segmented` default, or `commitlog`), `MaxConnections` (960), `MaxConnectionsPerIP`, `FdReserve` (64), `MaxFrameSize`, `MaxPendingBytes` (16 MiB queued per connection), `SendDeadline` (30s), `MaxTopics` (1024), `MaxListTopics`, `MaxFetchRecords` (1000 records per `FETCH`, whatever the client asks for), `TransactionTimeout` (900s; open transactions older than this are aborted, 0 = never), `IdleDeadline` (60s), `PreAuthReadDeadline` (5s), `HandshakeDeadline` (5s), `HeartbeatInterval` (30s), `HeartbeatMissThreshold` (3), `ProducerExpirySeconds` (86400), `ProducerExpiryCheckIntervalSeconds` (300), `Tls`, `MetricsHost` (127.0.0.1), `MetricsPort` (9090), `MetricsTls`, `MetricsAuth`, `Acks`, `Replication`, `Cluster`, `Autobalance` |
| `Server.GroupCommit` | `LingerMs` (2), `MaxWaiters` (64) — how long an fsync waits to batch acks, and how many it batches at most |
| `Server.Dlq` | `Suffix` (`.dlq`), `MaxDeliveries` (3) — see [dlq.md](dlq.md) |
| `Auth` | `Username` + either `PasswordHash` (`pbkdf2-sha256$…` or `scram-sha-256$…`) or plaintext `Password`; `MaxFailures`, `FailureWindow` (seconds), `BanDuration` (seconds, default 900) for per-IP lockout. `Users` (multi-tenant list with `Acls` + `Quota`) and `Quotas` (`Default`/`Topics`/`BurstSeconds`) — see [security.md](security.md) |
| `Logging` | `Level` (`DEBUG`/`INFO`/`WARN`/`ERROR`), `File` (empty = stderr only), `LogToStderr` (default true, tees both) |
| `Gateway` | `Host` (127.0.0.1), `Port` (8080), `Token` (**required**; or `MOONMQ_GATEWAY_TOKEN`), `MaxConnections` (64), `MoonMQHost` (127.0.0.1), `MoonMQPort` (`Server.Port`), `MoonMQUsername` (falls back to `Auth.Username`, with a superuser warning), `MoonMQPassword`, `MoonMQTimeout` (30s), `PoolMax` (8) — the HTTP gateway started by `make gateway` (`bin/gateway.lua`); see [HTTP gateway](#http-gateway) |

The metrics listener always starts; `MetricsPort` has no "off" value, so keep
`MetricsHost` on loopback or set `MetricsAuth` (see [security.md](security.md)).

## Connection limits

`MaxConnections` is a ceiling, not a promise: the event loop multiplexes with
`select(2)`, which cannot watch a descriptor numbered at or above
`FD_SETSIZE` (1024 on Linux). The broker clamps the configured value to
`FD_SETSIZE` minus a reserve for listeners and the log files each open
partition holds, logs the clamp, and refuses any connection whose descriptor
lands above the line — one refused connection instead of a dead broker. A node
with many partitions should raise the reserve (`Server.FdReserve`); watch
`moonmq_connections_refused_fd_total` to see whether the budget is too tight.

## Password hashes

600 000 PBKDF2 iterations per NIST 2024 guidance (existing hashes keep their
stored iteration count):

```bash
make hash PASSWORD=yourpw SCRAM=1   # scram-sha-256$… — preferred
make hash PASSWORD=yourpw           # pbkdf2-sha256$… — login-equivalent if leaked
lua5.4 bin/moonmq-hash.lua --scram  # reads the password from stdin
```

`make hash` pipes the password to the tool on stdin, so it never shows up in
the hasher's process arguments. A password passed as a command-line argument
still works, but the tool warns, since `ps` and shell history can expose it.

Password generation, SCRAM nonces and salts need a CSPRNG: `/dev/urandom`,
or luaossl's `openssl.rand` as a fallback. With neither (for example PUC Lua
on Windows without luaossl), the broker refuses to start rather than fall back
to `math.random`.

> [!IMPORTANT]
> Verification cost scales linearly with the iteration count, it runs **inline
> on the single reactor thread**, and the stored hash carries its own iteration
> count — so a 600 000-iteration hash generated on a host with luaossl is
> still accepted by a broker without it, where it takes minutes per login.
> The broker measures this at startup and warns when a login would cost more
> than a second; install luaossl, or re-hash with a count the host can afford
> (`lua bin/moonmq-hash.lua --iterations <n>`, password on stdin).

Users, ACLs, SCRAM, quotas and TLS: [security.md](security.md).

## HTTP gateway

`make gateway` starts `bin/gateway.lua`, a REST front end that holds a pool of
broker connections. Every request it relays runs as the account it logs in
with, so the gateway is locked down by default:

* **A bearer token is required.** Set `Gateway.Token` (or the
  `MOONMQ_GATEWAY_TOKEN` environment variable; the config value wins). The
  gateway refuses to start without one, and every route (including `/` and
  `/healthz`) answers `401` unless the request carries
  `Authorization: Bearer <token>`. The comparison is constant-time.
* **It binds loopback** (`127.0.0.1`) by default. Put TLS in front of it
  (a reverse proxy) before you expose it.
* **Log in with a dedicated account.** Set `Gateway.MoonMQUsername` and
  `MoonMQPassword` to a user whose ACLs cover only the topics the gateway
  should reach. If `MoonMQUsername` is not set, the gateway falls back to
  `Auth.Username` and logs a warning, because that account is a superuser.
* `Gateway.MaxConnections` (64) caps concurrent HTTP clients; extra
  connections are closed on accept.

```json
"Gateway": {
  "Host": "127.0.0.1", "Port": 8080,
  "Token": "",
  "MoonMQUsername": "gateway-svc", "MoonMQPassword": "..."
}
```

## Logging

There is no built-in log rotation — pair `Logging.File` with `logrotate(8)`
using `copytruncate` (the broker holds the FD open). If the path can't be
opened it logs a one-time WARN and falls back to stderr.

## Clustering

```json
"Server": {
  "Cluster":     { "BrokerId": "b1", "Port": 9095,
                   "Peers": [ { "Id": "b2", "Address": "10.0.0.2:9095" } ],
                   "Raft":  { "Enabled": true } },
  "Autobalance": { "IntervalSeconds": 60, "DryRun": true }
}
```

`Raft` makes the controller and partition ownership facts the cluster agrees
on: a majority-elected controller, ownership changes committed through a
replicated log, and the balance loop running only on the controller. Omit the
block to keep epoch fencing. Details: [cluster.md](cluster.md).

## Replication

```json
"Server": {
  "Acks": "all",
  "Replication": { "Enabled": true, "ReplicaId": 1, "Role": "leader",
                   "ReplicatePort": 9095, "ClientAddress": "10.0.0.1:9092",
                   "Peers": [ { "Id": 2, "Address": "10.0.0.2:9095", "ClientAddress": "10.0.0.2:9092" },
                              { "Id": 3, "Address": "10.0.0.3:9095", "ClientAddress": "10.0.0.3:9092" } ],
                   "Failover": { "Enabled": true } }
}
```

Without the `Failover` block, replication stays the static push mode.
Details: [replication.md](replication.md).
