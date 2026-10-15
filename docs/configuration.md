# Configuration

`appsettings.json` in the working directory, deep-merged with
`appsettings.<MOONMQ_ENVIRONMENT>.json` when present (default environment:
`Development`).

| Section | Keys |
| --- | --- |
| `Server` | `Host`, `Port` (9092), `DataDir`, `MaxConnections` (960), `MaxConnectionsPerIP`, `FdReserve` (64), `MaxFrameSize`, `MaxTopics` (1024), `MaxListTopics`, `IdleDeadline` (60s), `PreAuthReadDeadline` (5s), `HandshakeDeadline` (5s), `Tls`, `MetricsHost`, `MetricsPort` (`null` disables), `MetricsTls`, `MetricsAuth`, `Acks`, `Replication`, `Cluster`, `Autobalance` |
| `Auth` | `Username` + either `PasswordHash` (`pbkdf2-sha256$…` or `scram-sha-256$…`) or plaintext `Password`; `MaxFailures`, `FailureWindow`, `BanDuration` for per-IP lockout. `Users` (multi-tenant list with `Acls` + `Quota`) and `Quotas` (`Default`/`Topics`/`BurstSeconds`) — see [security.md](security.md) |
| `Logging` | `Level` (`DEBUG`/`INFO`/`WARN`/`ERROR`), `File` (empty = stderr only), `LogToStderr` (default true, tees both) |

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
```

> [!IMPORTANT]
> Verification cost scales linearly with the iteration count, it runs **inline
> on the single reactor thread**, and the stored hash carries its own iteration
> count — so a 600 000-iteration hash generated on a host with luaossl is
> still accepted by a broker without it, where it takes minutes per login.
> The broker measures this at startup and warns when a login would cost more
> than a second; install luaossl, or re-hash with a count the host can afford
> (`lua bin/moonmq-hash.lua <password> <iterations>`).

Users, ACLs, SCRAM, quotas and TLS: [security.md](security.md).

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
