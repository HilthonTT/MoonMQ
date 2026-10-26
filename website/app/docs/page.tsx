import type { Metadata } from "next";
import Link from "next/link";

import { pages } from "@/lib/docs";

export const metadata: Metadata = { title: "Introduction" };

const features = [
  ["Storage", "Segmented log with time + offset indexes, group-commit fsync, retention, crash recovery; jocko-style backend with key compaction"],
  ["Delivery", "Pull (FETCH) or push (SUBSCRIBE), record headers, consumer groups with durable offsets, DLQ with NACK, batching on both sides"],
  ["Correctness", "Idempotent producer, multi-partition transactions, read_committed"],
  ["Replication", "Raft-elected leader, in-sync replica set for acks=all, high watermark, epoch-based truncation, client leader-following"],
  ["Cluster", "Static peers, autobalancer, live partition migration, cluster-wide groups, optional Raft metadata controller"],
  ["Security", "TLS/mTLS with SIGHUP reload, per-user ACLs, SCRAM-SHA-256 with channel binding, SASL EXTERNAL, quotas"],
  ["Ops", "Prometheus /metrics, JSON /stats, per-IP lockout, SQL-like console (MQL)"],
];

export default function Introduction() {
  return (
    <>
      <h1>Introduction</h1>
      <p>
        MoonMQ is a log-structured, partitioned message broker written in pure
        Lua 5.4. Clients speak a compact binary protocol over TCP to produce,
        fetch or subscribe, manage topics and commit consumer offsets. Topics
        are partitioned append-only logs with CRC-checked on-disk records.
      </p>

      <h2 id="what-you-get">What you get</h2>
      <table>
        <tbody>
          {features.map(([area, text]) => (
            <tr key={area}>
              <td>
                <strong>{area}</strong>
              </td>
              <td>{text}</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h2 id="quick-start">Quick start</h2>
      <pre>
        <code>{`sudo apt install lua5.4 liblua5.4-dev luarocks lua-socket libssl-dev zlib1g-dev
make deps          # Lua dependencies
make run           # or: lua5.4 main.lua  (--repl for the MQL console)`}</code>
      </pre>
      <pre>
        <code>{`local Client = require("src.client")
local c = assert(Client.new{ host = "127.0.0.1", port = 9092,
                             username = "admin", password = "admin" })

assert(c:produce("orders", "key-1", "payload", { ["trace-id"] = "abc123" }))
local res = assert(c:join_group("billing", { "orders" }))`}</code>
      </pre>

      <blockquote className="alert alert-warning" data-kind="warning">
        <p>
          With no credential configured the broker starts <strong>open</strong>,
          every listener is <strong>plaintext</strong> until a <code>Tls</code>{" "}
          block says otherwise, and the metrics endpoints are unauthenticated
          by default. See <Link href="/docs/security/">Security</Link>.
        </p>
      </blockquote>

      <h2 id="testing">Testing</h2>
      <pre>
        <code>{`make check         # luacheck + busted + storage smoke test — what CI runs
make test          # busted only`}</code>
      </pre>

      <h2 id="guides">Guides</h2>
      <ul>
        {pages.map((p) => (
          <li key={p.slug}>
            <Link href={p.href}>{p.title}</Link> — {p.description}
          </li>
        ))}
      </ul>
    </>
  );
}
