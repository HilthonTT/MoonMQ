import Link from "next/link";
import {
  Activity,
  ArrowRight,
  Copy,
  Database,
  Lock,
  Network,
  Send,
  ShieldCheck,
} from "lucide-react";

import { Code } from "@/components/code";
import { CopyCommand } from "@/components/copy-command";
import { GithubIcon } from "@/components/github-icon";
import { LogoMark } from "@/components/logo";
import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { site } from "@/lib/site";

function Backdrop() {
  return (
    <div
      aria-hidden
      className="pointer-events-none absolute inset-x-0 top-0 -z-10 h-[56rem] overflow-hidden [mask-image:linear-gradient(to_bottom,black_45%,transparent)]"
    >
      <div className="bg-lanes absolute inset-0" />
      <div className="absolute top-[-14rem] left-1/2 size-[44rem] -translate-x-1/2 rounded-full bg-brand-deep/40 blur-3xl" />
      <div className="absolute top-24 left-[62%] size-56 rounded-full bg-moon/10 blur-3xl" />
    </div>
  );
}

function Section({
  id,
  eyebrow,
  title,
  intro,
  children,
}: {
  id?: string;
  eyebrow: string;
  title: React.ReactNode;
  intro?: string;
  children: React.ReactNode;
}) {
  return (
    <section id={id} className="mx-auto max-w-7xl scroll-mt-24 px-5 py-24">
      <p className="font-mono text-xs uppercase tracking-[0.25em] text-moon">
        {eyebrow}
      </p>
      <h2 className="mt-3 max-w-3xl text-3xl font-semibold tracking-tight text-white sm:text-4xl">
        {title}
      </h2>
      {intro && (
        <p className="mt-4 max-w-2xl text-lg leading-relaxed text-mist-300">
          {intro}
        </p>
      )}
      <div className="mt-12">{children}</div>
    </section>
  );
}

const partitions = [
  { records: 5, duration: 7, offset: 0 },
  { records: 3, duration: 9, offset: 1.5 },
  { records: 4, duration: 6, offset: 0.7 },
  { records: 3, duration: 8, offset: 2.4 },
];

const members = [
  { name: "member-a", parts: "p0 · p1" },
  { name: "member-b", parts: "p2 · p3" },
];

function Lanes() {
  return (
    <div className="grid items-center gap-6 rounded-3xl border border-white/[0.07] bg-ink-850 p-6 sm:p-8 lg:grid-cols-[10rem_1fr_11rem]">
      <div className="space-y-3">
        <p className="font-mono text-xs uppercase tracking-widest text-mist-400">
          Producers
        </p>
        {["checkout", "payments"].map((p) => (
          <div
            key={p}
            className="rounded-xl border border-white/10 bg-white/[0.03] px-4 py-2.5 font-mono text-sm text-mist-100"
          >
            {p}
          </div>
        ))}
      </div>

      <div>
        <p className="mb-3 font-mono text-xs uppercase tracking-widest text-mist-400">
          Topic <span className="text-brand-light">orders</span> · 4 partitions
        </p>
        <div className="space-y-2.5">
          {partitions.map((p, i) => (
            <div key={i} className="flex items-center gap-3">
              <span className="w-6 font-mono text-xs text-mist-400">p{i}</span>
              <div className="relative h-8 flex-1 overflow-hidden rounded-md bg-gradient-to-r from-brand-deep/50 to-brand/20 ring-1 ring-white/5">
                {Array.from({ length: p.records }, (_, r) => (
                  <span
                    key={r}
                    className="record absolute top-1/2 -mt-1.5 size-3 rounded-full bg-moon shadow-[0_0_12px] shadow-moon/60"
                    style={
                      {
                        "--duration": `${p.duration}s`,
                        "--delay": `${p.offset + (r * p.duration) / p.records}s`,
                        animationDelay: `-${p.offset + (r * p.duration) / p.records}s`,
                      } as React.CSSProperties
                    }
                  />
                ))}
              </div>
            </div>
          ))}
        </div>
        <p className="mt-3 text-xs text-mist-400">
          Append-only, CRC-checked segments with offset and time indexes.
        </p>
      </div>

      <div className="space-y-3">
        <p className="font-mono text-xs uppercase tracking-widest text-mist-400">
          Group <span className="text-brand-light">billing</span>
        </p>
        {members.map((m) => (
          <div
            key={m.name}
            className="rounded-xl border border-white/10 bg-white/[0.03] px-4 py-2.5"
          >
            <p className="font-mono text-sm text-mist-100">{m.name}</p>
            <p className="font-mono text-xs text-moon">{m.parts}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

const pillars = [
  {
    icon: Database,
    title: "Storage",
    text: "Segmented log with time and offset indexes, group-commit fsync, retention and crash recovery. A jocko-style backend adds key compaction.",
    href: "/docs/design/",
  },
  {
    icon: Send,
    title: "Delivery",
    text: "Pull with FETCH or push with SUBSCRIBE. Record headers, consumer groups with durable offsets, a DLQ with NACK, and batching on both sides.",
    href: "/docs/client/",
  },
  {
    icon: ShieldCheck,
    title: "Correctness",
    text: "Idempotent producers, multi-partition transactions and read_committed consumers.",
    href: "/docs/transactions/",
  },
  {
    icon: Copy,
    title: "Replication",
    text: "Raft-elected leaders, an in-sync replica set for acks=all, a high watermark, epoch-based truncation and clients that follow the leader.",
    href: "/docs/replication/",
  },
  {
    icon: Network,
    title: "Cluster",
    text: "Static peers, an autobalancer, live partition migration, cluster-wide groups and an optional Raft metadata controller.",
    href: "/docs/cluster/",
  },
  {
    icon: Lock,
    title: "Security",
    text: "TLS and mTLS with SIGHUP reload, per-user ACLs, SCRAM-SHA-256 with channel binding, SASL EXTERNAL and quotas.",
    href: "/docs/security/",
  },
  {
    icon: Activity,
    title: "Operations",
    text: "Prometheus /metrics, JSON /stats, per-IP lockout and an SQL-like console for poking at a live broker.",
    href: "/docs/mql/",
  },
];

const clientCode = `
local Client = require("src.client")
local c = assert(Client.new{ host = "127.0.0.1", port = 9092,
                             username = "admin", password = "admin" })

assert(c:produce("orders", "key-1", "payload", { ["trace-id"] = "abc123" }))
local res = assert(c:join_group("billing", { "orders" }))
`;

const mqlCode = `
mq> CREATE TOPIC orders PARTITIONS 4;
OK topic 'orders' created (4 partitions)

mq> PRODUCE INTO orders KEY 'k1' VALUE 'hello world';
OK produced to orders → partition 2, offset 0

mq> COMMIT orders PARTITION 2 OFFSET 1;
OK committed orders[2] @ offset 1
`;

export default function Home() {
  return (
    <>
      <SiteHeader />
      <main className="relative isolate">
        <Backdrop />

        <section className="mx-auto max-w-7xl px-5 pt-20 pb-16 text-center sm:pt-28">
          <LogoMark className="mx-auto size-24 drop-shadow-[0_0_40px_rgba(139,143,247,0.35)]" />
          <Link
            href="/docs/"
            className="mt-8 inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.03] px-4 py-1.5 text-xs text-mist-300 transition hover:border-white/25 hover:text-white"
          >
            <span className="size-1.5 rounded-full bg-moon" />
            Pure Lua 5.4 · binary protocol over TCP · MIT licensed
            <ArrowRight className="size-3" />
          </Link>
          <h1 className="mx-auto mt-8 max-w-4xl text-5xl font-semibold tracking-tight text-white sm:text-7xl">
            A message broker, <span className="text-glow">written in Lua.</span>
          </h1>
          <p className="mx-auto mt-6 max-w-2xl text-lg leading-relaxed text-mist-300 sm:text-xl">
            MoonMQ is a log-structured, partitioned broker. Produce, fetch or
            subscribe, run consumer groups and transactions, and replicate
            across a cluster — all in a codebase you can read in an afternoon.
          </p>
          <div className="mt-10 flex flex-col items-center gap-4">
            <CopyCommand command={site.install} />
            <div className="flex flex-wrap justify-center gap-3">
              <Link
                href="/docs/installation/"
                className="inline-flex items-center gap-2 rounded-xl bg-white px-5 py-2.5 text-sm font-semibold text-ink-900 transition hover:bg-mist-100"
              >
                Get started <ArrowRight className="size-4" />
              </Link>
              <a
                href={site.repo}
                className="inline-flex items-center gap-2 rounded-xl border border-white/10 px-5 py-2.5 text-sm font-semibold text-white transition hover:border-white/30"
              >
                <GithubIcon /> Star on GitHub
              </a>
            </div>
          </div>
        </section>

        <Section
          id="how-it-works"
          eyebrow="How it works"
          title={
            <>
              Topics are <span className="text-glow">partitioned logs</span>.
            </>
          }
          intro="Producers append records to a partition chosen by key. Each partition is an ordered, durable log. Consumer groups split partitions between members and commit how far they have read, so a restart picks up where it left off."
        >
          <Lanes />
        </Section>

        <Section
          id="features"
          eyebrow="Features"
          title={
            <>
              The parts of Kafka you actually use,{" "}
              <span className="text-glow">without the JVM</span>.
            </>
          }
          intro="Durable storage, exactly-once building blocks, replication with failover and a real security model, each covered by its own spec suite."
        >
          <div className="grid gap-px overflow-hidden rounded-3xl border border-white/[0.07] bg-white/[0.07] sm:grid-cols-2 lg:grid-cols-4">
            {pillars.map(({ icon: Icon, title, text, href }) => (
              <Link
                key={title}
                href={href}
                className="group bg-ink-850 p-7 transition hover:bg-ink-800"
              >
                <Icon className="size-5 text-brand" />
                <h3 className="mt-4 flex items-center gap-2 font-semibold text-white">
                  {title}
                  <ArrowRight className="size-3.5 opacity-0 transition group-hover:opacity-100" />
                </h3>
                <p className="mt-1.5 text-sm leading-relaxed text-mist-300">
                  {text}
                </p>
              </Link>
            ))}
            <div className="flex flex-col justify-center bg-ink-850 p-7">
              <p className="text-sm text-mist-400">Read how it fits together</p>
              <Link
                href="/docs/design/"
                className="mt-2 inline-flex items-center gap-2 font-semibold text-brand-light hover:underline"
              >
                Architecture <ArrowRight className="size-4" />
              </Link>
            </div>
          </div>
        </Section>

        <Section
          eyebrow="Use it"
          title="From Lua, or from the console."
          intro="The reference client covers produce, fetch, subscribe, groups, transactions and admin calls. The MQL console speaks a small SQL-like language to a running broker."
        >
          <div className="grid gap-5 lg:grid-cols-2">
            <Code code={clientCode} lang="lua" title="client.lua" />
            <Code code={mqlCode} lang="sql" title="lua5.4 main.lua --repl" />
          </div>
          <div className="mt-8 flex flex-wrap gap-6 text-sm">
            <Link
              href="/docs/client/"
              className="inline-flex items-center gap-2 font-semibold text-brand-light hover:underline"
            >
              Client API <ArrowRight className="size-4" />
            </Link>
            <Link
              href="/docs/mql/"
              className="inline-flex items-center gap-2 font-semibold text-brand-light hover:underline"
            >
              MQL reference <ArrowRight className="size-4" />
            </Link>
          </div>
        </Section>

        <section className="mx-auto max-w-7xl px-5 pb-28">
          <div className="relative overflow-hidden rounded-3xl border border-white/[0.07] bg-ink-850 px-8 py-16 text-center">
            <div
              aria-hidden
              className="absolute inset-0 -z-0 bg-gradient-to-br from-brand/10 via-transparent to-moon/10"
            />
            <h2 className="relative text-3xl font-semibold tracking-tight text-white sm:text-4xl">
              Run a broker in under a minute.
            </h2>
            <p className="relative mx-auto mt-4 max-w-xl text-mist-300">
              Install Lua 5.4 and a few rocks, then start it on port 9092.
              Turn on TLS and credentials before it leaves your machine.
            </p>
            <div className="relative mt-8 flex justify-center">
              <CopyCommand command={site.install} />
            </div>
          </div>
        </section>
      </main>
      <SiteFooter />
    </>
  );
}
