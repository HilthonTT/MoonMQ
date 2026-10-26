export type DocLink = { title: string; href: string };
export type DocPage = DocLink & { slug: string; file: string; description: string };
export type DocSection = { title: string; links: DocLink[] };

const page = (
  slug: string,
  file: string,
  title: string,
  description: string,
): DocPage => ({ slug, file, title, description, href: `/docs/${slug}/` });

export const pages: DocPage[] = [
  page("installation", "docs/installation.md", "Installation", "Dependencies, fallbacks, the luaposix build and Windows."),
  page("configuration", "docs/configuration.md", "Configuration", "appsettings.json, connection limits, password hashes, cluster and replication blocks."),
  page("client", "docs/client.md", "Client API", "The reference Lua client: produce, fetch, groups, lag, seeking by time, admin calls."),
  page("headers", "docs/headers.md", "Record headers", "Header API, limits, on-disk and wire encoding."),
  page("batching", "docs/batching.md", "Batching", "Batch formats on the produce and fetch sides."),
  page("dlq", "docs/dlq.md", "Dead-letter queue", "NACK, retries and dead-letter topics."),
  page("transactions", "docs/transactions.md", "Transactions", "Idempotent producers, multi-partition transactions and read_committed."),
  page("mql", "docs/mql.md", "MQL console", "The SQL-like interactive console and its grammar."),
  page("security", "docs/security.md", "Security", "TLS, users, ACLs, SCRAM, quotas and metrics auth."),
  page("replication", "docs/replication.md", "Replication", "Replication, failover and in-sync replicas."),
  page("cluster", "docs/cluster.md", "Clustering", "Clustering, reassignment, the autobalancer and the Raft controller."),
  page("design", "DESIGN.md", "Architecture", "How the broker is put together, and where to start reading."),
  page("roadmap", "docs/roadmap-security-consensus.md", "TLS & consensus notes", "Design notes behind TLS and controller consensus."),
  page("contributing", "CONTRIBUTING.md", "Contributing", "Setting up, testing and sending changes."),
];

const bySlug = new Map(pages.map((p) => [p.slug, p]));
const pick = (...slugs: string[]): DocLink[] =>
  slugs.map((s) => bySlug.get(s)!);

export const docs: DocSection[] = [
  {
    title: "Getting started",
    links: [{ title: "Introduction", href: "/docs/" }, ...pick("installation", "configuration")],
  },
  {
    title: "Producing & consuming",
    links: pick("client", "headers", "batching", "dlq", "transactions", "mql"),
  },
  {
    title: "Operating",
    links: pick("security", "replication", "cluster"),
  },
  {
    title: "Internals",
    links: pick("design", "roadmap", "contributing"),
  },
];

export const allDocs: DocLink[] = docs.flatMap((s) => s.links);

export function findPage(slug: string): DocPage | undefined {
  return bySlug.get(slug);
}

export function pageForFile(file: string): DocPage | undefined {
  return pages.find((p) => p.file === file);
}
