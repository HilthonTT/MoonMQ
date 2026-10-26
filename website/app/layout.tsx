import type { Metadata } from "next";
import { JetBrains_Mono, Sora } from "next/font/google";

import { asset, site } from "@/lib/site";
import "./globals.css";

const sora = Sora({
  subsets: ["latin"],
  variable: "--font-sora",
});
const jetbrains = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-jetbrains",
});

export const metadata: Metadata = {
  title: {
    default: `${site.name} — ${site.tagline}`,
    template: `%s · ${site.name}`,
  },
  description:
    "MoonMQ is a log-structured, partitioned message broker written in pure Lua: consumer groups, transactions, replication with failover, clustering, TLS, SCRAM and ACLs.",
  icons: { icon: asset("/favicon.svg") },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className={`${sora.variable} ${jetbrains.variable}`}>
      <body className="min-h-screen font-sans antialiased">{children}</body>
    </html>
  );
}
