import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { Pencil } from "lucide-react";

import { findPage, pages } from "@/lib/docs";
import { renderMarkdown } from "@/lib/markdown";
import { site } from "@/lib/site";

type Props = { params: Promise<{ slug: string }> };

export const dynamicParams = false;

export function generateStaticParams() {
    return pages.map((p) => ({ slug: p.slug }));
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
    const page = findPage((await params).slug);
    return page ? { title: page.title, description: page.description } : {};
}

export default async function DocPage({ params }: Props) {
    const page = findPage((await params).slug);
    if (!page) notFound();
    const html = await renderMarkdown(page.file);
    return (
        <>
            <div dangerouslySetInnerHTML={{ __html: html }} />
            <p className="not-prose mt-12 text-sm">
                <a
                    href={`${site.repo}/edit/${site.branch}/${page.file}`}
                    className="inline-flex items-center gap-2 text-mist-400 transition hover:text-white"
                >
                    <Pencil className="size-3.5" /> Edit {page.file} on GitHub
                </a>
            </p>
        </>
    );
}
