import { readFile } from "node:fs/promises";
import path from "node:path";
import { posix } from "node:path";

import type { Element, Root as HastRoot } from "hast";
import type { Blockquote, Root as MdastRoot, Text } from "mdast";
import rehypePrettyCode from "rehype-pretty-code";
import rehypeSlug from "rehype-slug";
import rehypeStringify from "rehype-stringify";
import remarkGfm from "remark-gfm";
import remarkParse from "remark-parse";
import remarkRehype from "remark-rehype";
import { unified } from "unified";
import { visit } from "unist-util-visit";

import { pageForFile } from "./docs";
import { asset, site } from "./site";

const repoRoot = path.resolve(process.cwd(), "..");

const alertKinds = ["NOTE", "TIP", "IMPORTANT", "WARNING", "CAUTION"];

function remarkAlerts() {
  return (tree: MdastRoot) => {
    visit(tree, "blockquote", (node: Blockquote) => {
      const first = node.children[0];
      if (first?.type !== "paragraph") return;
      const text = first.children[0];
      if (text?.type !== "text") return;
      const match = /^\[!(\w+)\]\s*/.exec(text.value);
      if (!match || !alertKinds.includes(match[1])) return;
      const kind = match[1].toLowerCase();
      (text as Text).value = text.value.slice(match[0].length);
      node.data = {
        ...node.data,
        hProperties: { className: ["alert", `alert-${kind}`], "data-kind": kind },
      };
    });
  };
}

function resolveHref(href: string, sourceFile: string): string {
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith("#")) return href;
  const [target, hash] = href.split("#");
  const anchor = hash ? `#${hash}` : "";
  const resolved = posix.normalize(posix.join(posix.dirname(sourceFile), target));
  const clean = resolved.replace(/\/$/, "");

  if (clean === "README.md" || clean === "docs") return asset(`/docs/${anchor}`);
  const doc = pageForFile(clean);
  if (doc) return asset(`${doc.href}${anchor}`);
  return `${site.repo}/blob/${site.branch}/${clean}${anchor}`;
}

function rehypeLinks(sourceFile: string) {
  return () => (tree: HastRoot) => {
    visit(tree, "element", (node: Element) => {
      if (node.tagName === "a" && typeof node.properties.href === "string") {
        const href = resolveHref(node.properties.href, sourceFile);
        node.properties.href = href;
        if (href.startsWith("http")) {
          node.properties.target = "_blank";
          node.properties.rel = ["noreferrer"];
        }
      }
      if (node.tagName === "img" && typeof node.properties.src === "string") {
        const src = node.properties.src;
        if (!/^https?:/.test(src)) {
          const file = posix.normalize(posix.join(posix.dirname(sourceFile), src));
          node.properties.src = `https://raw.githubusercontent.com/HilthonTT/MoonMQ/${site.branch}/${file}`;
        }
      }
    });
  };
}

export async function renderMarkdown(file: string): Promise<string> {
  const source = await readFile(path.join(repoRoot, file), "utf8");
  const result = await unified()
    .use(remarkParse)
    .use(remarkGfm)
    .use(remarkAlerts)
    .use(remarkRehype)
    .use(rehypeSlug)
    .use(rehypeLinks(file))
    .use(rehypePrettyCode, { theme: "tokyo-night", keepBackground: false })
    .use(rehypeStringify)
    .process(source);
  return String(result);
}
