import { codeToHtml } from "shiki";

export async function Code({
  code,
  lang,
  title,
}: {
  code: string;
  lang: string;
  title: string;
}) {
  const html = await codeToHtml(code.trim(), { lang, theme: "tokyo-night" });
  return (
    <div className="overflow-hidden rounded-3xl border border-white/[0.07] bg-ink-950">
      <div className="flex items-center gap-2 border-b border-white/[0.07] px-5 py-3">
        <span className="size-2.5 rounded-full bg-white/10" />
        <span className="size-2.5 rounded-full bg-white/10" />
        <span className="size-2.5 rounded-full bg-white/10" />
        <span className="ml-3 font-mono text-xs text-mist-400">{title}</span>
      </div>
      <div
        className="code-panel overflow-x-auto p-5 font-mono text-[0.82rem] leading-7"
        dangerouslySetInnerHTML={{ __html: html }}
      />
    </div>
  );
}
