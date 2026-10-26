export const site = {
  name: "MoonMQ",
  tagline: "A partitioned message broker in pure Lua",
  repo: "https://github.com/HilthonTT/MoonMQ",
  branch: "main",
  install:
    "git clone https://github.com/HilthonTT/MoonMQ && cd MoonMQ && make deps && make run",
};

export function asset(path: string): string {
  const base = process.env.NEXT_PUBLIC_BASE_PATH ?? "";
  return `${base}${path.startsWith("/") ? path : `/${path}`}`;
}
