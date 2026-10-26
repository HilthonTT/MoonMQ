# MoonMQ website

The documentation site for MoonMQ, built with Next.js (App Router, static export) and Tailwind CSS.

```sh
npm install
npm run dev        # http://localhost:3000
npm run build      # static site in out/
```

- The landing page is `app/page.tsx`; the docs introduction is `app/docs/page.tsx`.
- Every other docs page is rendered at build time from the Markdown in the repository (`docs/*.md`, `DESIGN.md`, `CONTRIBUTING.md`), so edit those files rather than the site. Links between them are rewritten to site routes; links to other repository files point at GitHub.
- The sidebar order and the file behind each page are in `lib/docs.ts`.
- Pushing to `main` deploys to GitHub Pages through `.github/workflows/website.yml`. Set `NEXT_PUBLIC_BASE_PATH` when serving from a sub-path.
