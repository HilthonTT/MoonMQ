## What & why

<!-- What does this change do, and what problem does it solve? -->

## How it works

<!-- Design notes worth keeping: invariants, ordering decisions, trade-offs. -->

## Checklist

- [ ] `make check` passes (luacheck + busted + smoke) — see CONTRIBUTING.md
- [ ] Bug fix → includes a regression test that fails without the fix
- [ ] Wire/on-disk/inter-broker format change → compatibility story described
- [ ] New config keys → wired in `main.lua`, documented, safe defaults
- [ ] Docs updated if behavior or layout changed (`README.md`, `docs/`)
