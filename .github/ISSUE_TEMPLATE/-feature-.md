---
name: "[FEATURE]"
about: " Suggest a broker capability, protocol op, or operational improvement"
title: ''
labels: enhancement
assignees: HilthonTT

---

**Is your feature request related to a problem? Please describe.**
A clear and concise description of what the problem is. Ex. I'm always frustrated when [...]
 
**Describe the solution you'd like**
A clear and concise description of what you want to happen.
 
**Area**
What does this touch?
 
- [ ] Wire protocol (new opcode / frame change)
- [ ] Storage / log format (segments, compaction, retention)
- [ ] Replication / high-watermark
- [ ] Consumer groups / offset coordination
- [ ] Transactions / idempotency (EOS)
- [ ] Auth / security
- [ ] Observability (metrics / stats)
- [ ] Client library
**Prior art**
How does Kafka (or another broker) handle this? MoonMQ tracks Kafka-style semantics — link the relevant design/KIP if there is one. See `docs/transactions.md` for the deferred transaction design.
 
**Protocol / compatibility impact**
- Does it add or change an opcode (`0x01`–`0x7F` request / `0x80`–`0xFE` reply)?
- Does it change the on-disk record/segment format or break existing logs?
- Backward-compatible with the current `HELLO` version negotiation?
**Describe alternatives you've considered**
A clear and concise description of any alternative solutions or features you've considered.
 
**Additional context**
- Configuration surface: any new `appsettings.json` keys?
- Runtime constraints: must it stay pure Lua 5.4, or is a LuaJIT/FFI or LuaRocks dependency acceptable?
- Add any other context or references here.
