---
name: "[BUG]"
about: Report a broker crash, protocol error, data-loss, or wrong behavior
title: ''
labels: bug
assignees: HilthonTT

---

**Describe the bug**
A clear and concise description of what the bug is.
 
**Component**
Which part is affected?
 
- [ ] TCP broker / connection lifecycle (`src/server/`)
- [ ] Wire protocol / framing (`protocol.lua` / `framer.lua`)
- [ ] Log / partition storage (append, read, recovery, CRC)
- [ ] Consumer offsets / groups (`COMMIT`)
- [ ] Idempotent producer (`INIT_PRODUCER_ID` / `PRODUCE_IDEMPOTENT`)
- [ ] Auth (PBKDF2 / HMAC / per-IP lockout)
- [ ] Metrics / stats HTTP endpoints
- [ ] Client library (`src/client.lua`)
**To Reproduce**
Steps to reproduce the behavior:
 
1. Start the broker with '...' (config / `MOONMQ_ENVIRONMENT`)
2. Connect / produce / fetch '...'
3. See error
Minimal Lua reproduction if you can:
 
```lua
local Client = require("src.client")
-- ...
```
 
**Expected behavior**
A clear and concise description of what you expected to happen (ack, record set, error code, ...).
 
**Actual behavior**
What actually happened. Include any protocol `ERROR` code + message, or a Lua traceback.
 
```
# Paste broker logs (set Logging.Level = "DEBUG" for the most detail) and any traceback
```
 
**Environment**
- OS / distro (e.g. Ubuntu 24.04, WSL2 on Windows 11, native Windows):
- Lua runtime: **Lua 5.4** or **LuaJIT** (Windows path via `src/io_sync.lua` FFI)? Paste `lua5.4 -v` / `luajit -v`:
- LuaSocket version:
- dkjson present? luaposix present? (`lua5.4 -e 'print(require("posix.unistd").fsync)'` should print a function on Linux):
- Optional modules involved (`zlib` compression, `libsnappy`, `replica`)?
- Commit / branch:
**Configuration**
Relevant `appsettings.json` (redact secrets/hashes) — `Server` (Host/Port/DataDir/MaxFrameSize/deadlines), `Auth` mode (open / plaintext / `pbkdf2-sha256$...`), `Logging`:
 
```json
{ }
```
 
**Durability / data context**
- Is this about persistence (`acks=1`, fsync, segment recovery)? Fresh `DataDir` or existing on-disk logs?
- Reproducible every time, or only under load / concurrent connections?
**Additional context**
Add any other context about the problem here.
