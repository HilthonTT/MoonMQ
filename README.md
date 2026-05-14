# Nuntius
A message broker coded in LUA

## Requirements

- Lua 5.4 (uses native bitwise operators and `goto`/labels)
- LuaSocket (`lua-socket`)
- luaposix (Linux only — used for real `fsync`/`ftruncate` so `acks=1` actually flushes to disk)

On Debian/Ubuntu:
```bash
sudo apt install lua5.4 lua-socket
```

### Installing luaposix for Lua 5.4

Ubuntu's `lua-posix` apt package ships binaries only for Lua 5.1/5.2/5.3, and LuaRocks may not work in every environment. Build from source:

```bash
sudo apt install liblua5.4-dev autoconf automake libtool
git clone --depth 1 --branch v36.2.1 https://github.com/luaposix/luaposix.git /tmp/luaposix
cd /tmp/luaposix
lua5.4 build-aux/luke package=luaposix version=36.2.1 \
    PREFIX=/usr/local LUA=lua5.4 \
    LUA_INCDIR=/usr/include/lua5.4 \
    LUA_LIBDIR=/usr/lib/x86_64-linux-gnu \
    INST_LIBDIR=/usr/local/lib/lua/5.4 \
    INST_LUADIR=/usr/local/share/lua/5.4
sudo lua5.4 build-aux/luke install package=luaposix version=36.2.1 \
    PREFIX=/usr/local LUA=lua5.4 \
    LUA_INCDIR=/usr/include/lua5.4 \
    LUA_LIBDIR=/usr/lib/x86_64-linux-gnu \
    INST_LIBDIR=/usr/local/lib/lua/5.4 \
    INST_LUADIR=/usr/local/share/lua/5.4
```

Verify with: `lua5.4 -e 'print(require("posix.unistd").fsync)'` — should print a function.

### Windows

`src/io_sync.lua` falls back to LuaJIT FFI on Windows (`_commit`, `_chsize_s` from the C runtime). Running on Windows therefore requires LuaJIT rather than stock Lua 5.4.

## Running

From the project root:
```bash
lua5.4 main.lua
```

`main.lua` is the project's runnable example: an end-to-end "order-events
pipeline" that wires every module under `src/` together and narrates each
stage against a local data directory at `./data_test`. It walks through:

1. Bootstrapping the broker and creating a topic
2. Producing events synchronously (`Producer`, `acks=1`)
3. Producing events asynchronously (`produce_async` + `Future`)
4. Consumer-group partition rebalancing (`ConsumerGroup`)
5. Consuming the topic and verifying nothing was lost
6. Synchronous batched appends (`BatchWriter`)
7. Asynchronous batched appends (`PartitionWriter`)
8. Resilient writes that retry transient I/O errors (`Partition:write_with_resilience`)
9. An accumulating byte buffer (`Buffer`)
10. A log that rolls into multiple segments (`SegmentedPartition`)
11. Message compression (`CompressedMessage` — optional, see below)
12. Leader-side replication (`ReplicatedPartition` — optional, see below)
13. Crash recovery on broker restart (CRC-validated log scan)

### Optional modules

Three modules depend on libraries that may not be present. `main.lua`
`pcall`-gates them and prints a "skipped" note when they are unavailable, so
the demo always runs to completion:

- **`src/compression.lua`** needs the `zlib` LuaRocks module (gzip support).
- **`src/snappy.lua`** needs LuaJIT's FFI and `libsnappy` (Snappy support).
- **`src/replica.lua`** loads on stock Lua, but real replication needs a peer
  broker exposing an HTTP `/replicate` endpoint; the demo only exercises the
  leader-side local-write path.

## Testing

The test suite lives under `spec/` and runs with [busted](https://lunarmodules.github.io/busted/).

### Installing test dependencies

`make deps` installs busted and luasocket under Lua 5.4. Two caveats on Debian/Ubuntu:

- The default `luarocks` launcher targets **Lua 5.1**; the Makefile invokes `luarocks-5.4` explicitly so deps land in `/usr/local/{share,lib}/lua/5.4`.
- LuaRocks' built-in HTTPS fetcher can fail with `bad argument #2 to 'method' (string expected, got light userdata)` on some boxes (broken luasec). The Makefile sets `fs_use_modules = false` in the user config, which routes downloads through `wget`/`curl` instead.

```bash
sudo apt install wget    # if not already present
make deps
```

If `make deps` runs as a non-root user and you need system-wide install, prefix with `sudo`; otherwise pass `--local` via `LUAROCKS="luarocks-5.4 --local"`.

### Running the suite

```bash
make test          # equivalent to: busted
```

The `busted` binary at `/usr/local/bin/busted` is installed by `make deps` and is bound to Lua 5.4. If you have a previous Lua-5.1 busted on PATH, `make deps` overwrites it (the old one is preserved as `busted~`).

Current coverage:

| Spec file                  | Module under test                                  |
| -------------------------- | -------------------------------------------------- |
| `buffer_spec.lua`          | `src/buffer.lua` — accumulating byte buffer        |
| `crc32_spec.lua`           | `src/crc32.lua` — IEEE 802.3 CRC-32                |
| `future_spec.lua`          | `src/future.lua` — one-shot coroutine future       |
| `message_spec.lua`         | `src/message.lua` — message wire format + pool     |
| `partition_spec.lua`       | `src/partition.lua` — append, read, recovery       |
| `time_spec.lua`            | `src/time.lua` — duration constants                |
| `topic_manager_spec.lua`   | `src/topic_manager.lua` — topic/partition creation |
| `util_spec.lua`            | `src/util.lua` — topic-name validation             |
