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

This runs the smoke test, which exercises the broker, producer, consumer, `BatchWriter`, and `PartitionWriter` against a local data directory at `./data_test`.
