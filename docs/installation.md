# Installation

```bash
sudo apt install lua5.4 liblua5.4-dev luarocks libssl-dev zlib1g-dev   # Debian/Ubuntu
make deps          # busted, dkjson, luasocket, luaposix, lua-zlib, luaossl, luasec,
                   # luacheck + the console line-editing rocks (sirocco, hump, lpeg, argparse, compat53)
```

## Requirements

- **Lua 5.4** — uses native bitwise operators and `goto`/labels
- **LuaSocket** — TCP networking
- **dkjson** — pure-Lua JSON, reads `appsettings.json`
- **luaposix** (Linux) — real `fsync`/`ftruncate`, so `acks=1` actually hits
  disk. Also gives `src/io/fs.lua` syscall-based directory access: without it
  every `is_dir`/`read_dir`/`mkdir` forks a process **on the reactor thread**
  (measured on ext4: 23 ms per `test -d` against 2.5 µs for a `stat`, and 42
  process spawns just to open ten partitions)
- **lua-zlib** — gzip compression *and* the native CRC-32 every record is
  checksummed with. The pure-Lua fallback runs at ~16 MB/s against ~740 MB/s
- **luaossl** — native PBKDF2 for password verification. Without it a login
  costs **3.2 s of reactor time at 10,000 iterations and ~190 s at the
  recommended 600,000**, versus 5 ms and 0.29 s natively. Needs `libssl-dev`
- **luasec** — TLS on the client, metrics, cluster and replication listeners.
  Without it the broker still boots and only refuses configurations that ask
  for TLS. Needs `libssl-dev`

luaposix is **required** on Linux — `src/io/io_sync.lua` has no other way to
fsync. lua-zlib, luaossl and luasec are optional: without them the broker still
boots and falls back to pure Lua (or, for luasec, to plaintext-only), logging
which backend it picked (`filesystem backend=…`, `PBKDF2 backend=…`) at
startup. Those fallbacks are correct, not fast — a production broker wants
them all. Snappy compression additionally needs LuaJIT FFI + `libsnappy`;
SHA-256/HMAC is vendored (`src/vendor/sha2.lua`) with nothing to install.

## Building luaposix for Lua 5.4

Ubuntu's package only ships 5.1–5.3.

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

Verify: `lua5.4 -e 'print(require("posix.unistd").fsync)'` should print a function.

## Debian/Ubuntu caveats

- The default `luarocks` launcher targets **Lua 5.1**; the Makefile invokes
  `luarocks-5.4` explicitly so deps land in `/usr/local/{share,lib}/lua/5.4`.
- LuaRocks' HTTPS fetcher can fail with `bad argument #2 to 'method' (string
  expected, got light userdata)` on boxes with broken luasec. The Makefile sets
  `fs_use_modules = false` so downloads route through `wget`/`curl` — run
  `sudo apt install wget` first.
- For a system-wide install run `make deps` with `sudo`; otherwise pass
  `LUAROCKS="luarocks-5.4 --local"`.
- `make deps` installs `busted` at `/usr/local/bin/busted` bound to Lua 5.4,
  overwriting any Lua-5.1 busted on PATH (preserved as `busted~`).

## Windows

`src/io/io_sync.lua` falls back to LuaJIT FFI (`_commit`, `_chsize_s` from the
C runtime), so Windows needs **LuaJIT** rather than stock Lua 5.4.
