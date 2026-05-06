# Nuntius
A message broker coded in LUA

## Requirements

- Lua 5.4 (uses native bitwise operators and `goto`/labels)
- LuaSocket (`lua-socket`)

On Debian/Ubuntu:
```bash
sudo apt install lua5.4 lua-socket
```

## Running

From the project root:
```bash
lua5.4 main.lua
```

This runs the smoke test, which exercises the broker, producer, consumer, `BatchWriter`, and `PartitionWriter` against a local data directory at `./data_test`.
