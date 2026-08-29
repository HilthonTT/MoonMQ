LUA      ?= lua5.4
LUAROCKS ?= luarocks-5.4

deps:
	mkdir -p $${HOME}/.luarocks
	$(LUAROCKS) config --scope user fs_use_modules false
	$(LUAROCKS) install busted
	$(LUAROCKS) install luasocket
	$(LUAROCKS) install dkjson
	# lua-zlib: gzip compression AND the fast CRC-32 (src/core/crc32.lua falls
	# back to a ~46x slower pure-Lua loop without it).
	$(LUAROCKS) install lua-zlib
	# luaposix: fsync/ftruncate, signal handling, and syscall-based file I/O.
	# Without it src/io/fs.lua forks a process for every directory check, on
	# the reactor thread.
	$(LUAROCKS) install luaposix
	# luaossl: native PBKDF2. Without it password verification runs in pure
	# Lua and costs seconds-to-minutes of reactor time per login (see the
	# table in src/server/auth.lua). Needs libssl headers: on Debian/Ubuntu
	# `apt-get install libssl-dev`.
	$(LUAROCKS) install luaossl
	$(LUAROCKS) install luacheck
	$(LUAROCKS) install sirocco
	$(LUAROCKS) install hump
	$(LUAROCKS) install lpeg
	$(LUAROCKS) install argparse
	$(LUAROCKS) install compat53
	bash scripts/setup-deps.sh

test:
	busted

# The smoke test bypasses busted (see its header) and isn't picked up by
# `make test`; run it explicitly. `check` is the full gate CI runs.
smoke:
	$(LUA) spec/storage_smoke.lua

lint:
	luacheck src bin main.lua spec

check: lint test smoke

run server:
	$(LUA) main.lua

gateway:
	$(LUA) -e "arg[0]='moonmq-gateway'" bin/gateway.lua

example:
	$(LUA) src/examples/tcp_client.lua

hash:
	@test -n "$(PASSWORD)" || (echo "usage: make hash PASSWORD=mypass [ITER=10000] [SCRAM=1]" && exit 1)
	@$(LUA) bin/moonmq-hash.lua "$(PASSWORD)" $(ITER) $(if $(SCRAM),--scram,)

.PHONY: deps test smoke lint check run server gateway example hash
