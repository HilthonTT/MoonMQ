# Default `luarocks` on Debian/Ubuntu targets Lua 5.1, but this project
# requires 5.4 (native bitops in src/crc32.lua etc.). Always invoke the
# 5.4-suffixed launcher so deps land in /usr/local/{share,lib}/lua/5.4.
LUAROCKS ?= luarocks-5.4

# Flip the downloader off luasec/https (which has a known SSL bug on some
# boxes) to plain wget/curl. Cheap to set every time; idempotent.
deps:
	mkdir -p $${HOME}/.luarocks
	$(LUAROCKS) config --scope user fs_use_modules false
	$(LUAROCKS) install busted
	$(LUAROCKS) install luasocket
	$(LUAROCKS) install dkjson
	$(LUAROCKS) install lua-zlib
	bash scripts/setup-deps.sh

test:
	busted
 
run:
	lua5.4 main.lua
 
hash:
	@test -n "$(PASSWORD)" || (echo "usage: make hash PASSWORD=mypass [ITER=10000]" && exit 1)
	@lua5.4 bin/moonmq-hash.lua "$(PASSWORD)" $(ITER)
 
.PHONY: deps test run hash
