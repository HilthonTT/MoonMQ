LUA      ?= lua5.4
LUAROCKS ?= luarocks-5.4

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

run server:
	$(LUA) main.lua

gateway:
	$(LUA) -e "arg[0]='moonmq-gateway'" bin/gateway.lua

example:
	$(LUA) src/examples/tcp_client.lua

hash:
	@test -n "$(PASSWORD)" || (echo "usage: make hash PASSWORD=mypass [ITER=10000]" && exit 1)
	@$(LUA) bin/moonmq-hash.lua "$(PASSWORD)" $(ITER)

.PHONY: deps test run server gateway example hash
