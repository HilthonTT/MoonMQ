# Default `luarocks` on Debian/Ubuntu targets Lua 5.1, but this project
# requires 5.4 (native bitops in src/core/crc32.lua etc.). Always invoke the
# 5.4-suffixed launcher so deps land in /usr/local/{share,lib}/lua/5.4.
LUA      ?= lua5.4
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

# Start the MoonMQ TCP broker. Binds to Server.Host:Server.Port from
# appsettings.json (default 0.0.0.0:9092). This is the listener that
# native TCP clients (src/client/init.lua, src/examples/tcp_client.lua, and
# the gateway's connection pool) connect to. Metrics on 127.0.0.1:9090.
run server:
	$(LUA) main.lua

# Start the HTTP→MoonMQ gateway. Reads Gateway.* from appsettings.json
# (default 0.0.0.0:8080, proxying to moonmq at 127.0.0.1:9092). The
# broker (`make server`) must already be running on its TCP port — the
# gateway opens a pool of TCP client connections at startup.
#
# bin/gateway.lua only runs as a daemon when arg[0] contains the string
# "moonmq-gateway"; we set that via -e so the file's main-guard fires.
gateway:
	$(LUA) -e "arg[0]='moonmq-gateway'" bin/gateway.lua

# Run the end-to-end TCP client demo against a running broker. Override
# any of MOONMQ_HOST / MOONMQ_PORT / MOONMQ_USER / MOONMQ_PASS /
# MOONMQ_TOPIC / MOONMQ_PARTITIONS via the environment, e.g.:
#   make example MOONMQ_USER=admin MOONMQ_PASS=secret
example:
	$(LUA) src/examples/tcp_client.lua

hash:
	@test -n "$(PASSWORD)" || (echo "usage: make hash PASSWORD=mypass [ITER=10000]" && exit 1)
	@$(LUA) bin/moonmq-hash.lua "$(PASSWORD)" $(ITER)

.PHONY: deps test run server gateway example hash
