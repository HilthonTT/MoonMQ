#!/usr/bin/env bash
# Platform-specific MoonMQ deps.
#
# luaposix provides posix.stdio / posix.unistd for the durable-fsync path
# in src/io/io_sync.lua. It's POSIX-only and won't build on native Windows,
# so it lives here instead of the Makefile's cross-platform `deps` list.
set -euo pipefail

LUAROCKS="${LUAROCKS:-luarocks}"

case "$(uname -s)" in
  Linux*|Darwin*)
    "$LUAROCKS" install luaposix
    ;;
  *)
    echo "setup-deps.sh: skipping luaposix on non-POSIX platform ($(uname -s))"
    echo "  io_sync.lua should use its non-fsync fallback there."
    ;;
esac
