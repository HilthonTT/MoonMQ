#!/usr/bin/env bash
set -euo pipefail

LUAROCKS="${LUAROCKS:-luarocks-5.4}"

case "$(uname -s)" in
  Linux*|Darwin*)
    $LUAROCKS install luaposix
    ;;
  *)
    echo "setup-deps.sh: skipping luaposix on non-POSIX platform ($(uname -s))"
    echo "  io_sync.lua should use its non-fsync fallback there."
    ;;
esac
