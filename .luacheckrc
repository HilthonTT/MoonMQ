-- Luacheck configuration. The primary goal is catching the class of bug that
-- has already shipped here (undefined globals / typo'd names — e.g. the
-- "_partition_for undefined" fix in c6493c1), not enforcing style. Stylistic
-- warnings (unused arguments, line length) are muted so the signal stays high.

std = "lua54"

-- `unpack` is the Lua 5.1 global; src/fsm/state_machine.lua reads it behind an
-- `unpack or table.unpack` compat shim (safe on 5.4, where it's nil). Declare
-- it so the shim doesn't trip the undefined-global check we actually want.
read_globals = { "unpack" }

-- luasocket exposes a global-ish `socket` only when used as a global; MoonMQ
-- always requires it locally, so no read globals need whitelisting. FFI (the
-- Windows io_sync path) is required locally too.
ignore = {
    "211",  -- unused local (dead-code hint; not the bug class we gate on)
    "212",  -- unused argument (common + intentional in callbacks/interfaces)
    "213",  -- unused loop variable
    "231",  -- local variable set but never accessed (e.g. `_ = load_err`)
    "542",  -- empty if branch (used as documentation in a couple of places)
    "621",  -- inconsistent indentation (mixed tabs in the vendored FSM port)
    "611",  -- line contains only whitespace
    "612",  -- line contains trailing whitespace
    "614",  -- trailing whitespace in comment
    "631",  -- line too long
}

-- Test files use busted's DSL globals.
files["spec/"] = {
    std = "+busted",
}
