-- PBKDF2-HMAC-SHA256, with a native backend when one is available.
--
-- Extracted from src/server/auth.lua when the CLIENT needed it too: a SCRAM
-- client derives the salted password itself (src/client/init.lua), which is
-- the entire point of the mechanism — the broker never sees the password and
-- never pays the derivation. Pulling in the whole server-side auth module,
-- with its user store and ACLs, to reach one function would have been the
-- wrong dependency for a client library.

local sha2   = require("src.vendor.sha2")
local socket = require("socket")
local log    = require("src.log.logger").get("auth")

local M = {}

-- Native PBKDF2 via luaossl, with the pure-Lua implementation below kept as
-- the fallback. This is not a micro-optimisation: PBKDF2 runs inline on the
-- single reactor thread, and in pure Lua it dominates everything else the
-- broker does. Measured here (Lua 5.4, one core):
--
--     iterations   pure Lua        luaossl
--     10,000        3.16 s          0.005 s
--     600,000     ~190 s (est.)     0.286 s
--
-- 600,000 is this module's own recommended cost. Before the native path, a
-- profile of the broker ingesting 6,000 records attributed 98.6% of samples
-- to sha2.lua — one login outweighing the entire workload — and an
-- unauthenticated peer sending wrong passwords could buy minutes of broker
-- CPU per packet, since a wrong guess never hits the success cache.
--
-- Both paths produce identical bytes (checked against RFC 6070-style vectors
-- and against each other in spec/auth_kdf_spec.lua), so stored hashes are
-- portable between hosts with and without luaossl.
local kdf_native
do
    local ok, kdf = pcall(require, "openssl.kdf")
    if ok and type(kdf) == "table" and type(kdf.derive) == "function" then
        kdf_native = kdf
    end
end

-- NIST SP 800-132 / OWASP 2024 guidance for PBKDF2-HMAC-SHA256.
-- 10000 (the previous default) is brute-forceable on commodity GPUs in
-- 2026. New hashes use 600k; existing stored hashes encode their own
-- iteration count so this only affects hash_password() callers.
local DEFAULT_PBKDF2_ITERATIONS = 600000

-- Exported so the hash CLI (bin/moonmq-hash.lua) and any other caller pick
-- up the same default instead of hard-coding a copy that silently drifts.
-- NOTE: verification cost scales linearly with this count, and PBKDF2 here
-- is pure-Lua. Three mitigations keep an unauthenticated attacker from
-- wedging the single reactor thread with it (each AUTH used to stall the
-- whole event loop for the full 600k iterations):
--   * verification yields back to the reactor every YIELD_EVERY iterations
--     when the Server installs a yield_fn (see Auth:set_yield_fn), so other
--     connections keep making progress during a derivation;
--   * a keyed digest of the last successful credentials short-circuits
--     re-verification, so legitimate reconnects don't pay PBKDF2 at all;
--   * at most max_inflight derivations run concurrently — beyond that AUTH
--     fails fast with "busy" (without counting toward the IP ban, so an
--     attacker can't use the gate to get victims banned).
M.DEFAULT_PBKDF2_ITERATIONS = DEFAULT_PBKDF2_ITERATIONS

-- How many PBKDF2 iterations run between yields when a yield_fn is
-- installed. ~8k iterations is a few ms of pure-Lua HMAC — long enough to
-- keep the derivation efficient, short enough that produce/fetch latency
-- stays flat while someone is authenticating.
local YIELD_EVERY = 8192

-- Upper bound on the iteration count parse_hash will accept. A corrupted or
-- typo'd config shouldn't be able to wedge auth in a 2^31-iter PBKDF2 loop.
-- Exported so the hash CLI (bin/moonmq-hash.lua) rejects the same ceiling and
-- can't emit a hash the broker then refuses to load at boot.
local MAX_PBKDF2_ITERATIONS = 1000000
M.MAX_PBKDF2_ITERATIONS = MAX_PBKDF2_ITERATIONS

local function xor_strings(a, b)
    assert(#a == #b, "xor_strings: length mismatch")
    local out = {}
    for i = 1, #a do
        out[i] = string.char(a:byte(i) ~ b:byte(i))
    end
    return table.concat(out)
end

-- opts (optional): { yield_fn = function() ... end } — called every
-- YIELD_EVERY iterations so a cooperative scheduler can interleave other
-- work. Callers without a scheduler (the hash CLI, tests) just omit it.
-- Only the pure-Lua path yields; the native one finishes in well under the
-- time a single yield slice would have taken.
local function pbkdf2_pure(password, salt, iterations, dklen, opts)
    assert(type(dklen) == "number" and dklen > 0, "dklen must be positive")
    local yield_fn = opts and opts.yield_fn
    local hLen = 32
    local blocks = math.ceil(dklen / hLen)
    local output = {}
    for block_i = 1, blocks do
        local int_be = string.pack(">I4", block_i)
        local u_hex  = sha2.hmac(sha2.sha256, password, salt .. int_be)
        local u_bin  = sha2.hex_to_bin(u_hex)
        local f      = u_bin

        for i = 2, iterations do
            u_hex = sha2.hmac(sha2.sha256, password, u_bin)
            u_bin = sha2.hex_to_bin(u_hex)
            f     = xor_strings(f, u_bin)
            if yield_fn and i % YIELD_EVERY == 0 then yield_fn() end
        end

        output[block_i] = f
    end

    return table.concat(output):sub(1, dklen)
end
M.pbkdf2_pure = pbkdf2_pure

-- Set once, the first time the native path lets us down, so a degraded
-- broker says so exactly once instead of on every AUTH.
local native_failed = false

local function pbkdf2_hmac_sha256(password, salt, iterations, dklen, opts)
    assert(type(dklen) == "number" and dklen > 0, "dklen must be positive")

    if kdf_native and not native_failed then
        local ok, out = pcall(kdf_native.derive, {
            type   = "PBKDF2",
            md     = "sha256",
            pass   = password,
            salt   = salt,
            iter   = iterations,
            outlen = dklen,
        })
        if ok and type(out) == "string" and #out == dklen then
            return out
        end
        -- Never fail an otherwise-valid login because the native backend
        -- misbehaved: fall through to the pure implementation, which
        -- produces the same bytes.
        native_failed = true
        log:warn("native PBKDF2 (luaossl) failed, falling back to pure Lua: %s",
            tostring(ok and "unexpected output" or out))
    end

    return pbkdf2_pure(password, salt, iterations, dklen, opts)
end
M.pbkdf2_hmac_sha256 = pbkdf2_hmac_sha256

-- "openssl" when derivations run natively, "lua" when they don't. Read by
-- the boot-time cost check below and reported in the server's startup log.
function M.kdf_backend()
    return (kdf_native and not native_failed) and "openssl" or "lua"
end

-- Rough seconds one verification of `iterations` will cost on THIS host,
-- measured rather than assumed (a Pi and a workstation differ by an order of
-- magnitude). Times a short pure-Lua derivation and extrapolates; only ever
-- called on the slow path, where the ~30ms probe is noise next to the
-- multi-second verify it is warning about.
function M.estimate_verify_seconds(iterations)
    assert(type(iterations) == "number", "iterations must be a number")
    if M.kdf_backend() == "openssl" then
        -- ~0.5us/iteration natively; far below any deadline worth warning on.
        return iterations * 0.0000005
    end
    local probe = 100
    local t0 = socket.gettime()
    pbkdf2_pure("cost-probe", "0123456789abcdef", probe, 32)
    local per_iteration = (socket.gettime() - t0) / probe
    return per_iteration * iterations
end

return M
