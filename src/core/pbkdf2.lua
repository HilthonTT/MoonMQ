local sha2   = require("src.vendor.sha2")
local socket = require("socket")
local log    = require("src.log.logger").get("auth")

local M = {}

local kdf_native
do
    local ok, kdf = pcall(require, "openssl.kdf")
    if ok and type(kdf) == "table" and type(kdf.derive) == "function" then
        kdf_native = kdf
    end
end

local DEFAULT_PBKDF2_ITERATIONS = 600000

M.DEFAULT_PBKDF2_ITERATIONS = DEFAULT_PBKDF2_ITERATIONS

local YIELD_EVERY = 8192

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
        native_failed = true
        log:warn("native PBKDF2 (luaossl) failed, falling back to pure Lua: %s",
            tostring(ok and "unexpected output" or out))
    end

    return pbkdf2_pure(password, salt, iterations, dklen, opts)
end
M.pbkdf2_hmac_sha256 = pbkdf2_hmac_sha256

function M.kdf_backend()
    return (kdf_native and not native_failed) and "openssl" or "lua"
end

function M.estimate_verify_seconds(iterations)
    assert(type(iterations) == "number", "iterations must be a number")
    if M.kdf_backend() == "openssl" then
        return iterations * 0.0000005
    end
    local probe = 100
    local t0 = socket.gettime()
    pbkdf2_pure("cost-probe", "0123456789abcdef", probe, 32)
    local per_iteration = (socket.gettime() - t0) / probe
    return per_iteration * iterations
end

return M
