-- IEEE 802.3 CRC-32, byte-compatible with Go's crc32.ChecksumIEEE.
-- Three implementations, probed in order:
--   1. zlib via LuaJIT FFI       — only exists under LuaJIT.
--   2. zlib via the lua-zlib rock — the fast path on PUC Lua 5.4, which is
--      what the Makefile, CI and every spec run actually use. lua-zlib is
--      already a declared dependency (src/record/compression.lua needs it
--      for gzip), so this costs nothing extra.
--   3. table-driven pure Lua      — correct everywhere, ~46x slower than
--      either native path (16 MB/s vs 737 MB/s measured on a 200B record).
-- All three produce identical values; the record format does not change.

local os_utils = require("src.core.os")

local has_ffi, ffi = pcall(require, "ffi")

local zlib
if has_ffi then
    ffi.cdef[[
        unsigned long crc32(unsigned long crc, const unsigned char *buf, unsigned int len);
    ]]

    local function try_load(name)
        local ok, lib = pcall(ffi.load, name)
        if ok then return lib end
    end

    if os_utils.IS_WINDOWS then
        -- zlib1.dll: official zlib Windows build, the most common name.
        -- zlibwapi.dll: older WinAPI variant, still seen in the wild.
        -- zlib.dll: occasional alias in third-party bundles.
        zlib = try_load("zlib1") or try_load("zlibwapi") or try_load("zlib")
    else
        zlib = try_load("z")
    end
end

if zlib then
    return function(data)
        return tonumber(zlib.crc32(0, data, #data)) % 0x100000000
    end
end

-- lua-zlib: `zlib.crc32()` returns a stateful updater; calling it with a
-- string returns the running CRC of everything fed so far, so a fresh
-- updater per call gives the one-shot checksum. The result comes back as a
-- Lua float, hence the math.floor — string.pack(">I4") rejects a float with
-- a fractional part, and an unfloored value would also fail == against the
-- pure-Lua path.
local has_zlib, zlib_rock = pcall(require, "zlib")
if has_zlib and type(zlib_rock) == "table" and zlib_rock.crc32 then
    local new_crc = zlib_rock.crc32
    return function(data)
        return math.floor(new_crc()(data)) % 0x100000000
    end
end

-- Pure Lua fallback (uses native Lua 5.3+ bitwise operators)
local crc_table = {}
for i = 0, 255 do
    local c = i
    for _ = 1, 8 do
        if (c & 1) ~= 0 then
            c = (c >> 1) ~ 0xEDB88320
        else
            c = c >> 1
        end
    end
    crc_table[i] = c
end

return function(data)
    local crc = 0xFFFFFFFF
    for i = 1, #data do
        crc = (crc >> 8) ~ crc_table[(crc ~ data:byte(i)) & 0xFF]
    end
    return (crc ~ 0xFFFFFFFF) % 0x100000000
end
