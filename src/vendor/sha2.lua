-- Self-contained pure-Lua SHA-256 + HMAC for Lua 5.4.
-- Provides exactly the surface src/server/auth.lua expects:
--   sha2.sha256(msg)            -> lowercase hex digest
--   sha2.hmac(hashfn, key, msg) -> lowercase hex digest
--   sha2.hex_to_bin(hex)        -> binary string
--   sha2.bin_to_hex(bin)        -> lowercase hex string
-- No LuaRocks / C dependency; uses native 5.4 bitwise ops and string.pack.

local M = {}

local MASK = 0xFFFFFFFF

local K = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
}

local function rrotate(x, n)
    return ((x >> n) | (x << (32 - n))) & MASK
end

local function sha256_bin(msg)
    local h0, h1, h2, h3 = 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a
    local h4, h5, h6, h7 = 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19

    local bitlen = #msg * 8
    msg = msg .. "\128"
    while (#msg % 64) ~= 56 do msg = msg .. "\0" end
    msg = msg .. string.pack(">I8", bitlen)

    local w = {}
    for chunk = 1, #msg, 64 do
        for j = 0, 15 do
            w[j] = string.unpack(">I4", msg, chunk + j * 4)
        end
        for j = 16, 63 do
            local x = w[j - 15]
            local y = w[j - 2]
            local s0 = rrotate(x, 7) ~ rrotate(x, 18) ~ (x >> 3)
            local s1 = rrotate(y, 17) ~ rrotate(y, 19) ~ (y >> 10)
            w[j] = (w[j - 16] + s0 + w[j - 7] + s1) & MASK
        end

        local a, b, c, d = h0, h1, h2, h3
        local e, f, g, h = h4, h5, h6, h7

        for j = 0, 63 do
            local S1 = rrotate(e, 6) ~ rrotate(e, 11) ~ rrotate(e, 25)
            local ch = (e & f) ~ ((~e & MASK) & g)
            local temp1 = (h + S1 + ch + K[j + 1] + w[j]) & MASK
            local S0 = rrotate(a, 2) ~ rrotate(a, 13) ~ rrotate(a, 22)
            local maj = (a & b) ~ (a & c) ~ (b & c)
            local temp2 = (S0 + maj) & MASK

            h = g
            g = f
            f = e
            e = (d + temp1) & MASK
            d = c
            c = b
            b = a
            a = (temp1 + temp2) & MASK
        end

        h0 = (h0 + a) & MASK
        h1 = (h1 + b) & MASK
        h2 = (h2 + c) & MASK
        h3 = (h3 + d) & MASK
        h4 = (h4 + e) & MASK
        h5 = (h5 + f) & MASK
        h6 = (h6 + g) & MASK
        h7 = (h7 + h) & MASK
    end

    return string.pack(">I4I4I4I4I4I4I4I4",
        h0, h1, h2, h3, h4, h5, h6, h7)
end

function M.bin_to_hex(s)
    return (s:gsub(".", function(c)
        return string.format("%02x", c:byte())
    end))
end

function M.hex_to_bin(h)
    return (h:gsub("%x%x", function(b)
        return string.char(tonumber(b, 16))
    end))
end

function M.sha256(msg)
    return M.bin_to_hex(sha256_bin(msg))
end

-- hashfn must return a hex digest (M.sha256 does). HMAC block size is 64
-- bytes for SHA-256.
function M.hmac(hashfn, key, msg)
    local blocksize = 64
    if #key > blocksize then
        key = M.hex_to_bin(hashfn(key))
    end
    if #key < blocksize then
        key = key .. string.rep("\0", blocksize - #key)
    end

    local ipad, opad = {}, {}
    for i = 1, blocksize do
        local kb = key:byte(i)
        ipad[i] = string.char(kb ~ 0x36)
        opad[i] = string.char(kb ~ 0x5c)
    end
    ipad = table.concat(ipad)
    opad = table.concat(opad)

    local inner = M.hex_to_bin(hashfn(ipad .. msg))
    return hashfn(opad .. inner)
end

return M
