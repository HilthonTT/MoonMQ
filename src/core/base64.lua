-- Base64 (RFC 4648) encode/decode.
--
-- Written for SCRAM (RFC 5802), whose message fields — nonces, salt, proofs —
-- are specified as base64 text. Keeping the wire bytes RFC-shaped means the
-- exchange can be checked against published SCRAM vectors instead of only
-- against ourselves.
--
-- decode is strict: it rejects anything outside the alphabet, misplaced
-- padding, and lengths that cannot be a valid encoding. The inputs here are
-- attacker-supplied (a client-first message arrives pre-authentication), so
-- "lenient" would mean accepting two encodings of the same nonce.

local M = {}

local ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

local DECODE = {}
for i = 1, #ALPHABET do
    DECODE[ALPHABET:sub(i, i)] = i - 1
end

function M.encode(data)
    assert(type(data) == "string", "data must be a string")

    local out = {}
    local n = #data
    local i = 1
    while i + 2 <= n do
        local a, b, c = data:byte(i, i + 2)
        local v = a << 16 | b << 8 | c
        out[#out + 1] = ALPHABET:sub((v >> 18) + 1, (v >> 18) + 1)
                     .. ALPHABET:sub((v >> 12 & 63) + 1, (v >> 12 & 63) + 1)
                     .. ALPHABET:sub((v >> 6 & 63) + 1, (v >> 6 & 63) + 1)
                     .. ALPHABET:sub((v & 63) + 1, (v & 63) + 1)
        i = i + 3
    end

    local rest = n - i + 1
    if rest == 1 then
        local a = data:byte(i)
        local v = a << 16
        out[#out + 1] = ALPHABET:sub((v >> 18) + 1, (v >> 18) + 1)
                     .. ALPHABET:sub((v >> 12 & 63) + 1, (v >> 12 & 63) + 1)
                     .. "=="
    elseif rest == 2 then
        local a, b = data:byte(i, i + 1)
        local v = a << 16 | b << 8
        out[#out + 1] = ALPHABET:sub((v >> 18) + 1, (v >> 18) + 1)
                     .. ALPHABET:sub((v >> 12 & 63) + 1, (v >> 12 & 63) + 1)
                     .. ALPHABET:sub((v >> 6 & 63) + 1, (v >> 6 & 63) + 1)
                     .. "="
    end

    return table.concat(out)
end

-- Returns (decoded, nil) or (nil, err). No whitespace is tolerated: SCRAM
-- fields are single-token, and silently skipping characters would let two
-- different strings decode to the same nonce.
function M.decode(text)
    if type(text) ~= "string" then return nil, "base64: not a string" end
    if #text % 4 ~= 0 then return nil, "base64: length not a multiple of 4" end
    if text == "" then return "", nil end

    local pad = 0
    if text:sub(-1) == "=" then pad = 1 end
    if text:sub(-2) == "==" then pad = 2 end
    -- Padding may only appear at the very end.
    local body = text:sub(1, #text - pad)
    if body:find("=", 1, true) then return nil, "base64: misplaced padding" end

    local out = {}
    local acc, bits = 0, 0
    for i = 1, #body do
        local d = DECODE[body:sub(i, i)]
        if not d then
            return nil, "base64: invalid character"
        end
        acc = acc << 6 | d
        bits = bits + 6
        if bits >= 8 then
            bits = bits - 8
            out[#out + 1] = string.char(acc >> bits & 0xFF)
        end
    end

    -- Leftover bits must be zero, otherwise the encoding is non-canonical.
    if bits > 0 and (acc & ((1 << bits) - 1)) ~= 0 then
        return nil, "base64: non-canonical trailing bits"
    end

    return table.concat(out), nil
end

return M
