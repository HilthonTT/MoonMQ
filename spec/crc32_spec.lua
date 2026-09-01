local crc32 = require("src.core.crc32")

describe("crc32 (IEEE 802.3)", function()
    it("returns 0 for an empty string", function()
        assert.are.equal(0, crc32(""))
    end)

    it("matches the standard '123456789' test vector", function()
        assert.are.equal(0xCBF43926, crc32("123456789"))
    end)

    it("matches the standard 'a' test vector", function()
        assert.are.equal(0xE8B7BE43, crc32("a"))
    end)

    it("matches the standard 'abc' test vector", function()
        assert.are.equal(0x352441C2, crc32("abc"))
    end)

    it("is deterministic across calls", function()
        local s = "the quick brown fox jumps over the lazy dog"
        assert.are.equal(crc32(s), crc32(s))
    end)

    it("differs for different inputs", function()
        assert.are_not.equal(crc32("foo"), crc32("bar"))
    end)

    it("handles binary data with embedded NULs", function()
        local v = crc32("\0\0\0\0")
        assert.is_number(v)
        assert.is_true(v >= 0 and v <= 0xFFFFFFFF)
    end)

    it("returns a uint32 in [0, 2^32)", function()
        local v = crc32("some long-ish payload to exercise the table path")
        assert.is_true(v >= 0 and v < 0x100000000)
    end)

    describe("active backend vs. an independent reference", function()
        local ref_table = {}
        for i = 0, 255 do
            local c = i
            for _ = 1, 8 do
                if (c & 1) ~= 0 then c = (c >> 1) ~ 0xEDB88320 else c = c >> 1 end
            end
            ref_table[i] = c
        end

        local function reference(data)
            local crc = 0xFFFFFFFF
            for i = 1, #data do
                crc = (crc >> 8) ~ ref_table[(crc ~ data:byte(i)) & 0xFF]
            end
            return (crc ~ 0xFFFFFFFF) % 0x100000000
        end

        it("agrees on strings of every length from 0 to 64", function()
            for n = 0, 64 do
                local s = string.rep("Mm\0\xFF9", math.ceil(n / 5)):sub(1, n)
                assert.are.equal(reference(s), crc32(s),
                    string.format("mismatch at length %d", n))
            end
        end)

        it("agrees on a payload larger than one record", function()
            local s = string.rep("moonmq-partition-payload-\xC3\xA9\0", 5000)
            assert.are.equal(reference(s), crc32(s))
        end)

        it("returns an integer, not a float (string.pack(\">I4\") needs one)", function()
            local v = crc32("pack me")
            assert.are.equal(math.type(v), "integer")
            assert.has_no.errors(function() local _ = string.pack(">I4", v) end)
        end)
    end)
end)
