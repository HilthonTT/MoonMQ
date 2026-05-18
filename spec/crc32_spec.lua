local crc32 = require("src.crc32")

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
end)
