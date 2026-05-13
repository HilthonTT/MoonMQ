-- spec/util_spec.lua
local util = require("src.util")

describe("validate_topic_name", function()
    it("accepts a simple alphanumeric name", function()
        local ok, err = util.validate_topic_name("orders")
        assert.is_true(ok)
        assert.is_nil(err)
    end)

    it("accepts names with '.', '_', '-' (not leading dash)", function()
        for _, n in ipairs({"a.b", "a_b", "a-b", "v1.0_test-2"}) do
            local ok, err = util.validate_topic_name(n)
            assert.is_true(ok, string.format("expected %q to be valid; got: %s", n, err or ""))
        end
    end)

    it("rejects non-string input", function()
        local ok, err = util.validate_topic_name(123)
        assert.is_nil(ok)
        assert.is_string(err)
    end)

    it("rejects empty string", function()
        local ok, err = util.validate_topic_name("")
        assert.is_nil(ok)
        assert.is_string(err)
    end)

    it("rejects names longer than 249 chars", function()
        local long = string.rep("a", 250)
        local ok, err = util.validate_topic_name(long)
        assert.is_nil(ok)
        assert.is_string(err)
    end)

    it("accepts names of exactly 249 chars", function()
        local edge = string.rep("a", 249)
        local ok, err = util.validate_topic_name(edge)
        assert.is_true(ok)
        assert.is_nil(err)
    end)

    it("rejects a leading dash", function()
        local ok, err = util.validate_topic_name("-bad")
        assert.is_nil(ok)
        assert.is_string(err)
    end)

    it("rejects '.' and '..'", function()
        assert.is_nil((util.validate_topic_name(".")))
        assert.is_nil((util.validate_topic_name("..")))
    end)

    it("rejects shell metacharacters", function()
        for _, n in ipairs({"a/b", "a b", "a;rm", "a$x", "a*", "a&b", "a\\b"}) do
            local ok, err = util.validate_topic_name(n)
            assert.is_nil(ok, string.format("expected %q to be rejected", n))
            assert.is_string(err)
        end
    end)
end)
