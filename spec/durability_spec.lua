local os_utils   = require("src.core.os")
local fs_m       = require("src.io.fs")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_durability_test"
                                      or "/tmp/moonmq_durability_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function read_file(path)
    local f = io.open(path, "rb")
    if not f then return nil end
    local body = f:read("*a")
    f:close()
    return body
end

describe("fs.atomic_write", function()
    before_each(function() rmdir(BASE_DIR); fs_m.mkdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("replaces the target and leaves no temp file behind", function()
        local path = fs_m.join_path(BASE_DIR, "state.json")
        assert.is_true(fs_m.atomic_write(path, "one"))
        assert.is_true(fs_m.atomic_write(path, "two"))
        assert.are.equal("two", read_file(path))
        assert.is_false(fs_m.exists(path .. ".tmp"))
    end)

    it("reports a failure instead of pretending the write happened", function()
        local path = fs_m.join_path(BASE_DIR, "missing-dir", "state.json")
        local ok, err = fs_m.atomic_write(path, "data")
        assert.is_nil(ok)
        assert.is_not_nil(err)
    end)
end)

