local os_utils = require("src.core.os")

local BASE = os_utils.IS_WINDOWS and "C:\\Temp\\lua_fs_test" or "/tmp/lua_fs_test"
local SEP  = os_utils.IS_WINDOWS and "\\" or "/"

local function nuke()
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', BASE:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", BASE))
    end
end

local function write_file(path, contents)
    local f = assert(io.open(path, "wb"))
    f:write(contents)
    f:close()
end

local function suite(fs)
    describe("(" .. fs.backend .. " backend)", function()
        before_each(nuke)
        after_each(nuke)

        it("creates nested directories and reports them as directories", function()
            local deep = table.concat({ BASE, "a", "b", "c" }, SEP)
            local ok, err = fs.mkdir(deep)
            assert.is_true(ok)
            assert.is_nil(err)
            assert.is_true(fs.is_dir(deep))
            assert.is_true(fs.is_dir(table.concat({ BASE, "a" }, SEP)))
            assert.is_truthy(fs.exists(deep))
        end)

        it("is idempotent when the directory already exists", function()
            assert.is_true(fs.mkdir(BASE))
            local ok, err = fs.mkdir(BASE)
            assert.is_true(ok)
            assert.is_nil(err)
        end)

        it("reports a missing path as neither existing nor a directory", function()
            local missing = table.concat({ BASE, "nope" }, SEP)
            assert.is_false(fs.is_dir(missing))
            assert.is_falsy(fs.exists(missing))
        end)

        it("distinguishes a plain file from a directory", function()
            assert.is_true(fs.mkdir(BASE))
            local file = table.concat({ BASE, "a-file" }, SEP)
            write_file(file, "hello")
            assert.is_truthy(fs.exists(file))
            assert.is_false(fs.is_dir(file))
        end)

        it("lists directory entries without . and ..", function()
            assert.is_true(fs.mkdir(BASE))
            write_file(table.concat({ BASE, "one" }, SEP), "1")
            write_file(table.concat({ BASE, "two" }, SEP), "2")
            assert.is_true(fs.mkdir(table.concat({ BASE, "sub" }, SEP)))

            local entries, err = fs.read_dir(BASE)
            assert.is_nil(err)
            table.sort(entries)
            assert.are.same({ "one", "sub", "two" }, entries)
        end)

        it("errors on read_dir of a path that is not a directory", function()
            local entries, err = fs.read_dir(table.concat({ BASE, "missing" }, SEP))
            assert.is_nil(entries)
            assert.is_string(err)
        end)

        it("globs by Lua pattern", function()
            assert.is_true(fs.mkdir(BASE))
            write_file(table.concat({ BASE, "00000000000000000000.log" }, SEP), "x")
            write_file(table.concat({ BASE, "00000000000000000042.log" }, SEP), "x")
            write_file(table.concat({ BASE, "00000000000000000000.meta" }, SEP), "x")

            local matches, err = fs.glob(BASE, "^(%d+)%.log$")
            assert.is_nil(err)
            assert.are.equal(2, #matches)
            for _, m in ipairs(matches) do
                assert.is_truthy(m:match("%.log$"))
                assert.is_truthy(fs.exists(m))
            end
        end)

        it("removes a tree recursively, and treats an absent path as success", function()
            local deep = table.concat({ BASE, "x", "y" }, SEP)
            assert.is_true(fs.mkdir(deep))
            write_file(table.concat({ deep, "leaf" }, SEP), "bye")

            local ok, err = fs.remove_all(BASE)
            assert.is_true(ok)
            assert.is_nil(err)
            assert.is_false(fs.is_dir(BASE))

            local ok2, err2 = fs.remove_all(BASE)
            assert.is_true(ok2)
            assert.is_nil(err2)
        end)

        it("returns an absolute cwd", function()
            local cwd, err = fs.getcwd()
            assert.is_nil(err)
            assert.is_string(cwd)
            assert.is_true(fs.is_abs(cwd))
            assert.is_true(fs.is_dir(cwd))
        end)

        it("resolves a relative path against the cwd", function()
            local abs, err = fs.abs_path("spec")
            assert.is_nil(err)
            assert.is_true(fs.is_abs(abs))
            assert.is_true(fs.is_dir(abs))
        end)
    end)
end

describe("fs", function()
    local fs = require("src.io.fs")
    suite(fs)

    it("reports the backend it selected", function()
        assert.is_true(fs.backend == "posix"
                    or fs.backend == "win32"
                    or fs.backend == "shell")
    end)
end)

describe("fs (fallback)", function()
    local primary = require("src.io.fs").backend
    if primary == "shell" or os_utils.IS_WINDOWS then
        pending("primary backend is already the shell fallback")
        return
    end

    local ok_stdlib, stdlib = pcall(require, "posix.stdlib")
    if not ok_stdlib then
        pending("luaposix (posix.stdlib) unavailable, cannot force the fallback")
        return
    end

    stdlib.setenv("MOONMQ_FS_BACKEND", "shell")
    package.loaded["src.io.fs"] = nil
    local shell_fs = require("src.io.fs")
    stdlib.setenv("MOONMQ_FS_BACKEND", nil)
    package.loaded["src.io.fs"] = nil
    require("src.io.fs")

    assert(shell_fs.backend == "shell", "expected the forced shell backend")
    suite(shell_fs)
end)
