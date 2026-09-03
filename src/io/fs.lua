local os_utils = require("src.core.os")
local io_sync  = require("src.io.io_sync")

local force_backend = os.getenv("MOONMQ_FS_BACKEND")

local px_stat, px_dirent, px_unistd
if force_backend ~= "shell" and not os_utils.IS_WINDOWS then
    local ok_stat,   stat   = pcall(require, "posix.sys.stat")
    local ok_dirent, dirent = pcall(require, "posix.dirent")
    local ok_unistd, unistd = pcall(require, "posix.unistd")
    if ok_stat and ok_dirent and ok_unistd then
        px_stat, px_dirent, px_unistd = stat, dirent, unistd
    end
end

local has_ffi, ffi = pcall(require, "ffi")
local kernel32
if os_utils.IS_WINDOWS and has_ffi then
    ffi.cdef[[
        unsigned long GetFileAttributesA(const char *lpFileName);
        unsigned long GetCurrentDirectoryA(unsigned long nBufferLength, char *lpBuffer);
        int           RemoveDirectoryA(const char *lpPathName);
    ]]
    local lok, k32 = pcall(ffi.load, "kernel32")
    if lok then kernel32 = k32 end
end
local has_bit, bit = pcall(require, "bit")

local function join_path(...)
    local sep   = os_utils.IS_WINDOWS and "\\" or "/"
    local parts = {}
    for i, p in ipairs({ ... }) do
        local stripped = p:gsub("[/\\]+$", "")
        if stripped ~= "" then
            parts[#parts+1] = stripped
        elseif i == 1 and p ~= "" then
            -- A bare root ("/" or "C:\" after stripping is non-empty, but
            -- "/" alone is) must not silently turn the result relative.
            parts[#parts+1] = ""
        end
    end
    return table.concat(parts, sep)
end

local function exec_ok(...)
    local r1, _, r3 = os.execute(...)
    if type(r1) == "boolean" then
        return r1, r3
    end
    return r1 == 0, r1
end

local function is_dir(path)
    assert(type(path) == "string", "path must be a string")
    if px_stat then
        local st = px_stat.stat(path)
        return st ~= nil and px_stat.S_ISDIR(st.st_mode) ~= 0
    end
    if os_utils.IS_WINDOWS and kernel32 and has_bit then
        local attr = kernel32.GetFileAttributesA(path)
        if tonumber(attr) == 0xFFFFFFFF then return false end
        local FILE_ATTRIBUTE_DIRECTORY = 0x10
        return bit.band(attr, FILE_ATTRIBUTE_DIRECTORY) ~= 0
    end
    local quoted = path:gsub("'", "'\\''")
    local ok = exec_ok(string.format("test -d '%s'", quoted))
    return ok
end

local function exists(path)
    assert(type(path) == "string", "path must be a string")
    if px_stat then
        return px_stat.stat(path) ~= nil
    end
    if os_utils.IS_WINDOWS and kernel32 then
        return tonumber(kernel32.GetFileAttributesA(path)) ~= 0xFFFFFFFF
    end
    local quoted = path:gsub("'", "'\\''")
    return exec_ok(string.format("test -e '%s'", quoted))
end

local function mkdir_posix(path)
    if is_dir(path) then return true, nil end

    local rooted = path:sub(1, 1) == "/"
    local built  = rooted and "" or nil
    for comp in path:gmatch("[^/]+") do
        built = built and (built .. "/" .. comp) or comp
        if px_stat.mkdir(built) ~= 0 and not is_dir(built) then
            return false, string.format("failed to create directory: %s", built)
        end
    end

    if not is_dir(path) then
        return false, string.format("failed to create directory: %s", path)
    end
    return true, nil
end

local function mkdir(path)
    assert(type(path) == "string", "path must be a string")

    if px_stat then
        return mkdir_posix(path)
    end

    local cmd
    if os_utils.IS_WINDOWS then
        cmd = string.format('mkdir "%s" 2>nul', path:gsub("/", "\\"))
    else
        cmd = string.format("mkdir -p '%s'", path:gsub("'", "'\\''"))
    end

    os.execute(cmd)
    if not is_dir(path) then
        return false, string.format("failed to create directory: %s", path)
    end
    return true, nil
end

local function read_dir(dir)
    assert(type(dir) == "string", "dir must be a string")

    if not is_dir(dir) then
        return nil, string.format("not a directory: %s", dir)
    end

    if px_dirent then
        local ok, names = pcall(px_dirent.dir, dir)
        if not ok then
            return nil, string.format("read_dir failed: %s", tostring(names))
        end
        local entries = {}
        for i = 1, #names do
            local name = names[i]
            if name ~= "." and name ~= ".." then
                entries[#entries + 1] = name
            end
        end
        return entries, nil
    end

    local cmd = os_utils.IS_WINDOWS
        and string.format('dir /b "%s" 2>nul', dir:gsub("/", "\\"))
        or  string.format("ls -1a '%s' 2>/dev/null", dir:gsub("'", "'\\''"))

    local pipe = io.popen(cmd)
    if not pipe then
        return nil, "failed to open pipe for read_dir"
    end

    local entries = {}
    local ok, iter_err = pcall(function()
        for name in pipe:lines() do
            if name ~= "." and name ~= ".." then
                entries[#entries + 1] = name
            end
        end
    end)
    pipe:close()
    if not ok then
        return nil, string.format("read_dir iterator failed: %s", tostring(iter_err))
    end
    return entries, nil
end

local function rmdir_empty(path)
    if px_unistd then
        if px_unistd.rmdir(path) == 0 then
            return true, nil
        end
        return false, string.format("rmdir failed: %s", path)
    end
    if os_utils.IS_WINDOWS then
        if kernel32 then
            if kernel32.RemoveDirectoryA(path) ~= 0 then
                return true, nil
            end
            return false, string.format("RemoveDirectoryA failed: %s", path)
        end
        if exec_ok(string.format('rmdir "%s"', path:gsub("/", "\\"))) then
            return true, nil
        end
        return false, string.format("rmdir failed: %s", path)
    end
    local ok, err = os.remove(path)
    if not ok then
        return false, string.format("rmdir failed %s: %s", path, tostring(err))
    end
    return true, nil
end

local remove_all
remove_all = function(path)
    assert(type(path) == "string", "path must be a string")
    if not exists(path) then
        return true, nil
    end

    if not is_dir(path) then
        local ok, err = os.remove(path)
        if not ok then
            return false, string.format("failed to remove %s: %s", path, tostring(err))
        end
        return true, nil
    end

    local entries, err = read_dir(path)
    if not entries then
        return false, err
    end
    for _, name in ipairs(entries) do
        local ok, rerr = remove_all(join_path(path, name))
        if not ok then
            return false, rerr
        end
    end

    return rmdir_empty(path)
end

local function glob(dir, pattern)
    assert(type(dir) == "string", "dir must be a string")
    assert(type(pattern) == "string", "pattern must be a string")

    local entries, err = read_dir(dir)
    if not entries then
        return nil, err
    end

    local matches = {}
    for _, name in ipairs(entries) do
        if name:match(pattern) then
            matches[#matches + 1] = join_path(dir, name)
        end
    end
    return matches, nil
end

local function is_abs(path)
    assert(type(path) == "string", "path must be a string")
    if os_utils.IS_WINDOWS then
        return path:match("^%a:[/\\]") ~= nil
            or path:match("^[/\\][/\\]") ~= nil
    end
    return path:sub(1, 1) == "/"
end

local function clean(path)
    local is_windows = os_utils.IS_WINDOWS
    local out_sep    = is_windows and "\\" or "/"

    local vol = ""
    local p   = path
    if is_windows then
        local drive = p:match("^(%a:)")
        if drive then
            vol = drive
            p   = p:sub(3)
        else
            local consumed = p:match("^([/\\][/\\][^/\\]+[/\\][^/\\]+)")
            if consumed then
                vol = consumed:gsub("/", "\\")
                p   = p:sub(#consumed + 1)
            end
        end
        p = p:gsub("\\", "/")
    end

    local rooted = p:sub(1, 1) == "/"

    local parts = {}
    for comp in p:gmatch("[^/]+") do
        if comp == "." then
        elseif comp == ".." then
            local n = #parts
            if n > 0 and parts[n] ~= ".." then
                parts[n] = nil
            elseif not rooted then
                parts[#parts + 1] = ".."
            end
        else
            parts[#parts + 1] = comp
        end
    end

    local body = table.concat(parts, out_sep)
    local result
    if rooted then
        result = vol .. out_sep .. body
    else
        result = vol .. body
    end

    if result == "" then
        return "."
    end
    return result
end

local function base(path)
    assert(type(path) == "string", "path must be a string")
    if path == "" then
        return "."
    end

    local p = path

    if os_utils.IS_WINDOWS then
        local drive = p:match("^(%a:)")
        if drive then
            p = p:sub(3)
        else
            local unc = p:match("^([/\\][/\\][^/\\]+[/\\][^/\\]+)")
            if unc then
                p = p:sub(#unc + 1)
            end
        end
        p = p:gsub("[/\\]+$", "")
        if p == "" then return "\\" end
        return p:match("[^/\\]+$")
    end

    p = p:gsub("/+$", "")
    if p == "" then return "/" end
    return p:match("[^/]+$")
end

local function getcwd()
    if px_unistd then
        local cwd = px_unistd.getcwd()
        if cwd then return cwd, nil end
        return nil, "getcwd failed"
    end
    if os_utils.IS_WINDOWS and kernel32 then
        local needed = tonumber(kernel32.GetCurrentDirectoryA(0, nil))
        if not needed or needed == 0 then
            return nil, "GetCurrentDirectoryA: size query failed"
        end
        local buf = ffi.new("char[?]", needed)
        local len = tonumber(kernel32.GetCurrentDirectoryA(needed, buf))
        if not len or len == 0 then
            return nil, "GetCurrentDirectoryA: read failed"
        end
        return ffi.string(buf, len), nil
    end

    local cmd  = os_utils.IS_WINDOWS and "cd" or "pwd"
    local pipe = io.popen(cmd)
    if not pipe then
        return nil, "failed to open pipe for getcwd"
    end
    local out = pipe:read("*l")
    pipe:close()
    if not out or out == "" then
        return nil, "failed to read cwd"
    end
    return out, nil
end

local function abs_path(path)
    assert(type(path) == "string", "path must be a string")

    if is_abs(path) then
        return clean(path), nil
    end

    local cwd, err = getcwd()
    if not cwd then
        return nil, err
    end

    return clean(cwd .. "/" .. path), nil
end

local backend
if px_stat then
    backend = "posix"
elseif os_utils.IS_WINDOWS and kernel32 then
    backend = "win32"
else
    backend = "shell"
end

local function atomic_write(path, data)
    assert(type(path) == "string", "path must be a string")
    local tmp = path .. ".tmp"
    local f, ferr = io.open(tmp, "wb")
    if not f then return nil, ferr end
    f:write(data)
    f:flush()
    f:close()
    return io_sync.atomic_rename(tmp, path)
end

return {
    backend    = backend,
    join_path  = join_path,
    mkdir      = mkdir,
    read_dir   = read_dir,
    glob       = glob,
    is_dir     = is_dir,
    exists     = exists,
    remove_all = remove_all,
    is_abs     = is_abs,
    clean      = clean,
    base       = base,
    getcwd     = getcwd,
    abs_path   = abs_path,
    atomic_write = atomic_write,
}
