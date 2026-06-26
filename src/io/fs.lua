local os_utils = require("src.core.os")

-- LuaJIT FFI is used on Windows so that is_dir does not have to write a
-- probe file (the old implementation did, which had two sharp edges:
-- read-only directories returned false, and on a fluke the probe could
-- linger if os.remove failed). On Unix we shell out to `test -d`.
-- The same FFI handle is reused for getcwd (GetCurrentDirectoryA) and
-- for removing emptied directories (RemoveDirectoryA).
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
    for _, p in ipairs({ ... }) do
        p = p:gsub("[/\\]+$", "") -- strip trailing slashes (both kinds)
        if p ~= "" then
            parts[#parts+1] = p
        end
    end
    return table.concat(parts, sep)
end

-- Normalize Lua's wildly version-dependent os.execute return shape.
-- LuaJIT (5.1 semantics): single number, 0 = success.
-- Lua 5.2+: (true|nil, "exit"|"signal", code).
local function exec_ok(...)
    local r1, _, r3 = os.execute(...)
    if type(r1) == "boolean" then
        return r1, r3
    end
    return r1 == 0, r1
end

local function is_dir(path)
    assert(type(path) == "string", "path must be a string")
    if os_utils.IS_WINDOWS and kernel32 and has_bit then
        local attr = kernel32.GetFileAttributesA(path)
        if tonumber(attr) == 0xFFFFFFFF then return false end
        local FILE_ATTRIBUTE_DIRECTORY = 0x10
        return bit.band(attr, FILE_ATTRIBUTE_DIRECTORY) ~= 0
    end
    -- Unix fallback (and Windows-without-FFI fallback).
    local quoted = path:gsub("'", "'\\''")
    local ok = exec_ok(string.format("test -d '%s'", quoted))
    return ok
end

--- True if anything (file, dir, symlink) exists at `path`. Equivalent to a
--- successful os.Stat with no IsNotExist. On Windows uses GetFileAttributesA
--- (0xFFFFFFFF == not found); elsewhere shells out to `test -e`. The
--- Windows-without-FFI path falls through to `test -e`, which only works under
--- a POSIX shell (WSL/git-bash), same limitation is_dir already carries.
local function exists(path)
    assert(type(path) == "string", "path must be a string")
    if os_utils.IS_WINDOWS and kernel32 then
        return tonumber(kernel32.GetFileAttributesA(path)) ~= 0xFFFFFFFF
    end
    local quoted = path:gsub("'", "'\\''")
    return exec_ok(string.format("test -e '%s'", quoted))
end

local function mkdir(path)
    assert(type(path) == "string", "path must be a string")

    local cmd
    if os_utils.IS_WINDOWS then
        -- Windows mkdir creates intermediate dirs by default and errors
        -- if the dir already exists; 2>nul swallows the "already exists"
        -- noise. The authoritative check is is_dir() afterward.
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

    local cmd = os_utils.IS_WINDOWS
        and string.format('dir /b "%s" 2>nul', dir:gsub("/", "\\"))
        or  string.format("ls -1a '%s' 2>/dev/null", dir:gsub("'", "'\\''"))

    local pipe = io.popen(cmd)
    if not pipe then
        return nil, "failed to open pipe for read_dir"
    end

    -- pcall so that an iterator error (child process death mid-read,
    -- pipe broken, etc.) doesn't leak the pipe handle. We always close
    -- the pipe, then re-surface any iterator error to the caller.
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

--- Remove a single EMPTY directory. POSIX os.remove (C remove()) dispatches
--- to rmdir for a directory, but Windows' CRT remove() only handles files,
--- so we need RemoveDirectoryA there (FFI), or a bare `rmdir` (no /s, since
--- the dir is already empty) when FFI is unavailable.
local function rmdir_empty(path)
    if os_utils.IS_WINDOWS then
        if kernel32 then
            if kernel32.RemoveDirectoryA(path) ~= 0 then -- nonzero == success
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

--- Recursively remove `path` and everything under it, matching Go's
--- os.RemoveAll: an absent path is success, not an error. Pure Lua, no shell
--- spawn -- depth-first, unlink files then remove the emptied directories.
---
--- CAVEAT vs Go: this follows symlinks-to-directories. is_dir() (test -d /
--- GetFileAttributesA) reports a dir-symlink as a directory, so we'd recurse
--- INTO the target and delete its contents. Go's RemoveAll removes the link
--- itself. Detecting links needs lstat (FFI) or `test -L`, which I left out
--- because a commit-log dir shouldn't contain symlinks -- flagging it in case
--- this ever gets pointed at a tree where it matters.
local remove_all
remove_all = function(path)
    assert(type(path) == "string", "path must be a string")
    if not exists(path) then
        return true, nil -- absent is success
    end

    if not is_dir(path) then
        -- plain file; os.remove unlinks files on every platform.
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

--- Returns a list of file paths matching a Lua pattern.
--- `dir`     - the directory to search in
--- `pattern` - a Lua pattern (e.g. "^partition%-.*%.log$"). NOT a shell glob.
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

--- True if `path` is rooted at a volume/filesystem root, matching Go's
--- filepath.IsAbs. Note: on Windows "C:foo" (drive-relative) and "\foo"
--- (rooted but no drive) are both NOT absolute, same as Go.
local function is_abs(path)
    assert(type(path) == "string", "path must be a string")
    if os_utils.IS_WINDOWS then
        return path:match("^%a:[/\\]") ~= nil    -- C:\ or C:/
            or path:match("^[/\\][/\\]") ~= nil   -- UNC \\host or //host
    end
    return path:sub(1, 1) == "/"
end

--- Pure lexical cleanup, equivalent to Go's filepath.Clean:
--- collapses redundant separators, drops "." elements, resolves inner
--- ".." against the preceding element, and discards leading ".." on a
--- rooted path. It does NOT touch the filesystem (no symlink resolution).
local function clean(path)
    local is_windows = os_utils.IS_WINDOWS
    local out_sep    = is_windows and "\\" or "/"

    -- Split off a non-collapsible volume prefix so we never mangle it.
    local vol = ""
    local p   = path
    if is_windows then
        local drive = p:match("^(%a:)")
        if drive then
            vol = drive
            p   = p:sub(3)
        else
            -- UNC: \\host\share is treated as one indivisible unit.
            local consumed = p:match("^([/\\][/\\][^/\\]+[/\\][^/\\]+)")
            if consumed then
                vol = consumed:gsub("/", "\\")
                p   = p:sub(#consumed + 1)
            end
        end
        p = p:gsub("\\", "/") -- process with a single separator kind
    end

    local rooted = p:sub(1, 1) == "/"

    local parts = {}
    for comp in p:gmatch("[^/]+") do
        if comp == "." then
            -- skip
        elseif comp == ".." then
            local n = #parts
            if n > 0 and parts[n] ~= ".." then
                parts[n] = nil               -- pop the preceding element
            elseif not rooted then
                parts[#parts + 1] = ".."     -- keep, can't resolve yet
            end
            -- a leading ".." on a rooted path is simply discarded
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
        return "." -- Go returns "." for an empty result
    end
    return result
end

--- Go's filepath.Base: the last element of path. Trailing separators are
--- stripped first. Returns "." for an empty path, and a single separator
--- for a path made entirely of separators. On Windows the volume prefix
--- (drive or UNC) is dropped before the last element is taken, and both
--- "/" and "\" count as separators; on Unix only "/" does (a backslash is
--- a legal filename byte there, so it is left untouched).
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
        p = p:gsub("[/\\]+$", "")     -- strip trailing separators
        if p == "" then return "\\" end
        return p:match("[^/\\]+$")
    end

    p = p:gsub("/+$", "")             -- strip trailing separators
    if p == "" then return "/" end
    return p:match("[^/]+$")
end

--- Current working directory. FFI on Windows (no shell spawn), shell
--- fallback elsewhere. Returns (path, nil) or (nil, err_string).
local function getcwd()
    if os_utils.IS_WINDOWS and kernel32 then
        -- First call with a 0 buffer returns the required size including
        -- the NUL terminator; the second call actually fills it.
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

    -- Unix (and Windows-without-FFI) fallback: shell out.
    local cmd  = os_utils.IS_WINDOWS and "cd" or "pwd"
    local pipe = io.popen(cmd)
    if not pipe then
        return nil, "failed to open pipe for getcwd"
    end
    local out = pipe:read("*l") -- first line, newline stripped
    pipe:close()
    if not out or out == "" then
        return nil, "failed to read cwd"
    end
    return out, nil
end

--- Go's filepath.Abs: returns an absolute, cleaned path. Absolute inputs
--- are just cleaned; relative inputs are joined against the cwd first.
--- Returns (path, nil) or (nil, err_string) if the cwd can't be read.
local function abs_path(path)
    assert(type(path) == "string", "path must be a string")

    if is_abs(path) then
        return clean(path), nil
    end

    local cwd, err = getcwd()
    if not cwd then
        return nil, err
    end

    -- Join with "/" unconditionally; clean() normalizes per-OS. We do NOT
    -- reuse join_path here: it strips trailing slashes, so join_path("/", x)
    -- collapses the root "/" to "" and drops it (would yield "x" not "/x").
    return clean(cwd .. "/" .. path), nil
end

return {
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
}
