local os_utils = require("src.utils.os")

-- LuaJIT FFI is used on Windows so that is_dir does not have to write a
-- probe file (the old implementation did, which had two sharp edges:
-- read-only directories returned false, and on a fluke the probe could
-- linger if os.remove failed). On Unix we shell out to `test -d`.
local has_ffi, ffi = pcall(require, "ffi")
local kernel32
if os_utils.IS_WINDOWS and has_ffi then
    ffi.cdef[[
        unsigned long GetFileAttributesA(const char *lpFileName);
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

    local entries = {}
    for name in pipe:lines() do
        if name ~= "." and name ~= ".." then
            entries[#entries + 1] = name
        end
    end
    pipe:close()
    return entries, nil
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

return {
    IS_WINDOWS = os_utils.IS_WINDOWS,
    join_path  = join_path,
    mkdir      = mkdir,
    read_dir   = read_dir,
    glob       = glob,
    is_dir     = is_dir,
}
