-- Real fsync + truncate.
--
-- file:flush() only pushes the C runtime's userspace buffer down to the OS
-- — it doesn't tell the OS to flush its own write cache to disk. For the
-- durability claims (acks=1, sync_every) to mean anything, we need fsync
-- on the underlying file descriptor.
--
-- Linux/POSIX: use luaposix (posix.stdio.fileno + posix.unistd.fsync/ftruncate).
-- Windows: fall back to LuaJIT FFI against the C runtime, since luaposix
-- doesn't ship there.

local os_utils = require("src.core.os")

-- atomic_rename: POSIX `rename(2)` is atomic over an existing target. On
-- Windows it isn't — the target must not exist, otherwise os.rename fails.
-- We fall back to remove-then-rename there. Not truly atomic, but the
-- checkpoint file's content (a base-10 integer) is safe to read as 0 if
-- a crash lands in the gap: the next recovery just re-scans more.
local function atomic_rename(from, to)
    assert(type(from) == "string", "from must be a string")
    assert(type(to)   == "string", "to must be a string")

    local ok, err = os.rename(from, to)
    if ok then return true, nil end
    if not os_utils.IS_WINDOWS then return false, err end

    os.remove(to)
    local ok2, err2 = os.rename(from, to)
    if not ok2 then return false, err2 end
    return true, nil
end

if not os_utils.IS_WINDOWS then
    local stdio   = require("posix.stdio")
    local unistd  = require("posix.unistd")

    local function sync(luafile)
        if not luafile then return false, "no file" end
        luafile:flush()                       -- userspace -> OS
        local fd = stdio.fileno(luafile)
        if not fd then return false, "fileno failed" end
        local ok, err = unistd.fsync(fd)
        if not ok then return false, "fsync failed: " .. tostring(err) end
        return true, nil
    end

    local function truncate(luafile, length)
        if not luafile then return false, "no file" end
        assert(type(length) == "number", "length must be a number")
        luafile:flush()
        local fd = stdio.fileno(luafile)
        if not fd then return false, "fileno failed" end
        local ok, err = unistd.ftruncate(fd, length)
        if not ok then return false, "ftruncate failed: " .. tostring(err) end
        return true, nil
    end

    return {
        sync          = sync,
        truncate      = truncate,
        atomic_rename = atomic_rename,
    }
end

-- Windows path: requires LuaJIT FFI.
--
-- Trick: LuaJIT's tostring(luafile) returns "file (0x...)"; the hex address
-- is the underlying FILE*. This is the documented idiom for handing a Lua
-- file to libc functions that take FILE*.

local ffi = require("ffi")

ffi.cdef[[
    typedef struct FILE FILE;
    int _fileno(FILE *stream);
    int _commit(int fd);
    int _chsize_s(int fd, int64_t size);
]]

local function file_ptr(luafile)
    local s = tostring(luafile)
    local hex = s:match("0x(%x+)")
    if not hex then
        return nil, "could not extract FILE* from " .. s
    end
    return ffi.cast("FILE*", tonumber(hex, 16))
end

local function file_fd(luafile)
    local fp, err = file_ptr(luafile)
    if not fp then return nil, err end
    return ffi.C._fileno(fp)
end

local function sync(luafile)
    if not luafile then return false, "no file" end
    luafile:flush()
    local fd, err = file_fd(luafile)
    if not fd then return false, err end
    if ffi.C._commit(fd) ~= 0 then
        return false, "_commit failed"
    end
    return true, nil
end

local function truncate(luafile, length)
    if not luafile then return false, "no file" end
    assert(type(length) == "number", "length must be a number")
    luafile:flush()
    local fd, err = file_fd(luafile)
    if not fd then return false, err end
    if ffi.C._chsize_s(fd, length) ~= 0 then
        return false, "_chsize_s failed"
    end
    return true, nil
end

return {
    sync          = sync,
    truncate      = truncate,
    atomic_rename = atomic_rename,
}
