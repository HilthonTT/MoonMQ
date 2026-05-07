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

local IS_WINDOWS = package.config:sub(1, 1) == "\\"

if not IS_WINDOWS then
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
        sync     = sync,
        truncate = truncate,
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
    sync     = sync,
    truncate = truncate,
}
