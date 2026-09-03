local os_utils = require("src.core.os")

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
    local fcntl   = require("posix.fcntl")

    local function sync(luafile)
        if not luafile then return false, "no file" end
        local fok, ferr = luafile:flush()
        if not fok then return false, "flush failed: " .. tostring(ferr) end
        local fd = stdio.fileno(luafile)
        if not fd then return false, "fileno failed" end
        local ok, err = unistd.fsync(fd)
        if not ok then return false, "fsync failed: " .. tostring(err) end
        return true, nil
    end

    local function sync_dir(path)
        if type(path) ~= "string" then return false, "path must be a string" end
        local fd, oerr = fcntl.open(path, fcntl.O_RDONLY)
        if not fd then return false, "open dir failed: " .. tostring(oerr) end
        local ok, ferr = unistd.fsync(fd)
        unistd.close(fd)
        if not ok then return false, "fsync dir failed: " .. tostring(ferr) end
        return true, nil
    end

    local function truncate(luafile, length)
        if not luafile then return false, "no file" end
        assert(type(length) == "number", "length must be a number")
        local fok, ferr = luafile:flush()
        if not fok then return false, "flush failed: " .. tostring(ferr) end
        local fd = stdio.fileno(luafile)
        if not fd then return false, "fileno failed" end
        local ok, err = unistd.ftruncate(fd, length)
        if not ok then return false, "ftruncate failed: " .. tostring(err) end
        return true, nil
    end

    return {
        sync          = sync,
        sync_dir      = sync_dir,
        truncate      = truncate,
        atomic_rename = atomic_rename,
    }
end


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
    local fok, ferr = luafile:flush()
    if not fok then return false, "flush failed: " .. tostring(ferr) end
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
    local fok, ferr = luafile:flush()
    if not fok then return false, "flush failed: " .. tostring(ferr) end
    local fd, err = file_fd(luafile)
    if not fd then return false, err end
    if ffi.C._chsize_s(fd, length) ~= 0 then
        return false, "_chsize_s failed"
    end
    return true, nil
end

local function sync_dir(_path)
    return true, nil
end

return {
    sync          = sync,
    sync_dir      = sync_dir,
    truncate      = truncate,
    atomic_rename = atomic_rename,
}
