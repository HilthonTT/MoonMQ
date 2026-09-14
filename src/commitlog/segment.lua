local fs_m      = require("src.io.fs")
local io_sync   = require("src.io.io_sync")
local message_m = require("src.record.message")
local index_m   = require("src.commitlog.index")
local log       = require("src.log.logger").get("commitlog.segment")

local LogFileSuffix   = ".log"
local IndexFileSuffix = ".index"

local Segment = {}
Segment.__index = Segment

local function log_path(dir, base_offset, suffix)
    return fs_m.join_path(dir,
        string.format("%020d%s", base_offset, LogFileSuffix .. (suffix or "")))
end

local function index_path(dir, base_offset, suffix)
    return fs_m.join_path(dir,
        string.format("%020d%s", base_offset, IndexFileSuffix .. (suffix or "")))
end

function Segment.new(dir, base_offset, max_bytes, suffix)
    assert(type(dir) == "string", "dir must be a string")
    assert(type(base_offset) == "number", "base_offset must be a number")
    assert(type(max_bytes) == "number", "max_bytes must be a number")
    suffix = suffix or ""

    local lp = log_path(dir, base_offset, suffix)
    local file, err = io.open(lp, "a+b")
    if not file then
        return nil, string.format("failed to open segment log %s: %s",
                                  lp, tostring(err))
    end

    local idx, ierr = index_m.Index.new(index_path(dir, base_offset, suffix),
                                        base_offset)
    if not idx then
        file:close()
        return nil, ierr
    end

    local s = setmetatable({
        dir         = dir,
        base_offset = base_offset,
        next_offset = base_offset,
        position    = 0,
        max_bytes   = max_bytes,
        suffix      = suffix,
        file        = file,
        index       = idx,
    }, Segment)

    local berr = s:build_index()
    if berr then
        s:close()
        return nil, berr
    end

    return s, nil
end

function Segment:build_index()
    local aerr = self.index:align()
    if aerr then return aerr end

    local existing = self.index:count()
    local trusted  = true

    local file_end = self.file:seek("end") or 0
    self.file:seek("set", 0)

    local next_offset = self.base_offset
    local position    = 0
    local slot        = 0

    while true do
        local size_bytes = self.file:read(8)
        if not size_bytes or #size_bytes < 8 then
            break
        end
        local total_size = string.unpack(">I8", size_bytes)
        if total_size < message_m.MIN_BODY
           or total_size > file_end - (position + 8) then
            break
        end

        self.file:seek("set", position)
        local msg, framed = message_m.deserialize_record(self.file)
        if not msg then
            break
        end

        local offset = next_offset
        if trusted then
            local abs, ipos
            if slot < existing then
                abs, ipos = self.index:entry_at(slot)
            end
            if abs and ipos == position and abs >= next_offset then
                offset = abs
            else
                trusted = false
                local terr = self.index:truncate_entries(slot)
                if terr then return terr end
            end
        end
        if not trusted then
            local werr = self.index:write_entry(offset, position)
            if werr then return werr end
        end

        self.file:seek("set", position + framed)
        position    = position + framed
        next_offset = offset + 1
        slot        = slot + 1
    end

    if trusted and slot < existing then
        local terr = self.index:truncate_entries(slot)
        if terr then return terr end
    end

    if position < file_end then
        log:warn("segment %020d: trimming %d byte(s) past last valid record at %d",
            self.base_offset, file_end - position, position)
        local ok, trerr = io_sync.truncate(self.file, position)
        if not ok then
            return string.format("failed to trim torn tail: %s", tostring(trerr))
        end
    end

    self.next_offset = next_offset
    self.position    = position
    return nil
end

function Segment:is_full()
    if self.max_bytes <= 0 then
        return false
    end
    return self.position >= self.max_bytes
end

function Segment:write(record)
    assert(type(record) == "string", "record must be a string")

    local pos, serr = self.file:seek("end")
    if not pos then
        return false, string.format("log seek failed: %s", tostring(serr))
    end

    if pos ~= self.position then
        local tok, terr = io_sync.truncate(self.file, self.position)
        if not tok then
            return false, string.format(
                "log EOF %d != tracked %d and truncate failed: %s",
                pos, self.position, tostring(terr))
        end
        self.file:seek("end")
    end

    local ok, werr = self.file:write(record)
    if not ok then
        return false, string.format("log write failed: %s", tostring(werr))
    end
    self.position    = self.position + #record
    self.next_offset = self.next_offset + 1
    return true, nil
end

function Segment:rewind(position, next_offset)
    assert(type(position) == "number", "position must be a number")
    assert(type(next_offset) == "number", "next_offset must be a number")
    local ok, err = io_sync.truncate(self.file, position)
    if not ok then return false, err end
    self.position    = position
    self.next_offset = next_offset
    return true, nil
end

function Segment:read_at(position)
    assert(type(position) == "number", "position must be a number")
    local pos, serr = self.file:seek("set", position)
    if not pos then
        return nil, nil, string.format("seek failed: %s", tostring(serr))
    end
    return message_m.deserialize_record(self.file)
end

function Segment:append_at(record, offset)
    assert(type(offset) == "number", "offset must be a number")
    if offset < self.next_offset then
        return false, string.format("offset %d below next offset %d",
                                    offset, self.next_offset)
    end
    local position = self.position
    local ok, werr = self:write(record)
    if not ok then return false, werr end
    local ierr = self.index:write_entry(offset, position)
    if ierr then
        self:rewind(position, self.next_offset - 1)
        return false, ierr
    end
    self.next_offset = offset + 1
    return true, nil
end

function Segment:each(callback)
    local count = self.index:count()
    for slot = 0, count - 1 do
        local offset, position = self.index:entry_at(slot)
        if not offset then break end
        self.file:seek("set", position)
        local msg = message_m.deserialize_record(self.file)
        if not msg then break end
        callback(offset, msg, position)
    end
end

function Segment:sync()
    local ok, err = io_sync.sync(self.file)
    if not ok then return false, err end
    return true, nil
end

function Segment:sync_all()
    local ok, err = self:sync()
    if not ok then return false, err end
    local iok, ierr = self.index:sync()
    if not iok then return false, ierr end
    return true, nil
end

function Segment:close()
    if self.file then
        io_sync.sync(self.file)
        self.file:close()
        self.file = nil
    end
    if self.index then
        self.index:close()
        self.index = nil
    end
end

function Segment:delete()
    self:close()
    local lp = log_path(self.dir, self.base_offset, self.suffix)
    local ip = index_path(self.dir, self.base_offset, self.suffix)
    local ok, err = os.remove(lp)
    if not ok then
        return string.format("failed to remove %s: %s", lp, tostring(err))
    end
    os.remove(ip)
    return nil
end

-- (Re)open this segment on its canonical (suffix-less) files and rebuild
-- the index from whatever log is there.
function Segment:_open_canonical()
    local canon_log = log_path(self.dir, self.base_offset, "")
    local canon_idx = index_path(self.dir, self.base_offset, "")

    self.suffix = ""
    local file, ferr = io.open(canon_log, "a+b")
    if not file then
        return string.format("reopen %s failed: %s", canon_log, tostring(ferr))
    end
    self.file = file

    local idx, ierr = index_m.Index.new(canon_idx, self.base_offset)
    if not idx then
        return ierr
    end
    self.index = idx

    return self:build_index()
end

function Segment:replace(old)
    local sok, serr = self:sync_all()
    if not sok then
        return string.format("sync cleaned segment failed: %s", tostring(serr))
    end

    old:close()
    self:close()

    local canon_log = log_path(self.dir, self.base_offset, "")
    local canon_idx = index_path(self.dir, self.base_offset, "")
    local clean_log = log_path(self.dir, self.base_offset, self.suffix)
    local clean_idx = index_path(self.dir, self.base_offset, self.suffix)

    -- On failure the caller keeps using `old`, so it must be reopened on
    -- whatever now sits at the canonical path (build_index re-derives the
    -- index, so a half-done rename pair is still consistent).
    local function fail(msg)
        local rerr = old:_open_canonical()
        if rerr then
            msg = msg .. string.format(" (and reopening the original failed: %s)", rerr)
        end
        return msg
    end

    local ok, err = io_sync.atomic_rename(clean_log, canon_log)
    if not ok then
        return fail(string.format("rename %s -> %s failed: %s",
                                  clean_log, canon_log, tostring(err)))
    end
    ok, err = io_sync.atomic_rename(clean_idx, canon_idx)
    if not ok then
        return fail(string.format("rename %s -> %s failed: %s",
                                  clean_idx, canon_idx, tostring(err)))
    end

    local dok, derr = io_sync.sync_dir(self.dir)
    if not dok then
        log:warn("segment %020d: dir fsync after compaction replace failed: %s",
            self.base_offset, tostring(derr))
    end

    return self:_open_canonical()
end

return {
    Segment         = Segment,
    LogFileSuffix   = LogFileSuffix,
    IndexFileSuffix = IndexFileSuffix,
    log_path        = log_path,
    index_path      = index_path,
}
