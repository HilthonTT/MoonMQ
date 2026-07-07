-- A single log segment: one ".log" file holding a contiguous run of records
-- plus a ".index" sidecar mapping offsets to byte positions. Faithful port of
-- jocko's commitlog/segment.go, adapted to MoonMQ's self-framing record format
-- (see src/record/message.lua) instead of jocko's MessageSet.
--
-- Offsets are message counts (jocko semantics): base_offset is the first
-- offset in the segment, next_offset the offset the next appended record will
-- receive, and position the number of bytes written so far. A record on disk
-- is length-prefixed and CRC-protected, so the log is self-describing — the
-- index is an acceleration structure, rebuildable from the log at any time.

local fs_m      = require("src.io.fs")
local io_sync   = require("src.io.io_sync")
local message_m = require("src.record.message")
local index_m   = require("src.commitlog.index")
local log       = require("src.log.logger").get("commitlog.segment")

local LogFileSuffix   = ".log"
local IndexFileSuffix = ".index"

local Segment = {}
Segment.__index = Segment

-- Path helpers. `suffix` is "" for live segments and ".cleaned" for the
-- temporary segment a compaction writes before atomically replacing the
-- original (jocko's cleanedSuffix scheme).
local function log_path(dir, base_offset, suffix)
    return fs_m.join_path(dir,
        string.format("%020d%s", base_offset, LogFileSuffix .. (suffix or "")))
end

local function index_path(dir, base_offset, suffix)
    return fs_m.join_path(dir,
        string.format("%020d%s", base_offset, IndexFileSuffix .. (suffix or "")))
end

-- new opens (creating if absent) the segment at base_offset in `dir`, then
-- rebuilds its index from the log — which also recovers next_offset/position
-- and trims any torn tail left by a crash mid-append. Returns (segment, nil)
-- or (nil, err).
function Segment.new(dir, base_offset, max_bytes, suffix)
    assert(type(dir) == "string", "dir must be a string")
    assert(type(base_offset) == "number", "base_offset must be a number")
    assert(type(max_bytes) == "number", "max_bytes must be a number")
    suffix = suffix or ""

    local lp = log_path(dir, base_offset, suffix)
    -- "a+b": append-only writes (records always land at EOF, so a torn write
    -- can't corrupt earlier data) while still allowing seek+read for lookups.
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

-- build_index discards the on-disk index and rebuilds it by scanning the log,
-- one record at a time. It (re)derives next_offset and position from the log
-- itself — the log is authoritative — and physically truncates any trailing
-- partial record so subsequent appends stay contiguous. Returns nil on
-- success, err string otherwise.
function Segment:build_index()
    local terr = self.index:truncate_entries(0)
    if terr then return terr end

    -- File size up front so a corrupt length prefix can't drive a huge
    -- allocation via file:read(total_size) — we bound each record against the
    -- bytes actually remaining before trusting its length.
    local file_end = self.file:seek("end") or 0
    self.file:seek("set", 0)

    local next_offset = self.base_offset
    local position    = 0

    while true do
        -- Peek the 8-byte length prefix, bound it against the file, then rewind
        -- and decode the whole record through deserialize_record so the CRCs
        -- are validated here — the same way segment_verify does for the
        -- segmented backend. A short read, an out-of-range length, or a CRC
        -- mismatch is treated as a torn tail: stop and truncate below. (The
        -- old fast-path framed by length alone and skipped CRC entirely, so a
        -- mid-log bit-flip was silently indexed and only faulted at read_at.)
        local size_bytes = self.file:read(8)
        if not size_bytes or #size_bytes < 8 then
            break
        end
        local total_size = string.unpack(">I8", size_bytes)
        if total_size < 0 or position + 8 + total_size > file_end then
            break  -- length runs past EOF: torn/corrupt tail
        end

        self.file:seek("set", position)  -- rewind to the record start
        local msg, framed = message_m.deserialize_record(self.file)
        if not msg then
            break  -- short read or CRC mismatch: torn/corrupt tail
        end

        local werr = self.index:write_entry(next_offset, position)
        if werr then return werr end

        position    = position + framed
        next_offset = next_offset + 1
    end

    -- Drop any bytes past the last whole record (a half-written tail from a
    -- crash, or everything after an interior corruption). Without this, the
    -- next append would sit after the garbage. file_end was computed up front
    -- and the scan is read-only, so it still reflects the on-disk size.
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

-- is_full reports whether the segment has reached its byte cap. A max_bytes of
-- 0 means "no cap" (never full) — the CommitLog applies its own default before
-- creating segments, so this is just a safety valve.
function Segment:is_full()
    if self.max_bytes <= 0 then
        return false
    end
    return self.position >= self.max_bytes
end

-- write appends a pre-serialized record's bytes, bumping next_offset and
-- position. It does NOT touch the index — the CommitLog writes the index entry
-- after a successful write (jocko splits the responsibility the same way).
-- Returns (true, nil) or (false, err).
function Segment:write(record)
    assert(type(record) == "string", "record must be a string")

    -- Reposition to EOF before writing. The log handle is opened "a+b" (an
    -- update stream) and read_at/each seek+read from it; C stdio forbids a write
    -- directly after a read on an update stream without an intervening
    -- positioning call, and Windows UCRT enforces it — the write returns nil and
    -- the append fails. A lagging consumer reading a non-tail record would
    -- otherwise poison this handle so the next produce fails. seek("end") is the
    -- required reposition (append mode already targets EOF, so it's a no-op to
    -- the byte position).
    self.file:seek("end")
    local ok, werr = self.file:write(record)
    if not ok then
        return false, string.format("log write failed: %s", tostring(werr))
    end
    self.position    = self.position + #record
    self.next_offset = self.next_offset + 1
    return true, nil
end

-- rewind undoes the most recent write(s): it truncates the log back to byte
-- `position` and restores the offset/position counters. The CommitLog uses
-- this when an index write fails right after a log write, so the segment never
-- reports a record the index doesn't cover. Returns (true, nil) or (false, err).
function Segment:rewind(position, next_offset)
    assert(type(position) == "number", "position must be a number")
    assert(type(next_offset) == "number", "next_offset must be a number")
    local ok, err = io_sync.truncate(self.file, position)
    if not ok then return false, err end
    self.position    = position
    self.next_offset = next_offset
    return true, nil
end

-- read_at reads the record stored at byte `position`, returning
-- (Message, framed_size, nil) or (nil, nil, err).
function Segment:read_at(position)
    assert(type(position) == "number", "position must be a number")
    local pos, serr = self.file:seek("set", position)
    if not pos then
        return nil, nil, string.format("seek failed: %s", tostring(serr))
    end
    return message_m.deserialize_record(self.file)
end

-- each iterates every record in offset order, invoking
-- callback(offset, msg, position). Stops at the first short/corrupt record
-- (EOF or torn tail). Used by the compaction cleaner.
function Segment:each(callback)
    self.file:seek("set", 0)
    local offset   = self.base_offset
    local position = 0
    while true do
        local msg, framed = message_m.deserialize_record(self.file)
        if not msg then break end
        callback(offset, msg, position)
        offset   = offset + 1
        position = position + framed
    end
end

function Segment:sync()
    local ok, err = io_sync.sync(self.file)
    if not ok then return false, err end
    return true, nil
end

function Segment:close()
    if self.file then
        -- fsync, not just flush: flush only pushes the CRT buffer down to the
        -- OS page cache, so a "clean shutdown" of a commitlog-backed partition
        -- would still lose acks=0 writes on a power/OS crash. This matches
        -- SegmentedPartition:close(), which already fsyncs on close.
        io_sync.sync(self.file)
        self.file:close()
        self.file = nil
    end
    if self.index then
        self.index:close()
        self.index = nil
    end
end

-- delete closes the segment and removes both its files from disk. Returns nil
-- on success, err string otherwise.
function Segment:delete()
    self:close()
    local lp = log_path(self.dir, self.base_offset, self.suffix)
    local ip = index_path(self.dir, self.base_offset, self.suffix)
    local ok, err = os.remove(lp)
    if not ok then
        return string.format("failed to remove %s: %s", lp, tostring(err))
    end
    -- The index may legitimately be absent; ignore its removal error.
    os.remove(ip)
    return nil
end

-- replace makes this (".cleaned") segment take the place of `old`: both are
-- closed, this segment's files are renamed over old's canonical names, then it
-- is reopened suffix-less and its index rebuilt. Returns nil on success, err
-- otherwise. Mirrors jocko's Segment.Replace, used by compaction.
function Segment:replace(old)
    -- Make the cleaned replacement durable BEFORE the rename. On Windows
    -- atomic_rename is remove-then-rename (non-atomic), so a crash can leave
    -- only the ".cleaned" file as the surviving copy — CommitLog:open adopts
    -- it on recovery, but only if its bytes actually reached disk.
    local sok, serr = self:sync()
    if not sok then
        return string.format("sync cleaned segment failed: %s", tostring(serr))
    end

    old:close()
    self:close()

    local canon_log = log_path(self.dir, self.base_offset, "")
    local canon_idx = index_path(self.dir, self.base_offset, "")
    local clean_log = log_path(self.dir, self.base_offset, self.suffix)
    local clean_idx = index_path(self.dir, self.base_offset, self.suffix)

    -- atomic_rename removes an existing target first on Windows (POSIX rename
    -- replaces atomically), so the canonical files are overwritten cleanly.
    local ok, err = io_sync.atomic_rename(clean_log, canon_log)
    if not ok then
        return string.format("rename %s -> %s failed: %s",
                             clean_log, canon_log, tostring(err))
    end
    ok, err = io_sync.atomic_rename(clean_idx, canon_idx)
    if not ok then
        return string.format("rename %s -> %s failed: %s",
                             clean_idx, canon_idx, tostring(err))
    end

    -- Persist the directory entries: on POSIX the two renames above aren't
    -- crash-durable until the containing directory is fsynced, or a power loss
    -- could leave the canonical name pointing at a partially-linked file.
    -- Non-fatal: the rename already succeeded in the page cache.
    local dok, derr = io_sync.sync_dir(self.dir)
    if not dok then
        log:warn("segment %020d: dir fsync after compaction replace failed: %s",
            self.base_offset, tostring(derr))
    end

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

return {
    Segment         = Segment,
    LogFileSuffix   = LogFileSuffix,
    IndexFileSuffix = IndexFileSuffix,
    log_path        = log_path,
    index_path      = index_path,
}
