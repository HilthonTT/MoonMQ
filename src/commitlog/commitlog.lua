-- CommitLog: an append-only, segmented message log, a Lua port of jocko's
-- commitlog/commitlog.go (https://github.com/travisjeffery/jocko).
--
-- A log is a directory of segments. Each segment is a "<base_offset>.log" file
-- (a run of self-framing, CRC-protected records — see src/record/message.lua)
-- paired with a "<base_offset>.index" sidecar (offset -> byte position). The
-- active (newest) segment receives appends; when it fills past
-- max_segment_bytes a new one is rolled and a cleaner runs over the set to
-- enforce the cleanup policy (delete old data by byte budget, or compact by
-- key). Offsets are message counts and increase monotonically across the log.
--
-- This is a self-contained storage subsystem. The production server path uses
-- src/storage/segmentation.lua (SegmentedPartition) instead; CommitLog is the
-- direct jocko analogue, exposed via src/commitlog/init.lua.

local compact_cleaner = require("src.commitlog.compact_cleaner")
local delete_cleaner  = require("src.commitlog.delete_cleaner")
local segment_m       = require("src.commitlog.segment")
local fs_m            = require("src.io.fs")
local io_sync         = require("src.io.io_sync")
local string_m        = require("src.core.string")
local message_m       = require("src.record.message")
local log             = require("src.log.logger").get("commitlog")

local Segment = segment_m.Segment

local M = {}

local CleanupPolicy = {
    Delete  = "delete",
    Compact = "compact",
}

local LogFileSuffix   = ".log"
local IndexFileSuffix = ".index"

-- Rolled when the active segment would exceed this. jocko leaves a 0 here as a
-- TODO; we pick a concrete default so a zero/omitted option still rolls.
local DEFAULT_MAX_SEGMENT_BYTES = 64 * 1024 * 1024  -- 64 MiB

-- max_log_bytes == -1 means "retain everything" (the DeleteCleaner skips). We
-- map a 0 (the zero value of the option) to that so an unconfigured log never
-- silently discards data on the first roll.
local RETAIN_ALL = -1

-- ---------------------------------------------------------------------------
-- Options
-- ---------------------------------------------------------------------------
local Options = {}
Options.__index = Options

function Options.new(path, max_segment_bytes, max_log_bytes, cleanup_policy)
    assert(type(path) == "string", "path must be a string")
    max_segment_bytes = max_segment_bytes or 0
    max_log_bytes     = max_log_bytes     or 0
    cleanup_policy    = cleanup_policy    or ""
    assert(type(max_segment_bytes) == "number", "max_segment_bytes must be a number")
    assert(type(max_log_bytes) == "number", "max_log_bytes must be a number")
    assert(type(cleanup_policy) == "string", "cleanup_policy must be a string")

    return setmetatable({
        path              = path,
        max_segment_bytes = max_segment_bytes,
        max_log_bytes     = max_log_bytes,
        cleanup_policy    = cleanup_policy,
    }, Options)
end

-- ---------------------------------------------------------------------------
-- CommitLog
-- ---------------------------------------------------------------------------
local CommitLog = {}
CommitLog.__index = CommitLog

function CommitLog.new(options)
    assert(getmetatable(options) == Options, "options must be an Options instance")

    if options.path == "" then
        return nil, "path is empty"
    end

    -- Fill defaults for the zero values jocko left as TODOs.
    if options.max_segment_bytes == 0 then
        options.max_segment_bytes = DEFAULT_MAX_SEGMENT_BYTES
    end
    if options.max_log_bytes == 0 then
        options.max_log_bytes = RETAIN_ALL
    end
    if options.cleanup_policy == "" then
        options.cleanup_policy = CleanupPolicy.Delete
    end

    local cleaner
    if options.cleanup_policy == CleanupPolicy.Delete then
        cleaner = delete_cleaner.new(options.max_log_bytes)
    else
        cleaner = compact_cleaner.new()
    end

    local path, perr = fs_m.abs_path(options.path)
    if not path then return nil, perr end

    local l = setmetatable({
        options        = options,
        path           = path,
        name           = fs_m.base(path),
        cleaner        = cleaner,
        segments       = {},
        active_segment = nil,
    }, CommitLog)

    local ierr = l:init()
    if ierr then return nil, ierr end

    local oerr = l:open()
    if oerr then return nil, oerr end

    return l, nil
end

function CommitLog:init()
    local ok, err = fs_m.mkdir(self.path)
    if not ok then return err end
    return nil
end

-- open scans the log directory, loads every ".log" segment, drops any orphan
-- ".index" (one whose ".log" is gone), and — if the directory is empty —
-- creates an initial segment at offset 0. Segments are sorted by base_offset
-- and the newest becomes active.
function CommitLog:open()
    local files, ferr = fs_m.read_dir(self.path)
    if not files then
        return string.format("read dir failed: %s", tostring(ferr))
    end

    -- Recover from a crash mid-compaction. Segment:replace renames the
    -- ".cleaned" twin over the canonical name, but on Windows that rename is
    -- remove-then-rename (non-atomic). A crash in the gap can leave the
    -- canonical ".log" gone with only "<base>.log.cleaned" — the fully-written,
    -- fsync'd replacement — surviving. Promote such an orphan; if the canonical
    -- ".log" still exists, compaction didn't complete and the twin is
    -- discardable. ".index.cleaned" is always discardable (indexes rebuild from
    -- the log). Re-read the directory afterward so the promoted names are seen.
    local CLEANED = ".cleaned"
    local repaired = false
    for _, name in ipairs(files) do
        if string_m.endswith(name, LogFileSuffix .. CLEANED) then
            local canon      = string_m.trimsuffix(name, CLEANED)
            local canon_full = fs_m.join_path(self.path, canon)
            local clean_full = fs_m.join_path(self.path, name)
            if not fs_m.exists(canon_full) then
                local ok, rerr = io_sync.atomic_rename(clean_full, canon_full)
                if not ok then
                    return string.format("adopt orphan %s failed: %s",
                                         name, tostring(rerr))
                end
                log:warn("recovered interrupted compaction: promoted %s", name)
            else
                os.remove(clean_full)
            end
            repaired = true
        elseif string_m.endswith(name, IndexFileSuffix .. CLEANED) then
            os.remove(fs_m.join_path(self.path, name))
            repaired = true
        end
    end
    if repaired then
        files, ferr = fs_m.read_dir(self.path)
        if not files then
            return string.format("read dir failed: %s", tostring(ferr))
        end
    end

    for _, name in ipairs(files) do
        if string_m.endswith(name, IndexFileSuffix) then
            -- Orphan-index check: a ".index" with no matching ".log" is
            -- leftover junk (e.g. a crashed compaction). Remove it.
            local log_name = string_m.trimsuffix(name, IndexFileSuffix) .. LogFileSuffix
            local log_full = fs_m.join_path(self.path, log_name)
            if not fs_m.exists(log_full) then
                os.remove(fs_m.join_path(self.path, name))
            end
        elseif string_m.endswith(name, LogFileSuffix) then
            local offset_str  = string_m.trimsuffix(name, LogFileSuffix)
            local base_offset = tonumber(offset_str)
            if not base_offset then
                return string.format("invalid segment filename: %s", name)
            end
            local segment, serr = Segment.new(self.path, base_offset,
                                               self.options.max_segment_bytes)
            if not segment then return serr end
            self.segments[#self.segments + 1] = segment
        end
    end

    if #self.segments == 0 then
        local segment, serr = Segment.new(self.path, 0,
                                          self.options.max_segment_bytes)
        if not segment then return serr end
        self.segments[#self.segments + 1] = segment
    end

    -- read_dir order isn't guaranteed; segment logic relies on ascending
    -- base_offset.
    table.sort(self.segments, function(a, b)
        return a.base_offset < b.base_offset
    end)

    self.active_segment = self.segments[#self.segments]
    return nil
end

-- check_split reports whether the active segment is full and a roll is due.
function CommitLog:check_split()
    return self.active_segment:is_full()
end

-- split rolls a new active segment at the current newest offset and runs the
-- cleaner over the full segment set, replacing the kept list. Mirrors jocko's
-- split(): the cleaner may delete (DeleteCleaner) or rewrite (CompactCleaner)
-- segments. Returns nil on success, err string otherwise.
function CommitLog:split()
    local segment, serr = Segment.new(self.path, self:newest_offset(),
                                      self.options.max_segment_bytes)
    if not segment then return serr end

    local segments = {}
    for i = 1, #self.segments do segments[i] = self.segments[i] end
    segments[#segments + 1] = segment

    local cleaned, cerr = self.cleaner:clean(segments)
    if cerr then return cerr end

    self.segments = cleaned
    -- The newest segment is always the last of the returned list. We can't
    -- reuse the local `segment` handle directly: the CompactCleaner replaces
    -- each segment (including this fresh one) with a ".cleaned" twin and
    -- closes the original, so the live active is whatever sits at the tail.
    self.active_segment = cleaned[#cleaned]
    return nil
end

-- append writes one pre-serialized record, assigning it the next offset.
-- Returns (offset, nil) or (nil, err). Use append_message for the common case
-- of appending a Message.
function CommitLog:append(record)
    assert(type(record) == "string", "record must be a string")

    if self:check_split() then
        local serr = self:split()
        if serr then return nil, serr end
    end

    local seg      = self.active_segment
    local position = seg.position
    local offset   = seg.next_offset

    local ok, werr = seg:write(record)
    if not ok then return nil, werr end

    -- Index the record after the log write succeeds, so we never point the
    -- index at bytes that failed to land. If indexing fails, roll the log
    -- write back: otherwise the segment's next_offset/position would count a
    -- record the index can't resolve, and a later read of that tail offset
    -- would fault. `offset` is the pre-write next_offset, so it's exactly the
    -- counter value to restore.
    local ierr = seg.index:write_entry(offset, position)
    if ierr then
        seg:rewind(position, offset)
        return nil, ierr
    end

    return offset, nil
end

-- append_message serializes `msg` (a src/record/message.lua Message) and
-- appends it. Returns (offset, nil) or (nil, err).
function CommitLog:append_message(msg)
    assert(getmetatable(msg) == message_m.Message, "msg must be a Message instance")
    local record, serr = message_m.serialize_message(msg)
    if not record then
        return nil, string.format("failed to serialize message: %s", tostring(serr))
    end
    return self:append(record)
end

-- segment_for_offset returns the segment owning `offset`
-- (base_offset <= offset < next_offset), or nil.
function CommitLog:segment_for_offset(offset)
    for i = 1, #self.segments do
        local s = self.segments[i]
        if offset >= s.base_offset and offset < s.next_offset then
            return s
        end
    end
    return nil
end

-- read_at returns (Message, next_offset, nil) for the record at `offset`, or
-- (nil, nil, err). next_offset is offset+1 (message-count offsets), the value
-- to pass on the next call to stream forward.
function CommitLog:read_at(offset)
    assert(type(offset) == "number", "offset must be a number")

    local seg = self:segment_for_offset(offset)
    if not seg then
        return nil, nil, string.format("offset %d out of range", offset)
    end

    local position, lerr = seg.index:lookup(offset)
    if not position then
        return nil, nil, lerr
    end

    local msg, _, derr = seg:read_at(position)
    if not msg then
        return nil, nil, derr
    end
    return msg, offset + 1, nil
end

-- newest_offset is the offset the next appended record will receive (jocko's
-- NewestOffset == activeSegment.NextOffset).
function CommitLog:newest_offset()
    return self.active_segment.next_offset
end

-- oldest_offset is the base offset of the oldest retained segment.
function CommitLog:oldest_offset()
    return self.segments[1].base_offset
end

-- sync fsyncs the active segment's log to disk. The index is rebuildable from
-- the log on recovery, so only the log needs the durability barrier. Used by
-- the acks=1 path. Returns (true, nil) or (false, err).
function CommitLog:sync()
    if not self.active_segment then
        return false, "no active segment"
    end
    return self.active_segment:sync()
end

function CommitLog:close()
    for i = 1, #self.segments do
        self.segments[i]:close()
    end
    self.active_segment = nil
    return nil
end

-- delete removes the whole log directory and its contents.
function CommitLog:delete()
    self:close()
    local ok, err = fs_m.remove_all(self.path)
    if not ok then return err end
    return nil
end

-- truncate removes every segment whose base_offset is below `offset`,
-- discarding that data (jocko's Truncate granularity is a whole segment).
-- If the cut would remove the active segment too, a fresh empty segment is
-- opened at `offset` so the log stays writable. Returns nil on success.
function CommitLog:truncate(offset)
    assert(type(offset) == "number", "offset must be a number")

    local kept = {}
    for i = 1, #self.segments do
        local segment = self.segments[i]
        if segment.base_offset < offset and segment ~= self.active_segment then
            local derr = segment:delete()
            if derr then return derr end
        else
            kept[#kept + 1] = segment
        end
    end

    -- If the active segment itself was below the cut, it's still in `kept`
    -- (we never delete the active one above) — drop it now and start fresh
    -- so reads below `offset` truly fail.
    if self.active_segment and self.active_segment.base_offset < offset then
        self.active_segment:delete()
        local fresh = {}
        for i = 1, #kept do
            if kept[i] ~= self.active_segment then fresh[#fresh + 1] = kept[i] end
        end
        kept = fresh
    end

    if #kept == 0 then
        local segment, serr = Segment.new(self.path, offset,
                                          self.options.max_segment_bytes)
        if not segment then return serr end
        kept[1] = segment
    end

    self.segments       = kept
    self.active_segment = kept[#kept]
    return nil
end

M.CommitLog     = CommitLog
M.Options       = Options
M.CleanupPolicy = CleanupPolicy

return M
