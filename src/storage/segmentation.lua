local topic_m = require("src.storage.topic")
local time_m = require("src.core.time")
local fs_m = require("src.io.fs")
local msg_m = require("src.record.message")
local socket = require("socket")
local io_sync = require("src.io.io_sync")
local verify_file = require("src.storage.segment_verify")
local time_index = require("src.storage.time_index")
local GroupCommitter = require("src.storage.group_committer")
local log = require("src.log.logger").get("segmentation")
-- Metrics. Loaded lazily-tolerant: src.metrics needs luasocket, which a
-- pure-storage embedding may not have; fall back to a no-op shim so the
-- production path stays instrumented without forcing test scaffolds to
-- mock anything out.
local ok_m, metrics = pcall(require, "src.metrics")
if not ok_m then
    metrics = {
        inc     = function(...) end,
        set     = function(...) end,
        observe = function(...) end,
        timer   = function(...) return function() end end,
    }
end

-- Optional: luafilesystem for accurate per-segment mtime fallback. Without
-- it, segments fall back to their .meta sidecar or (last resort) "now".
local has_lfs, lfs = pcall(require, "lfs")

local LogSegment = {}
LogSegment.__index = LogSegment

-- bytes_written: explicit byte counter. Initialised here from the file's
-- current end position so callers don't need to pass it during recovery.
-- We track this ourselves instead of seeking on every size() call because
-- the file is opened with "a+b" and the seek dance interacts poorly with
-- append semantics (writes always go to EOF, and a fresh a+b handle starts
-- at position 0 rather than EOF — the old size() restored the wrong pos).
--
-- index_file (optional): handle to the segment's .timeindex sidecar. When
-- nil, write_message skips indexing for this segment — handy for legacy
-- callers/tests that don't care about time-based seek. Always non-nil on
-- the SegmentedPartition path.
--
-- last_indexed_at: file position (within the segment) at which the most
-- recent timeindex entry's message starts. nil means "no entry yet" — the
-- next write is unconditionally indexed so every segment has at least one
-- entry, which simplifies binary search.
function LogSegment.new(file, base_offset, start_time, bytes_written,
                        index_file, last_indexed_at)
    assert(type(base_offset) == "number", "base_offset must be a number")
    assert(type(start_time)  == "number", "start_time must be a number")

    if not bytes_written then
        bytes_written = file:seek("end") or 0
    end

    return setmetatable({
        file            = file,
        base_offset     = base_offset,
        start_time      = start_time,
        bytes_written   = bytes_written,
        index_file      = index_file,
        last_indexed_at = last_indexed_at,
    }, LogSegment)
end

function LogSegment:size()
    return self.bytes_written
end

function LogSegment:close()
    if self.file then
        self.file:close()
        self.file = nil
    end
    if self.index_file then
        self.index_file:close()
        self.index_file = nil
    end
end

function LogSegment:file_path(dir)
    return fs_m.join_path(dir, string.format("%020d.log", self.base_offset))
end

function LogSegment:meta_path(dir)
    return fs_m.join_path(dir, string.format("%020d.meta", self.base_offset))
end

function LogSegment:index_path(dir)
    return fs_m.join_path(dir, string.format("%020d.timeindex", self.base_offset))
end

local SegmentedPartition = {}
SegmentedPartition.__index = SegmentedPartition

local DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 1024  -- 1 GiB
local DEFAULT_RETENTION        = 7 * 24 * time_m.HOUR
local DEFAULT_CLEANER_INTERVAL = 1 * time_m.HOUR

-- Sparse timestamp index: one (ts, file_pos) entry per this many bytes of
-- log written. Smaller = faster time-based seek but more index bytes;
-- larger = bigger linear-scan window in offset_for_timestamp. 4 KiB is
-- the Kafka-tradition default for time index sparseness.
local DEFAULT_INDEX_INTERVAL_BYTES = 4096

-- Anchored Lua pattern (NOT a shell glob). fs_m.glob takes a Lua pattern.
local LOG_FILE_PATTERN = "^%d+%.log$"

-- Read a segment's .meta sidecar (creation time written when the segment
-- was first opened). Returns the timestamp as a number, or nil if absent
-- or malformed.
local function read_meta(meta_path)
    local f = io.open(meta_path, "rb")
    if not f then return nil end
    local body = f:read("*a") or ""
    f:close()
    return tonumber(body)
end

-- Write the .meta sidecar with the segment's creation timestamp. Best-effort
-- — failure to write the sidecar is logged but doesn't fail the segment
-- create (retention will fall back to LFS mtime or to a fresh timestamp).
local function write_meta(meta_path, t)
    local f, err = io.open(meta_path, "wb")
    if not f then
        log:error("segment meta write %s: %s", meta_path, tostring(err))
        return
    end
    f:write(tostring(t))
    f:flush()
    f:close()
end

-- opts (optional): per-topic config overrides
--   max_segment_size      bytes before rolling to a new segment (default 1 GiB)
--   retention             seconds to keep sealed segments       (default 7d)
--   cleaner_interval      seconds between retention sweeps      (default 1h)
--   index_interval_bytes  bytes between timestamp-index entries (default 4 KiB)
-- Unknown keys are ignored; nil/missing keys fall back to the defaults.
-- Callers wanting per-topic tuning pass these via TopicManager:create_topic.
function SegmentedPartition.new(topic, id, dir, opts)
    assert(getmetatable(topic) == topic_m, "topic must be a Topic instance")
    assert(type(id)  == "number", "id must be a number")
    assert(type(dir) == "string", "dir must be a string")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end
    opts = opts or {}

    -- Timeindex entries pack the in-segment file position as u32, so a
    -- segment larger than 4 GiB would make maybe_append raise mid-produce —
    -- AFTER the record is on disk, so the producer's retry would duplicate.
    -- Reject the misconfiguration up front (the commitlog backend does too).
    if opts.max_segment_size and opts.max_segment_size > 0xFFFFFFFF then
        return nil, string.format(
            "max_segment_size %d exceeds the 4 GiB timeindex limit",
            opts.max_segment_size)
    end

    local partition_dir = fs_m.join_path(dir, string.format("partition-%d", id))
    local ok, err = fs_m.mkdir(partition_dir)
    if not ok then
        return nil, err
    end

    local sp = setmetatable({
        topic                = topic,
        id                   = id,
        dir                  = partition_dir,
        max_segment_size     = opts.max_segment_size     or DEFAULT_MAX_SEGMENT_SIZE,
        retention            = opts.retention            or DEFAULT_RETENTION,
        cleaner_interval     = opts.cleaner_interval     or DEFAULT_CLEANER_INTERVAL,
        index_interval_bytes = opts.index_interval_bytes or DEFAULT_INDEX_INTERVAL_BYTES,
        segments             = {},
        active_segment       = nil,
        offset               = 0,   -- leader-end / next-write offset
    }, SegmentedPartition)

    local lerr = sp:load_segments()
    if lerr then
        return nil, lerr
    end

    if #sp.segments == 0 then
        local cerr = sp:create_new_segment(0)
        if cerr then
            return nil, cerr
        end
    else
        sp.active_segment = sp.segments[#sp.segments]
        sp.offset = sp.active_segment.base_offset + sp.active_segment.bytes_written
    end

    -- Spawn the cleaner coroutine and pump it once so it reaches its first
    -- yield and reports when it next wants to run. The owner drives further
    -- ticks via :tick_cleaner() from their main loop.
    sp.cleaner = coroutine.create(function() sp:segment_cleaner_loop() end)
    local cok, next_wake = coroutine.resume(sp.cleaner)
    if not cok then
        return nil, string.format("failed to start cleaner coroutine: %s", next_wake)
    end
    sp.cleaner_next_wake = next_wake

    return sp, nil
end

-- create_new_segment opens a new segment file at base_offset and makes it
-- the active segment. Returns nil on success, err string on failure.
function SegmentedPartition:create_new_segment(base_offset)
    assert(type(base_offset) == "number", "base_offset must be a number")

    local file_name = string.format("%020d.log", base_offset)
    local file_path = fs_m.join_path(self.dir, file_name)

    local file, err = io.open(file_path, "a+b")
    if not file then
        return string.format("failed to create segment file: %s", err)
    end

    -- Time-index sidecar lives next to the .log. We open it "a+b" so
    -- writes still append (POSIX append semantics) but seek/read works
    -- for binary search in offset_for_timestamp later.
    local index_path = fs_m.join_path(self.dir,
        string.format("%020d.timeindex", base_offset))
    local index_file, ierr = io.open(index_path, "a+b")
    if not index_file then
        -- Don't leak the .log handle if the .timeindex open failed.
        pcall(function() file:close() end)
        return string.format("failed to create timeindex file: %s", ierr)
    end

    local now = socket.gettime()
    local segment = LogSegment.new(file, base_offset, now, 0, index_file, nil)
    table.insert(self.segments, segment)
    self.active_segment = segment
    self.offset = base_offset

    -- Durable creation timestamp. Without this, a restart on a system
    -- without LFS would reset every segment's start_time to "now" and
    -- defeat retention entirely.
    write_meta(segment:meta_path(self.dir), now)

    -- Persist the new .log/.timeindex/.meta directory entries. On POSIX the
    -- files aren't crash-durable until the parent dir is fsynced. Non-fatal:
    -- an empty just-created segment is reconstructable, but this keeps a roll
    -- from being lost by a crash immediately after.
    io_sync.sync_dir(self.dir)

    return nil
end

-- load_segments loads existing log segments. Performs crash recovery:
-- segments above the recovery checkpoint (or all segments if the previous
-- shutdown wasn't clean) are scanned with verify_file, which truncates the
-- tail at the first CRC failure.
function SegmentedPartition:load_segments()
    local matches, err = fs_m.glob(self.dir, LOG_FILE_PATTERN)
    if not matches then
        return string.format("failed to glob segment files: %s", err)
    end

    -- Read checkpoint and clean-shutdown flag. The clean flag is only
    -- consumed AFTER recovery succeeds (see end of function) — if we
    -- crashed mid-recovery, the next boot must still see it as unclean.
    local checkpoint = 0
    local cp_path = fs_m.join_path(self.dir, "recovery-checkpoint")
    local cp_file = io.open(cp_path, "rb")
    if cp_file then
        checkpoint = tonumber(cp_file:read("*a") or "0") or 0
        cp_file:close()
    end

    local clean_path     = fs_m.join_path(self.dir, ".clean-shutdown")
    local clean_file     = io.open(clean_path, "rb")
    local clean_shutdown = clean_file ~= nil
    if clean_file then clean_file:close() end

    -- Decorate-sort by numeric base_offset.
    local decorated = {}
    for i = 1, #matches do
        local p = matches[i]
        local n = tonumber(p:match("(%d+)%.log$"))
        if not n then
            return string.format("invalid segment filename: %s", p)
        end
        decorated[i] = { path = p, n = n }
    end
    table.sort(decorated, function(a, b) return a.n < b.n end)

    -- Any error return below aborts the load, and SegmentedPartition.new
    -- discards the half-built object. Without this the file/index handles of
    -- every segment already pushed into self.segments would leak until GC —
    -- which compounds under create_topic's partial-failure retry loop. Route
    -- all error returns through fail() so those handles are closed first.
    local function fail(msg)
        for _, s in ipairs(self.segments) do
            pcall(function() s.file:close() end)
            if s.index_file then pcall(function() s.index_file:close() end) end
        end
        self.segments = {}
        return msg
    end

    local now = socket.gettime()
    for i, entry in ipairs(decorated) do
        local path        = entry.path
        local base_offset = entry.n
        local is_active   = (i == #decorated)

        local sf = io.open(path, "rb")
        if not sf then return fail(string.format("cannot open %s", path)) end
        local on_disk_size = sf:seek("end") or 0
        sf:close()
        local seg_end_virtual = base_offset + on_disk_size

        -- Verification policy:
        --   * Active segment: always verify (it's where a crashing write
        --     would leave a torn record).
        --   * Sealed segments after an unclean shutdown: verify if they
        --     extend past the durable checkpoint.
        --   * Sealed segments after a clean shutdown: trust the file.
        local need_verify
        if is_active then
            need_verify = true
        elseif not clean_shutdown then
            need_verify = (seg_end_virtual > checkpoint)
        else
            need_verify = false
        end

        local post_verify_size = on_disk_size
        if need_verify then
            local start_at = 0
            if checkpoint > base_offset and checkpoint < seg_end_virtual then
                start_at = checkpoint - base_offset
            end
            local last_good, verr = verify_file(path, start_at)
            if verr then
                return fail(string.format("verify %s: %s", path, verr))
            end
            -- verify_file truncates at the first bad frame and returns the
            -- post-truncate byte position. Use it as authoritative size.
            post_verify_size = last_good or on_disk_size
        end

        local file, ferr = io.open(path, "a+b")
        if not file then
            return fail(string.format("failed to open segment file %s: %s",
                                 path, ferr))
        end

        -- start_time: prefer the .meta sidecar (written at create time),
        -- fall back to LFS mtime, fall back to "now" (cleaner won't age
        -- this segment out until the wall clock advances past retention).
        local meta_path = fs_m.join_path(self.dir,
            string.format("%020d.meta", base_offset))
        local start_time = read_meta(meta_path)
        if not start_time and has_lfs then
            local mtime = lfs.attributes(path, "modification")
            if type(mtime) == "number" then start_time = mtime end
        end
        if not start_time then
            start_time = now
        end

        -- Open (or create) the .timeindex sidecar. Missing file is
        -- normal for segments that predate this feature; we just open
        -- a fresh one — the segment will index from its next write.
        --
        -- If the segment was tail-truncated by verify_file, any index
        -- entries pointing past post_verify_size are stale. Walk the
        -- index from the end and ftruncate at the last entry whose
        -- file_pos < post_verify_size. last_indexed_at is set to that
        -- entry's file_pos so we don't double-index.
        local index_path = fs_m.join_path(self.dir,
            string.format("%020d.timeindex", base_offset))
        local index_file, ierr = io.open(index_path, "a+b")
        if not index_file then
            pcall(function() file:close() end)
            return fail(string.format("failed to open timeindex %s: %s",
                                 index_path, ierr))
        end

        local last_indexed_at, tierr = time_index.recover(index_file, post_verify_size)
        if tierr then
            pcall(function() file:close() end)
            pcall(function() index_file:close() end)
            return fail(string.format(
                "failed to truncate stale timeindex %s: %s",
                index_path, tierr))
        end

        table.insert(self.segments,
            LogSegment.new(file, base_offset, start_time, post_verify_size,
                           index_file, last_indexed_at))
    end

    -- Consume the clean-shutdown flag only now that recovery has fully
    -- succeeded. A crash above this line still leaves the flag in place
    -- on the *previous* boot's terms, but a crash here is benign: the
    -- next boot will re-run verify on segments above the checkpoint
    -- (idempotent — they're already truncated).
    if clean_shutdown then
        os.remove(clean_path)
    end

    return nil
end

function SegmentedPartition:_write_checkpoint(value)
    local cp_path = fs_m.join_path(self.dir, "recovery-checkpoint")
    local tmp     = cp_path .. ".tmp"
    local f, ferr = io.open(tmp, "wb")
    if not f then return false, ferr end
    f:write(tostring(value))
    io_sync.sync(f)          -- the temp file's bytes must land before the rename
    f:close()
    local ok, rerr = io_sync.atomic_rename(tmp, cp_path)
    if not ok then return false, rerr end
    -- Persist the rename itself: the dir entry isn't crash-durable until the
    -- directory is fsynced. Non-fatal — a lost checkpoint just makes the next
    -- boot re-scan more of the log.
    io_sync.sync_dir(self.dir)
    return true, nil
end

-- Internal: roll the active segment once it's full. Flushes + fsyncs the
-- segment we're sealing, advances the on-disk checkpoint past it, and
-- opens the next segment at the new base offset.
-- _roll_now seals the active segment at its TRACKED size and opens the next
-- one. Shared by the size-triggered roll and the append-position guard below.
function SegmentedPartition:_roll_now()
    local new_base = self.active_segment.base_offset
                   + self.active_segment.bytes_written

    self.active_segment.file:flush()
    local sok, serr = io_sync.sync(self.active_segment.file)
    if not sok then
        return string.format("failed to fsync before roll: %s", serr)
    end

    -- Flush + fsync the sealing segment's timeindex too. Without this,
    -- a crash right after the roll could lose the last few index
    -- entries for a now-sealed segment — recovery would still work
    -- (offset_for_timestamp's linear-scan tail handles gaps) but a
    -- larger linear-scan window per query is wasteful.
    if self.active_segment.index_file then
        self.active_segment.index_file:flush()
        local iok, ierr = io_sync.sync(self.active_segment.index_file)
        if not iok then
            log:warn("timeindex fsync before roll segment=%020d: %s",
                self.active_segment.base_offset, tostring(ierr))
        end
    end

    self:_write_checkpoint(new_base)

    local cerr = self:create_new_segment(new_base)
    if cerr then
        return string.format("failed to roll segment: %s", cerr)
    end

    -- Counter: one increment per successful roll. Labels by topic so
    -- a hot topic's roll rate is observable independent of others.
    metrics.inc("moonmq_segment_rolls_total", 1,
        { topic = self.topic.name })

    return nil
end

function SegmentedPartition:_roll_if_full(incoming_bytes)
    local current_size = self.active_segment.bytes_written
    if current_size + incoming_bytes <= self.max_segment_size then
        return nil
    end
    -- Allow a single record to overshoot if the segment is empty —
    -- otherwise an oversized message could never be written.
    if current_size == 0 then
        return nil
    end
    return self:_roll_now()
end

-- Position the active segment for an append and verify the physical EOF
-- matches the tracked size. They diverge when a previous write FAILED after
-- partially flushing (ENOSPC/EIO can land a prefix of the record): the
-- garbage sits at physical EOF while bytes_written still points before it.
-- Appending on top would put acked records at physical positions that don't
-- match their advertised offsets — unreadable immediately, and destroyed by
-- recovery's torn-tail truncation. Instead, seal this segment at its tracked
-- size (the garbage past it is invisible to reads and truncated by the next
-- recovery) and roll to a fresh segment.
-- Returns (nil) when ready to append, or an error string.
function SegmentedPartition:_ensure_append_position()
    local pos, perr = self.active_segment.file:seek("end")
    if not pos then
        return string.format("failed to seek segment: %s", tostring(perr))
    end
    if pos == self.active_segment.bytes_written then
        return nil
    end

    log:error("segment %020d: physical EOF %d != tracked %d "
        .. "(previous failed write left partial bytes); sealing and rolling",
        self.active_segment.base_offset, pos, self.active_segment.bytes_written)
    local rerr = self:_roll_now()
    if rerr then return rerr end

    local npos, nerr = self.active_segment.file:seek("end")
    if not npos then
        return string.format("failed to seek fresh segment: %s", tostring(nerr))
    end
    if npos ~= 0 then
        return string.format("fresh segment not empty (%d bytes)", npos)
    end
    return nil
end

-- write_message serializes msg via msg_m.serialize_message and appends it
-- to the active segment, rolling if the segment is full.
-- Returns (global_offset, nil) on success, (nil, err) on failure.
function SegmentedPartition:write_message(msg)
    assert(getmetatable(msg) == msg_m.Message, "msg must be a Message instance")

    local bytes, serr = msg_m.serialize_message(msg)
    if not bytes then
        return nil, string.format("failed to serialize message: %s", serr)
    end

    local rerr = self:_roll_if_full(#bytes)
    if rerr then return nil, rerr end

    -- Position at EOF and verify it matches the tracked size (also satisfies
    -- stdio's read→write positioning requirement on this "a+b" handle; see
    -- _ensure_append_position for the failed-write divergence case).
    local perr = self:_ensure_append_position()
    if perr then return nil, perr end

    -- file_pos_in_segment = bytes_written BEFORE the append. This is
    -- both the seek-target for read_message and the value we record in
    -- the timeindex entry.
    local file_pos_in_segment = self.active_segment.bytes_written
    local global_offset = self.active_segment.base_offset + file_pos_in_segment

    local ok, werr = self.active_segment.file:write(bytes)
    if not ok then
        return nil, string.format("failed to write message: %s", werr)
    end
    self.active_segment.file:flush()

    self.active_segment.bytes_written = self.active_segment.bytes_written + #bytes
    self.offset = self.offset + #bytes

    -- Index this message if it crosses the interval (or is the segment's
    -- first). Done after the log write so we never index a record that
    -- failed to land on disk.
    time_index.maybe_append(self.active_segment, msg.timestamp,
        file_pos_in_segment, self.index_interval_bytes)

    -- log_bytes gauge: total bytes on disk across all segments. Cheap
    -- to maintain incrementally; the alternative is a full segments
    -- walk per scrape. Cardinality: bounded by max_topics * partitions.
    metrics.set("moonmq_partition_log_bytes", self.offset,
        { topic = self.topic.name,
          partition = tostring(self.id) })

    return global_offset, nil
end

-- write appends raw, pre-serialized bytes (used by BatchWriter, which
-- concats many serialized messages into one syscall). Rolls the segment
-- if the incoming bytes won't fit. Returns (true, nil) / (false, err).
function SegmentedPartition:write(data)
    assert(type(data) == "string", "data must be a string")

    local rerr = self:_roll_if_full(#data)
    if rerr then return false, rerr end

    -- Position at EOF, verify against the tracked size (see write_message).
    local perr = self:_ensure_append_position()
    if perr then return false, perr end

    local ok, werr = self.active_segment.file:write(data)
    if not ok then return false, werr end

    self.active_segment.bytes_written = self.active_segment.bytes_written + #data
    self.offset = self.offset + #data

    return true, nil
end

-- sync fsyncs the active segment's file. Replaces direct
-- io_sync.sync(partition.file) calls that the flat Partition exposed.
function SegmentedPartition:sync()
    if not self.active_segment or not self.active_segment.file then
        return false, "no active segment"
    end
    -- Histogram so operators can see fsync tail-latency per topic.
    -- p99 fsync time is the single most useful signal for "is the
    -- disk slow / am I hitting fsync contention" diagnosis.
    local stop = metrics.timer("moonmq_fsync_duration_seconds",
        { topic = self.topic.name })
    local ok, err = io_sync.sync(self.active_segment.file)
    stop()
    return ok, err
end

-- ---------------------------------------------------------------------------
-- Group commit (Kafka-style)
--
-- attach_committer wires this partition to a scheduler so concurrent
-- acks=1 producers can coalesce their fsyncs into one. Without it,
-- request_sync() falls back to an immediate per-call fsync (the
-- original behaviour, kept for unit tests and the synchronous BatchWriter
-- path). The batching/linger machinery itself lives in
-- src/storage/group_committer.lua; see there for the scheduler contract
-- and opts (linger_s, max_waiters).
function SegmentedPartition:attach_committer(scheduler, opts)
    self.committer = GroupCommitter.new(self, scheduler, opts)
end

function SegmentedPartition:detach_committer()
    local c = self.committer
    if not c then return end
    c:drain()
    self.committer = nil
end

-- request_sync is the group-commit-aware entrypoint used by the producer
-- when acks=1. No committer attached → immediate fsync (same as :sync()),
-- keeping the sync test paths working.
function SegmentedPartition:request_sync()
    local c = self.committer
    if not c then return self:sync() end
    return c:request_sync()
end

-- Locate the segment that owns `offset` (base_offset <= offset < base_offset
-- + bytes_written).
--
-- Fast path: a one-slot memo (`_last_seg`) covers the sequential-read
-- hot path — consumers walking a partition step through offsets in one
-- segment until they cross the boundary, so the same segment satisfies
-- many calls in a row.
--
-- Slow path: binary search on `self.segments`. The list is implicitly
-- sorted by `base_offset` (append-only — new segments come in via
-- create_new_segment which always extends, and clean_old_segments
-- preserves order). For partitions with thousands of segments this
-- beats the previous linear scan; for small partitions it costs ~1
-- extra comparison.
--
-- The memo is invalidated whenever `self.segments` shrinks (clean) —
-- additions don't invalidate because a memoized hit on an older
-- segment is still correct.
function SegmentedPartition:_segment_for_offset(offset)
    local memo = self._last_seg
    if memo
       and offset >= memo.base_offset
       and offset < memo.base_offset + memo.bytes_written
    then
        return memo
    end

    local segs = self.segments
    local lo, hi = 1, #segs
    while lo <= hi do
        local mid = (lo + hi) // 2
        local s = segs[mid]
        if offset < s.base_offset then
            hi = mid - 1
        elseif offset >= s.base_offset + s.bytes_written then
            lo = mid + 1
        else
            self._last_seg = s
            return s
        end
    end
    return nil
end

-- oldest_offset: the first offset still readable after retention cleaning —
-- the oldest surviving segment's base. Consumers whose committed offset aged
-- out resume here instead of skipping to the tail (which would silently drop
-- every still-retained record).
function SegmentedPartition:oldest_offset()
    local first = self.segments[1]
    if first then return first.base_offset end
    local active = self.active_segment
    return active and active.base_offset or 0
end

-- read_message: same on-disk format as Partition:read_message — the segment
-- file holds a sequence of `len(8) | header(12) | hdr_crc(4) | payload |
-- payload_crc(4)` records. We just need to find the segment first.
function SegmentedPartition:read_message(offset)
    assert(type(offset) == "number", "offset must be a number")

    local seg = self:_segment_for_offset(offset)
    if not seg then
        return nil, offset, "failed to read message: unexpected EOF"
    end

    local local_pos = offset - seg.base_offset
    local pos, serr = seg.file:seek("set", local_pos)
    if not pos then
        return nil, offset, string.format("failed to seek offset: %s", serr)
    end

    local size_bytes = seg.file:read(8)
    if not size_bytes or #size_bytes < 8 then
        return nil, offset, "failed to read message size: unexpected EOF"
    end
    local total_size = string.unpack(">I8", size_bytes)

    -- Bound total_size against the bytes remaining in this segment BEFORE
    -- allocating the read. offset is a caller-supplied FETCH cursor; if it
    -- lands mid-record the 8 bytes decode to an arbitrary (or negative, via the
    -- u64 high bit) length, and reading it blindly is a memory-exhaustion DoS.
    -- seg.bytes_written is the in-memory segment size, so no extra syscall.
    if total_size < msg_m.MIN_BODY then
        return nil, offset, "corrupt header: total_size too small"
    end
    if total_size > seg.bytes_written - (local_pos + 8) then
        return nil, offset, "corrupt length prefix: exceeds remaining segment bytes"
    end

    local body = seg.file:read(total_size)
    if not body or #body < total_size then
        return nil, offset, "failed to read message body: unexpected EOF"
    end

    -- Decode + CRC-validate through the single authoritative codec so the
    -- on-disk format lives in exactly one place (src/record/message.lua).
    local m, derr = msg_m.decode_body(body)
    if not m then
        return nil, offset, derr
    end
    local next_offset = offset + 8 + total_size

    return m, next_offset, nil
end

-- scan iterates every record in the partition in offset order, calling
-- fn(offset, msg). Byte offsets are dense and contiguous across segments here
-- (no compaction renumbering, unlike the commitlog backend), so this is mostly
-- for a uniform replay interface across backends. Each segment stops at its
-- first torn/corrupt record. Returns nil (no fatal error path today).
function SegmentedPartition:scan(fn)
    assert(type(fn) == "function", "fn must be a function")
    for _, seg in ipairs(self.segments) do
        seg.file:seek("set", 0)
        local pos = 0
        while true do
            local msg, framed = msg_m.deserialize_record(seg.file)
            if not msg then break end   -- EOF or torn/corrupt tail
            fn(seg.base_offset + pos, msg)
            pos = pos + framed
        end
    end
    return nil
end

-- offset_for_timestamp returns the global offset of the EARLIEST message
-- whose timestamp is >= ts, mirroring Kafka's offsetForTimes semantics.
-- Returns nil when the partition is empty or no message satisfies (i.e.
-- ts is past every message's timestamp).
--
-- Strategy: the timeindex is sparse (one entry per index_interval_bytes),
-- so we use it to pick a starting segment + file_pos, then linear-scan
-- forward through records via read_message. The scan window is bounded
-- by index_interval_bytes per query.
function SegmentedPartition:offset_for_timestamp(ts)
    assert(type(ts) == "number", "ts must be a number")
    if #self.segments == 0 then return nil end

    -- Pick the latest segment whose first index entry's ts is <= target.
    -- That's the earliest segment that could still contain ts >= target
    -- (later segments start later in time; earlier ones may overshoot but
    -- the scan below catches that case). If no segment's first entry
    -- qualifies, the target predates everything — start at the partition
    -- head.
    local start_seg     = self.segments[1]
    local start_file_pos = 0
    for i = #self.segments, 1, -1 do
        local s = self.segments[i]
        if s.index_file then
            local n = time_index.count(s.index_file)
            if n > 0 then
                local first_ts = time_index.read_entry(s.index_file, 0)
                if first_ts and first_ts <= ts then
                    start_seg = s
                    start_file_pos = time_index.floor_pos(s.index_file, n, ts) or 0
                    break
                end
            end
        end
    end

    -- Linear scan forward, crossing segment boundaries naturally via
    -- read_message + _segment_for_offset.
    local global = start_seg.base_offset + start_file_pos
    while global < self.offset do
        local m, next_offset, rerr = self:read_message(global)
        if not m then
            -- Mid-record tear or CRC failure — treat as no answer rather
            -- than guessing past corruption.
            return nil
        end
        if m.timestamp >= ts then
            return global
        end
        global = next_offset
    end
    return nil
end

-- clean_old_segments removes segments whose start_time is older than the
-- retention window. The active segment is never removed; the single newest
-- "old" segment is also kept so consumers retain some historical data.
function SegmentedPartition:clean_old_segments()
    if #self.segments <= 1 then
        return
    end

    local cutoff = socket.gettime() - self.retention
    local kept = {}

    for i, segment in ipairs(self.segments) do
        local keep = false

        if segment == self.active_segment then
            keep = true
        elseif segment.start_time > cutoff then
            keep = true
        elseif i < #self.segments and self.segments[i + 1].start_time > cutoff then
            -- Newest "old" segment: the last one whose successor is still
            -- inside the retention window. Keep it so consumers can read
            -- recent-ish history after the sweep. This must fire even when
            -- it's the only old segment (i == 1) — the old `i > 1` guard
            -- wrongly deleted a lone sealed segment.
            keep = true
        end

        if keep then
            table.insert(kept, segment)
        else
            local path  = segment:file_path(self.dir)
            local meta  = segment:meta_path(self.dir)
            local index = segment:index_path(self.dir)
            log:info("removing old segment %s (created at %s)",
                path, os.date("!%Y-%m-%dT%H:%M:%SZ",
                              math.floor(segment.start_time)))
            segment:close()
            os.remove(path)
            os.remove(meta)
            os.remove(index)
            -- Drop the segment-lookup memo if it pointed at this
            -- segment; otherwise next call would return a closed file.
            if self._last_seg == segment then
                self._last_seg = nil
            end
        end
    end

    self.segments = kept
end

function SegmentedPartition:close()
    self:stop_cleaner()

    if self.active_segment then
        self.active_segment.file:flush()
        io_sync.sync(self.active_segment.file)

        -- Flush + fsync the timeindex too. A clean shutdown promises
        -- recovery doesn't re-verify sealed segments — same promise
        -- has to extend to the index, otherwise the next boot reopens
        -- with stale tail entries and lookups skew.
        if self.active_segment.index_file then
            self.active_segment.index_file:flush()
            io_sync.sync(self.active_segment.index_file)
        end

        local leo = self.active_segment.base_offset
                  + self.active_segment.bytes_written
        self:_write_checkpoint(leo)
    end

    for _, seg in ipairs(self.segments) do
        seg:close()
    end

    -- Write the flag last. If we crash before this line, recovery
    -- (correctly) treats the next boot as unclean.
    local clean_path = fs_m.join_path(self.dir, ".clean-shutdown")
    local f = io.open(clean_path, "wb")
    if f then f:write("1"); f:close() end
end

function SegmentedPartition:segment_cleaner_loop()
    while true do
        coroutine.yield(socket.gettime() + self.cleaner_interval)

        local ok, err = pcall(self.clean_old_segments, self)
        if not ok then
            log:error("segment cleaner: %s", err)
        end
    end
end

-- tick_cleaner drives the cleaner coroutine. Call this from your broker's
-- main loop. It's a cheap no-op if the cleaner isn't due to wake yet, so
-- calling it on every loop iteration is fine.
--
-- Returns true if a cleanup pass ran on this call, false otherwise.
function SegmentedPartition:tick_cleaner()
    if not self.cleaner or coroutine.status(self.cleaner) == "dead" then
        return false
    end
    if socket.gettime() < self.cleaner_next_wake then
        return false
    end

    local ok, next_wake = coroutine.resume(self.cleaner)
    if not ok then
        log:error("segment cleaner crashed: %s", next_wake)
        self.cleaner = nil
        return false
    end

    self.cleaner_next_wake = next_wake
    return true
end

-- stop_cleaner halts the cleaner. The coroutine is left for GC.
function SegmentedPartition:stop_cleaner()
    self.cleaner = nil
    self.cleaner_next_wake = nil
end

return {
    LogSegment = LogSegment,
    SegmentedPartition = SegmentedPartition,
}
