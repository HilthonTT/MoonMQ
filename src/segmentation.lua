local topic_m = require("src.topic")
local time_m = require("src.time")
local fs_m = require("src.fs")
local msg_m = require("src.message")
local socket = require("socket")

-- Optional: luafilesystem for accurate per-segment mtime. Without it,
-- retention falls back to load-time (legacy behavior).
local has_lfs, lfs = pcall(require, "lfs")

local LogSegment = {}
LogSegment.__index = LogSegment

function LogSegment.new(file, base_offset, start_time)
    assert(type(base_offset) == "number", "base_offset must be a number")
    assert(type(start_time) == "number", "start_time must be a number")

    return setmetatable({
        file = file,
        base_offset = base_offset,
        start_time = start_time,
    }, LogSegment)
end

function LogSegment:size()
    local pos  = self.file:seek()
    local size = self.file:seek("end")
    self.file:seek("set", pos)
    return size
end

function LogSegment:close()
    if self.file then
        self.file:close()
        self.file = nil
    end
end

function LogSegment:file_path(dir)
    return fs_m.join_path(dir, string.format("%020d.log", self.base_offset))
end

local SegmentedPartition = {}
SegmentedPartition.__index = SegmentedPartition

local DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 1024  -- 1 GiB
local DEFAULT_RETENTION        = 7 * 24 * time_m.HOUR
local DEFAULT_CLEANER_INTERVAL = 1 * time_m.HOUR

-- Anchored Lua pattern (NOT a shell glob). fs_m.glob takes a Lua pattern.
local LOG_FILE_PATTERN = "^%d+%.log$"

function SegmentedPartition.new(topic, id, dir)
    assert(getmetatable(topic) == topic_m, "topic must be a Topic instance")
    assert(type(id)  == "number", "id must be a number")
    assert(type(dir) == "string", "dir must be a string")

    local partition_dir = fs_m.join_path(dir, string.format("partition-%d", id))
    local ok, err = fs_m.mkdir(partition_dir)
    if not ok then
        return nil, err
    end

    local sp = setmetatable({
        topic            = topic,
        id               = id,
        dir              = partition_dir,
        max_segment_size = DEFAULT_MAX_SEGMENT_SIZE,
        retention        = DEFAULT_RETENTION,
        cleaner_interval = DEFAULT_CLEANER_INTERVAL,
        segments         = {},
        active_segment   = nil,
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

    local segment = LogSegment.new(file, base_offset, socket.gettime())
    table.insert(self.segments, segment)
    self.active_segment = segment

    return nil
end

-- load_segments loads existing log segments
function SegmentedPartition:load_segments()
    local matches, err = fs_m.glob(self.dir, LOG_FILE_PATTERN)
    if not matches then
        return string.format("failed to glob segment files: %s", err)
    end

    -- Decorate-sort-undecorate: parse the offset once per entry instead
    -- of twice per comparison (O(n log n) string.match calls → O(n)).
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

    local now = socket.gettime()
    for _, entry in ipairs(decorated) do
        local path        = entry.path
        local base_offset = entry.n

        local file, ferr = io.open(path, "a+b")
        if not file then
            return string.format("failed to open segment file %s: %s", path, ferr)
        end

        -- Prefer real mtime when luafilesystem is available so retention
        -- survives a restart. Without lfs we approximate as "now" — the
        -- legacy behavior — which means retention is reset on reload.
        local start_time = now
        if has_lfs then
            local mtime = lfs.attributes(path, "modification")
            if type(mtime) == "number" then start_time = mtime end
        end

        local segment = LogSegment.new(file, base_offset, start_time)
        table.insert(self.segments, segment)
    end

    return nil
end

-- write_message serializes msg via msg_m.serialize_message and appends it
-- to the active segment, rolling if the segment is full.
-- Returns (global_offset, nil) on success, (nil, err) on failure.
function SegmentedPartition:write_message(msg)
    assert(getmetatable(msg) == msg_m.Message, "msg must be a Message instance")

    local current_size = self.active_segment:size()

    -- Roll if the active segment is full.
    if current_size >= self.max_segment_size then
        local new_base = self.active_segment.base_offset + current_size
        local cerr = self:create_new_segment(new_base)
        if cerr then
            return nil, string.format("failed to roll segment: %s", cerr)
        end
        -- The Go original reuses the stale `info.Size()` here, which
        -- double-counts the old segment's size into the next global
        -- offset. After a roll, the new segment is empty.
        current_size = 0
    end

    local global_offset = self.active_segment.base_offset + current_size

    local bytes, serr = msg_m.serialize_message(msg)
    if not bytes then
        return nil, string.format("failed to serialize message: %s", serr)
    end

    local ok, werr = self.active_segment.file:write(bytes)
    if not ok then
        return nil, string.format("failed to write message: %s", werr)
    end

    -- Push to the OS buffer. Not fsync — the Go version doesn't fsync
    -- either. If durability matters, do it at a higher layer.
    self.active_segment.file:flush()

    return global_offset, nil
end

-- clean_old_segments removes segments whose start_time is older than the
-- retention window. The active segment is never removed; the single newest
-- "old" segment is also kept so consumers retain some historical data.
--
-- Call this periodically from your own loop (no goroutine equivalent).
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
        elseif i > 1
            and self.segments[i - 1].start_time < cutoff
            and (i == #self.segments or self.segments[i + 1].start_time > cutoff)
        then
            -- Boundary segment: last "old" one before any "new" ones. Keep
            -- it so consumers can still read recent-ish history after sweep.
            keep = true
        end

        if keep then
            table.insert(kept, segment)
        else
            local path = segment:file_path(self.dir)
            print(string.format("Removing old segment %s (created at %s)",
                path, os.date("!%Y-%m-%dT%H:%M:%SZ",
                              math.floor(segment.start_time))))
            segment:close()
            os.remove(path)
        end
    end

    self.segments = kept
end

function SegmentedPartition:segment_cleaner_loop()
    while true do
        coroutine.yield(socket.gettime() + self.cleaner_interval)

        local ok, err = pcall(self.clean_old_segments, self)
        if not ok then
            print(string.format("segment cleaner: %s", err))
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
        print(string.format("segment cleaner crashed: %s", next_wake))
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
