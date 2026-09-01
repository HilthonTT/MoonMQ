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
local ok_m, metrics = pcall(require, "src.metrics")
if not ok_m then
    metrics = {
        inc     = function(...) end,
        set     = function(...) end,
        observe = function(...) end,
        timer   = function(...) return function() end end,
    }
end

local has_lfs, lfs = pcall(require, "lfs")

local LogSegment = {}
LogSegment.__index = LogSegment

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

local DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 1024
local DEFAULT_RETENTION        = 7 * 24 * time_m.HOUR
local DEFAULT_CLEANER_INTERVAL = 1 * time_m.HOUR

local DEFAULT_INDEX_INTERVAL_BYTES = 4096

local LOG_FILE_PATTERN = "^%d+%.log$"

local function read_meta(meta_path)
    local f = io.open(meta_path, "rb")
    if not f then return nil end
    local body = f:read("*a") or ""
    f:close()
    return tonumber(body)
end

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

function SegmentedPartition.new(topic, id, dir, opts)
    assert(getmetatable(topic) == topic_m, "topic must be a Topic instance")
    assert(type(id)  == "number", "id must be a number")
    assert(type(dir) == "string", "dir must be a string")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end
    opts = opts or {}

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
        offset               = 0,
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

    sp.cleaner = coroutine.create(function() sp:segment_cleaner_loop() end)
    local cok, next_wake = coroutine.resume(sp.cleaner)
    if not cok then
        return nil, string.format("failed to start cleaner coroutine: %s", next_wake)
    end
    sp.cleaner_next_wake = next_wake

    return sp, nil
end

function SegmentedPartition:create_new_segment(base_offset)
    assert(type(base_offset) == "number", "base_offset must be a number")

    local file_name = string.format("%020d.log", base_offset)
    local file_path = fs_m.join_path(self.dir, file_name)

    local file, err = io.open(file_path, "a+b")
    if not file then
        return string.format("failed to create segment file: %s", err)
    end

    local index_path = fs_m.join_path(self.dir,
        string.format("%020d.timeindex", base_offset))
    local index_file, ierr = io.open(index_path, "a+b")
    if not index_file then
        pcall(function() file:close() end)
        return string.format("failed to create timeindex file: %s", ierr)
    end

    local now = socket.gettime()
    local segment = LogSegment.new(file, base_offset, now, 0, index_file, nil)
    table.insert(self.segments, segment)
    self.active_segment = segment
    self.offset = base_offset

    write_meta(segment:meta_path(self.dir), now)

    io_sync.sync_dir(self.dir)

    return nil
end

function SegmentedPartition:load_segments()
    local matches, err = fs_m.glob(self.dir, LOG_FILE_PATTERN)
    if not matches then
        return string.format("failed to glob segment files: %s", err)
    end

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
            post_verify_size = last_good or on_disk_size
        end

        local file, ferr = io.open(path, "a+b")
        if not file then
            return fail(string.format("failed to open segment file %s: %s",
                                 path, ferr))
        end

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
    io_sync.sync(f)
    f:close()
    local ok, rerr = io_sync.atomic_rename(tmp, cp_path)
    if not ok then return false, rerr end
    io_sync.sync_dir(self.dir)
    return true, nil
end

function SegmentedPartition:_roll_now()
    local new_base = self.active_segment.base_offset
                   + self.active_segment.bytes_written

    self.active_segment.file:flush()
    local sok, serr = io_sync.sync(self.active_segment.file)
    if not sok then
        return string.format("failed to fsync before roll: %s", serr)
    end

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

    metrics.inc("moonmq_segment_rolls_total", 1,
        { topic = self.topic.name })

    return nil
end

function SegmentedPartition:_roll_if_full(incoming_bytes)
    local current_size = self.active_segment.bytes_written
    if current_size + incoming_bytes <= self.max_segment_size then
        return nil
    end
    if current_size == 0 then
        return nil
    end
    return self:_roll_now()
end

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

function SegmentedPartition:write_message(msg)
    assert(getmetatable(msg) == msg_m.Message, "msg must be a Message instance")

    local bytes, serr = msg_m.serialize_message(msg)
    if not bytes then
        return nil, string.format("failed to serialize message: %s", serr)
    end

    local rerr = self:_roll_if_full(#bytes)
    if rerr then return nil, rerr end

    local perr = self:_ensure_append_position()
    if perr then return nil, perr end

    local file_pos_in_segment = self.active_segment.bytes_written
    local global_offset = self.active_segment.base_offset + file_pos_in_segment

    local ok, werr = self.active_segment.file:write(bytes)
    if not ok then
        return nil, string.format("failed to write message: %s", werr)
    end
    self.active_segment.file:flush()

    self.active_segment.bytes_written = self.active_segment.bytes_written + #bytes
    self.offset = self.offset + #bytes

    time_index.maybe_append(self.active_segment, msg.timestamp,
        file_pos_in_segment, self.index_interval_bytes)

    metrics.set("moonmq_partition_log_bytes", self.offset,
        { topic = self.topic.name,
          partition = tostring(self.id) })

    return global_offset, nil
end

function SegmentedPartition:write(data)
    assert(type(data) == "string", "data must be a string")

    local rerr = self:_roll_if_full(#data)
    if rerr then return false, rerr end

    local perr = self:_ensure_append_position()
    if perr then return false, perr end

    local ok, werr = self.active_segment.file:write(data)
    if not ok then return false, werr end

    self.active_segment.bytes_written = self.active_segment.bytes_written + #data
    self.offset = self.offset + #data

    return true, nil
end

function SegmentedPartition:sync()
    if not self.active_segment or not self.active_segment.file then
        return false, "no active segment"
    end
    local stop = metrics.timer("moonmq_fsync_duration_seconds",
        { topic = self.topic.name })
    local ok, err = io_sync.sync(self.active_segment.file)
    stop()
    return ok, err
end

function SegmentedPartition:attach_committer(scheduler, opts)
    self.committer = GroupCommitter.new(self, scheduler, opts)
end

function SegmentedPartition:detach_committer()
    local c = self.committer
    if not c then return end
    c:drain()
    self.committer = nil
end

function SegmentedPartition:request_sync()
    local c = self.committer
    if not c then return self:sync() end
    return c:request_sync()
end

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

function SegmentedPartition:oldest_offset()
    local first = self.segments[1]
    if first then return first.base_offset end
    local active = self.active_segment
    return active and active.base_offset or 0
end

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

    local m, derr = msg_m.decode_body(body)
    if not m then
        return nil, offset, derr
    end
    local next_offset = offset + 8 + total_size

    return m, next_offset, nil
end

function SegmentedPartition:scan(fn)
    assert(type(fn) == "function", "fn must be a function")
    for _, seg in ipairs(self.segments) do
        seg.file:seek("set", 0)
        local pos = 0
        while true do
            local msg, framed = msg_m.deserialize_record(seg.file)
            if not msg then break end
            fn(seg.base_offset + pos, msg)
            pos = pos + framed
        end
    end
    return nil
end

function SegmentedPartition:offset_for_timestamp(ts)
    assert(type(ts) == "number", "ts must be a number")
    if #self.segments == 0 then return nil end

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

    local global = start_seg.base_offset + start_file_pos
    while global < self.offset do
        local m, next_offset, rerr = self:read_message(global)
        if not m then
            return nil
        end
        if m.timestamp >= ts then
            return global
        end
        global = next_offset
    end
    return nil
end

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

function SegmentedPartition:stop_cleaner()
    self.cleaner = nil
    self.cleaner_next_wake = nil
end

return {
    LogSegment = LogSegment,
    SegmentedPartition = SegmentedPartition,
}
