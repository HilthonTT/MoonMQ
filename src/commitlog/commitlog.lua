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

local DEFAULT_MAX_SEGMENT_BYTES = 64 * 1024 * 1024

local RETAIN_ALL = -1

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

local CommitLog = {}
CommitLog.__index = CommitLog

function CommitLog.new(options)
    assert(getmetatable(options) == Options, "options must be an Options instance")

    if options.path == "" then
        return nil, "path is empty"
    end

    if options.max_segment_bytes == 0 then
        options.max_segment_bytes = DEFAULT_MAX_SEGMENT_BYTES
    end
    if options.max_log_bytes == 0 then
        options.max_log_bytes = RETAIN_ALL
    end

    local MAX_SEGMENT_BYTES_LIMIT = 0xFFFFFFFF - (64 * 1024 * 1024)
    if options.max_segment_bytes > MAX_SEGMENT_BYTES_LIMIT then
        return nil, string.format(
            "max_segment_bytes %d exceeds limit %d (index positions are u32)",
            options.max_segment_bytes, MAX_SEGMENT_BYTES_LIMIT)
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

function CommitLog:open()
    local files, ferr = fs_m.read_dir(self.path)
    if not files then
        return string.format("read dir failed: %s", tostring(ferr))
    end

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

    table.sort(self.segments, function(a, b)
        return a.base_offset < b.base_offset
    end)

    self.active_segment = self.segments[#self.segments]
    return nil
end

function CommitLog:check_split()
    return self.active_segment:is_full()
end

function CommitLog:split()
    local segment, serr = Segment.new(self.path, self:newest_offset(),
                                      self.options.max_segment_bytes)
    if not segment then return serr end

    local dok, derr = io_sync.sync_dir(self.path)
    if not dok then
        log:warn("dir fsync after segment roll failed: %s", tostring(derr))
    end

    local segments = {}
    for i = 1, #self.segments do segments[i] = self.segments[i] end
    segments[#segments + 1] = segment

    local cleaned, cerr = self.cleaner:clean(segments)
    if cleaned then
        self.segments = cleaned
        self.active_segment = cleaned[#cleaned]
    end
    if cerr then return cerr end
    return nil
end

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

    local ierr = seg.index:write_entry(offset, position)
    if ierr then
        seg:rewind(position, offset)
        return nil, ierr
    end

    return offset, nil
end

function CommitLog:append_message(msg)
    assert(getmetatable(msg) == message_m.Message, "msg must be a Message instance")
    local record, serr = message_m.serialize_message(msg)
    if not record then
        return nil, string.format("failed to serialize message: %s", tostring(serr))
    end
    return self:append(record)
end

function CommitLog:segment_for_offset(offset)
    for i = 1, #self.segments do
        local s = self.segments[i]
        if offset >= s.base_offset and offset < s.next_offset then
            return s
        end
    end
    return nil
end

function CommitLog:next_readable_offset(offset)
    for i = 1, #self.segments do
        local s = self.segments[i]
        if offset < s.next_offset then
            if offset >= s.base_offset then
                return offset
            end
            return s.base_offset
        end
    end
    return nil
end

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

function CommitLog:each_message(fn)
    assert(type(fn) == "function", "fn must be a function")
    for i = 1, #self.segments do
        self.segments[i]:each(function(offset, msg)
            fn(offset, msg)
        end)
    end
end

function CommitLog:newest_offset()
    return self.active_segment.next_offset
end

function CommitLog:oldest_offset()
    return self.segments[1].base_offset
end

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

function CommitLog:delete()
    self:close()
    local ok, err = fs_m.remove_all(self.path)
    if not ok then return err end
    return nil
end

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
