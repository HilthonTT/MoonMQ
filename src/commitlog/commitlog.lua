local compact_cleaner = require("src.commitlog.compact_cleaner")
local delete_cleaner = require("src.commitlog.delete_cleaner")
local fs_m = require("src.io.fs")

local M = {}

local CleanupPolicy = {
    Delete = "delete",
    Compact = "compact",

    LogFileSuffix = ".log",
    IndexFileSuffix = ".index",
}

local Options = {}
Options.__index = Options

function Options.new(path, max_segment_bytes, max_log_bytes, cleanup_policy)
    assert(type(path) == "string", "path must be a string")
    assert(type(cleanup_policy) == "string", "cleanup_policy must be a string")
    assert(type(max_segment_bytes) == "number", "max_segment_bytes must be a number")
    assert(type(max_log_bytes) == "number", "max_log_bytes must be a number")

    local o = setmetatable({
        path = path,
        max_segment_bytes = max_segment_bytes,
        max_log_bytes = max_log_bytes,
        cleanup_policy = cleanup_policy,
    }, Options)

    return o
end

local CommitLog = {}
CommitLog.__index = CommitLog

function CommitLog.new(options)
    assert(getmetatable(options) == Options, "options must be an Options instance")

    if options.path == "" then
        return nil, "path is empty"
    end

    if options.max_segment_bytes == 0 then
        -- TODO: default here
    end

    if options.cleanup_policy == "" then
        options.cleanup_policy = CleanupPolicy.Delete
    end

    local cleaner
    if options.cleanup_policy == CleanupPolicy.Delete then
        cleaner = delete_cleaner.new(options.max_segment_bytes)
    else
        cleaner = compact_cleaner.new()
    end

    local path, perr = fs_m.abs_path(options.path)
    if perr then return nil, perr end

    local l = setmetatable({
        options = options,
        name = fs_m.base(path),
        cleaner = cleaner,
        segments = {},
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
    if ferr or not files then return ferr, ferr.." read dir failed" end

    for _, file in ipairs(files) do
        -- TODO: if this file is an index file, make sure it has a corresponding .log file
    end

    if #self.segments == 0 then
        -- TODO: Create new segment and append it
    end

    return nil
end

function CommitLog:append()
    -- TODO: Implement this
end

function CommitLog:read(p)
    -- TODO: Add mutex here and return n (int) and error (string)
end

function CommitLog:newest_offset()
    -- TODO: Implement this
end

function CommitLog:oldest_offset()
    -- TODO: Implement this
end

function CommitLog:close()
    -- TODO: Implement this
    return nil
end

function CommitLog:delete()
    local ok, err = fs_m.remove_all(self.path)
    if not ok then return err end
    return nil
end

function CommitLog:truncate(offset)
    assert(type(offset) == "number", "offset must be a number")
    --- TODO: Truncate the segments past the offset
    return nil
end

return M
