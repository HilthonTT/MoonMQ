local fs_m       = require("src.io.fs")
local commitlog  = require("src.commitlog.commitlog")
local message_m  = require("src.record.message")

local DEFAULT_MAX_SEGMENT_BYTES = 64 * 1024 * 1024

local CommitLogPartition = {}
CommitLogPartition.__index = CommitLogPartition

function CommitLogPartition.new(topic, id, dir, opts)
    assert(type(topic) == "table", "topic must be a Topic")
    assert(type(id) == "number", "id must be a number")
    assert(type(dir) == "string", "dir must be a string")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end
    opts = opts or {}

    local partition_dir = fs_m.join_path(dir, string.format("partition-%d", id))

    local options = commitlog.Options.new(
        partition_dir,
        opts.max_segment_size or DEFAULT_MAX_SEGMENT_BYTES,
        opts.max_log_bytes or 0,
        opts.cleanup_policy or "")

    local cl, err = commitlog.CommitLog.new(options)
    if not cl then
        return nil, err
    end

    return setmetatable({
        topic     = topic,
        id        = id,
        commitlog = cl,
        offset    = cl:newest_offset(),
        committer = nil,
    }, CommitLogPartition), nil
end

function CommitLogPartition:write_message(msg)
    assert(getmetatable(msg) == message_m.Message, "msg must be a Message instance")

    local offset, err = self.commitlog:append_message(msg)
    if err then
        return -1, err
    end
    self.offset = self.commitlog:newest_offset()
    return offset, nil
end

function CommitLogPartition:read_message(offset)
    assert(type(offset) == "number", "offset must be a number")

    local msg, next_offset, err = self.commitlog:read_at(offset)
    if not msg then
        if offset < self.commitlog:oldest_offset() then
            return nil, offset,
                string.format("offset %d below oldest retained: unexpected EOF",
                              offset)
        end
        local nxt = self.commitlog:next_readable_offset(offset)
        if nxt and nxt > offset then
            local gmsg, gnext, gerr = self.commitlog:read_at(nxt)
            if gmsg then
                return gmsg, gnext, nil
            end
            err = gerr
        end
        return nil, offset, err
    end
    return msg, next_offset, nil
end

function CommitLogPartition:oldest_offset()
    return self.commitlog:oldest_offset()
end

function CommitLogPartition:offset_for_timestamp(ts)
    assert(type(ts) == "number", "ts must be a number")
    local oldest = self.commitlog:oldest_offset()
    local newest = self.commitlog:newest_offset()
    for off = oldest, newest - 1 do
        local msg = self.commitlog:read_at(off)
        if msg and msg.timestamp >= ts then
            return off
        end
    end
    return nil
end

function CommitLogPartition:scan(fn)
    assert(type(fn) == "function", "fn must be a function")
    self.commitlog:each_message(fn)
    return nil
end

function CommitLogPartition:sync()
    return self.commitlog:sync()
end

function CommitLogPartition:request_sync()
    return self:sync()
end

function CommitLogPartition:attach_committer(scheduler, opts)
    assert(scheduler ~= nil, "scheduler required")
    self.committer = { scheduler = scheduler, opts = opts or {} }
end

function CommitLogPartition:detach_committer()
    self.committer = nil
end

function CommitLogPartition:tick_cleaner()
    return false
end

function CommitLogPartition:close()
    if self.commitlog then
        self.commitlog:close()
    end
end

return CommitLogPartition
