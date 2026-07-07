-- CommitLogPartition adapts the jocko-style commit log (src/commitlog) to the
-- duck-typed partition interface the rest of the broker speaks, so a topic can
-- be backed by a CommitLog instead of a SegmentedPartition. It is selected per
-- topic via the `backend = "commitlog"` option (see src/storage/topic_manager.lua).
--
-- The interface the producer / consumer / server / broker rely on:
--   .id                          partition id (1-based)
--   .topic                       owning Topic (used for metric labels / replica)
--   .offset                      tail cursor: the offset the next append will get
--   :write_message(msg)          -> (offset, err)
--   :read_message(offset)        -> (msg, next_offset, err)
--   :sync() / :request_sync()    -> (ok, err)
--   :attach_committer/:detach_committer
--   :close()
--   :tick_cleaner()              (no-op; the CommitLog cleans synchronously on roll)
--
-- OFFSET SEMANTICS. SegmentedPartition uses byte offsets; the CommitLog uses
-- message-count offsets. Both are opaque cursors to the consumer, which only
-- ever advances by the next_offset returned from read_message and compares
-- against partition.offset, so the swap is transparent within a single topic's
-- lifetime. Do NOT switch a topic's backend after it has data — the stored
-- offsets are not comparable across backends.

local fs_m       = require("src.io.fs")
-- Require the concrete module (not the src/commitlog/init.lua package entry) so
-- the broker path doesn't depend on the interpreter's "?/init.lua" search path.
local commitlog  = require("src.commitlog.commitlog")
local message_m  = require("src.record.message")

local DEFAULT_MAX_SEGMENT_BYTES = 64 * 1024 * 1024  -- 64 MiB

local CommitLogPartition = {}
CommitLogPartition.__index = CommitLogPartition

-- new opens (or recovers) a commit log under <dir>/partition-<id>/ — the same
-- directory layout SegmentedPartition uses, so Broker:load_topics discovers it
-- by globbing "partition-<id>" dirs without caring about the backend.
--
-- opts (optional, all forwarded from the topic config sidecar):
--   max_segment_size   bytes before a segment rolls   (default 64 MiB)
--   max_log_bytes      retention byte budget; 0/absent -> retain all
--   cleanup_policy     "delete" (default) or "compact"
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
        opts.max_log_bytes or 0,          -- 0 -> "retain all" (Options maps it)
        opts.cleanup_policy or "")        -- "" -> "delete" (Options maps it)

    local cl, err = commitlog.CommitLog.new(options)
    if not cl then
        return nil, err
    end

    return setmetatable({
        topic     = topic,
        id        = id,
        commitlog = cl,
        -- Tail cursor. Mirrors SegmentedPartition.offset so consumer.poll's
        -- `offset < partition.offset` check works unchanged.
        offset    = cl:newest_offset(),
        committer = nil,
    }, CommitLogPartition), nil
end

-- write_message appends one Message and returns (offset, err). Advances the
-- tail cursor on success.
function CommitLogPartition:write_message(msg)
    assert(getmetatable(msg) == message_m.Message, "msg must be a Message instance")

    local offset, err = self.commitlog:append_message(msg)
    if err then
        return -1, err
    end
    self.offset = self.commitlog:newest_offset()
    return offset, nil
end

-- read_message returns (msg, next_offset, err) for `offset`. Offsets that have
-- aged out of the retention window are reported with an "unexpected EOF" error
-- so the consumer's existing skip-to-tail handling kicks in — the same way
-- SegmentedPartition signals a read below its oldest retained byte.
function CommitLogPartition:read_message(offset)
    assert(type(offset) == "number", "offset must be a number")

    local msg, next_offset, err = self.commitlog:read_at(offset)
    if not msg then
        if offset < self.commitlog:oldest_offset() then
            return nil, offset,
                string.format("offset %d below oldest retained: unexpected EOF",
                              offset)
        end
        -- The offset may have landed in a compaction gap (oldest <= offset <
        -- newest, but owned by no segment because compaction renumbered
        -- survivors and left holes between segments). read_at returns a fatal
        -- "out of range" error there, which the consumer would hit on every
        -- poll forever. Skip the gap to the next real record instead.
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

-- offset_for_timestamp returns the offset of the earliest retained message
-- whose timestamp is >= ts, or nil if none qualifies. The CommitLog has no
-- time index (unlike SegmentedPartition), so this is a linear scan from the
-- oldest retained offset — fine for the occasional seek, not a hot path.
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

-- scan iterates every retained record in the partition in offset order, calling
-- fn(offset, msg). Robust to the offset gaps compaction leaves between segments
-- (see CommitLog:each_message), which the offset-arithmetic read_message loop is
-- not. Used for full-log replay such as OffsetManager recovery.
function CommitLogPartition:scan(fn)
    assert(type(fn) == "function", "fn must be a function")
    self.commitlog:each_message(fn)
    return nil
end

-- sync issues an fsync on the active segment. (ok, err).
function CommitLogPartition:sync()
    return self.commitlog:sync()
end

-- request_sync is the acks=1 durability entrypoint. The CommitLog backend does
-- not coalesce concurrent fsyncs the way SegmentedPartition's group committer
-- does; it performs an immediate, correct fsync per call. attach_committer is
-- still honoured for API compatibility (the server attaches one to every
-- partition) but does not change behaviour here.
function CommitLogPartition:request_sync()
    return self:sync()
end

-- attach_committer / detach_committer exist so the server's committer factory
-- (which calls attach_committer on every partition) and Broker:detach_committers
-- work uniformly across backends. We retain the scheduler reference but do not
-- run a linger loop — see request_sync.
function CommitLogPartition:attach_committer(scheduler, opts)
    assert(scheduler ~= nil, "scheduler required")
    self.committer = { scheduler = scheduler, opts = opts or {} }
end

function CommitLogPartition:detach_committer()
    self.committer = nil
end

-- tick_cleaner is a no-op: the CommitLog runs its cleaner synchronously on each
-- segment roll, so there is no background coroutine to pump. Returns false to
-- mean "no cleanup pass ran", matching SegmentedPartition's contract.
function CommitLogPartition:tick_cleaner()
    return false
end

function CommitLogPartition:close()
    if self.commitlog then
        self.commitlog:close()
    end
end

return CommitLogPartition
