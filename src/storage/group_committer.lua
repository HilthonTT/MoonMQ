-- Group commit (Kafka-style): coalesces concurrent acks=1 fsyncs into one.
--
-- A GroupCommitter is attached to a partition by
-- SegmentedPartition:attach_committer. Concurrent producers calling
-- request_sync within one linger window park on a shared batch; a single
-- fsync commits all of them. This module is pure scheduling — it knows
-- nothing about the on-disk format and only ever touches the partition
-- through partition:sync() (and partition.committer, to detect a shutdown
-- race in the linger coroutine).
--
-- scheduler is duck-typed; it must provide:
--   :spawn(fn)     — start fn in a new coroutine, return the coroutine
--   :sleep(s)      — yield the current coroutine for s seconds
--   :schedule(co)  — re-queue a parked coroutine for resumption (no args)
--
-- The Reactor in src/server/reactor.lua satisfies this interface as-is.

local GroupCommitter = {}
GroupCommitter.__index = GroupCommitter

-- opts (optional):
--   linger_s     max wait before fsync if batch isn't full (default 0.002)
--   max_waiters  size cap — batch is committed immediately when reached
--                regardless of linger (default 64)
function GroupCommitter.new(partition, scheduler, opts)
    assert(scheduler ~= nil, "scheduler required")
    opts = opts or {}
    return setmetatable({
        partition   = partition,
        scheduler   = scheduler,
        linger_s    = opts.linger_s    or 0.002,
        max_waiters = opts.max_waiters or 64,
        waiters     = {},
        linger_co   = nil,
        -- Generation counter: every commit bumps it. A stale linger
        -- coroutine that wakes after its batch was force-committed sees
        -- the mismatch and exits silently. Without this, a force-commit
        -- followed by a no-op committer wake would issue an extra fsync.
        generation  = 0,
    }, GroupCommitter)
end

-- Drain any in-flight waiters synchronously. Their parked coroutines
-- will be rescheduled with their result cell populated — even if the
-- reactor is shutting down and never resumes them, the data is on
-- disk so we haven't lost durability, only the ACK.
function GroupCommitter:drain()
    if #self.waiters > 0 then
        self:commit_now()
    end
end

-- Do one fsync, populate every parked waiter's result cell, and
-- reschedule every waiter EXCEPT skip_co (single-threaded reactor →
-- no lock needed; nothing else can interleave between self.waiters and
-- the fsync).
--
-- skip_co exists for the request_sync fast path: when a caller's own
-- arrival fills the batch, it calls commit_now inline without yielding.
-- Its coroutine is in self.waiters (so it gets ok/err set) but must NOT be
-- rescheduled — it never parked, and resume() on a still-running or
-- already-returned coroutine errors with "cannot resume dead coroutine".
function GroupCommitter:commit_now(skip_co)
    local waiters = self.waiters
    self.waiters = {}
    self.linger_co = nil
    self.generation = self.generation + 1

    local ok, err = self.partition:sync()

    for i = 1, #waiters do
        local w = waiters[i]
        -- Per-waiter result cells. Sharing a single last_ok/last_err
        -- would race: a waker that yields after reading the shared value
        -- could see a NEXT batch's result by the time it resumes.
        w.ok, w.err = ok, err
        if w.co ~= skip_co then
            self.scheduler:schedule(w.co)
        end
    end
    return ok, err
end

-- The group-commit-aware sync entrypoint used by the producer when acks=1.
-- Behaviour:
--   * Called from the main thread → immediate fsync (same as
--     partition:sync()). Keeps the sync test paths working.
--   * Inside a coroutine → register as a waiter; the first waiter spawns
--     a "linger" coroutine that fsyncs after linger_s. Hitting max_waiters
--     short-circuits the linger and fsyncs immediately. The caller yields
--     and is resumed with (ok, err) of that batch's fsync.
function GroupCommitter:request_sync()
    local co, is_main = coroutine.running()
    if is_main or not co then
        return self.partition:sync()
    end

    local w = { co = co, ok = nil, err = nil }
    self.waiters[#self.waiters + 1] = w

    -- Fast path: if this waiter just filled the batch, commit
    -- synchronously. The current coroutine is mid-execution and is in
    -- the waiter list; commit_now will populate w.ok/w.err before
    -- returning, so we don't need to yield.
    if #self.waiters >= self.max_waiters then
        self:commit_now(co)
        return w.ok, w.err
    end

    -- First waiter arms the linger. Subsequent waiters in the same
    -- window just join the list — a single linger fires for all of them.
    if not self.linger_co then
        local my_gen = self.generation
        local sched = self.scheduler
        self.linger_co = sched:spawn(function()
            sched:sleep(self.linger_s)
            -- Skip stale wake-ups:
            --   * generation advanced  → batch already committed via the
            --     max_waiters fast path.
            --   * committer detached/replaced → shutdown raced us.
            if self.partition.committer ~= self then return end
            if self.generation ~= my_gen then return end
            self:commit_now()
        end)
    end

    coroutine.yield()
    return w.ok, w.err
end

return GroupCommitter
