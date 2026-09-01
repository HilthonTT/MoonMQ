local GroupCommitter = {}
GroupCommitter.__index = GroupCommitter

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
        generation  = 0,
    }, GroupCommitter)
end

function GroupCommitter:drain()
    if #self.waiters > 0 then
        self:commit_now()
    end
end

function GroupCommitter:commit_now(skip_co)
    local waiters = self.waiters
    self.waiters = {}
    self.linger_co = nil
    self.generation = self.generation + 1

    local ok, err = self.partition:sync()

    for i = 1, #waiters do
        local w = waiters[i]
        w.ok, w.err = ok, err
        if w.co ~= skip_co then
            self.scheduler:schedule(w.co)
        end
    end
    return ok, err
end

function GroupCommitter:request_sync()
    local co, is_main = coroutine.running()
    if is_main or not co then
        return self.partition:sync()
    end

    local w = { co = co, ok = nil, err = nil }
    self.waiters[#self.waiters + 1] = w

    if #self.waiters >= self.max_waiters then
        self:commit_now(co)
        return w.ok, w.err
    end

    if not self.linger_co then
        local my_gen = self.generation
        local sched = self.scheduler
        self.linger_co = sched:spawn(function()
            sched:sleep(self.linger_s)
            if self.partition.committer ~= self then return end
            if self.generation ~= my_gen then return end
            self:commit_now()
        end)
    end

    coroutine.yield()
    return w.ok, w.err
end

return GroupCommitter
