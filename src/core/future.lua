-- A one-shot promise/future. Caller awaits from inside a coroutine;
-- producer resolves once with a value that gets delivered to all waiters.

local Future = {}
Future.__index = Future

function Future.new(scheduler)
    return setmetatable({
        scheduler = scheduler,
        ready     = false,
        value     = nil,
        waiters   = {},
    }, Future)
end

function Future:resolve(value)
    if self.ready then return end          -- one-shot; ignore re-resolve
    self.ready = true
    self.value = value

    local waiters = self.waiters
    self.waiters = nil                     -- drop refs so coroutines can GC
    for i = 1, #waiters do
        self.scheduler.resume(waiters[i], value)
    end
end

-- Must be called from a coroutine. Returns the resolved value.
function Future:await()
    if self.ready then
        return self.value
    end

    local co = assert(coroutine.running(), "await must run inside a coroutine")
    -- waiters can be nil if resolve() ran between the ready check above
    -- and this point. Single-threaded coroutines can't actually hit this,
    -- but guarding here is cheap and keeps the invariant local.
    if not self.waiters then
        return self.value
    end
    self.waiters[#self.waiters+1] = co
    return coroutine.yield()
end

function Future:is_ready() return self.ready end

return Future
