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
    if self.ready then return end
    self.ready = true
    self.value = value

    local waiters = self.waiters
    self.waiters = nil
    for i = 1, #waiters do
        self.scheduler.resume(waiters[i], value)
    end
end

function Future:await()
    if self.ready then
        return self.value
    end

    local co = assert(coroutine.running(), "await must run inside a coroutine")
    if not self.waiters then
        return self.value
    end
    self.waiters[#self.waiters+1] = co
    return coroutine.yield()
end

function Future:is_ready() return self.ready end

return Future
