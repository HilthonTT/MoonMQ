local time   = require("src.time")
local fs     = require("src.fs")
local socket = require("socket")

local Partition = {}
Partition.__index = Partition

function Partition.new(topic, id, topicDir)
    local logFileName = ("partition-%d.log"):format(id)
    local filePath    = fs.join_path(topicDir, logFileName)

    local f, err = io.open(filePath, "a+b")
    if not f then return nil, err end

    local size = f:seek("end")

    return setmetatable({
        topic      = topic,
        id         = id,
        file       = f,
        offset     = size,
        sync_every = 50 * time.MILLISECOND,
        last_sync  = socket.gettime(),
        _coroutine = nil,
        running    = false,
    }, Partition), nil
end

function Partition:write(data)
    assert(type(data) == "string", "data must be a string")
    if not self.file then return false, "no file open" end

    self.file:write(data)
    self.offset = self.offset + #data
    return true, nil
end

function Partition:sync()
    if self.file then
        self.file:flush()
    end
    self.last_sync = socket.gettime()
end

function Partition:sync_loop()
    self.running    = true
    self._coroutine = coroutine.create(function()
        while self.running do
            socket.sleep(self.sync_every)

            local elapsed = socket.gettime() - self.last_sync
            if elapsed >= self.sync_every then
                self:sync()
            end

            coroutine.yield()
        end
    end)
end

function Partition:tick()
    if self._coroutine and self.running then
        local ok, err = coroutine.resume(self._coroutine)
        if not ok then
            print("sync_loop error:", err)
            self.running = false
        end
    end
end

function Partition:close()
    self.running = false
    if self.file then
        self.file:flush()
        self.file:close()
        self.file = nil
    end
end

return Partition
