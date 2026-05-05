local time   = require("src.time")
local fs     = require("src.fs")
local socket = require("socket")
local message = require("src.message")

local Partition = {}
Partition.__index = Partition

function Partition.new(topic, id, topicDir)
    assert(type(topic) == "table", "topic must be a Topic")
    assert(type(id) == "number", "id must be a number")
    assert(type(topicDir) == "string", "topicDir must be a string")

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

function Partition:write_message(msg)
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")

    -- Record te current offset before writing
    local current_offset = self.offset

    -- Calculate header size and total size
    local key_size = #msg.key

    -- Serialize header
    local header = string.pack(">I4I8", key_size, msg.timestamp)

    -- Calculate total message size
    local total_size = #header + #msg.key + #msg.value

    -- Write length prefix
    local size = string.pack(">I8", total_size)
    local ok, err = self.file:write(size)
    if not ok then return -1, ("failed to write message size: %s"):format(err) end

    -- Write header
    ok, err = self.file:write(header)
    if not ok then return -1, ("failed to write message header: %s"):format(err) end

    -- Write key (if present)
    if #msg.key > 0 then
        ok, err = self.file:write(msg.key)
        if not ok then return -1, ("failed to write message key: %s"):format(err) end
    end

    -- Write value
    ok, err = self.file:write(msg.value)
    if not ok then return -1, ("failed to write message value: %s"):format(err) end

    self.offset = self.offset + (8 + total_size)

    local elapsed = socket.gettime() - self.last_sync
    if elapsed >= self.sync_every then
        self.file:flush()
        self.last_sync = socket.gettime()
    end

    return current_offset, nil
end


function Partition:read_message(offset) 
    assert(type(offset) == "number", "offset must be a number")

    local pos, err = self.file:seek("set", offset)
    if not pos then return nil, offset, ("failed to seek to offset: %s"):format(err) end

    local size_bytes, err = self.file:read(8)
    if not size_bytes or err then
        return nil, offset, ("failed to read message size: %s"):format(err or "unexpected EOF")
    end

    local total_size = string.unpack(">I8", size_bytes)

    local header_bytes, err = self.file:read(12)
    if not header_bytes then
        return nil, offset, ("failed to read message header: %s"):format(err or "unexpected EOF")
    end

    local key_size, timestamp = string.unpack(">I4I8", header_bytes)

    local key = ""
    if key_size > 0 then
        key, err = self.file:read(key_size)
        if not key then
            return nil, offset, ("failed to read message key: %s"):format(err or "unexpected EOF")
        end
    end

    local value_size = total_size - 12 - key_size
    local value, err = self.file:read(value_size)
    if not value then
        return nil, offset, ("failed to read message value: %s"):format(err or "unexpected EOF")
    end

    local msg = message.Message.new(key, value, timestamp)
    local next_offset = offset + 8 + total_size

    return msg, next_offset, nil
end

return Partition
