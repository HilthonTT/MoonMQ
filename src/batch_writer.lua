local message = require("src.message")
local prt = require("src.partition")

local BatchWriter = {}
BatchWriter.__index = BatchWriter

function BatchWriter.new(partition, max_size, max_messages)
    assert(getmetatable(partition) == prt.Partition, "partition must be a Partition instance")
    assert(type(max_size) == "number", "max_size must be a number")
    assert(type(max_messages) == "number", "max_messages must be a number")

    return setmetatable({
        partition = partition,
        buffer = {},
        buffer_size = 0,
        max_size = max_size,
        max_messages = max_messages,
        count = 0,
    }, BatchWriter)
end

-- Add a message to the batch
function BatchWriter:add(msg)
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")

    -- Serialize message
    local msg_bytes, err = message.serialize_message(msg)
    if not msg_bytes then return err end

    -- Check if we could exceed batch size
    if self.buffer_size + #msg_bytes > self.max_size or self.count >= self.max_messages then
        local err = self:flush()
        if err then return err end
    end

    self.buffer[#self.buffer + 1] = msg_bytes
    self.buffer_size = self.buffer_size + #msg_bytes
    self.count = self.count + 1

    return nil
end

-- Flush writes the batch to disk
function BatchWriter:flush()
    if #self.buffer == 0 then
        return nil
    end

    -- Write buffer to disk
    local data = table.concat(self.buffer)
    local ok, err = self.partition:write(data)
    if not ok then return err end

    -- Reset buffer
    self.buffer = {}
    self.buffer_size = 0
    self.count = 0

    return nil
end

return BatchWriter
