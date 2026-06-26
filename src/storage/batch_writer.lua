local msg_m = require("src.record.message")
local seg_m = require("src.storage.segmentation")

local BatchWriter = {}
BatchWriter.__index = BatchWriter

function BatchWriter.new(partition, max_size, max_messages)
    assert(getmetatable(partition) == seg_m.SegmentedPartition,
        "partition must be a SegmentedPartition instance")
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

-- Add a message to the batch. Returns (true, nil) on success, (nil, err) on failure.
function BatchWriter:add(msg)
    assert(getmetatable(msg) == msg_m.Message, "msg must be a Message instance")

    local msg_bytes = msg_m.serialize_message(msg)

    -- Flush BEFORE adding when adding would push us over either cap;
    -- this preserves the requested cap as a hard upper bound on batch
    -- size (rather than an "approximate after the fact" bound).
    if self.buffer_size + #msg_bytes > self.max_size or self.count >= self.max_messages then
        local fok, ferr = self:flush()
        if not fok then return nil, ferr end
    end

    self.buffer[#self.buffer + 1] = msg_bytes
    self.buffer_size = self.buffer_size + #msg_bytes
    self.count = self.count + 1

    return true, nil
end

-- Flush writes the batch to disk and forces an fsync so the durability
-- guarantee actually holds. Returns (true, nil) on success, (nil, err) on failure.
function BatchWriter:flush()
    if #self.buffer == 0 then
        return true, nil
    end

    local data = table.concat(self.buffer)
    local ok, err = self.partition:write(data)
    if not ok then return nil, err end

    -- Push userspace + OS buffers to disk. Without this, BatchWriter
    -- claims to have committed N messages while they linger in the
    -- C runtime buffer until the next process exit.
    local sok, serr = self.partition:sync()
    if not sok then return nil, serr end

    self.buffer = {}
    self.buffer_size = 0
    self.count = 0

    return true, nil
end

return BatchWriter
