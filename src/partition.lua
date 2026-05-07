local time   = require("src.time")
local fs     = require("src.fs")
local socket = require("socket")
local message = require("src.message")
local crc32 = require("src.crc32")

local Partition = {}
Partition.__index = Partition

function Partition.new(topic, id, topic_dir)
    assert(type(topic) == "table", "topic must be a Topic")
    assert(type(id) == "number", "id must be a number")
    assert(type(topic_dir) == "string", "topic_dir must be a string")

    local log_file_name = ("partition-%d.log"):format(id)
    local file_path     = fs.join_path(topic_dir, log_file_name)

    local f, err = io.open(file_path, "a+b")
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

    local ok, err = self.file:write(data)
    if not ok then return false, err end

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

function Partition:write_message_with_integrity(msg)
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")

    local current_offset = self.offset

    local header  = string.pack(">I4I8", #msg.key, msg.timestamp)
    local payload = msg.key .. msg.value

    local header_crc  = crc32(header)
    local payload_crc = crc32(payload)

    -- total_size excludes the 8-byte length prefix itself
    local total_size = #header + 4 + #payload + 4

    -- Pack the whole record and write in one syscall. Keeps the record
    -- atomic against signal-interrupted writes and means a single
    -- file.flush() can't catch us mid-record.
    local record = string.pack(">I8", total_size)
                .. header
                .. string.pack(">I4", header_crc)
                .. payload
                .. string.pack(">I4", payload_crc)

    local ok, err = self.file:write(record)
    if not ok then
        return -1, ("failed to write message: %s"):format(err)
    end

    self.offset = self.offset + 8 + total_size

    local elapsed = socket.gettime() - self.last_sync
    if elapsed >= self.sync_every then
        self.file:flush()
        self.last_sync = socket.gettime()
    end

    return current_offset, nil
end

function Partition:read_message_with_integrity(offset)
    assert(type(offset) == "number", "offset must be a number")

    local pos, serr = self.file:seek("set", offset)
    if not pos then
        return nil, offset, ("failed to seek offset: %s"):format(serr)
    end

    local size_bytes = self.file:read(8)
    if not size_bytes or #size_bytes < 8 then
        return nil, offset, "failed to read message size: unexpected EOF"
    end
    local total_size = string.unpack(">I8", size_bytes)

    -- Pull the whole record body in one read; slicing strings is much
    -- cheaper than four seeks/reads, and we need the bytes anyway to CRC.
    local body = self.file:read(total_size)
    if not body or #body < total_size then
        return nil, offset, "failed to read message body: unexpected EOF"
    end

    local HEADER_LEN = 12
    local header_bytes       = body:sub(1, HEADER_LEN)
    local stored_header_crc  = string.unpack(">I4", body, HEADER_LEN + 1)
    local payload_start      = HEADER_LEN + 4 + 1
    local payload_end        = #body - 4
    local payload            = body:sub(payload_start, payload_end)
    local stored_payload_crc = string.unpack(">I4", body, payload_end + 1)

    if crc32(header_bytes) ~= stored_header_crc then
        return nil, offset, "header checksum mismatch"
    end
    if crc32(payload) ~= stored_payload_crc then
        return nil, offset, "payload checksum mismatch"
    end

    local key_size, timestamp = string.unpack(">I4I8", header_bytes)
    local key   = payload:sub(1, key_size)
    local value = payload:sub(key_size + 1)

    local msg         = message.Message.new(key, value, timestamp)
    local next_offset = offset + 8 + total_size

    return msg, next_offset, nil
end

local PartitionWriter = {}
PartitionWriter.__index = PartitionWriter

local DEFAULT_MAX_SIZE     = 1 * 1024 * 1024   -- 1 MiB
local DEFAULT_MAX_MESSAGES = 1000
local DEFAULT_FLUSH_EVERY  = 10 * time.MILLISECOND

function PartitionWriter.new(partition, opts)
    assert(getmetatable(partition) == Partition, "partition must be a Partition instance")
    opts = opts or {}

    local self = setmetatable({}, PartitionWriter)
    self.partition    = partition
    self.queue        = {}
    self.head         = 1
    self.tail         = 0
    self.suspended    = nil
    self.running      = false
    self.max_size     = opts.max_size     or DEFAULT_MAX_SIZE
    self.max_messages = opts.max_messages or DEFAULT_MAX_MESSAGES
    self.flush_every  = opts.flush_every  or DEFAULT_FLUSH_EVERY
    self.batch        = {}            -- accumulated serialized message bytes
    self.batch_size   = 0
    self.waiters      = {}            -- list of { co = co, offset = predicted_offset }
    self.last_flush   = socket.gettime()
    return self
end

-- Flush the in-memory batch to the partition file and resume all waiters
-- with their predicted offsets (or the write error).
function PartitionWriter:_flush_batch(scheduler)
    if #self.batch == 0 then
        self.last_flush = socket.gettime()
        return
    end

    local data    = table.concat(self.batch)
    local ok, err = self.partition:write(data)

    local waiters   = self.waiters
    self.batch      = {}
    self.batch_size = 0
    self.waiters    = {}
    self.last_flush = socket.gettime()

    if ok then
        for i = 1, #waiters do
            scheduler.resume(waiters[i].co, waiters[i].offset, nil)
        end
    else
        for i = 1, #waiters do
            scheduler.resume(waiters[i].co, nil, err)
        end
    end
end

-- Run on your scheduler as a long-lived coroutine.
function PartitionWriter:run(scheduler)
    self.running = true
    while self.running do
        -- Drain the request queue into the batch.
        while self.head <= self.tail do
            local req = self.queue[self.head]
            self.queue[self.head] = nil
            self.head = self.head + 1

            local msg_bytes, serr = message.serialize_message(req.msg)
            if not msg_bytes then
                scheduler.resume(req.co, nil, serr)
            else
                if self.batch_size + #msg_bytes > self.max_size
                   or #self.batch >= self.max_messages then
                    self:_flush_batch(scheduler)
                end

                local predicted_offset = self.partition.offset + self.batch_size
                self.batch[#self.batch + 1]     = msg_bytes
                self.batch_size                 = self.batch_size + #msg_bytes
                self.waiters[#self.waiters + 1] = { co = req.co, offset = predicted_offset }
            end
        end

        -- Time-based flush: matches the article's 10ms ticker so messages
        -- don't linger in the buffer indefinitely.
        if self.batch_size > 0
           and (socket.gettime() - self.last_flush) >= self.flush_every then
            self:_flush_batch(scheduler)
        end

        -- Yield until a submit/tick/stop wakes us.
        if self.running and self.head > self.tail then
            self.suspended = coroutine.running()
            coroutine.yield()
        end
    end

    -- Graceful shutdown: flush whatever is buffered, then cancel any
    -- requests that arrived after we stopped accepting work.
    self:_flush_batch(scheduler)
    while self.head <= self.tail do
        local req = self.queue[self.head]
        self.queue[self.head] = nil
        self.head = self.head + 1
        scheduler.resume(req.co, nil, "writer stopped")
    end
end

-- Called from a caller coroutine. Returns offset, err.
function PartitionWriter:submit(scheduler, msg)
    local co = assert(coroutine.running(), "submit must run inside a coroutine")
    self.tail = self.tail + 1
    self.queue[self.tail] = { msg = msg, co = co }

    if self.suspended then
        local worker = self.suspended
        self.suspended = nil
        scheduler.resume(worker)                    -- wake the writer
    end

    return coroutine.yield()
end

-- Drive the time-based flush from outside. Call periodically (e.g. from
-- the same scheduler tick that drives Partition:sync_loop). Wakes the
-- writer if its batch has aged past flush_every.
function PartitionWriter:tick(scheduler)
    if self.batch_size > 0
       and (socket.gettime() - self.last_flush) >= self.flush_every
       and self.suspended then
        local worker = self.suspended
        self.suspended = nil
        scheduler.resume(worker)
    end
end

function PartitionWriter:stop(scheduler)
    self.running = false
    if self.suspended then
        local worker = self.suspended
        self.suspended = nil
        scheduler.resume(worker)
    end
end

return {
    Partition = Partition,
    PartitionWriter = PartitionWriter,
}
