local time_m    = require("src.core.time")
local fs_m      = require("src.io.fs")
local socket  = require("socket")
local msg_m = require("src.record.message")
local crc32   = require("src.core.crc32")
local io_sync = require("src.io.io_sync")
local log     = require("src.log.logger").get("partition")

-- Patterns we treat as transient. We can't introspect errno from plain
-- io.write — Lua gives us the system's strerror text. These cover Linux/
-- macOS strings plus a few Windows equivalents you'll hit on your WSL +
-- native split. Extend as you find cases that should retry but don't.
local RETRYABLE_PATTERNS = {
    "resource temporarily unavailable",   -- EAGAIN / EWOULDBLOCK
    "interrupted system call",            -- EINTR
    "device or resource busy",            -- EBUSY
    "no buffer space available",          -- ENOBUFS
    "try again",                          -- some Linux variants
    -- Windows-flavored strings:
    "being used by another process",
    "the operation could not be completed",
}

local function is_retryable_error(err)
    assert(type(err) == "string", "err must be a string")

    if not err then return false end
    local low = err:lower()

    for i = 1, #RETRYABLE_PATTERNS do
        if low:find(RETRYABLE_PATTERNS[i], 1, true) then
            return true
        end 
    end
    return false
end

local Partition = {}
Partition.__index = Partition

function Partition.new(topic, id, topic_dir)
    assert(type(topic) == "table", "topic must be a Topic")
    assert(type(id) == "number", "id must be a number")
    assert(type(topic_dir) == "string", "topic_dir must be a string")

    local log_file_name = string.format("partition-%d.log", id)
    local file_path     = fs_m.join_path(topic_dir, log_file_name)

    local f, err = io.open(file_path, "a+b")
    if not f then return nil, err end

    local size = f:seek("end")

    return setmetatable({
        topic      = topic,
        id         = id,
        file       = f,
        offset     = size,
        sync_every = 50 * time_m.MILLISECOND,
        last_sync  = socket.gettime(),
    }, Partition), nil
end

-- Append raw bytes. Bumps offset by #data on success. Note: Lua's io.write
-- doesn't surface partial writes — a partial write is reported as an error,
-- not a short count, so we treat any non-nil return as a complete write.
function Partition:write(data)
    assert(type(data) == "string", "data must be a string")
    if not self.file then return false, "no file open" end

    local ok, err = self.file:write(data)
    if not ok then return false, err end

    self.offset = self.offset + #data
    return true, nil
end

-- Force userspace + OS buffers to disk. Use after acks=1 writes or
-- whenever durability matters more than throughput.
function Partition:sync()
    if not self.file then return false, "no file open" end
    local ok, err = io_sync.sync(self.file)
    self.last_sync = socket.gettime()
    return ok, err
end

function Partition:close()
    if self.file then
        self.file:flush()
        self.file:close()
        self.file = nil
    end
end

-- Append a Message in the CRC-protected wire format. Single file:write so
-- the record is atomic against signal-interrupted writes — we never leave
-- a half-record on disk for the recovery scanner to puzzle over.
function Partition:write_message(msg)
    assert(getmetatable(msg) == msg_m.Message, "msg must be a Message instance")

    local current_offset = self.offset

    local header  = string.pack(">I4I8", #msg.key, msg.timestamp)
    local payload = msg.key .. msg.value

    local header_crc  = crc32(header)
    local payload_crc = crc32(payload)

    -- total_size excludes the 8-byte length prefix itself.
    local total_size = #header + 4 + #payload + 4

    local record = string.pack(">I8", total_size)
                .. header
                .. string.pack(">I4", header_crc)
                .. payload
                .. string.pack(">I4", payload_crc)

    local ok, err = self.file:write(record)
    if not ok then
        return -1, string.format("failed to write message: %s", err)
    end

    self.offset = self.offset + 8 + total_size

    -- Time-based flush; cheap and gives bounded data-loss on crash.
    local elapsed = socket.gettime() - self.last_sync
    if elapsed >= self.sync_every then
        self.file:flush()
        self.last_sync = socket.gettime()
    end

    return current_offset, nil
end

function Partition:read_message(offset)
    assert(type(offset) == "number", "offset must be a number")

    local pos, serr = self.file:seek("set", offset)
    if not pos then
        return nil, offset, string.format("failed to seek offset: %s", serr)
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
    if total_size < HEADER_LEN + 4 + 4 then
        return nil, offset, "corrupt header: total_size too small"
    end

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
    if key_size < 0 or key_size > #payload then
        return nil, offset, "corrupt header: key_size out of range"
    end

    local key   = payload:sub(1, key_size)
    local value = payload:sub(key_size + 1)

    local msg         = msg_m.Message.new(key, value, timestamp)
    local next_offset = offset + 8 + total_size

    return msg, next_offset, nil
end

function Partition:write_with_resilience(msg)
    assert(getmetatable(msg) == msg_m.Message, "msg must be Message instance")

    local offset, err
    local last_attempt = 0

    for retries = 0, 2 do
        last_attempt = retries
        offset, err = self:write_message(msg)
        if not err then break end

        if not is_retryable_error(err) then
            return -1, err
        end

        -- Sleep before the next attempt. Skip on the final iteration —
        -- the Go version sleeps here unconditionally, which is a wasted
        -- 200ms when we're about to give up anyway.
        if retries < 2 then
            socket.sleep((retries + 1) * 0.1)
        end
    end

    if not err and last_attempt > 0 then
        log:warn("partition %d write succeeded after %d retries",
            self.id, last_attempt)
    end

    return offset, err
end

local PartitionWriter = {}
PartitionWriter.__index = PartitionWriter

local DEFAULT_MAX_SIZE     = 1 * 1024 * 1024   -- 1 MiB
local DEFAULT_MAX_MESSAGES = 1000
local DEFAULT_FLUSH_EVERY  = 10 * time_m.MILLISECOND

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
-- with their predicted offsets (or the write error). Predicted offsets
-- assume Partition is single-writer; callers that mix sync write_message
-- with PartitionWriter on the same Partition will get bogus offsets.
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

            local msg_bytes, serr = msg_m.serialize_message(req.msg)
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

        -- Yield only when the queue is empty. If a submit landed between
        -- the drain loop above and this point, loop again to consume it.
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
-- your scheduler tick) so a partially-full batch eventually flushes when
-- no new submits are arriving.
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
