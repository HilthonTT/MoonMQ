local fs = require("src.fs")
local ffi = require("ffi")

ffi.cdef[[
    int ftruncate(int fd, long length);
    int fileno(void *stream);
]]

local HEADER_SIZE = 8   -- uint64 length prefix, big-endian

local function truncate(file, length)
    local fd = ffi.C.fileno(file)
    if ffi.C.ftruncate(fd, length) ~= 0 then
        return false, "ftruncate failed"
    end
    return true
end

local function recover_partition(topic_dir, partition_id)
    assert(type(topic_dir) == "string", "topic_dir must be a string")
    assert(type(partition_id) == "string", "partition_id must be a string")

    local log_file_name = ("partition-%d.log"):format(partition_id)
    local file_path = fs.join_path(topic_dir, log_file_name)

    -- Open the file
    local file, oerr = io.open(file_path, "r+b")
    if not file then
        return nil, "failed to open partition file: " .. tostring(oerr)
    end

    -- Scan the file to find the valid end
    local file_size = file:seek("end") or 0
    file:seek("set", 0)

    local offset = 0
    local truncate_at = nil

    while offset < file_size do
        local current = offset

        -- Header would run past EOF -> partial write
        if current + HEADER_SIZE > file_size then
            io.stderr:write(string.format(
                "Partial write at offset %d, truncating\n", current))
            truncate_at = current
            break
        end

        local size_bytes = file:read(HEADER_SIZE)
        local messages_size = string.unpack(">I8", size_bytes)
        local record_end = current + HEADER_SIZE + messages_size

        -- Body would run past EOF -> partial message
        if record_end > file_size then
            io.stderr:write(string.format(
                "Partial message at offset %d, truncating\n", current))
            truncate_at = current
            break
        end

        -- Skip the body without reading it
        if not file:seek("set", record_end) then
            file:close()
            return nil, "seek failed"
        end

        offset = record_end
    end

    if truncate_at then
        local ok, terr = truncate(file, truncate_at)
        if not ok then
            file:close()
            return nil, terr
        end
        offset = truncate_at
    end

    file:seek("set", offset)

    local Partition = require("src.partition")
    local Time = require("src.time")

    local p = Partition.new({
        id         = partition_id,
        file       = file,
        offset     = offset,
        sync_every = 50 * Time.MILLISECOND,
    })

    p:sync_loop()

    return p
end

return recover_partition
