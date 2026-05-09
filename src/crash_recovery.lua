-- Recover a partition file after an unclean shutdown.
--
-- Strategy: scan from byte 0, validating each record's framing AND its
-- CRCs (header + payload). The first record that fails any check is the
-- truncation boundary. Truncate, fsync, close, return last good offset.
--
-- This runs BEFORE topic_manager:create_topic opens the file for normal
-- use. We don't construct a Partition here — recovery is a filesystem
-- operation, not a stateful one.

local fs_m      = require("src.fs")
local crc32   = require("src.crc32")
local io_sync = require("src.io_sync")

local HEADER_SIZE = 8   -- uint64 length prefix, big-endian
local MSG_HEADER_LEN = 12

local function recover_partition(topic_dir, partition_id)
    assert(type(topic_dir) == "string", "topic_dir must be a string")
    assert(type(partition_id) == "number", "partition_id must be a number")

    local log_file_name = ("partition-%d.log"):format(partition_id)
    local file_path     = fs_m.join_path(topic_dir, log_file_name)

    local file, oerr = io.open(file_path, "r+b")
    if not file then
        -- Missing file is not a recovery error: there's nothing to recover.
        return 0, nil
    end

    local file_size = file:seek("end") or 0
    file:seek("set", 0)

    local last_good = 0
    local truncate_at = nil

    while last_good < file_size do
        local current = last_good

        if current + HEADER_SIZE > file_size then
            io.stderr:write(string.format(
                "Partial length prefix at offset %d, truncating\n", current))
            truncate_at = current
            break
        end

        local size_bytes = file:read(HEADER_SIZE)
        if not size_bytes or #size_bytes < HEADER_SIZE then
            truncate_at = current
            break
        end
        local total_size = string.unpack(">I8", size_bytes)
        local record_end = current + HEADER_SIZE + total_size

        if record_end > file_size then
            io.stderr:write(string.format(
                "Partial record at offset %d, truncating\n", current))
            truncate_at = current
            break
        end

        if total_size < MSG_HEADER_LEN + 4 + 4 then
            io.stderr:write(string.format(
                "Record at offset %d has impossible total_size %d, truncating\n",
                current, total_size))
            truncate_at = current
            break
        end

        local body = file:read(total_size)
        if not body or #body < total_size then
            truncate_at = current
            break
        end

        local header_bytes       = body:sub(1, MSG_HEADER_LEN)
        local stored_header_crc  = string.unpack(">I4", body, MSG_HEADER_LEN + 1)
        local payload_end        = #body - 4
        local payload            = body:sub(MSG_HEADER_LEN + 4 + 1, payload_end)
        local stored_payload_crc = string.unpack(">I4", body, payload_end + 1)

        if crc32(header_bytes) ~= stored_header_crc then
            io.stderr:write(string.format(
                "Header CRC mismatch at offset %d, truncating\n", current))
            truncate_at = current
            break
        end
        if crc32(payload) ~= stored_payload_crc then
            io.stderr:write(string.format(
                "Payload CRC mismatch at offset %d, truncating\n", current))
            truncate_at = current
            break
        end

        last_good = record_end
    end

    if truncate_at then
        local ok, terr = io_sync.truncate(file, truncate_at)
        if not ok then
            file:close()
            return nil, terr
        end
        local sok, serr = io_sync.sync(file)
        if not sok then
            file:close()
            return nil, serr
        end
        last_good = truncate_at
    end

    file:close()
    return last_good, nil
end

return recover_partition
