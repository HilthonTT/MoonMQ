local crc32   = require("src.crc32")
local io_sync = require("src.io_sync")
local log     = require("src.log.logger").get("segment_verify")

local HEADER_SIZE    = 8
local MSG_HEADER_LEN = 12

-- Scan `file_path` from byte `start_at` to EOF. Truncate (+fsync) at the
-- first bad record. Returns the byte position right after the last valid
-- record. If the file doesn't exist there's nothing to verify; returns
-- `start_at`.
-- "no such file" / "cannot find" patterns we treat as a clean ENOENT.
-- Anything else from io.open is surfaced — locked files and permission
-- errors must not be silently treated as "verified up to start_at".
local function is_missing_file_err(err)
    if type(err) ~= "string" then return false end
    local low = err:lower()
    return low:find("no such file", 1, true)
        or low:find("cannot find",   1, true)
        or low:find("does not exist",1, true)
end

local function verify_file(file_path, start_at)
    assert(type(file_path) == "string", "file_path must be a string")
    start_at = start_at or 0

    local f, oerr = io.open(file_path, "r+b")
    if not f then
        if is_missing_file_err(oerr) then
            return start_at, nil
        end
        return nil, string.format("failed to open %s: %s", file_path, tostring(oerr))
    end


    local file_size = f:seek("end") or 0
    f:seek("set", start_at)

    local last_good = start_at
    local truncate_at = nil

    while last_good < file_size do
        local current = last_good

        if current + HEADER_SIZE > file_size then
            truncate_at = current
            break
        end

        local size_bytes = f:read(HEADER_SIZE)
        if not size_bytes or #size_bytes < HEADER_SIZE then
            truncate_at = current
            break
        end
        local total_size = string.unpack(">I8", size_bytes)
        local record_end = current + HEADER_SIZE + total_size

        if record_end > file_size
           or total_size < MSG_HEADER_LEN + 4 + 4 then
            log:error("%s: bad framing at %d, truncating", file_path, current)
            truncate_at = current
            break
        end

        local body = f:read(total_size)
        if not body or #body < total_size then
            truncate_at = current
            break
        end

        local header_bytes = body:sub(1, MSG_HEADER_LEN)
        local stored_header_crc= string.unpack(">I4", body, MSG_HEADER_LEN + 1)
        local payload_end = #body - 4
        local payload = body:sub(MSG_HEADER_LEN + 4 + 1, payload_end)
        local stored_payload_crc = string.unpack(">I4", body, payload_end + 1)

        if crc32(header_bytes) ~= stored_header_crc or crc32(payload) ~= stored_payload_crc then
            log:error("%s: CRC mismatch at %d, truncating", file_path, current)
            truncate_at = current
            break
        end

        last_good = record_end
    end

    if truncate_at then
        local ok, terr = io_sync.truncate(f, truncate_at)
        if not ok then f:close(); return nil, terr end
        local sok, serr = io_sync.sync(f)
        if not sok then f:close(); return nil, serr end
        last_good = truncate_at
    end

    f:close()
    return last_good, nil
end

return verify_file
