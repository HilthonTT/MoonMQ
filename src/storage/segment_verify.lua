local io_sync = require("src.io.io_sync")
local msg_m   = require("src.record.message")
local log     = require("src.log.logger").get("segment_verify")

local HEADER_SIZE = 8   -- the 8-byte length prefix in front of each record body

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

        -- Bound total_size against the bytes REMAINING rather than computing
        -- `current + HEADER_SIZE + total_size` and comparing that sum: the
        -- length prefix is covered by neither CRC, so corruption can set it to
        -- any u64, and a value just under 2^63 made the sum wrap to a negative
        -- Lua integer. The wrapped sum passed the `> file_size` test, so a
        -- bogus ~9 exabyte length reached f:read() below and raised an
        -- uncaught "not enough memory" — crashing recovery instead of
        -- truncating a torn tail. The subtraction can't overflow (both
        -- operands are file positions), and a negative total_size (high bit
        -- set) still fails the MIN_BODY test. Same bound the other two readers
        -- already apply (message.deserialize_record, read_message).
        if total_size < msg_m.MIN_BODY
           or total_size > file_size - (current + HEADER_SIZE) then
            log:error("%s: bad framing at %d, truncating", file_path, current)
            truncate_at = current
            break
        end
        local record_end = current + HEADER_SIZE + total_size

        local body = f:read(total_size)
        if not body or #body < total_size then
            truncate_at = current
            break
        end

        -- Validate framing + both CRCs through the single authoritative codec.
        local _, derr = msg_m.decode_body(body)
        if derr then
            log:error("%s: %s at %d, truncating", file_path, derr, current)
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
