local io_sync = require("src.io.io_sync")
local log     = require("src.log.logger").get("timeindex")

local M = {}

M.ENTRY_SIZE = 12

function M.maybe_append(segment, ts, file_pos, interval_bytes)
    if not segment.index_file then return end
    if type(ts) ~= "number" then return end

    local last = segment.last_indexed_at
    if last ~= nil and (file_pos - last) < interval_bytes then
        return
    end

    if segment.last_indexed_ts == nil then
        local n = M.count(segment.index_file)
        if n > 0 then
            local tail_ts = M.read_entry(segment.index_file, n - 1)
            segment.last_indexed_ts = tail_ts
        end
    end
    if segment.last_indexed_ts and ts < segment.last_indexed_ts then
        return
    end

    local entry = string.pack(">I8I4", ts, file_pos)
    segment.index_file:seek("end")
    local ok, err = segment.index_file:write(entry)
    if not ok then
        log:warn("timeindex write segment=%020d: %s",
            segment.base_offset, tostring(err))
        return
    end
    segment.index_file:flush()
    segment.last_indexed_at = file_pos
    segment.last_indexed_ts = ts
end

function M.count(index_file)
    local size = index_file:seek("end") or 0
    return math.floor(size / M.ENTRY_SIZE)
end

function M.read_entry(index_file, idx)
    index_file:seek("set", idx * M.ENTRY_SIZE)
    local b = index_file:read(M.ENTRY_SIZE)
    if not b or #b < M.ENTRY_SIZE then return nil end
    return string.unpack(">I8I4", b)
end

function M.floor_pos(index_file, n, target)
    local lo, hi = 0, n - 1
    local result_pos
    while lo <= hi do
        local mid = (lo + hi) // 2
        local ts, pos = M.read_entry(index_file, mid)
        if not ts then break end
        if ts <= target then
            result_pos = pos
            lo = mid + 1
        else
            hi = mid - 1
        end
    end
    return result_pos
end

function M.recover(index_file, post_verify_size)
    local index_size = index_file:seek("end") or 0
    if index_size == 0 then return nil, nil end

    local entry_count = math.floor(index_size / M.ENTRY_SIZE)
    local last_valid_entries = entry_count
    local last_indexed_at = nil
    while last_valid_entries > 0 do
        local pos = (last_valid_entries - 1) * M.ENTRY_SIZE
        index_file:seek("set", pos)
        local raw_entry = index_file:read(M.ENTRY_SIZE)
        if not raw_entry or #raw_entry < M.ENTRY_SIZE then
            last_valid_entries = last_valid_entries - 1
        else
            local _, file_pos = string.unpack(">I8I4", raw_entry)
            if file_pos < post_verify_size then
                last_indexed_at = file_pos
                break
            end
            last_valid_entries = last_valid_entries - 1
        end
    end

    local truncate_to = last_valid_entries * M.ENTRY_SIZE
    if truncate_to < index_size then
        local tok, terr = io_sync.truncate(index_file, truncate_to)
        if not tok then
            return nil, terr
        end
    end
    return last_indexed_at, nil
end

return M
