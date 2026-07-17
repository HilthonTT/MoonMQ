-- Sparse timestamp index for the segmented backend, one `.timeindex`
-- sidecar per segment. Entries are fixed-width `ts(u64) | file_pos(u32)`
-- pairs, appended roughly every index_interval_bytes of log so
-- offset_for_timestamp can binary-search to a nearby file position and
-- bounded-linear-scan from there (mirroring Kafka's time index).
--
-- The commitlog backend keeps its (dense, offset→position) counterpart in
-- src/commitlog/index.lua; this module is the segmented backend's analogue.
-- SegmentedPartition owns the sidecar file handles (open/close/fsync on
-- roll); this module owns the entry format and everything that reads or
-- writes entries.

local io_sync = require("src.io.io_sync")
local log     = require("src.log.logger").get("timeindex")

local M = {}

-- ts(u64) + file_pos(u32), big-endian.
M.ENTRY_SIZE = 12

-- Append a (ts, file_pos) entry to the segment's timeindex if either
-- (a) this is the segment's first message or (b) interval_bytes of log
-- have been written since the previous entry.
--
-- Errors are non-fatal — the index is advisory, and a transient write
-- failure shouldn't fail the produce. We do log it so persistent index
-- breakage is visible. Failed writes leave last_indexed_at unchanged, so
-- the next call will try again.
function M.maybe_append(segment, ts, file_pos, interval_bytes)
    if not segment.index_file then return end
    if type(ts) ~= "number" then return end

    local last = segment.last_indexed_at
    if last ~= nil and (file_pos - last) < interval_bytes then
        return
    end

    -- Monotonicity guard: floor_pos binary-searches this index, which is only
    -- correct if entry timestamps never decrease. Timestamps are
    -- client-supplied and can arrive out of order — skip (don't index) any
    -- record older than the newest indexed timestamp, like Kafka does. The
    -- record itself is still findable: lookups linear-scan from the
    -- preceding entry. Lazily seed the high-water mark from the index tail
    -- so a reopened segment keeps the invariant across restarts.
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
    -- Reposition to EOF before writing: offset_for_timestamp reads this "a+b"
    -- index handle, and a write directly after a read is stdio UB that Windows
    -- UCRT rejects — which would silently stop all further time-indexing for
    -- this segment (the failure is only warn-logged).
    segment.index_file:seek("end")
    local ok, err = segment.index_file:write(entry)
    if not ok then
        log:warn("timeindex write segment=%020d: %s",
            segment.base_offset, tostring(err))
        return
    end
    -- Flush so a separate reader (recovery on next boot, or a
    -- cross-process tail) sees the entry. Lua's userspace buffer
    -- otherwise holds 12-byte writes for a long time. Sync waits for
    -- the roll/close fsync; we don't pay that cost per entry.
    segment.index_file:flush()
    segment.last_indexed_at = file_pos
    segment.last_indexed_ts = ts
end

-- Number of complete entries in the index. Index files are short arrays of
-- fixed-size entries — seek + read is fine even mid-append (POSIX a+b
-- semantics: writes go to EOF, reads honour the seek).
function M.count(index_file)
    local size = index_file:seek("end") or 0
    return math.floor(size / M.ENTRY_SIZE)
end

function M.read_entry(index_file, idx)
    index_file:seek("set", idx * M.ENTRY_SIZE)
    local b = index_file:read(M.ENTRY_SIZE)
    if not b or #b < M.ENTRY_SIZE then return nil end
    return string.unpack(">I8I4", b)  -- ts, file_pos
end

-- Binary-search for the greatest entry index whose timestamp <= target.
-- Returns the entry's file_pos, or nil if no entry qualifies.
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

-- Boot-time recovery: if the segment was tail-truncated by verify_file, any
-- index entries pointing past post_verify_size are stale. Walk the index
-- from the end and ftruncate at the last entry whose file_pos is still
-- inside the post-verify log. Cheap — most loads truncate zero entries.
--
-- Returns (last_indexed_at, nil) on success — the surviving last entry's
-- file_pos, or nil when no entry survives — so the caller can seed the
-- segment without double-indexing. Returns (nil, err) when the truncate
-- itself fails; the caller decides how fatal that is.
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
