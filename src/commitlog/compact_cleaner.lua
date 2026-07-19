-- CompactCleaner performs key-based log compaction: for each key it retains
-- only the record with the highest offset, dropping superseded versions. Port
-- of jocko's commitlog/compact_cleaner.go. Jocko keys its latest-offset map on
-- a 64-bit xxhash of the key; we key on the raw key bytes directly (Lua tables
-- hash by string value), which is collision-free — a hashed key would let two
-- distinct keys collide and silently drop one key's records entirely.
--
-- Two passes, exactly as jocko:
--   1. Scan every segment building key -> latest-offset.
--   2. Re-scan each segment into a ".cleaned" twin, writing back only records
--      whose key still maps to an offset <= the record's own (i.e. this record
--      is that key's latest), then atomically replacing the original.
--
-- CAVEAT (shared with jocko): records here don't carry their offset on disk,
-- so a rewritten segment's offsets are renumbered contiguously from its
-- base_offset when its index is rebuilt. Compaction therefore changes the
-- offsets of surviving records. This matches jocko's behaviour; consumers that
-- need stable offsets across compaction should not enable the compact policy.

local segment_m = require("src.commitlog.segment")
local message_m = require("src.record.message")

local Segment = segment_m.Segment

local CompactCleaner = {}
CompactCleaner.__index = CompactCleaner

function CompactCleaner.new()
    return setmetatable({
        -- key -> latest offset seen. Carried across calls, like jocko's.
        -- Keyed on the raw key bytes (Lua tables key on string value), so
        -- distinct keys never collide the way a 32-bit hash would — a
        -- collision there would silently drop one key's records entirely.
        m = {},
    }, CompactCleaner)
end

-- clean returns (segments', err?). The returned list is ALWAYS the valid,
-- open segment set the caller should adopt — even alongside an error:
--   * a failure while building the ".cleaned" twins leaves every original
--     untouched, so the input list comes back with the error;
--   * a failure while swapping returns the already-swapped prefix plus the
--     untouched original suffix, so at most ONE segment (the mid-swap
--     casualty) is degraded instead of the previous behaviour where the
--     caller kept a list of closed, renamed-over segment objects and every
--     subsequent read raised "attempt to use a closed file".
function CompactCleaner:clean(segments)
    if #segments == 0 then
        return segments, nil
    end

    -- Pass 1: latest offset per key.
    for _, seg in ipairs(segments) do
        seg:each(function(offset, msg)
            self.m[msg.key] = offset
        end)
    end

    -- Pass 2a: build EVERY ".cleaned" twin before touching any original, so a
    -- write failure here aborts the whole compaction with the live segment
    -- set fully intact.
    local twins = {}
    local function abort_twins(err)
        for _, twin in ipairs(twins) do twin:delete() end
        return segments, err
    end

    for i, seg in ipairs(segments) do
        -- Start the ".cleaned" twin from empty. Segment.new opens "a+b" and its
        -- build_index adopts whatever is already in the file, so a leftover twin
        -- from a compaction that failed mid-run (without a process restart, so
        -- open()'s startup recovery never ran) would have its stale — possibly
        -- superseded — records scanned in and then re-appended, resurrecting
        -- values compaction was meant to drop. Remove both twin files first.
        os.remove(segment_m.log_path(seg.dir, seg.base_offset, ".cleaned"))
        os.remove(segment_m.index_path(seg.dir, seg.base_offset, ".cleaned"))

        local cs, err = Segment.new(seg.dir, seg.base_offset, seg.max_bytes, ".cleaned")
        if not cs then return abort_twins(err) end
        twins[i] = cs

        local write_err
        seg:each(function(offset, msg)
            if write_err then return end
            -- Kafka-style tombstones: a zero-length value marks the key as
            -- deleted. Pass 1 already made the tombstone the key's latest
            -- offset (dropping every superseded record below); skipping the
            -- tombstone itself here removes the key from the log entirely.
            -- Safe to drop immediately (no delete.retention.ms grace) because
            -- readers of compacted internal topics replay front-to-back on
            -- boot — there is no mid-stream consumer relying on seeing it.
            if self.m[msg.key] <= offset and #msg.value > 0 then
                local record, serr = message_m.serialize_message(msg)
                if not record then
                    write_err = string.format("serialize during compaction: %s",
                                              tostring(serr))
                    return
                end
                local ok, werr = cs:write(record)
                if not ok then write_err = werr end
            end
        end)
        if write_err then return abort_twins(write_err) end

        -- Flush the rewritten log before any rename so a crash can't leave a
        -- ".cleaned" with buffered-but-unwritten records.
        cs:sync()
    end

    -- Pass 2b: swap originals for twins, updating the result list as each
    -- swap lands. A replace failure keeps the already-swapped prefix and the
    -- still-open original suffix; the unapplied twins are deleted (the one
    -- mid-replace is left for open()'s ".cleaned" orphan recovery).
    local out = {}
    for i = 1, #segments do out[i] = segments[i] end
    for i, seg in ipairs(segments) do
        local rerr = twins[i]:replace(seg)
        if rerr then
            for j = i + 1, #twins do twins[j]:delete() end
            return out, rerr
        end
        out[i] = twins[i]
    end

    return out, nil
end

return CompactCleaner
