-- CompactCleaner performs key-based log compaction: for each key it retains
-- only the record with the highest offset, dropping superseded versions. Port
-- of jocko's commitlog/compact_cleaner.go (xxhash swapped for the project's
-- CRC-32, which is the hash already vendored here).
--
-- Two passes, exactly as jocko:
--   1. Scan every segment building key-hash -> latest-offset.
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
local crc32     = require("src.core.crc32")

local Segment = segment_m.Segment

local function hash(b)
    return crc32(b)
end

local CompactCleaner = {}
CompactCleaner.__index = CompactCleaner

function CompactCleaner.new()
    return setmetatable({
        -- key-hash -> latest offset seen. Carried across calls, like jocko's.
        m = {},
    }, CompactCleaner)
end

-- clean returns (cleaned_segments, nil) or (nil, err).
function CompactCleaner:clean(segments)
    if #segments == 0 then
        return segments, nil
    end

    -- Pass 1: latest offset per key.
    for _, seg in ipairs(segments) do
        seg:each(function(offset, msg)
            self.m[hash(msg.key)] = offset
        end)
    end

    -- Pass 2: rewrite each segment, keeping only latest-per-key records.
    local cleaned = {}
    for _, seg in ipairs(segments) do
        local cs, err = Segment.new(seg.dir, seg.base_offset, seg.max_bytes, ".cleaned")
        if not cs then return nil, err end

        local write_err
        seg:each(function(offset, msg)
            if write_err then return end
            if self.m[hash(msg.key)] <= offset then
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
        if write_err then
            cs:delete()
            return nil, write_err
        end

        -- Flush the rewritten log before the rename so a crash can't leave a
        -- ".cleaned" with buffered-but-unwritten records.
        cs:sync()

        local rerr = cs:replace(seg)
        if rerr then return nil, rerr end

        cleaned[#cleaned + 1] = cs
    end

    return cleaned, nil
end

return CompactCleaner
