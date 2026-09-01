local segment_m = require("src.commitlog.segment")
local message_m = require("src.record.message")

local Segment = segment_m.Segment

local CompactCleaner = {}
CompactCleaner.__index = CompactCleaner

function CompactCleaner.new()
    return setmetatable({
        m = {},
    }, CompactCleaner)
end

function CompactCleaner:clean(segments)
    if #segments == 0 then
        return segments, nil
    end

    for _, seg in ipairs(segments) do
        seg:each(function(offset, msg)
            self.m[msg.key] = offset
        end)
    end

    local twins = {}
    local function abort_twins(err)
        for _, twin in ipairs(twins) do twin:delete() end
        return segments, err
    end

    for i, seg in ipairs(segments) do
        os.remove(segment_m.log_path(seg.dir, seg.base_offset, ".cleaned"))
        os.remove(segment_m.index_path(seg.dir, seg.base_offset, ".cleaned"))

        local cs, err = Segment.new(seg.dir, seg.base_offset, seg.max_bytes, ".cleaned")
        if not cs then return abort_twins(err) end
        twins[i] = cs

        local write_err
        seg:each(function(offset, msg)
            if write_err then return end
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

        cs:sync()
    end

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
