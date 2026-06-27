-- DeleteCleaner enforces a byte budget by deleting the oldest segments. Port
-- of jocko's commitlog/delete_cleaner.go.
--
-- Given the segment list (oldest first, newest last) it always keeps the
-- newest segment, then walks backwards accumulating each segment's byte size,
-- keeping segments until the running total would exceed the retention budget.
-- Everything older than that is deleted from disk. A budget of -1 means
-- "retain everything" and the cleaner is a no-op.

local Retention = {}
Retention.__index = Retention

function Retention.new(bytes)
    assert(type(bytes) == "number", "bytes must be a number")
    return setmetatable({ bytes = bytes }, Retention)
end

local DeleteCleaner = {}
DeleteCleaner.__index = DeleteCleaner

function DeleteCleaner.new(bytes)
    return setmetatable({
        retention = Retention.new(bytes),
    }, DeleteCleaner)
end

-- clean returns the list of segments to keep (oldest first), having deleted
-- the rest. Returns (kept, nil) or (nil, err).
function DeleteCleaner:clean(segments)
    local n = #segments
    if n == 0 or self.retention.bytes == -1 then
        return segments, nil
    end

    -- Always keep the newest segment; seed the running total with its size.
    local kept        = { segments[n] }
    local total_bytes = segments[n].position

    if n > 1 then
        -- Walk older segments newest->oldest. `stop` marks the first segment
        -- (and everything older) that pushes us past the budget; those are
        -- deleted.
        local stop
        for i = n - 1, 1, -1 do
            local s = segments[i]
            total_bytes = total_bytes + s.position
            if total_bytes > self.retention.bytes then
                stop = i
                break
            end
            table.insert(kept, 1, s)
        end

        if stop then
            for i = stop, 1, -1 do
                local derr = segments[i]:delete()
                if derr then return nil, derr end
            end
        end
    end

    return kept, nil
end

return DeleteCleaner
