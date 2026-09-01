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

function DeleteCleaner:clean(segments)
    local n = #segments
    if n == 0 or self.retention.bytes == -1 then
        return segments, nil
    end

    local kept        = { segments[n] }
    local total_bytes = segments[n].position

    if n > 1 then
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
