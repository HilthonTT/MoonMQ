local CompactCleaner = {}
CompactCleaner.__index = CompactCleaner

function CompactCleaner.new()
    return setmetatable({
        m = {},
    }, CompactCleaner)
end

function CompactCleaner:clean(segments)
    -- TODO: implement this
    return {}, nil
end

return CompactCleaner