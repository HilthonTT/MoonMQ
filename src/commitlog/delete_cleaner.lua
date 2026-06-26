local Retention = {}
Retention.__index = Retention

function Retention.new(bytes)
    assert(type(bytes) == "number", "bytes must be a number")

    return setmetatable({
        bytes = bytes,
    }, Retention)
end

local DeleteCleaner = {}
DeleteCleaner.__index = DeleteCleaner

function DeleteCleaner.new(bytes)
    local retention = Retention.new(bytes)
    local cleaner = setmetatable({
        retention = retention,
    }, DeleteCleaner)
    return cleaner
end

function DeleteCleaner:clean(segments)
    -- TODO: implement cleaner

    return {}, nil
end

return DeleteCleaner
