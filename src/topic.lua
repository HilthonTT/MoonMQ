local Topic = {}
Topic.__index = Topic

function Topic.new(name)
    return setmetatable({
        name       = name,
        partitions = {},
    }, Topic)
end

return Topic
