local Topic = {}
Topic.__index = Topic

function Topic.new(name)
    assert(type(name) == "string", "name must be a string")

    return setmetatable({
        name       = name,
        partitions = {},
    }, Topic)
end

return Topic
