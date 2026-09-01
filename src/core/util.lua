local M = {}
M.__index = M

function M.validate_topic_name(name)
    if type(name) ~= "string" then
        return nil, "topic name must be a string"
    end
    if #name == 0 then
        return nil, "topic name must not be empty"
    end
    if #name > 249 then
        return nil, "topic name must be at most 249 characters"
    end
    if name:sub(1, 1) == "-" then
        return nil, "topic name must not start with '-'"
    end
    if name == "." or name == ".." then
        return nil, "topic name must not be '.' or '..'"
    end
    if name:match("[^A-Za-z0-9._%-]") then
        return nil, "topic name may only contain letters, digits, '.', '_' and '-'"
    end
    return true, nil
end

return M
