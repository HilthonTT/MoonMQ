local busted = require("busted")
local TopicManager = require("src.storage.topic_manager")

local opened = {}
local topic_manager_new = TopicManager.new

function TopicManager.new(...)
    local tm = topic_manager_new(...)
    opened[#opened + 1] = tm
    return tm
end

busted.subscribe({ "test", "end" }, function()
    for i = #opened, 1, -1 do
        pcall(opened[i].close, opened[i])
    end
    opened = {}
    return nil, true
end)
