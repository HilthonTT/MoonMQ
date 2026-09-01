local M = {}

function M.collect(broker, assignments)
    local loads = {}
    local traffic = broker.traffic
    for name, topic in pairs(broker.topic_manager.topics) do
        if name:sub(1, 2) ~= "__" then
            for _, p in ipairs(topic.partitions) do
                if assignments:owned_by_self(name, p.id) then
                    local bin, bout = 0, 0
                    if traffic then bin, bout = traffic:totals(name, p.id) end
                    loads[#loads + 1] = {
                        topic           = name,
                        partition       = p.id,
                        disk_bytes      = p.offset or 0,
                        bytes_in_total  = bin,
                        bytes_out_total = bout,
                    }
                end
            end
        end
    end
    return loads
end

return M
