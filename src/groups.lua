local broker_m = require("src.broker")

local GroupMember = {}
GroupMember.__index = GroupMember

function GroupMember.new(id, topics, partitions, last_heartbeat)
    assert(type(id) == "string", "id must be a string")
    last_heartbeat = last_heartbeat or os.time()
    assert(type(last_heartbeat) == "number", "last_heartbeat must be a number")

   return setmetatable({
        id             = id,
        topics         = topics,         -- array of topic names
        partitions     = partitions,             -- topic_name -> array of partition ids
        last_heartbeat = last_heartbeat,
    }, GroupMember)
end


local function range_assignment_strategy(members, topics)
    local assignments = {}

    -- Seed each member with an empty array for every topic they subscribe to.
    for member_id, member in pairs(members) do
        assignments[member_id] = {}
        for _, topic in ipairs(member.topics) do
            assignments[member_id][topic] = {}
        end
    end

    -- Build topic -> [member_id, ...] for members subscribed to that topic.
    local topic_members = {}
    for topic in pairs(topics) do
        topic_members[topic] = {}
    end

    for member_id, member in pairs(members) do
        for _, topic in ipairs(member.topics) do
            if topics[topic] then
                table.insert(topic_members[topic], member_id)
            end
        end
    end

    -- Assign partitions per topic.
    for topic, partitions in pairs(topics) do
        local interested = topic_members[topic]
        if #interested > 0 then
            table.sort(interested)

            local num_partitions = #partitions
            local num_members    = #interested
            local per_member     = math.floor(num_partitions / num_members)
            local remainder      = num_partitions % num_members

            local start = 1
            for i, member_id in ipairs(interested) do
                local count = per_member

                if i <= remainder then
                    count = count + 1
                end

                local stop = start + count - 1
                if stop > num_partitions then
                    stop = num_partitions
                end

                for j = start, stop do
                    table.insert(assignments[member_id][topic], partitions[j])
                end

                start = stop + 1
            end
        end
    end

    return assignments
end

local ConsumerGroup = {}
ConsumerGroup.__index = ConsumerGroup

function ConsumerGroup.new(broker, group_id)
    assert(getmetatable(broker) == broker_m.Broker, "broker must be Broker instance")
    assert(type(group_id) == "string", "group_id must be a string")

    return setmetatable({
        broker   = broker,
        group_id = group_id,
        members  = {},                            -- member_id -> GroupMember
        topics   = {},                            -- topic_name -> array of partition ids
        strategy = range_assignment_strategy,
    }, ConsumerGroup)
end

-- Add a consumer to the group.
-- Returns: (assigned_partitions, err)
--   assigned_partitions = { [topic] = { partition_id, ... }, ... }
function ConsumerGroup:join(member_id, topics)
    assert(type(member_id) == "string", "member_id must be a string")
    local member = self.members[member_id]
    if member then
        member.topics = topics
        member.last_heartbeat = os.time()
    else
        member = GroupMember.new(member_id, topics)
        self.members[member_id] = member
    end

    -- Lazily load partition lists for any unseen topics.
    for _, topic_name in ipairs(topics) do
        if not self.topics[topic_name] then
            local topic, err = self.broker:get_topic(topic_name)
            if not topic then
                return nil, string.format("failed to get topic %s: %s", topic_name, err)
            end

            -- Partition ids are 0-based to match the Go version.
            -- If your Lua broker uses 1-based partition ids, change this to `i`.
            local partitions = {}
            for i = 1, #topic.partitions do
                partitions[i] = i - 1
            end
            self.topics[topic_name] = partitions
        end
    end

    local assignments = self:_rebalance()
    return assignments[member_id], nil
end

-- Remove a consumer from the group.
function ConsumerGroup:leave(member_id)
    self.members[member_id] = nil
    self:_rebalance()
    return nil
end

-- Update a member's heartbeat. Returns (ok, err).
function ConsumerGroup:heartbeat(member_id)
    local member = self.members[member_id]
    if not member then
        return false, string.format("member %s does not exist", member_id)
    end
    member.last_heartbeat = os.time()
    return true, nil
end

-- Evict members that haven't sent a heartbeat in the last 30 seconds.
function ConsumerGroup:check_heartbeats()
    local expired  = {}
    local deadline = os.time() - 30

    for member_id, member in pairs(self.members) do
        if member.last_heartbeat < deadline then
            table.insert(expired, member_id)
        end
    end

    for _, member_id in ipairs(expired) do
        self.members[member_id] = nil
    end

    if #expired > 0 then
        self:_rebalance()
    end
end

-- Private: reassign partitions to current members.
function ConsumerGroup:_rebalance()
    local assignments = self.strategy(self.members, self.topics)

    for member_id, topic_partitions in pairs(assignments) do
        local member = self.members[member_id]
        if member then
            member.partitions = topic_partitions
        end
    end

    return assignments
end


return {
    GroupMember = GroupMember,
    ConsumerGroup = ConsumerGroup,
    range_assignment_strategy = range_assignment_strategy,
}
