local broker_m = require("src.broker")
local Machine  = require("src.fsm.state_machine")
local socket   = require("socket")
local log      = require("src.log.logger").get("group")

local STATES = {
    EMPTY      = "empty",
    PREPARING  = "preparing_rebalance",
    COMPLETING = "completing_rebalance",
    STABLE     = "stable",
    DEAD       = "dead",
}

local GroupMember = {}
GroupMember.__index = GroupMember

function GroupMember.new(id, topics, partitions, last_heartbeat, origin)
    assert(type(id) == "string", "id must be a string")
    last_heartbeat = last_heartbeat or socket.gettime()
    assert(type(last_heartbeat) == "number", "last_heartbeat must be a number")

   return setmetatable({
        id             = id,
        topics         = topics,
        partitions     = partitions,
        last_heartbeat = last_heartbeat,
        origin         = origin,
    }, GroupMember)
end


local function assign_range(assignments, topic, partitions, interested)
    if #interested == 0 then return end
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

local function seed_assignments(members, topics)
    local assignments, topic_members = {}, {}
    for topic in pairs(topics) do
        topic_members[topic] = {}
    end
    for member_id, member in pairs(members) do
        assignments[member_id] = {}
        for _, topic in ipairs(member.topics) do
            assignments[member_id][topic] = {}
            if topics[topic] then
                table.insert(topic_members[topic], member_id)
            end
        end
    end
    return assignments, topic_members
end

local function range_assignment_strategy(members, topics)
    local assignments, topic_members = seed_assignments(members, topics)
    for topic, partitions in pairs(topics) do
        assign_range(assignments, topic, partitions, topic_members[topic])
    end
    return assignments
end

local function ownership_aware_range_strategy(members, topics, ownership)
    local assignments, topic_members = seed_assignments(members, topics)

    for topic, partitions in pairs(topics) do
        local buckets = {}
        for _, p in ipairs(partitions) do
            local owner = ownership(topic, p)
            local b = buckets[owner]
            if not b then b = {}; buckets[owner] = b end
            b[#b + 1] = p
        end

        for owner, plist in pairs(buckets) do
            local interested = {}
            for _, member_id in ipairs(topic_members[topic]) do
                if members[member_id].origin == owner then
                    interested[#interested + 1] = member_id
                end
            end
            assign_range(assignments, topic, plist, interested)
        end
    end

    return assignments
end

local ConsumerGroup = {}
ConsumerGroup.__index = ConsumerGroup

local function make_lifecycle_fsm(group_id)
    return Machine.create({
        initial = STATES.EMPTY,
        events = {
            { name = "prepare",   from = { STATES.EMPTY, STATES.STABLE, STATES.COMPLETING }, to = STATES.PREPARING },
            { name = "complete",  from = STATES.PREPARING,  to = STATES.COMPLETING },
            { name = "stabilize", from = STATES.COMPLETING, to = STATES.STABLE },
            { name = "empty_out", from = { STATES.PREPARING, STATES.COMPLETING, STATES.STABLE }, to = STATES.EMPTY },
            { name = "die",       from = "*", to = STATES.DEAD },
        },
        callbacks = {
            onstatechange = function(_, event, from, to)
                log:debug("group '%s': %s -> %s (%s)", group_id, from, to, event)
            end,
        },
    })
end

function ConsumerGroup.new(broker, group_id, opts)
    assert(getmetatable(broker) == broker_m.Broker, "broker must be Broker instance")
    assert(type(group_id) == "string", "group_id must be a string")
    opts = opts or {}

    local strategy = range_assignment_strategy
    if opts.ownership then
        local ownership = opts.ownership
        strategy = function(members, topics)
            return ownership_aware_range_strategy(members, topics, ownership)
        end
    end

    return setmetatable({
        broker   = broker,
        group_id = group_id,
        members  = {},
        topics   = {},
        strategy = strategy,
        fsm      = make_lifecycle_fsm(group_id),
    }, ConsumerGroup)
end

function ConsumerGroup:state()
    return self.fsm.current
end

function ConsumerGroup:_run_rebalance()
    if not self.fsm:is(STATES.PREPARING) then
        self.fsm:prepare()
    end

    local assignments = self:_rebalance()

    self.fsm:complete()
    self.fsm:stabilize()

    return assignments
end

function ConsumerGroup:join(member_id, topics, origin)
    assert(type(member_id) == "string", "member_id must be a string")
    if self.fsm:is(STATES.DEAD) then
        return nil, "group is dead"
    end

    local resolved = {}
    for _, topic_name in ipairs(topics) do
        if self.topics[topic_name] then
            resolved[topic_name] = self.topics[topic_name]
        else
            local topic, err = self.broker:get_topic(topic_name)
            if not topic then
                return nil, string.format("failed to get topic %s: %s", topic_name, err)
            end

            local partitions = {}
            for i = 1, #topic.partitions do
                partitions[i] = i
            end
            resolved[topic_name] = partitions
        end
    end

    if not self.fsm:is(STATES.PREPARING) then
        self.fsm:prepare()
    end

    for topic_name, partitions in pairs(resolved) do
        self.topics[topic_name] = partitions
    end

    local member = self.members[member_id]
    if member then
        member.topics = topics
        member.origin = origin
        member.last_heartbeat = socket.gettime()
    else
        self.members[member_id] = GroupMember.new(member_id, topics, nil, nil, origin)
    end

    local assignments = self:_run_rebalance()
    return assignments[member_id], nil
end

function ConsumerGroup:leave(member_id)
    if self.fsm:is(STATES.DEAD) then
        return nil, "group is dead"
    end

    self.members[member_id] = nil

    if next(self.members) == nil then
        self.topics = {}
        self.fsm:empty_out()
        return true, nil
    end

    self:_run_rebalance()
    return true, nil
end

function ConsumerGroup:close()
    self.fsm:die()
    self.members = {}
    self.topics  = {}
    return true, nil
end

function ConsumerGroup:heartbeat(member_id)
    if self.fsm:is(STATES.DEAD) then
        return false, "group is dead"
    end
    local member = self.members[member_id]
    if not member then
        return false, string.format("member %s does not exist", member_id)
    end
    member.last_heartbeat = socket.gettime()
    return true, nil
end

function ConsumerGroup:check_heartbeats()
    local expired  = {}
    local deadline = socket.gettime() - 30

    for member_id, member in pairs(self.members) do
        if member.last_heartbeat < deadline then
            table.insert(expired, member_id)
        end
    end

    for _, member_id in ipairs(expired) do
        self.members[member_id] = nil
    end

    if #expired > 0 then
        if next(self.members) == nil then
            self.topics = {}
            self.fsm:empty_out()
        else
            self:_run_rebalance()
        end
    end

    return expired
end

function ConsumerGroup:member_count()
    local n = 0
    for _ in pairs(self.members) do n = n + 1 end
    return n
end

function ConsumerGroup:describe()
    local ids = {}
    for member_id in pairs(self.members) do ids[#ids + 1] = member_id end
    table.sort(ids)

    local members = {}
    for i, member_id in ipairs(ids) do
        members[i] = {
            member_id  = member_id,
            assignment = self.members[member_id].partitions or {},
        }
    end

    return { group_id = self.group_id, state = self:state(), members = members }
end

function ConsumerGroup:forget_topic(topic_name)
    assert(type(topic_name) == "string", "topic_name must be a string")
    if self.fsm:is(STATES.DEAD) then return false end
    if not self.topics[topic_name] then return false end

    self.topics[topic_name] = nil

    for _, member in pairs(self.members) do
        local kept = {}
        for _, t in ipairs(member.topics or {}) do
            if t ~= topic_name then kept[#kept + 1] = t end
        end
        member.topics = kept
        if member.partitions then member.partitions[topic_name] = nil end
    end

    if next(self.topics) == nil then
        if not self.fsm:is(STATES.EMPTY) then
            self.fsm:empty_out()
        end
    else
        self:_run_rebalance()
    end
    return true
end

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
    ownership_aware_range_strategy = ownership_aware_range_strategy,
    STATES = STATES,
}
