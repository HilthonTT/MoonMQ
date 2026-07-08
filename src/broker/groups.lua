local broker_m = require("src.broker")
local Machine  = require("src.fsm.state_machine")
local socket   = require("socket")
local log      = require("src.log.logger").get("group")

-- Consumer-group lifecycle states, mirroring Kafka's GroupCoordinator.
--
--   empty        -> no members; the resting state of a brand-new or
--                   drained group.
--   preparing    -> a member has joined or left and the group is gathering
--                   the current membership before it can assign partitions.
--   completing   -> membership is settled; assignments have been computed and
--                   are being handed back to members (the "sync" phase).
--   stable       -> every member holds its assignment; steady-state consume.
--   dead         -> group torn down; no further transitions are legal.
--
-- A single broker drives join/leave synchronously, so a rebalance walks
-- preparing -> completing -> stable within one call rather than waiting on
-- a network round of JoinGroup/SyncGroup the way a distributed Kafka does.
-- The FSM still earns its keep: it makes the lifecycle inspectable
-- (group:state()), guards operations that are illegal in the current state
-- (heartbeating a dead group), and gives us one place to log transitions.
local STATES = {
    EMPTY      = "empty",
    PREPARING  = "preparing_rebalance",
    COMPLETING = "completing_rebalance",
    STABLE     = "stable",
    DEAD       = "dead",
}

local GroupMember = {}
GroupMember.__index = GroupMember

function GroupMember.new(id, topics, partitions, last_heartbeat)
    assert(type(id) == "string", "id must be a string")
    last_heartbeat = last_heartbeat or socket.gettime()
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

-- Build the lifecycle FSM for one group. Events are the only legal edges;
-- Machine:can() returns false for anything not listed, which is what lets
-- callers guard against out-of-state operations. `die` is reachable from any
-- state ('*') because a group can be torn down at any point.
local function make_lifecycle_fsm(group_id)
    return Machine.create({
        initial = STATES.EMPTY,
        events = {
            -- A member joined or left: (re)gather membership.
            { name = "prepare",   from = { STATES.EMPTY, STATES.STABLE, STATES.COMPLETING }, to = STATES.PREPARING },
            -- Membership settled; assignments computed and being distributed.
            { name = "complete",  from = STATES.PREPARING,  to = STATES.COMPLETING },
            -- Assignments delivered; back to steady-state.
            { name = "stabilize", from = STATES.COMPLETING, to = STATES.STABLE },
            -- Last member gone: collapse straight back to empty.
            { name = "empty_out", from = { STATES.PREPARING, STATES.COMPLETING, STATES.STABLE }, to = STATES.EMPTY },
            -- Terminal.
            { name = "die",       from = "*", to = STATES.DEAD },
        },
        callbacks = {
            onstatechange = function(_, event, from, to)
                log:debug("group '%s': %s -> %s (%s)", group_id, from, to, event)
            end,
        },
    })
end

function ConsumerGroup.new(broker, group_id)
    assert(getmetatable(broker) == broker_m.Broker, "broker must be Broker instance")
    assert(type(group_id) == "string", "group_id must be a string")

    return setmetatable({
        broker   = broker,
        group_id = group_id,
        members  = {},                            -- member_id -> GroupMember
        topics   = {},                            -- topic_name -> array of partition ids
        strategy = range_assignment_strategy,
        fsm      = make_lifecycle_fsm(group_id),
    }, ConsumerGroup)
end

-- Current lifecycle state (one of the STATES values).
function ConsumerGroup:state()
    return self.fsm.current
end

-- Drive the FSM through a full rebalance: preparing -> completing -> stable.
-- Called after membership/topic state has been mutated. `prepare` is fired
-- from whatever the current resting state is (empty/stable/completing); if the
-- group is already mid-rebalance (preparing), the prepare edge is a no-op-ish
-- self-absorb and we just recompute. Returns the fresh assignment table.
function ConsumerGroup:_run_rebalance()
    -- Fire prepare unless we're already gathering membership.
    if not self.fsm:is(STATES.PREPARING) then
        self.fsm:prepare()
    end

    local assignments = self:_rebalance()

    self.fsm:complete()
    self.fsm:stabilize()

    return assignments
end

-- Add a consumer to the group.
-- Returns: (assigned_partitions, err)
--   assigned_partitions = { [topic] = { partition_id, ... }, ... }
function ConsumerGroup:join(member_id, topics)
    assert(type(member_id) == "string", "member_id must be a string")
    if self.fsm:is(STATES.DEAD) then
        return nil, "group is dead"
    end

    -- Resolve every subscribed topic's partition list BEFORE touching any
    -- group state. A bad topic must leave the group exactly as it was — no
    -- phantom member, no FSM stranded mid-rebalance — which matters now that
    -- joins arrive over the wire from arbitrary clients.
    local resolved = {}
    for _, topic_name in ipairs(topics) do
        if self.topics[topic_name] then
            resolved[topic_name] = self.topics[topic_name]
        else
            local topic, err = self.broker:get_topic(topic_name)
            if not topic then
                return nil, string.format("failed to get topic %s: %s", topic_name, err)
            end

            -- Broker partition ids are 1-based (see topic_manager:create_topic).
            local partitions = {}
            for i = 1, #topic.partitions do
                partitions[i] = i
            end
            resolved[topic_name] = partitions
        end
    end

    -- Commit point: nothing below can fail, so membership and the FSM stay
    -- consistent. A join always triggers a rebalance — enter `preparing` to
    -- reflect that the group is in flux.
    if not self.fsm:is(STATES.PREPARING) then
        self.fsm:prepare()
    end

    for topic_name, partitions in pairs(resolved) do
        self.topics[topic_name] = partitions
    end

    local member = self.members[member_id]
    if member then
        member.topics = topics
        member.last_heartbeat = socket.gettime()
    else
        self.members[member_id] = GroupMember.new(member_id, topics)
    end

    -- Membership/topics are settled; run completing -> stable. We're already
    -- in preparing, so _run_rebalance won't re-fire prepare.
    local assignments = self:_run_rebalance()
    return assignments[member_id], nil
end

-- Remove a consumer from the group. Returns (true, nil), or (nil, err) if the
-- group is already dead.
function ConsumerGroup:leave(member_id)
    if self.fsm:is(STATES.DEAD) then
        return nil, "group is dead"
    end

    self.members[member_id] = nil

    -- An emptied group collapses back to `empty` rather than churning through
    -- a pointless assignment of zero partitions to zero members.
    if next(self.members) == nil then
        self.topics = {}
        self.fsm:empty_out()
        return true, nil
    end

    self:_run_rebalance()
    return true, nil
end

-- Tear the group down. Terminal: after this, join/leave/heartbeat all reject.
function ConsumerGroup:close()
    self.fsm:die()
    self.members = {}
    self.topics  = {}
    return true, nil
end

-- Update a member's heartbeat. Returns (ok, err).
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

-- Evict members that haven't sent a heartbeat in the last 30 seconds.
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
        -- Same collapse-to-empty rule as leave(): evicting the last member
        -- returns the group to `empty`, otherwise rebalance the survivors.
        if next(self.members) == nil then
            self.topics = {}
            self.fsm:empty_out()
        else
            self:_run_rebalance()
        end
    end

    return expired
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
    STATES = STATES,
}
