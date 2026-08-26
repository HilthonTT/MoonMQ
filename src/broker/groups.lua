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

function GroupMember.new(id, topics, partitions, last_heartbeat, origin)
    assert(type(id) == "string", "id must be a string")
    last_heartbeat = last_heartbeat or socket.gettime()
    assert(type(last_heartbeat) == "number", "last_heartbeat must be a number")

   return setmetatable({
        id             = id,
        topics         = topics,         -- array of topic names
        partitions     = partitions,             -- topic_name -> array of partition ids
        last_heartbeat = last_heartbeat,
        -- Broker the member's connection lives on (cluster mode). Assignment
        -- only hands a member partitions its own broker serves, because
        -- fetches are local — nil outside a cluster.
        origin         = origin,
    }, GroupMember)
end


-- Range-assign `partitions` of `topic` among the (sorted) `interested`
-- member ids, appending into assignments[member_id][topic].
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

-- Seed assignments (every member gets an empty list per subscribed topic) and
-- build topic -> [member_id...] for subscribed members.
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

-- Cluster-aware variant: fetches are local, so a partition may only be
-- assigned to a member whose connection lives on the broker that OWNS it.
-- Partitions are bucketed by owner (via `ownership(topic, partition)`), then
-- each bucket is range-assigned among that owner's subscribed members. A
-- partition whose owner has no subscribed member connected goes unassigned —
-- consuming it requires a member on the owning broker (there is no cross-
-- broker fetch forwarding).
local function ownership_aware_range_strategy(members, topics, ownership)
    local assignments, topic_members = seed_assignments(members, topics)

    for topic, partitions in pairs(topics) do
        -- owner broker id -> array of partition ids
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

-- opts (optional):
--   ownership  fn(topic, partition) -> broker_id. When set (cluster mode),
--              assignment is ownership-aware: a member only receives
--              partitions owned by the broker its connection lives on
--              (member.origin). Absent = single-broker range assignment.
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
        members  = {},                            -- member_id -> GroupMember
        topics   = {},                            -- topic_name -> array of partition ids
        strategy = strategy,
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

-- Add a consumer to the group. `origin` (optional) is the broker id the
-- member's connection lives on — used by ownership-aware assignment.
-- Returns: (assigned_partitions, err)
--   assigned_partitions = { [topic] = { partition_id, ... }, ... }
function ConsumerGroup:join(member_id, topics, origin)
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
        member.origin = origin
        member.last_heartbeat = socket.gettime()
    else
        self.members[member_id] = GroupMember.new(member_id, topics, nil, nil, origin)
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

-- member_count returns how many members the group currently holds.
function ConsumerGroup:member_count()
    local n = 0
    for _ in pairs(self.members) do n = n + 1 end
    return n
end

-- describe returns a plain snapshot for DESCRIBE_GROUP:
--   { group_id, state, members = { { member_id, assignment }, ... } }
-- Members are sorted by id so the wire bytes are deterministic. `assignment`
-- is the member's current partition assignment ({ topic -> {ids} }), which is
-- nil for a member that joined but has not been through a rebalance yet -- an
-- empty table is substituted so callers never have to nil-check it.
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

-- forget_topic drops `topic_name` from the group: out of the subscribed-topic
-- table, out of every member's subscription list, and then out of every
-- assignment via a rebalance. Called when the topic is deleted.
--
-- Without this a deleted topic keeps living inside the group: `self.topics`
-- still maps it to a partition list, so the next rebalance happily assigns
-- partitions of a log that no longer exists, and consumers chase reads against
-- it until something errors. Returns true when the group actually held it.
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

    -- A group whose only subscription just vanished has nothing left to
    -- assign. Collapse to `empty` rather than running a rebalance that would
    -- hand every member an empty assignment -- same rule leave() applies when
    -- the last member goes.
    if next(self.topics) == nil then
        -- empty_out has no edge from `empty` itself. Reaching here from an
        -- already-empty group would need topics with no members, which join()
        -- and leave() between them never leave behind -- but the FSM raises on
        -- an illegal edge, so guard rather than rely on that invariant holding
        -- for every future caller.
        if not self.fsm:is(STATES.EMPTY) then
            self.fsm:empty_out()
        end
    else
        self:_run_rebalance()
    end
    return true
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
    ownership_aware_range_strategy = ownership_aware_range_strategy,
    STATES = STATES,
}
