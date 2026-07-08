-- Consumer-group coordination, server-side. Owns the registry of live
-- ConsumerGroup coordinators (one per group_id, shared across connections),
-- enforces the max_groups ceiling, scopes each connection's Consumer to its
-- assigned partitions, fences lapsed members, and ages out members that
-- stopped heartbeating.
--
-- The coordination *algorithm* (membership FSM, range assignment strategy,
-- heartbeat deadlines, rebalance) lives in src/broker/groups.lua. This module
-- is the ownership/wiring layer the server drives: opcode handlers in
-- src/server/handlers.lua call into it, and the server's reaper loop ticks
-- :reap() periodically.

local groups_m = require("src.broker.groups")

local GroupCoordinator = {}
GroupCoordinator.__index = GroupCoordinator

function GroupCoordinator.new(broker, opts)
    opts = opts or {}
    return setmetatable({
        broker = broker,
        -- Coordinators keyed by group_id, created lazily on first JOIN_GROUP.
        groups = {},
        -- Cap on distinct consumer groups, mirroring max_topics. Without it an
        -- authenticated client could JOIN_GROUP with unbounded distinct group
        -- ids and grow the registry without limit. reap() drops emptied groups
        -- so this is a ceiling on *live* groups, not a lifetime total.
        max_groups = assert(opts.max_groups, "opts.max_groups required"),
    }, GroupCoordinator)
end

function GroupCoordinator:get(group_id)
    return self.groups[group_id]
end

-- Lazily fetch (or create) the coordinator for a group. Returns (group, nil)
-- or (nil, err) when creating a new group would exceed max_groups.
function GroupCoordinator:get_or_create(group_id)
    local group = self.groups[group_id]
    if not group then
        -- Count live groups; reap() drops emptied ones so this is a
        -- ceiling on concurrent groups, not a lifetime total.
        local n = 0
        for _ in pairs(self.groups) do n = n + 1 end
        if n >= self.max_groups then
            return nil, string.format("group limit reached (%d)", self.max_groups)
        end
        group = groups_m.ConsumerGroup.new(self.broker, group_id)
        self.groups[group_id] = group
    end
    return group
end

-- True iff (group_id, member_id) names a live member. Used to fence commits
-- from a connection whose membership has lapsed (evicted or left) — without
-- this it could keep committing and clobber the offset the partition's new
-- owner is advancing.
function GroupCoordinator:member_alive(group_id, member_id)
    local group = self.groups[group_id]
    return (group and group.members[member_id]) ~= nil
end

-- Sync the connection's Consumer to the partitions the coordinator has
-- assigned this member, so poll()/FETCH only touch owned partitions. This is
-- what makes a consumer group actually divide work — the coordinator computed
-- assignments before, but the read path ignored them and every member read
-- every partition. Called before each poll and right after JOIN_GROUP.
--
-- No-op unless the connection has a Consumer. A member the coordinator has
-- since evicted (present group, absent member) is pinned to "own nothing" so a
-- zombie can't keep draining partitions until it notices its heartbeat was
-- rejected. A connection that isn't a joined member -- standalone, or one that
-- just left/was removed from a group -- has its filter cleared so it reverts to
-- reading every subscribed partition; returning early there instead would leave
-- a stale assignment pinned after LEAVE_GROUP, so the consumer would keep
-- reading only its former partitions.
function GroupCoordinator:apply_assignment(conn)
    if not conn.consumer then return end
    if not (conn.group_id and conn.member_id) then
        conn.consumer:set_assignment(nil)
        return
    end
    local group  = self.groups[conn.group_id]
    local member = group and group.members[conn.member_id]
    conn.consumer:set_assignment(member and member.partitions or {})
end

-- A member's connection dropping is an implicit LEAVE_GROUP — depart now and
-- rebalance survivors rather than waiting out the heartbeat deadline. The
-- caller (Server:_unregister_conn) guarantees this runs exactly once per
-- connection.
function GroupCoordinator:handle_disconnect(conn)
    if not conn.group_id then return end
    local group = self.groups[conn.group_id]
    if group then group:leave(conn.member_id) end
    conn.group_id  = nil
    conn.member_id = nil
end

-- One reaper pass: evict members that stopped heartbeating (rebalancing
-- survivors) and drop coordinators with no members, so an idle broker doesn't
-- accumulate empty groups forever (and so max_groups counts only live groups).
-- The server ticks this on group_reaper_interval.
function GroupCoordinator:reap()
    for gid, group in pairs(self.groups) do
        group:check_heartbeats()
        -- Safe to delete the current key during pairs(). A stale connection
        -- still holding this group_id will get GROUP_MEMBER_UNKNOWN on its
        -- next heartbeat/commit and rejoin.
        if next(group.members) == nil then
            self.groups[gid] = nil
        end
    end
end

return GroupCoordinator
