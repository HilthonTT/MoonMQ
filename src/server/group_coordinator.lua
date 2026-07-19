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
--
-- CLUSTER MODE (opts.cluster set): each group has ONE coordinator broker,
-- chosen deterministically — hash(group_id) over the sorted ids of every
-- cluster member — so all brokers agree without talking. A JOIN / HEARTBEAT /
-- LEAVE arriving on a non-coordinator broker is forwarded to the coordinator
-- over the cluster HTTP layer (POST /cluster/group/*), which makes membership
-- and rebalancing span the whole cluster. Because fetches are local (there is
-- no cross-broker fetch forwarding), the coordinator computes OWNERSHIP-AWARE
-- assignments: a member only receives partitions owned by the broker its
-- connection lives on (see groups.lua ownership_aware_range_strategy).
-- Forwarded members cache their assignment locally; every heartbeat response
-- carries the current assignment, so a rebalance on the coordinator reaches
-- remote members within one heartbeat interval.

local groups_m = require("src.broker.groups")
local log      = require("src.log.logger").get("group_coordinator")

local GroupCoordinator = {}
GroupCoordinator.__index = GroupCoordinator

-- FNV-1a (32-bit) — deterministic across brokers/platforms so every broker
-- maps a group id to the same coordinator.
local function fnv1a(s)
    local hash = 2166136261
    for i = 1, #s do
        hash = (hash ~ s:byte(i)) & 0xFFFFFFFF
        hash = (hash * 16777619) & 0xFFFFFFFF
    end
    return hash
end

-- opts:
--   max_groups  required ceiling on distinct live groups.
--   cluster     optional { self_id = string, peers = { [id] = Peer } }.
--               Present = cluster mode (coordinator hashing + forwarding).
function GroupCoordinator.new(broker, opts)
    opts = opts or {}
    local self = setmetatable({
        broker = broker,
        -- Coordinators keyed by group_id, created lazily on first JOIN_GROUP.
        -- Only populated for groups THIS broker coordinates.
        groups = {},
        -- Cap on distinct consumer groups, mirroring max_topics. Without it an
        -- authenticated client could JOIN_GROUP with unbounded distinct group
        -- ids and grow the registry without limit. reap() drops emptied groups
        -- so this is a ceiling on *live* groups, not a lifetime total.
        max_groups = assert(opts.max_groups, "opts.max_groups required"),
        cluster    = opts.cluster,
        -- Forwarded-membership cache: group_id -> member_id -> assignment
        -- ({ topic -> [partition ids] }). Source of truth stays on the
        -- coordinator broker; this is what apply_assignment / member_alive
        -- consult for members this broker merely proxies.
        remote_members = {},
    }, GroupCoordinator)

    if self.cluster then
        local ids = { self.cluster.self_id }
        for id in pairs(self.cluster.peers or {}) do ids[#ids + 1] = id end
        table.sort(ids)
        self.cluster_ids = ids
    end
    return self
end

-- coordinator_for maps a group id to the broker that coordinates it. Returns
-- self's id outside a cluster.
function GroupCoordinator:coordinator_for(group_id)
    if not self.cluster then return nil end   -- single broker: always local
    local ids = self.cluster_ids
    return ids[(fnv1a(group_id) % #ids) + 1]
end

-- True when this broker coordinates group_id (always true outside a cluster).
function GroupCoordinator:coordinates(group_id)
    local c = self:coordinator_for(group_id)
    return c == nil or c == self.cluster.self_id
end

function GroupCoordinator:get(group_id)
    return self.groups[group_id]
end

-- Lazily fetch (or create) the coordinator for a group. Returns (group, nil)
-- or (nil, err) when creating a new group would exceed max_groups. Only valid
-- on the broker that coordinates the group.
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
        local opts
        if self.cluster then
            local assignments = self.broker.cluster_assignments
            local self_id = self.cluster.self_id
            opts = {
                ownership = function(topic, partition)
                    if not assignments then return self_id end
                    return assignments:owner(topic, partition)
                end,
            }
        end
        group = groups_m.ConsumerGroup.new(self.broker, group_id, opts)
        self.groups[group_id] = group
    end
    return group
end

local function cache_slot(self, group_id)
    local t = self.remote_members[group_id]
    if not t then t = {}; self.remote_members[group_id] = t end
    return t
end

-- join registers (member_id, topics) with the group's coordinator — locally
-- or forwarded — and returns (assignment, nil, nil) or (nil, err, code) where
-- code is "limit" | "topic" | "internal" for the handler's error mapping.
-- `origin` defaults to this broker's id.
function GroupCoordinator:join(group_id, member_id, topics)
    if self:coordinates(group_id) then
        local group, gerr = self:get_or_create(group_id)
        if not group then return nil, gerr, "limit" end
        local origin = self.cluster and self.cluster.self_id or nil
        local assignment, jerr = group:join(member_id, topics, origin)
        if not assignment then
            local code = (jerr and jerr:find("get topic", 1, true))
                and "topic" or "internal"
            return nil, jerr, code
        end
        return assignment
    end

    local coord = self:coordinator_for(group_id)
    local peer = self.cluster.peers[coord]
    if not peer then
        return nil, string.format(
            "group %s is coordinated by %s but no such peer is configured",
            group_id, tostring(coord)), "internal"
    end
    local assignment, err, code = peer:group_join(
        group_id, member_id, topics, self.cluster.self_id)
    if not assignment then return nil, err, code or "internal" end
    cache_slot(self, group_id)[member_id] = assignment
    return assignment
end

-- heartbeat renews the member's lease on the coordinator. For forwarded
-- members the response carries the CURRENT assignment, which refreshes the
-- local cache — this is how a rebalance on the coordinator propagates here.
-- Returns (true, nil) or (nil, err); a nil return means the membership has
-- lapsed (the caller should surface GROUP_MEMBER_UNKNOWN).
function GroupCoordinator:heartbeat(group_id, member_id)
    if self:coordinates(group_id) then
        local group = self.groups[group_id]
        if not group then return nil, "unknown group" end
        return group:heartbeat(member_id)
    end

    local coord = self:coordinator_for(group_id)
    local peer = self.cluster.peers[coord]
    if not peer then return nil, "coordinator peer not configured" end
    local assignment, err = peer:group_heartbeat(group_id, member_id)
    local slot = cache_slot(self, group_id)
    if not assignment then
        -- Lapsed (or coordinator unreachable): drop the cache so
        -- member_alive fences commits until the client rejoins.
        slot[member_id] = nil
        return nil, err
    end
    slot[member_id] = assignment
    return true
end

-- leave departs the group on its coordinator. Best-effort for forwarded
-- members: an unreachable coordinator will reap the member by heartbeat
-- timeout anyway. Always drops the local cache.
function GroupCoordinator:leave(group_id, member_id)
    if self:coordinates(group_id) then
        local group = self.groups[group_id]
        if not group then return nil, "unknown group" end
        return group:leave(member_id)
    end

    local slot = self.remote_members[group_id]
    if slot then
        slot[member_id] = nil
        if next(slot) == nil then self.remote_members[group_id] = nil end
    end
    local coord = self:coordinator_for(group_id)
    local peer = self.cluster.peers[coord]
    if peer then
        local ok, err = peer:group_leave(group_id, member_id)
        if not ok then
            log:warn("forwarded LEAVE for %s/%s failed (%s); coordinator will reap it",
                group_id, member_id, tostring(err))
        end
    end
    return true
end

-- True iff (group_id, member_id) names a live member. Used to fence commits
-- from a connection whose membership has lapsed (evicted or left) — without
-- this it could keep committing and clobber the offset the partition's new
-- owner is advancing. For forwarded members this consults the local cache,
-- which heartbeat() keeps honest (an evicted member's next heartbeat clears
-- it) — fencing is therefore at-most-one-heartbeat-interval stale.
function GroupCoordinator:member_alive(group_id, member_id)
    if self:coordinates(group_id) then
        local group = self.groups[group_id]
        return (group and group.members[member_id]) ~= nil
    end
    local slot = self.remote_members[group_id]
    return (slot and slot[member_id]) ~= nil
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
    if self:coordinates(conn.group_id) then
        local group  = self.groups[conn.group_id]
        local member = group and group.members[conn.member_id]
        conn.consumer:set_assignment(member and member.partitions or {})
        return
    end
    local slot = self.remote_members[conn.group_id]
    conn.consumer:set_assignment(slot and slot[conn.member_id] or {})
end

-- A member's connection dropping is an implicit LEAVE_GROUP — depart now and
-- rebalance survivors rather than waiting out the heartbeat deadline. The
-- caller (Server:_unregister_conn) guarantees this runs exactly once per
-- connection.
function GroupCoordinator:handle_disconnect(conn)
    if not conn.group_id then return end
    self:leave(conn.group_id, conn.member_id)
    conn.group_id  = nil
    conn.member_id = nil
end

-- One reaper pass: evict members that stopped heartbeating (rebalancing
-- survivors) and drop coordinators with no members, so an idle broker doesn't
-- accumulate empty groups forever (and so max_groups counts only live groups).
-- The server ticks this on group_reaper_interval. Only groups this broker
-- coordinates are reaped — forwarded members are reaped by THEIR coordinator
-- when the forwarded heartbeats stop.
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
