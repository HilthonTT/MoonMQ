local groups_m = require("src.broker.groups")
local log      = require("src.log.logger").get("group_coordinator")
local hash_m = require("src.core.hash")

local GroupCoordinator = {}
GroupCoordinator.__index = GroupCoordinator

function GroupCoordinator.new(broker, opts)
    opts = opts or {}
    local self = setmetatable({
        broker = broker,
        groups = {},
        max_groups = assert(opts.max_groups, "opts.max_groups required"),
        cluster    = opts.cluster,
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

function GroupCoordinator:coordinator_for(group_id)
    if not self.cluster then return nil end
    local ids = self.cluster_ids
    return ids[(hash_m.fnv1a(group_id) % #ids) + 1]
end

function GroupCoordinator:coordinates(group_id)
    local c = self:coordinator_for(group_id)
    return c == nil or c == self.cluster.self_id
end

function GroupCoordinator:get(group_id)
    return self.groups[group_id]
end

function GroupCoordinator:get_or_create(group_id)
    local group = self.groups[group_id]
    if not group then
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
        slot[member_id] = nil
        return nil, err
    end
    slot[member_id] = assignment
    return true
end

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

function GroupCoordinator:member_alive(group_id, member_id)
    if self:coordinates(group_id) then
        local group = self.groups[group_id]
        return (group and group.members[member_id]) ~= nil
    end
    local slot = self.remote_members[group_id]
    return (slot and slot[member_id]) ~= nil
end

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

function GroupCoordinator:handle_disconnect(conn)
    if not conn.group_id then return end
    self:leave(conn.group_id, conn.member_id)
    conn.group_id  = nil
    conn.member_id = nil
end

function GroupCoordinator:list()
    local seen = {}
    local out  = {}

    for group_id, group in pairs(self.groups) do
        seen[group_id] = true
        out[#out + 1] = {
            group_id     = group_id,
            state        = group:state(),
            member_count = group:member_count(),
        }
    end

    local offsets = self.broker.offsets
    if offsets then
        for _, group_id in ipairs(offsets:groups()) do
            if not seen[group_id] then
                out[#out + 1] = {
                    group_id = group_id, state = "empty", member_count = 0,
                }
            end
        end
    end

    table.sort(out, function(a, b) return a.group_id < b.group_id end)
    return out
end

function GroupCoordinator:describe(group_id)
    assert(type(group_id) == "string", "group_id must be a string")

    local offsets = self.broker.offsets
    local committed = offsets and offsets:offsets_for_group(group_id) or {}

    local group = self.groups[group_id]
    if not group and #committed == 0 then
        return nil, string.format("group '%s' does not exist", group_id)
    end

    local desc
    if group then
        desc = group:describe()
    else
        desc = { group_id = group_id, state = "empty", members = {} }
    end
    desc.offsets = committed
    return desc, nil
end

function GroupCoordinator:delete(group_id)
    assert(type(group_id) == "string", "group_id must be a string")

    local group = self.groups[group_id]
    if group and group:member_count() > 0 then
        return nil, string.format(
            "group '%s' still has %d member(s)", group_id, group:member_count()), true
    end

    local offsets = self.broker.offsets
    local committed = offsets and offsets:offsets_for_group(group_id) or {}
    if not group and #committed == 0 then
        return nil, string.format("group '%s' does not exist", group_id)
    end

    local n = 0
    if offsets then
        local deleted, derr = offsets:delete_group(group_id)
        if not deleted then return nil, derr end
        n = deleted
    end

    if group then
        group:close()
        self.groups[group_id] = nil
    end
    self.remote_members[group_id] = nil

    return n, nil
end

function GroupCoordinator:forget_topic(topic_name)
    assert(type(topic_name) == "string", "topic_name must be a string")

    local n = 0
    for _, group in pairs(self.groups) do
        if group:forget_topic(topic_name) then n = n + 1 end
    end
    return n
end

function GroupCoordinator:reap()
    for gid, group in pairs(self.groups) do
        group:check_heartbeats()
        if next(group.members) == nil then
            self.groups[gid] = nil
        end
    end
end

return GroupCoordinator
