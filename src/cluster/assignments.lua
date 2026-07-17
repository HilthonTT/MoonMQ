-- Durable partition-ownership table: which broker in the cluster owns each
-- topic-partition. This is the source of truth the produce router and the
-- reassigner consult; flipping an entry here IS the reassignment cutover.
--
-- Ownership is tracked sparsely: a partition with no entry is owned by the
-- local broker (`self_id`). That keeps the single-broker deployment zero-config
-- — the file only exists once a reassignment has happened.
--
-- Persisted at <data_dir>/cluster-assignments.json, written atomically
-- (tmp + rename) like the topic.config sidecar, and loaded on boot so
-- ownership survives a restart.

local json    = require("dkjson")
local fs_m    = require("src.io.fs")
local io_sync = require("src.io.io_sync")

local FILE_NAME = "cluster-assignments.json"

local Assignments = {}
Assignments.__index = Assignments

local function key(topic, partition)
    return topic .. "\0" .. tostring(partition)
end

function Assignments.new(data_dir, self_id)
    assert(type(data_dir) == "string", "data_dir must be a string")
    assert(type(self_id) == "string", "self_id must be a string")

    local self = setmetatable({
        path    = fs_m.join_path(data_dir, FILE_NAME),
        self_id = self_id,
        map     = {},   -- key(topic, partition) -> broker_id
    }, Assignments)

    local lerr = self:_load()
    if lerr then return nil, lerr end
    return self
end

function Assignments:_load()
    local f = io.open(self.path, "rb")
    if not f then return nil end   -- no file: everything is local (fresh node)
    local body = f:read("*a") or ""
    f:close()
    if body == "" then return nil end

    local parsed, _, perr = json.decode(body)
    if type(parsed) ~= "table" then
        -- Malformed ownership data is a routing hazard — refuse to boot rather
        -- than silently treating moved partitions as local again.
        return string.format("%s: %s", self.path, tostring(perr or "not a JSON object"))
    end
    for _, e in ipairs(parsed.entries or {}) do
        if type(e.topic) == "string" and type(e.partition) == "number"
            and type(e.owner) == "string" then
            self.map[key(e.topic, e.partition)] = e.owner
        end
    end
    return nil
end

function Assignments:_save()
    local entries = {}
    for k, owner in pairs(self.map) do
        local topic, partition = k:match("^(.*)%z(%d+)$")
        entries[#entries + 1] = {
            topic = topic, partition = tonumber(partition), owner = owner,
        }
    end
    -- Stable order so the file diffs cleanly between saves.
    table.sort(entries, function(a, b)
        if a.topic ~= b.topic then return a.topic < b.topic end
        return a.partition < b.partition
    end)

    local tmp = self.path .. ".tmp"
    local f, ferr = io.open(tmp, "wb")
    if not f then return nil, ferr end
    f:write(json.encode({ entries = entries }, { indent = true }))
    f:flush()
    f:close()
    return io_sync.atomic_rename(tmp, self.path)
end

function Assignments:owner(topic, partition)
    return self.map[key(topic, partition)] or self.self_id
end

function Assignments:owned_by_self(topic, partition)
    return self:owner(topic, partition) == self.self_id
end

-- Flip ownership and persist. Setting the owner back to self removes the
-- sparse entry (keeps the file minimal). Returns (true, nil) or (nil, err) —
-- and on save failure the in-memory flip is rolled back, so routing never
-- diverges from what a restart would reload.
function Assignments:set_owner(topic, partition, owner)
    assert(type(topic) == "string", "topic must be a string")
    assert(type(partition) == "number", "partition must be a number")
    assert(type(owner) == "string", "owner must be a string")

    local k = key(topic, partition)
    local prev = self.map[k]
    self.map[k] = (owner ~= self.self_id) and owner or nil

    local ok, err = self:_save()
    if not ok then
        self.map[k] = prev
        return nil, string.format("persist assignments: %s", tostring(err))
    end
    return true
end

-- All non-local entries, as an array of { topic, partition, owner }.
function Assignments:entries()
    local out = {}
    for k, owner in pairs(self.map) do
        local topic, partition = k:match("^(.*)%z(%d+)$")
        out[#out + 1] = { topic = topic, partition = tonumber(partition), owner = owner }
    end
    return out
end

Assignments.FILE_NAME = FILE_NAME
return Assignments
