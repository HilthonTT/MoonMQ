local json    = require("dkjson")
local fs_m    = require("src.io.fs")

local FILE_NAME = "txn-aborts.json"

local AbortIndex = {}
AbortIndex.__index = AbortIndex

local function key(topic, partition)
    return topic .. "\0" .. tostring(partition)
end

function AbortIndex.new(data_dir)
    assert(type(data_dir) == "string", "data_dir must be a string")
    local self = setmetatable({
        path = fs_m.join_path(data_dir, FILE_NAME),
        map  = {},
    }, AbortIndex)
    local lerr = self:_load()
    if lerr then return nil, lerr end
    return self
end

function AbortIndex:_load()
    local f = io.open(self.path, "rb")
    if not f then return nil end
    local body = f:read("*a") or ""
    f:close()
    if body == "" then return nil end

    local parsed, _, perr = json.decode(body)
    if type(parsed) ~= "table" then
        return string.format("%s: %s", self.path, tostring(perr or "not a JSON object"))
    end
    for _, e in ipairs(parsed.entries or {}) do
        if type(e.topic) == "string" and type(e.partition) == "number"
            and type(e.pid) == "number" and type(e.first) == "number"
            and type(e.upto) == "number" then
            local k = key(e.topic, e.partition)
            local list = self.map[k]
            if not list then list = {}; self.map[k] = list end
            local epoch = e.epoch
            if type(epoch) ~= "number" then epoch = 0 end
            list[#list + 1] = {
                pid = e.pid, epoch = epoch, first = e.first, upto = e.upto,
            }
        end
    end
    return nil
end

function AbortIndex:_save()
    local entries = {}
    for k, list in pairs(self.map) do
        local topic, partition = k:match("^(.*)%z(%d+)$")
        for _, e in ipairs(list) do
            entries[#entries + 1] = {
                topic = topic, partition = tonumber(partition),
                pid = e.pid, epoch = e.epoch, first = e.first, upto = e.upto,
            }
        end
    end
    table.sort(entries, function(a, b)
        if a.topic ~= b.topic then return a.topic < b.topic end
        if a.partition ~= b.partition then return a.partition < b.partition end
        return a.first < b.first
    end)

    return fs_m.atomic_write(self.path,
        json.encode({ entries = entries }, { indent = true }))
end

function AbortIndex:add(topic, partition, pid, epoch, first, upto)
    assert(type(topic) == "string" and type(partition) == "number")
    assert(type(pid) == "number" and type(first) == "number" and type(upto) == "number")
    assert(type(epoch) == "number", "epoch must be a number")
    if upto <= first then return true end

    local k = key(topic, partition)
    local list = self.map[k]
    if not list then list = {}; self.map[k] = list end
    for _, e in ipairs(list) do
        if e.pid == pid and e.epoch == epoch and e.first == first and e.upto == upto then
            return true
        end
    end
    list[#list + 1] = { pid = pid, epoch = epoch, first = first, upto = upto }

    local ok, err = self:_save()
    if not ok then
        list[#list] = nil
        if #list == 0 then self.map[k] = nil end
        return nil, string.format("persist abort index: %s", tostring(err))
    end
    return true
end

function AbortIndex:is_aborted(topic, partition, pid, epoch, offset)
    local list = self.map[key(topic, partition)]
    if not list then return false end
    for _, e in ipairs(list) do
        if e.pid == pid and e.epoch == epoch
            and offset >= e.first and offset < e.upto then
            return true
        end
    end
    return false
end

function AbortIndex:prune(topic, partition, oldest)
    local k = key(topic, partition)
    local list = self.map[k]
    if not list then return end
    local kept, dropped = {}, 0
    for _, e in ipairs(list) do
        if e.upto > oldest then kept[#kept + 1] = e else dropped = dropped + 1 end
    end
    if dropped == 0 then return end
    self.map[k] = (#kept > 0) and kept or nil
    self:_save()
end

function AbortIndex:entries(topic, partition)
    return self.map[key(topic, partition)] or {}
end

AbortIndex.FILE_NAME = FILE_NAME
return AbortIndex
