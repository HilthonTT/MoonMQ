local json = require("dkjson")
local fs_m = require("src.io.fs")

local FILE_NAME = "replication-epochs.json"

local EpochCache = {}
EpochCache.__index = EpochCache

local function key(topic, partition)
    return topic .. "\0" .. tostring(partition)
end

EpochCache.key = key

function EpochCache.new(data_dir)
    assert(type(data_dir) == "string", "data_dir must be a string")
    local self = setmetatable({
        path    = fs_m.join_path(data_dir, FILE_NAME),
        entries = {},
        uids    = {},
    }, EpochCache)
    local lerr = self:_load()
    if lerr then return nil, lerr end
    return self
end

function EpochCache:_load()
    local f = io.open(self.path, "rb")
    if not f then return nil end
    local body = f:read("*a") or ""
    f:close()
    if body == "" then return nil end

    local parsed, _, perr = json.decode(body)
    if type(parsed) ~= "table" then
        return string.format("%s: %s", self.path, tostring(perr or "not a JSON object"))
    end

    for _, p in ipairs(parsed.partitions or {}) do
        if type(p) == "table" and type(p.topic) == "string"
           and type(p.partition) == "number" and type(p.epochs) == "table" then
            local list = {}
            for _, e in ipairs(p.epochs) do
                if type(e) == "table" and type(e[1]) == "number" and type(e[2]) == "number" then
                    list[#list + 1] = { epoch = e[1], start = e[2] }
                end
            end
            if #list > 0 then self.entries[key(p.topic, p.partition)] = list end
        end
    end
    for name, uid in pairs(type(parsed.topics) == "table" and parsed.topics or {}) do
        if type(name) == "string" and type(uid) == "string" then self.uids[name] = uid end
    end
    return nil
end

function EpochCache:_save()
    local partitions = {}
    for k, list in pairs(self.entries) do
        local topic, partition = k:match("^(.*)%z(%d+)$")
        local epochs = {}
        for i, e in ipairs(list) do epochs[i] = { e.epoch, e.start } end
        partitions[#partitions + 1] = {
            topic = topic, partition = tonumber(partition), epochs = epochs,
        }
    end
    table.sort(partitions, function(a, b)
        if a.topic ~= b.topic then return a.topic < b.topic end
        return a.partition < b.partition
    end)
    local ok, err = fs_m.atomic_write(self.path,
        json.encode({ partitions = partitions, topics = self.uids }))
    if not ok then
        return nil, string.format("persist leader epochs: %s", tostring(err))
    end
    return true
end

function EpochCache:latest(topic, partition)
    local list = self.entries[key(topic, partition)]
    if not list then return 0, nil end
    local last = list[#list]
    return last.epoch, last.start
end

function EpochCache:epoch_at(topic, partition, offset)
    local list = self.entries[key(topic, partition)]
    if not list then return 0 end
    local found = 0
    for _, e in ipairs(list) do
        if e.start <= offset then found = e.epoch else break end
    end
    return found
end

function EpochCache:has(topic, partition)
    return self.entries[key(topic, partition)] ~= nil
end

function EpochCache:assign(topic, partition, epoch, start)
    assert(type(epoch) == "number" and type(start) == "number")
    local k = key(topic, partition)
    local list = self.entries[k]
    if list and list[#list].epoch >= epoch then return true end

    local prev = list
    local fresh = {}
    for _, e in ipairs(list or {}) do
        if e.start < start then fresh[#fresh + 1] = e end
    end
    fresh[#fresh + 1] = { epoch = epoch, start = start }
    self.entries[k] = fresh

    local ok, err = self:_save()
    if not ok then
        self.entries[k] = prev
        return nil, err
    end
    return true
end

function EpochCache:assign_many(items)
    local changed, prev = false, {}
    for _, it in ipairs(items) do
        local k = key(it.topic, it.partition)
        local list = self.entries[k]
        if not (list and list[#list].epoch >= it.epoch) then
            if prev[k] == nil then prev[k] = list or false end
            local fresh = {}
            for _, e in ipairs(list or {}) do
                if e.start < it.start then fresh[#fresh + 1] = e end
            end
            fresh[#fresh + 1] = { epoch = it.epoch, start = it.start }
            self.entries[k] = fresh
            changed = true
        end
    end
    if not changed then return true end
    local ok, err = self:_save()
    if not ok then
        for k, list in pairs(prev) do self.entries[k] = list or nil end
        return nil, err
    end
    return true
end

function EpochCache:end_offset_for(topic, partition, epoch, log_end)
    local list = self.entries[key(topic, partition)]
    if not list then return epoch, log_end end

    local found
    for i, e in ipairs(list) do
        if e.epoch <= epoch then found = i else break end
    end
    if not found then
        return epoch, list[1].start
    end
    if found == #list then
        return list[found].epoch, log_end
    end
    return list[found].epoch, list[found + 1].start
end

function EpochCache:truncate_from(topic, partition, offset)
    local k = key(topic, partition)
    local list = self.entries[k]
    if not list then return true end
    local kept = {}
    for _, e in ipairs(list) do
        if e.start < offset then kept[#kept + 1] = e end
    end
    if #kept == #list then return true end
    self.entries[k] = (#kept > 0) and kept or nil
    local ok, err = self:_save()
    if not ok then
        self.entries[k] = list
        return nil, err
    end
    return true
end

function EpochCache:clear(topic, partition)
    local k = key(topic, partition)
    local list = self.entries[k]
    if not list then return true end
    self.entries[k] = nil
    local ok, err = self:_save()
    if not ok then
        self.entries[k] = list
        return nil, err
    end
    return true
end

function EpochCache:uid(topic)
    return self.uids[topic]
end

function EpochCache:set_uid(topic, uid)
    if self.uids[topic] == uid then return true end
    local prev = self.uids[topic]
    self.uids[topic] = uid
    local ok, err = self:_save()
    if not ok then
        self.uids[topic] = prev
        return nil, err
    end
    return true
end

function EpochCache:forget_topic(topic)
    local prefix = topic .. "\0"
    local changed = self.uids[topic] ~= nil
    self.uids[topic] = nil
    for k in pairs(self.entries) do
        if k:sub(1, #prefix) == prefix and k:sub(#prefix + 1):match("^%d+$") then
            self.entries[k] = nil
            changed = true
        end
    end
    if not changed then return true end
    return self:_save()
end

EpochCache.FILE_NAME = FILE_NAME
return EpochCache
