-- AbortIndex — the durable record of which transactional writes were aborted,
-- per partition. The read_committed consumer path uses it to filter aborted
-- data records out below the LSO (see docs/transactions.md §2).
--
-- One entry per (aborted txn, participant partition):
--   { pid, epoch, first, upto }
-- meaning: records in [first, upto) of that partition written by producer
-- session (pid, epoch) with the transactional attr bit set belong to an
-- aborted transaction. `first` is the partition LEO captured when the txn
-- enrolled the partition (before its first append there), `upto` the offset
-- of the ABORT marker — so the range is conservative-but-safe: it can only
-- cover records of that exact producer session, and a session has at most
-- one transaction in flight at a time.
--
-- Persisted at <data_dir>/txn-aborts.json (atomic tmp+rename, like
-- cluster-assignments.json) and loaded on boot. Entries are pruned when their
-- whole range has aged out below the partition's oldest retained offset.

local json    = require("dkjson")
local fs_m    = require("src.io.fs")
local io_sync = require("src.io.io_sync")

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
        map  = {},   -- key(topic, partition) -> array of { pid, epoch, first, upto }
    }, AbortIndex)
    local lerr = self:_load()
    if lerr then return nil, lerr end
    return self
end

function AbortIndex:_load()
    local f = io.open(self.path, "rb")
    if not f then return nil end   -- no file: nothing was ever aborted
    local body = f:read("*a") or ""
    f:close()
    if body == "" then return nil end

    local parsed, _, perr = json.decode(body)
    if type(parsed) ~= "table" then
        -- A malformed abort index would silently un-filter aborted records
        -- for read_committed consumers — refuse to boot instead.
        return string.format("%s: %s", self.path, tostring(perr or "not a JSON object"))
    end
    for _, e in ipairs(parsed.entries or {}) do
        if type(e.topic) == "string" and type(e.partition) == "number"
            and type(e.pid) == "number" and type(e.first) == "number"
            and type(e.upto) == "number" then
            local k = key(e.topic, e.partition)
            local list = self.map[k]
            if not list then list = {}; self.map[k] = list end
            list[#list + 1] = {
                pid = e.pid, epoch = e.epoch or 0, first = e.first, upto = e.upto,
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

    local tmp = self.path .. ".tmp"
    local f, ferr = io.open(tmp, "wb")
    if not f then return nil, ferr end
    f:write(json.encode({ entries = entries }, { indent = true }))
    f:flush()
    f:close()
    return io_sync.atomic_rename(tmp, self.path)
end

-- add records an aborted range. Duplicate entries (crash-recovery re-drives
-- the abort finish) are collapsed so retries don't grow the file. Returns
-- (true, nil) or (nil, err); on save failure the in-memory add is rolled back
-- so a restart never knows less than what this process serves from.
function AbortIndex:add(topic, partition, pid, epoch, first, upto)
    assert(type(topic) == "string" and type(partition) == "number")
    assert(type(pid) == "number" and type(first) == "number" and type(upto) == "number")
    if upto <= first then return true end   -- empty range: txn wrote nothing here

    local k = key(topic, partition)
    local list = self.map[k]
    if not list then list = {}; self.map[k] = list end
    for _, e in ipairs(list) do
        if e.pid == pid and e.epoch == epoch and e.first == first and e.upto == upto then
            return true   -- already recorded (recovery retry)
        end
    end
    list[#list + 1] = { pid = pid, epoch = epoch or 0, first = first, upto = upto }

    local ok, err = self:_save()
    if not ok then
        list[#list] = nil
        if #list == 0 then self.map[k] = nil end
        return nil, string.format("persist abort index: %s", tostring(err))
    end
    return true
end

-- is_aborted reports whether the record at `offset` written by producer
-- session (pid, epoch) falls in an aborted range of (topic, partition).
function AbortIndex:is_aborted(topic, partition, pid, epoch, offset)
    local list = self.map[key(topic, partition)]
    if not list then return false end
    for _, e in ipairs(list) do
        if e.pid == pid and e.epoch == (epoch or 0)
            and offset >= e.first and offset < e.upto then
            return true
        end
    end
    return false
end

-- prune drops entries whose whole range is below `oldest` (retention already
-- deleted every record they could match). Persisted lazily: a failed save
-- keeps the (harmless) extra entries in memory for next time.
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

-- entries exposes the raw list for (topic, partition) — tests/observability.
function AbortIndex:entries(topic, partition)
    return self.map[key(topic, partition)] or {}
end

AbortIndex.FILE_NAME = FILE_NAME
return AbortIndex
