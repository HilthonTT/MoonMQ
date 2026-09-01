local json    = require("dkjson")
local fs_m    = require("src.io.fs")
local io_sync = require("src.io.io_sync")

local FILE_NAME = "controller-epoch.json"

local ControllerFence = {}
ControllerFence.__index = ControllerFence

function ControllerFence.new(data_dir)
    assert(type(data_dir) == "string", "data_dir must be a string")
    local self = setmetatable({
        path     = fs_m.join_path(data_dir, FILE_NAME),
        epoch    = 0,
        claimant = nil,
    }, ControllerFence)
    local lerr = self:_load()
    if lerr then return nil, lerr end
    return self
end

function ControllerFence:_load()
    local f = io.open(self.path, "rb")
    if not f then return nil end
    local body = f:read("*a") or ""
    f:close()
    if body == "" then return nil end
    local parsed, _, perr = json.decode(body)
    if type(parsed) ~= "table" or type(parsed.epoch) ~= "number" then
        return string.format("%s: %s", self.path, tostring(perr or "not a JSON object"))
    end
    self.epoch    = parsed.epoch
    self.claimant = parsed.claimant
    return nil
end

function ControllerFence:_save()
    local tmp = self.path .. ".tmp"
    local f, ferr = io.open(tmp, "wb")
    if not f then return nil, ferr end
    f:write(json.encode({ epoch = self.epoch, claimant = self.claimant },
        { indent = true }))
    f:flush()
    f:close()
    return io_sync.atomic_rename(tmp, self.path)
end

function ControllerFence:highest()
    return self.epoch, self.claimant
end

function ControllerFence:observe(epoch, claimant)
    if type(epoch) ~= "number" or epoch < 1 or epoch % 1 ~= 0 then
        return nil, "bad controller epoch"
    end
    if epoch > self.epoch then
        local prev_e, prev_c = self.epoch, self.claimant
        self.epoch, self.claimant = epoch, claimant
        local ok, serr = self:_save()
        if not ok then
            self.epoch, self.claimant = prev_e, prev_c
            return nil, string.format("persist controller epoch: %s", tostring(serr))
        end
        return true
    end
    if epoch == self.epoch and claimant ~= nil and claimant == self.claimant then
        return true
    end
    return nil, string.format(
        "stale controller epoch %d (current %d held by %s)",
        epoch, self.epoch, tostring(self.claimant))
end

function ControllerFence:claim(claimant)
    assert(type(claimant) == "string", "claimant must be a string")
    local prev_e, prev_c = self.epoch, self.claimant
    self.epoch, self.claimant = self.epoch + 1, claimant
    local ok, serr = self:_save()
    if not ok then
        self.epoch, self.claimant = prev_e, prev_c
        return nil, string.format("persist controller epoch: %s", tostring(serr))
    end
    return self.epoch
end

ControllerFence.FILE_NAME = FILE_NAME
return ControllerFence
