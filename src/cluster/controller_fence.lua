local json    = require("dkjson")
local fs_m    = require("src.io.fs")

local FILE_NAME = "controller-epoch.json"
local MAX_EPOCH = (1 << 53) - 1

local ControllerFence = {}
ControllerFence.__index = ControllerFence

local function valid_epoch(epoch, min)
    return math.type(epoch) == "integer" and epoch >= min and epoch <= MAX_EPOCH
end

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
    if not valid_epoch(parsed.epoch, 0) then
        return string.format("%s: epoch must be an integer in [0, %d]", self.path, MAX_EPOCH)
    end
    if parsed.claimant ~= nil and type(parsed.claimant) ~= "string" then
        return string.format("%s: claimant must be a string", self.path)
    end
    self.epoch    = parsed.epoch
    self.claimant = parsed.claimant
    return nil
end

function ControllerFence:_save()
    return fs_m.atomic_write(self.path,
        json.encode({ epoch = self.epoch, claimant = self.claimant },
            { indent = true }))
end

function ControllerFence:highest()
    return self.epoch, self.claimant
end

function ControllerFence:observe(epoch, claimant)
    if not valid_epoch(epoch, 1) then
        return nil, "bad controller epoch"
    end
    if claimant ~= nil and type(claimant) ~= "string" then
        return nil, "bad controller claimant"
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
    if self.epoch >= MAX_EPOCH then
        return nil, "controller epoch exhausted"
    end
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
