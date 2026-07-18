-- ControllerFence — epoch-based fencing for the balance-loop controller.
--
-- The balance loop was "one controller by convention": nothing stopped two
-- brokers from both running it and executing competing plans. This module
-- closes that hazard without a consensus protocol, in the same spirit as the
-- rest of the cluster layer (static membership, documented boundaries):
--
--   * Every broker durably tracks the highest controller epoch it has seen
--     and who claimed it (<data_dir>/controller-epoch.json).
--   * A broker that wants to act as controller CLAIMS epoch = highest + 1 on
--     itself and broadcasts the claim to every peer (POST
--     /cluster/controller/claim).
--   * Cluster mutations (append/ensure/owner/offsets) may carry the
--     controller's epoch; a receiving broker REJECTS any epoch below its
--     highest, or equal but from a different claimant. A superseded
--     controller therefore cannot land a migration batch anywhere that has
--     seen the newer claim — its move fails before the ownership cutover.
--
-- This fences the losing controller wherever the newer claim has propagated.
-- It is NOT consensus: two brokers claiming at the same instant against
-- disjoint reachable peers can both believe they hold the crown until their
-- claims meet — at which point the higher epoch (ties: first claim seen)
-- wins. Requests carrying no epoch (older peers, hand-driven reassignment)
-- bypass the fence, preserving backward compatibility.

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
    if not f then return nil end   -- never fenced: epoch 0
    local body = f:read("*a") or ""
    f:close()
    if body == "" then return nil end
    local parsed, _, perr = json.decode(body)
    if type(parsed) ~= "table" or type(parsed.epoch) ~= "number" then
        -- A lost fence file would let a stale controller act again — refuse
        -- to boot rather than silently resetting to epoch 0.
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

-- observe validates a claimed epoch against the highest seen, advancing (and
-- persisting) when the claim is newer. Returns (true, nil) when the claimant
-- may act, (nil, err) when it is stale. Equal epoch is honoured only for the
-- SAME claimant (its own later requests); a rival claiming an already-taken
-- epoch is rejected — it must claim higher.
function ControllerFence:observe(epoch, claimant)
    if type(epoch) ~= "number" or epoch < 1 or epoch % 1 ~= 0 then
        return nil, "bad controller epoch"
    end
    if epoch > self.epoch then
        local prev_e, prev_c = self.epoch, self.claimant
        self.epoch, self.claimant = epoch, claimant
        local ok, serr = self:_save()
        if not ok then
            -- Couldn't make the new epoch durable: don't grant it, or a
            -- restart would resurrect the epoch we just fenced.
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

-- claim takes the next epoch for `claimant` on THIS broker and persists it.
-- The caller is responsible for broadcasting the claim to peers. Returns
-- (epoch, nil) or (nil, err).
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
