-- HTTP client for one cluster peer — the transport half of the reassignment
-- layer. One Peer per remote broker, speaking to that broker's cluster_server.
-- Mirrors ReplicaClient's shape (per-request socket timeout, JSON responses)
-- so tests can swap in a duck-typed local mock; the Reassigner and the produce
-- router only rely on the method surface, never on HTTP.

local socket = require("socket")
local http   = require("socket.http")
local ltn12  = require("ltn12")
local json   = require("dkjson")

local DEFAULT_TIMEOUT = 5

local Peer = {}
Peer.__index = Peer

function Peer.new(id, address, opts)
    assert(type(id) == "string", "peer id must be a string")
    assert(type(address) == "string", "peer address must be a string")
    opts = opts or {}

    return setmetatable({
        id      = id,
        address = address,
        token   = opts.token,
        timeout = opts.timeout or DEFAULT_TIMEOUT,
    }, Peer)
end

-- One HTTP round trip. Returns (decoded_json_table, nil) or (nil, err).
function Peer:_request(method, path, headers, body)
    local timeout = self.timeout
    local response_body = {}

    headers = headers or {}
    headers["Content-Length"] = tostring(#(body or ""))
    if self.token then headers["X-Cluster-Token"] = self.token end
    -- Controller identity (set via set_controller): lets the peer fence this
    -- request if it knows of a newer controller.
    if self.controller then
        headers["X-Controller-Epoch"] = tostring(self.controller.epoch)
        headers["X-Controller-Id"]    = self.controller.id
    end

    local _, code, _, status = http.request{
        url     = string.format("http://%s%s", self.address, path),
        method  = method,
        headers = headers,
        source  = body and ltn12.source.string(body) or nil,
        sink    = ltn12.sink.table(response_body),
        create  = function()
            local s = socket.tcp()
            s:settimeout(timeout)
            return s
        end,
    }

    if type(code) ~= "number" then
        return nil, string.format("peer %s unreachable: %s",
            self.id, tostring(code or status))
    end
    local text = table.concat(response_body)
    if code ~= 200 then
        return nil, string.format("peer %s HTTP %d: %s", self.id, code, text)
    end
    local parsed, _, perr = json.decode(text)
    if type(parsed) ~= "table" then
        return nil, string.format("peer %s: invalid JSON: %s", self.id, tostring(perr))
    end
    return parsed, nil
end

-- Make sure the topic exists on the peer with at least `partitions`
-- partitions. Idempotent. Returns (true, nil) or (nil, err).
function Peer:ensure_topic(topic, partitions)
    local resp, err = self:_request("POST", "/cluster/ensure",
        { ["Content-Type"] = "application/json" },
        json.encode({ topic = topic, partitions = partitions }))
    if not resp then return nil, err end
    return true
end

-- Append one or more serialized records (concatenated len(8)|body frames, as
-- produced by message.serialize_message) to the peer's partition. `forwarded`
-- marks the batch as forwarded PRODUCE traffic (counted by the owner's NW_IN
-- feed) rather than a migration copy. Returns (peer_leo, nil) or (nil, err).
function Peer:append(topic, partition, payload, forwarded)
    local headers = {
        ["Content-Type"] = "application/octet-stream",
        ["X-Topic"]      = topic,
        ["X-Partition"]  = tostring(partition),
    }
    if forwarded then headers["X-Forwarded-Produce"] = "1" end
    local resp, err = self:_request("POST", "/cluster/append", headers, payload)
    if not resp then return nil, err end
    if type(resp.offset) ~= "number" then
        return nil, string.format("peer %s: response missing offset", self.id)
    end
    return resp.offset
end

-- Log-end offset of the peer's partition. Returns (leo, nil) or (nil, err).
function Peer:leo(topic, partition)
    local resp, err = self:_request("GET",
        string.format("/cluster/leo?topic=%s&partition=%d", topic, partition))
    if not resp then return nil, err end
    if type(resp.offset) ~= "number" then
        return nil, string.format("peer %s: response missing offset", self.id)
    end
    return resp.offset
end

-- Record on the peer that `owner` now owns (topic, partition). Returns
-- (true, nil) or (nil, err).
function Peer:set_owner(topic, partition, owner)
    local resp, err = self:_request("POST", "/cluster/owner",
        { ["Content-Type"] = "application/json" },
        json.encode({ topic = topic, partition = partition, owner = owner }))
    if not resp then return nil, err end
    return true
end

-- set_controller attaches a controller identity to every subsequent request
-- from this Peer, so the remote can fence a superseded controller. Pass nils
-- to clear.
function Peer:set_controller(epoch, id)
    self.controller = epoch and { epoch = epoch, id = id } or nil
end

-- claim_controller announces `(epoch, id)` as controller to this peer.
-- Returns (true, highest) when accepted, (false, highest, reason) when the
-- peer knows a newer claim, (nil, err) when unreachable.
function Peer:claim_controller(epoch, id)
    local resp, err = self:_request("POST", "/cluster/controller/claim",
        { ["Content-Type"] = "application/json" },
        json.encode({ epoch = epoch, broker_id = id }))
    if not resp then return nil, err end
    if resp.accepted == true then return true, resp.highest end
    return false, resp.highest, resp.reason
end

-- Ship committed consumer offsets for one migrated partition to the peer.
-- `offsets` is { [group] = offset }. The peer applies each commit unless it
-- already holds a higher one. Returns (applied_count, nil) or (nil, err).
function Peer:push_offsets(topic, partition, offsets)
    local resp, err = self:_request("POST", "/cluster/offsets",
        { ["Content-Type"] = "application/json" },
        json.encode({ topic = topic, partition = partition, offsets = offsets }))
    if not resp then return nil, err end
    return resp.applied or 0
end

-- Per-partition load report for the autobalancer's cluster model. Returns
-- (array of { topic, partition, disk_bytes }, nil) or (nil, err).
function Peer:loads()
    local resp, err = self:_request("GET", "/cluster/loads")
    if not resp then return nil, err end
    if type(resp.loads) ~= "table" then
        return nil, string.format("peer %s: response missing loads", self.id)
    end
    return resp.loads
end

return Peer
