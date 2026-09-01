local socket = require("socket")
local http   = require("socket.http")
local ltn12  = require("ltn12")
local json   = require("dkjson")
local tls_m  = require("src.io.tls")

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
        tls     = opts.tls,
    }, Peer)
end

function Peer:_request(method, path, headers, body)
    local timeout = self.timeout
    local response_body = {}

    headers = headers or {}
    headers["Content-Length"] = tostring(#(body or ""))
    if self.token then headers["X-Cluster-Token"] = self.token end
    if self.controller then
        headers["X-Controller-Epoch"] = tostring(self.controller.epoch)
        headers["X-Controller-Id"]    = self.controller.id
    end

    local _, code, _, status = http.request{
        url     = string.format("%s://%s%s",
                                self.tls and "https" or "http", self.address, path),
        method  = method,
        headers = headers,
        source  = body and ltn12.source.string(body) or nil,
        sink    = ltn12.sink.table(response_body),
        create  = self.tls
            and tls_m.http_create(self.tls.params, timeout, self.tls.server_name)
            or function()
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

function Peer:ensure_topic(topic, partitions)
    local resp, err = self:_request("POST", "/cluster/ensure",
        { ["Content-Type"] = "application/json" },
        json.encode({ topic = topic, partitions = partitions }))
    if not resp then return nil, err end
    return true
end

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

function Peer:leo(topic, partition)
    local resp, err = self:_request("GET",
        string.format("/cluster/leo?topic=%s&partition=%d", topic, partition))
    if not resp then return nil, err end
    if type(resp.offset) ~= "number" then
        return nil, string.format("peer %s: response missing offset", self.id)
    end
    return resp.offset
end

function Peer:set_owner(topic, partition, owner)
    local resp, err = self:_request("POST", "/cluster/owner",
        { ["Content-Type"] = "application/json" },
        json.encode({ topic = topic, partition = partition, owner = owner }))
    if not resp then return nil, err end
    return true
end

function Peer:set_controller(epoch, id)
    self.controller = epoch and { epoch = epoch, id = id } or nil
end

function Peer:claim_controller(epoch, id)
    local resp, err = self:_request("POST", "/cluster/controller/claim",
        { ["Content-Type"] = "application/json" },
        json.encode({ epoch = epoch, broker_id = id }))
    if not resp then return nil, err end
    if resp.accepted == true then return true, resp.highest end
    return false, resp.highest, resp.reason
end

function Peer:push_offsets(topic, partition, offsets)
    local resp, err = self:_request("POST", "/cluster/offsets",
        { ["Content-Type"] = "application/json" },
        json.encode({ topic = topic, partition = partition, offsets = offsets }))
    if not resp then return nil, err end
    return resp.applied or 0
end

function Peer:txn_enroll(txn_id, topic, partition, first_offset)
    local resp, err = self:_request("POST", "/cluster/txn/enroll",
        { ["Content-Type"] = "application/json" },
        json.encode({ txn = txn_id, topic = topic, partition = partition,
                      first_offset = first_offset }))
    if not resp then return nil, err end
    return true
end

function Peer:txn_resolve(txn_id, topic, partition, opts)
    opts = opts or {}
    local resp, err = self:_request("POST", "/cluster/txn/resolve",
        { ["Content-Type"] = "application/json" },
        json.encode({ txn = txn_id, topic = topic, partition = partition,
                      aborted = opts.aborted or false,
                      pid = opts.pid, epoch = opts.epoch,
                      first = opts.first, upto = opts.upto }))
    if not resp then return nil, err end
    return true
end

function Peer:group_join(group_id, member_id, topics, origin)
    local resp, err = self:_request("POST", "/cluster/group/join",
        { ["Content-Type"] = "application/json" },
        json.encode({ group = group_id, member = member_id,
                      topics = topics, origin = origin }))
    if not resp then return nil, err, "internal" end
    if resp.ok == false then
        return nil, resp.reason or "join refused", resp.code or "internal"
    end
    if type(resp.assignment) ~= "table" then
        return nil, string.format("peer %s: response missing assignment", self.id),
            "internal"
    end
    return resp.assignment
end

function Peer:group_heartbeat(group_id, member_id)
    local resp, err = self:_request("POST", "/cluster/group/heartbeat",
        { ["Content-Type"] = "application/json" },
        json.encode({ group = group_id, member = member_id }))
    if not resp then return nil, err end
    if resp.ok == false then return nil, resp.reason or "membership lapsed" end
    return resp.assignment or {}
end

function Peer:group_leave(group_id, member_id)
    local resp, err = self:_request("POST", "/cluster/group/leave",
        { ["Content-Type"] = "application/json" },
        json.encode({ group = group_id, member = member_id }))
    if not resp then return nil, err end
    return true
end

function Peer:loads()
    local resp, err = self:_request("GET", "/cluster/loads")
    if not resp then return nil, err end
    if type(resp.loads) ~= "table" then
        return nil, string.format("peer %s: response missing loads", self.id)
    end
    return resp.loads
end

return Peer
