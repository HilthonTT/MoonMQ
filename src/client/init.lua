local socket = require("socket")
local proto = require("src.server.protocol")
local uuid = require("src.server.uuid")

local DEFAULT_TIMEOUT = 30

local Client = {}
Client.__index = Client

function Client.new(opts) 
    opts = opts or {}
    local host = opts.host or "127.0.0.1"
    local port = opts.port or 9092

    local sock, cerr = socket.connect(host, port)
    if not sock then
        return nil, string.format("connect %s:%d: %s", host, port, tostring(cerr))
    end

    local c = setmetatable({
        sock = sock,
        reactor = opts.reactor,
        timeout = opts.timeout or DEFAULT_TIMEOUT,
        host = host,
        port = port,
        closed = false,
        push_handler = nil,
        -- Idempotent producer state. pid is assigned by the broker via
        -- INIT_PRODUCER_ID if opts.idempotent is true; once set, every
        -- subsequent :produce() routes through OP_PRODUCE_IDEMPOTENT
        -- and carries the next per-topic sequence number. Retries of
        -- the same (topic, seq) get the original (partition, offset)
        -- back from the broker — at-most-once delivery without
        -- application-level dedup.
        pid = nil,
        next_seq = {},  -- topic -> next u32 seq to send
        -- Consumer-group membership, populated by join_group(). The
        -- broker-assigned member_id is reused by group_heartbeat/leave_group
        -- so callers don't have to thread it through every call.
        group_id = nil,
        member_id = nil,
    }, Client)

    if not c.reactor then
        sock:settimeout(c.timeout)
    end

    local hcorrel = uuid.bytes()
    local ok, err = c:_write(proto.encode_hello(hcorrel))
    if not ok then c:close(); return nil, "send hello: " .. err end
 
    local op, _, payload, rerr = c:_read_until(hcorrel)
    if not op then c:close(); return nil, "read welcome: " .. rerr end
    if op == proto.OP_ERROR then
        local e = proto.decode_error(payload)
        c:close()
        return nil, "hello: " .. (e and e.message or "?")
    end
    if op ~= proto.OP_WELCOME then
        c:close()
        return nil, string.format("expected WELCOME, got 0x%02x", op)
    end

    -- IDENTIFY_CLIENT (optional)
    if opts.client_name then
        local idc = uuid.bytes()
        c:_write(proto.encode_identify_client(idc,
            opts.client_name, opts.client_version or "0.0.0"))
        c:_read_until(idc)  -- ack is informational, ignore content
    end
 
    --  AUTH (skip if no username — server may be in OPEN mode)
    if opts.username then
        local acorrel = uuid.bytes()
        ok, err = c:_write(proto.encode_auth(acorrel,
            opts.username, opts.password or ""))
        if not ok then c:close(); return nil, "send auth: " .. err end

        op, _, payload, rerr = c:_read_until(acorrel)
        if not op then c:close(); return nil, "read auth_ok: " .. rerr end
        if op == proto.OP_ERROR then
            local e = proto.decode_error(payload)
            c:close()
            return nil, "auth: " .. (e and e.message or "?")
        end
        if op ~= proto.OP_AUTH_OK then
            c:close()
            return nil, string.format("expected AUTH_OK, got 0x%02x", op)
        end
    end

    -- Idempotent producer init (optional). On reconnect the caller must
    -- pass idempotent=true again — PIDs are not durable across the
    -- broker process or this socket. See docs/transactions.md for the
    -- coordinator-backed design that would make PIDs survivable.
    if opts.idempotent then
        local icorrel = uuid.bytes()
        ok, err = c:_write(proto.encode_init_producer_id(icorrel))
        if not ok then c:close(); return nil, "send init_producer_id: " .. err end

        op, _, payload, rerr = c:_read_until(icorrel)
        if not op then c:close(); return nil, "read producer_id: " .. rerr end
        if op == proto.OP_ERROR then
            local e = proto.decode_error(payload)
            c:close()
            return nil, "init_producer_id: " .. (e and e.message or "?")
        end
        if op ~= proto.OP_PRODUCER_ID then
            c:close()
            return nil, string.format("expected PRODUCER_ID, got 0x%02x", op)
        end
        local pinfo, perr = proto.decode_producer_id(payload)
        if not pinfo then
            c:close()
            return nil, "decode producer_id: " .. perr
        end
        c.pid = pinfo.pid
    end

    return c
end

function Client:_write(data)
    if self.closed then return nil, "closed" end

    if self.reactor then
        return self.reactor:send_all(self.sock, data, nil)
    end
    local sent, err = self.sock:send(data)
    if err then return nil, err end
    return sent == #data, nil
end

function Client:_read_bytes(n)
    if self.closed then return nil, "closed" end
    if self.reactor then
        return self.reactor:read_exact(self.sock, n, nil)
    end
    return self.sock:receive(n)
end

function Client:_read_frame()
    local len_bytes, err = self:_read_bytes(4)
    if not len_bytes then return nil, nil, nil, err end
    local frame_len = string.unpack(">I4", len_bytes)
    if frame_len > 16 * 1024 * 1024 then
        return nil, nil, nil, "frame too large from server"
    end
    local body, berr = self:_read_bytes(frame_len)
    if not body then return nil, nil, nil, berr end
    return proto.parse_frame(body)
end
 
-- Reads frames until one matches `target_correl`. Heartbeat requests
-- get replied inline (so a long fetch keeps the connection alive).
-- Push RECORD frames (correl == uuid.ZERO) are passed to push_handler
-- if registered, dropped otherwise.
function Client:_read_until(target_correl)
    while true do
        local op, c, payload, err = self:_read_frame()
        if not op then return nil, nil, nil, err end
 
        if op == proto.OP_HEARTBEAT_REQ then
            self:_write(proto.encode_heartbeat_resp(c))
        elseif op == proto.OP_HEARTBEAT_RESP then
            -- Server responding to a probe we didn't send; ignore.
        elseif c == target_correl then
            return op, c, payload, nil
        elseif c == uuid.ZERO and op == proto.OP_RECORD and self.push_handler then
            local rec = self:_decode_record(payload)
            if rec then self.push_handler(rec) end
        end
        -- Anything else (stray correl IDs, unexpected ops) — drop and continue.
    end
end

function Client:_decode_record(payload)
    local topic, p, err = proto.decode_string(payload, 1)
    if not topic then return nil, err end
    local partition, offset, timestamp = string.unpack(">I4I8I8", payload, p)
    p = p + 4 + 8 + 8
    local key, p2, kerr = proto.decode_string(payload, p)
    if not key then return nil, kerr end
    local value, _, verr = proto.decode_string(payload, p2)
    if not value then return nil, verr end
    return {
        topic     = topic,
        partition = partition,
        offset    = offset,
        timestamp = timestamp,
        key       = key,
        value     = value,
    }
end

function Client:produce(topic, key, value)
    assert(type(topic) == "string", "topic must be a string")
    assert(type(key) == "string", "key must be a string")

    local correl = uuid.bytes()
    local frame
    local seq_used

    if self.pid then
        -- Idempotent path: include PID + per-topic monotonic sequence.
        -- The broker memoizes (PID, topic, seq) → (partition, offset),
        -- so a retry with the same seq returns the original ack
        -- without re-appending. On error we DON'T bump next_seq —
        -- the caller can retry with the same seq for at-most-once
        -- delivery. On success we commit the bump.
        seq_used = self.next_seq[topic] or 0
        frame = proto.encode_produce_idempotent(
            correl, self.pid, seq_used, topic, key, value)
    else
        frame = proto.encode_produce(correl, topic, key, value)
    end

    local ok, err = self:_write(frame)
    if not ok then return nil, err end

    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end

    if op == proto.OP_ERROR or not payload then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    local partition, offset = string.unpack(">I4I8", payload)
    if seq_used ~= nil then
        -- Only advance after a confirmed success; failures leave the
        -- seq slot unused so a retry replays the same sequence number.
        self.next_seq[topic] = seq_used + 1
    end
    return { partition = partition, offset = offset, seq = seq_used }
end

-- Inject a duplicate (PID, topic, seq) into the next produce — testing
-- helper that bypasses the next_seq counter, used by the example below
-- to demonstrate broker-side dedup.
function Client:produce_at_seq(topic, key, value, seq)
    assert(self.pid, "produce_at_seq requires an idempotent client")
    assert(type(seq) == "number" and seq >= 0, "seq must be a non-negative number")

    local correl = uuid.bytes()
    local frame = proto.encode_produce_idempotent(
        correl, self.pid, seq, topic, key, value)
    local ok, err = self:_write(frame)
    if not ok then return nil, err end

    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR or not payload then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    local partition, offset = string.unpack(">I4I8", payload)
    return { partition = partition, offset = offset, seq = seq }
end

function Client:fetch(topic, group, max_records)
    max_records = max_records or 100
    local correl = uuid.bytes()
    local data = proto.encode_fetch(correl, topic, group, max_records)
    local ok, err = self:_write(data)
    if not ok then return nil end

    local records = {}
    while true do
        local op, _, payload, rerr = self:_read_until(correl)
        if not op then return nil, rerr end
        if op == proto.OP_ERROR then
            return nil, (proto.decode_error(payload) or { message = "?" }).message
        end
        if op == proto.OP_OK then return records end
        if op == proto.OP_RECORD then
            local r, derr = self:_decode_record(payload)
            if not r then return nil, "decode record: " .. tostring(derr) end
            records[#records + 1] = r
        else
            return nil, string.format("unexpected op 0x%02x during fetch", op)
        end
    end
end

function Client:subscribe(topic, group)
    local correl = uuid.bytes()
    local ok, err = self:_write(proto.encode_subscribe(correl, topic, group))
    if not ok then return nil, err end
 
    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    if op ~= proto.OP_OK then
        return nil, string.format("expected OK for subscribe, got 0x%02x", op)
    end
    return true
end

-- After subscribing, returns the next pushed record. Blocks until one
-- arrives or `timeout` (seconds) elapses. Pass nil for no timeout.
function Client:next_record(timeout)
    local deadline = timeout and (socket.gettime() + timeout) or nil

    -- Without the reactor, we poll the socket with short timeouts so we
    -- can check the deadline. With the reactor, read_exact yields.
    local original_timeout = self.timeout
    if not self.reactor then
        self.sock:settimeout(timeout and math.min(0.1, timeout) or 0.1)
    end

    local function restore()
        if not self.reactor then self.sock:settimeout(original_timeout) end
    end

    while true do
        if deadline and socket.gettime() > deadline then
            restore()
            return nil, "timeout"
        end

        local op, c, payload, err = self:_read_frame()
        if not op then
            if err == "timeout" then
                -- Loop and re-check deadline
            else
                restore()
                return nil, err
            end
        elseif op == proto.OP_HEARTBEAT_REQ then
            self:_write(proto.encode_heartbeat_resp(c))
        elseif op == proto.OP_RECORD and c == uuid.ZERO then
            restore()
            return self:_decode_record(payload)
        end
        -- Other frames ignored (heartbeat resps, etc.)
    end
end

function Client:commit(topic, partition, offset)
    local correl = uuid.bytes()
    local data = proto.encode_commit(correl, topic, partition, offset)
    local ok, err = self:_write(data)
    if not ok then return nil, err end

    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    return true
end

function Client:create_topic(name, num_partitions)
    local correl = uuid.bytes()
    local ok, err = self:_write(
        proto.encode_create_topic(correl, name, num_partitions))
    if not ok then return nil, err end
 
    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    return true
end

function Client:list_topics()
    local correl = uuid.bytes()
    local ok, err = self:_write(proto.encode_list_topics(correl))
    if not ok then return nil, err end
 
    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    return proto.decode_topic_list(payload)
end

-- join_group registers this client as a member of `group_id` subscribing to
-- `topics` (a topic name or array of names) and returns the broker's
-- assignment: { member_id = "...", assignment = { [topic] = { part_id, ... } } }.
-- Pass member_id to rejoin under a known identity; omit it on first join and
-- the broker assigns one (remembered on the client for heartbeat/leave).
function Client:join_group(group_id, topics, member_id)
    assert(type(group_id) == "string", "group_id must be a string")
    if type(topics) == "string" then topics = { topics } end
    assert(type(topics) == "table" and #topics > 0, "topics must be a non-empty list")

    local correl = uuid.bytes()
    local ok, err = self:_write(
        proto.encode_join_group(correl, group_id, member_id or "", topics))
    if not ok then return nil, err end

    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    if op ~= proto.OP_GROUP_ASSIGNMENT then
        return nil, string.format("expected GROUP_ASSIGNMENT, got 0x%02x", op)
    end

    local res, derr = proto.decode_group_assignment(payload)
    if not res then return nil, "decode assignment: " .. tostring(derr) end
    self.group_id  = group_id
    self.member_id = res.member_id
    return res
end

-- group_heartbeat renews this member's lease. Returns (true, nil), or
-- (nil, err) — notably when the broker has reaped the member, in which case
-- the caller should join_group again. Defaults to the joined group/member.
function Client:group_heartbeat(group_id, member_id)
    group_id  = group_id  or self.group_id
    member_id = member_id or self.member_id
    assert(group_id and member_id, "not a member of any group (call join_group first)")

    local correl = uuid.bytes()
    local ok, err = self:_write(
        proto.encode_group_heartbeat(correl, group_id, member_id))
    if not ok then return nil, err end

    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    return true
end

-- leave_group departs the group. Returns (true, nil) or (nil, err).
function Client:leave_group(group_id, member_id)
    group_id  = group_id  or self.group_id
    member_id = member_id or self.member_id
    assert(group_id and member_id, "not a member of any group (call join_group first)")

    local correl = uuid.bytes()
    local ok, err = self:_write(
        proto.encode_leave_group(correl, group_id, member_id))
    if not ok then return nil, err end

    local op, _, payload, rerr = self:_read_until(correl)
    if not op then return nil, rerr end
    if op == proto.OP_ERROR then
        return nil, (proto.decode_error(payload) or { message = "?" }).message
    end
    self.group_id  = nil
    self.member_id = nil
    return true
end

function Client:close()
    if self.closed then return end
    self.closed = true
    pcall(function()
        self:_write(proto.encode_goodbye(uuid.bytes()))
        self.sock:close()
    end)
end
 

return Client
