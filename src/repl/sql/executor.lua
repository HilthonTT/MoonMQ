-- Executor for MoonMQ's SQL-like console.
--
-- Owns the session state (the live broker connection and any joined group)
-- and turns a parsed statement into a broker call via src/client. Every
-- statement returns a uniform result table the REPL knows how to render:
--
--   { ok = true,  kind = "message", message = "..." }
--   { ok = true,  kind = "rows", columns = {...}, rows = {{...}, ...}, note = "..." }
--   { ok = true,  kind = "help", topic = "fetch"|nil }
--   { ok = true,  kind = "quit" }
--   { ok = false, error = "..." }
--
-- The executor never prints; keeping I/O in the REPL makes the command layer
-- straightforward to unit-test with a stub client.

local Session = {}
Session.__index = Session

-- opts.client_factory lets tests inject a fake connector. It must have the
-- same shape as src.client.Client.new: (opts) -> (client, err).
--
-- Auth note: the broker requires an AUTH frame to leave the handshake even in
-- OPEN mode (a connection that only says HELLO is dropped by the handshake
-- watchdog after a few seconds). So we always authenticate, defaulting to the
-- project's shipped admin/admin credentials; override per-connection with
-- CONNECT ... USER '...' PASSWORD '...'.
function Session.new(opts)
    opts = opts or {}
    return setmetatable({
        client         = nil,
        client_factory = opts.client_factory,
        host           = opts.host or "127.0.0.1",
        port           = opts.port or 9092,
        user           = opts.user or "admin",
        password       = opts.password or "admin",
        -- Parameters of the last successful connect, replayed to transparently
        -- reconnect if the broker drops an idle socket between statements.
        last_connect   = nil,
        -- Remembered from the last CREATE/JOIN GROUP so SHOW GROUP and
        -- offset-less commands have context.
        group          = nil,
        assignment     = nil,
    }, Session)
end

local function ok_msg(msg)          return { ok = true, kind = "message", message = msg } end
local function fail(msg)            return { ok = false, error = msg } end

local function client_module()
    -- Required lazily: pulls in luasocket, which the pure lexer/parser layers
    -- (and their tests) must not depend on.
    return require("src.client")
end

function Session:is_connected()
    return self.client ~= nil and not self.client.closed
end

-- Guard for statements that need a live connection.
function Session:require_client()
    if not self:is_connected() then
        return nil, "not connected — run CONNECT first"
    end
    return self.client
end

function Session:connect(node)
    if self:is_connected() then
        self.client:close()
        self.client = nil
    end

    local host = node.host or self.host
    local port = node.port or self.port
    local user = node.user or self.user
    local password = node.password or self.password
    local factory = self.client_factory or client_module().new

    local c, err = factory({
        host     = host,
        port     = port,
        username = user,
        password = password,
    })
    if not c then
        return fail(string.format("connect %s:%d failed: %s", host, port, tostring(err)))
    end

    self.client, self.host, self.port, self.user, self.password = c, host, port, user, password
    self.last_connect = { host = host, port = port, user = user, password = password }
    self.group, self.assignment = nil, nil
    return ok_msg(string.format("connected to %s:%d%s",
        host, port, user and (" as " .. user) or ""))
end

function Session:disconnect()
    if not self:is_connected() then
        return ok_msg("already disconnected")
    end
    self.client:close()
    self.client = nil
    self.group, self.assignment = nil, nil
    return ok_msg("disconnected")
end

function Session:create_topic(node)
    local c, err = self:require_client()
    if not c then return fail(err) end
    local ok, cerr = c:create_topic(node.name, node.partitions)
    if not ok then return fail(cerr) end
    return ok_msg(string.format("topic '%s' created (%d partition%s)",
        node.name, node.partitions, node.partitions == 1 and "" or "s"))
end

function Session:list_topics()
    local c, err = self:require_client()
    if not c then return fail(err) end
    local topics, lerr = c:list_topics()
    if not topics then return fail(lerr) end

    local rows = {}
    for _, t in ipairs(topics) do
        -- The wire list may be plain strings or {name, partitions} tables
        -- depending on protocol version; handle both.
        if type(t) == "table" then
            rows[#rows + 1] = { t.name or t[1] or "?", t.partitions or t[2] or "" }
        else
            rows[#rows + 1] = { t, "" }
        end
    end
    return {
        ok = true, kind = "rows",
        columns = { "topic", "partitions" },
        rows = rows,
        note = string.format("%d topic%s", #rows, #rows == 1 and "" or "s"),
    }
end

function Session:produce(node)
    local c, err = self:require_client()
    if not c then return fail(err) end
    local ack, perr = c:produce(node.topic, node.key, node.value)
    if not ack then return fail(perr) end
    return ok_msg(string.format("produced to %s → partition %d, offset %d",
        node.topic, ack.partition, ack.offset))
end

function Session:fetch(node)
    local c, err = self:require_client()
    if not c then return fail(err) end
    local records, ferr = c:fetch(node.topic, node.group, node.limit)
    if not records then return fail(ferr) end
    return self:_records_result(records)
end

function Session:subscribe(node)
    local c, err = self:require_client()
    if not c then return fail(err) end
    local ok, serr = c:subscribe(node.topic, node.group)
    if not ok then return fail(serr) end

    local records = {}
    local limit = node.limit
    -- Read pushed records until the per-record timeout elapses or LIMIT is hit.
    while not (limit and #records >= limit) do
        local rec, rerr = c:next_record(node.timeout)
        if not rec then
            if rerr == "timeout" then break end
            return fail(rerr)
        end
        records[#records + 1] = rec
    end
    return self:_records_result(records)
end

function Session:_records_result(records)
    local rows = {}
    for _, r in ipairs(records) do
        rows[#rows + 1] = { r.partition, r.offset, r.key, r.value }
    end
    return {
        ok = true, kind = "rows",
        columns = { "partition", "offset", "key", "value" },
        rows = rows,
        note = string.format("%d record%s", #rows, #rows == 1 and "" or "s"),
    }
end

function Session:commit(node)
    local c, err = self:require_client()
    if not c then return fail(err) end
    local ok, cerr = c:commit(node.topic, node.partition, node.offset)
    if not ok then return fail(cerr) end
    return ok_msg(string.format("committed %s[%d] @ offset %d",
        node.topic, node.partition, node.offset))
end

function Session:join_group(node)
    local c, err = self:require_client()
    if not c then return fail(err) end
    local res, jerr = c:join_group(node.group, node.topics)
    if not res then return fail(jerr) end
    self.group, self.assignment = node.group, res.assignment
    return self:_assignment_result(node.group, res.member_id, res.assignment)
end

function Session:_assignment_result(group, member_id, assignment)
    local rows = {}
    for topic, parts in pairs(assignment or {}) do
        rows[#rows + 1] = { topic, table.concat(parts, ",") }
    end
    table.sort(rows, function(a, b) return a[1] < b[1] end)
    return {
        ok = true, kind = "rows",
        columns = { "topic", "partitions" },
        rows = rows,
        note = string.format("joined group '%s' as %s", group, member_id or "?"),
    }
end

function Session:show_group()
    if not self.group then
        return ok_msg("not a member of any group in this session")
    end
    return self:_assignment_result(self.group, self.client and self.client.member_id, self.assignment)
end

function Session:leave_group()
    local c, err = self:require_client()
    if not c then return fail(err) end
    if not self.group then return ok_msg("not a member of any group") end
    local ok, lerr = c:leave_group()
    if not ok then return fail(lerr) end
    local left = self.group
    self.group, self.assignment = nil, nil
    return ok_msg(string.format("left group '%s'", left))
end

function Session:heartbeat()
    local c, err = self:require_client()
    if not c then return fail(err) end
    if not self.group then return fail("not a member of any group") end
    local ok, herr = c:group_heartbeat()
    if not ok then return fail(herr) end
    return ok_msg(string.format("heartbeat sent for group '%s'", self.group))
end

-- Dispatch table keyed by AST node type.
local HANDLERS = {
    connect      = Session.connect,
    disconnect   = function(s) return s:disconnect() end,
    create_topic = Session.create_topic,
    list_topics  = function(s) return s:list_topics() end,
    produce      = Session.produce,
    fetch        = Session.fetch,
    subscribe    = Session.subscribe,
    commit       = Session.commit,
    join_group   = Session.join_group,
    leave_group  = function(s) return s:leave_group() end,
    show_group   = function(s) return s:show_group() end,
    heartbeat    = function(s) return s:heartbeat() end,
    help         = function(_, node) return { ok = true, kind = "help", topic = node.topic } end,
    quit         = function() return { ok = true, kind = "quit" } end,
}

-- Statements safe to replay verbatim after a transparent reconnect. These
-- either don't reach the broker when the socket is already dead (a failed
-- write never appends) or are naturally idempotent. Group statements are
-- excluded: a reconnect drops the member_id, so retrying would be misleading —
-- the user should re-JOIN instead.
local RETRYABLE = {
    create_topic = true,
    list_topics  = true,
    produce      = true,
    fetch        = true,
    commit       = true,
}

-- Does this error look like the connection died (rather than an application
-- error the broker deliberately returned)?
local function looks_disconnected(err)
    err = tostring(err or ""):lower()
    return err:find("closed") or err:find("broken pipe")
        or err:find("refused") or err:find("aborted")
        or err:find("reset") or err:find("timeout")
end

-- Execute one AST node; returns a result table (never throws for broker
-- errors, which come back as { ok = false, error }).
function Session:execute(node)
    local handler = HANDLERS[node.type]
    if not handler then
        return fail("internal: no handler for '" .. tostring(node.type) .. "'")
    end

    local res = handler(self, node)

    -- The broker drops idle sockets (handshake/idle/heartbeat deadlines). If a
    -- retryable statement failed because the connection died, silently
    -- re-establish it with the last-used parameters and try once more.
    if res and res.ok == false and RETRYABLE[node.type]
        and self.last_connect and looks_disconnected(res.error) then
        local rc = self:connect(self.last_connect)
        if rc.ok then
            res = handler(self, node)
        end
    end

    return res
end

return Session
