local socket = require("socket")
local json = require("dkjson")
local Reactor = require("src.server.reactor")
local Client = require("src.client")
local Config = require("src.server.config")

local Pool = {}
Pool.__index = Pool

function Pool.new(opts)
    return setmetatable({
        opts      = opts,                  -- forwarded to Client.new
        reactor   = opts.reactor,
        max       = opts.max or 8,
        available = {},                    -- ready-to-use clients
        active    = 0,                     -- total clients in existence
        waiters   = {},                    -- coroutines parked on acquire
    }, Pool)
end

function Pool:acquire()
    -- Drop dead clients off the available queue.
    while #self.available > 0 do
        local c = table.remove(self.available)
        if not c.closed then return c end
        self.active = self.active - 1
    end

    -- Room to grow?
    if self.active < self.max then
        self.active = self.active + 1
        local c, err = Client.new(self.opts)
        if not c then
            self.active = self.active - 1
            return nil, err
        end
        return c
    end

    -- Park until something is released.
    local co = assert(coroutine.running(), "Pool:acquire needs a coroutine")
    table.insert(self.waiters, co)
    coroutine.yield()
 
    -- We were woken because something was put back.
    while #self.available > 0 do
        local c = table.remove(self.available)
        if not c.closed then return c end
        self.active = self.active - 1
    end
    return nil, "pool empty after wakeup"
end

function Pool:release(c)
    if not c then return end

    if c.closed then
        self.active = self.active - 1
    else
        table.insert(self.available, c)
    end
    if #self.waiters > 0 then
        local co = table.remove(self.waiters, 1)
        self.reactor:schedule(co)
    end
end

function Pool:with(fn)
    local c, err = self:acquire()
    if not c then return nil, err end
    local ok, result, oerr = pcall(fn, c)
    self:release(c)
    if not ok then return nil, "client error: " .. tostring(result) end
    return result, oerr
end

local READ_DEADLINE  = 30
local WRITE_DEADLINE = 30
local MAX_HEADERS = 16 * 1024
local MAX_BODY = 16 * 1024 * 1024

local STATUS = {
    [200] = "OK", [201] = "Created", [204] = "No Content",
    [400] = "Bad Request", [404] = "Not Found",
    [405] = "Method Not Allowed", [413] = "Payload Too Large",
    [500] = "Internal Server Error", [502] = "Bad Gateway",
    [503] = "Service Unavailable",
}

local function read_http_request(reactor, sock)
    -- Non-blocking: the loop below drives readiness through the reactor's
    -- wait_readable on "timeout". settimeout(0), NOT gettimeout(0) — the
    -- latter is a getter that ignores its argument and leaves the socket
    -- blocking, so one slow client would stall the whole gateway.
    sock:settimeout(0)
    local deadline = socket.gettime() + READ_DEADLINE
    local buf = ""

    -- Read until headers terminator
    while not buf:find("\r\n\r\n", 1, true) do
        if #buf > MAX_HEADERS then return nil, "headers too large" end
        local chunk, err, partial = sock:receive(8192)
        if chunk then
            buf = buf .. chunk
        elseif partial and #partial > 0 then
            buf = buf .. partial
        end
        -- Check the terminator BEFORE parking on wait_readable: a request
        -- shorter than the receive window arrives as `partial` + "timeout",
        -- and the peer sends nothing further until it gets a response —
        -- waiting first would park this coroutine until the peer gives up.
        if buf:find("\r\n\r\n", 1, true) then break end
        if err == "timeout" then
            if socket.gettime() > deadline then return nil, "read deadline" end
            reactor:wait_readable(sock)
        elseif err == "closed" then
            return nil, "peer closed"
        elseif err then
            return nil, err
        end
    end

    local sep = buf:find("\r\n\r\n", 1, true)
    local head = buf:sub(1, sep - 1)
    local body = buf:sub(sep + 4)

    local first_line, rest = head:match("^([^\r\n]+)\r\n(.*)$")
    if not first_line then return nil, "bad request line" end

    local method, path, _version = first_line:match("^(%S+)%s+(%S+)%s+(%S+)$")
    if not method then return nil, "malformed request line" end

    local headers = {}
    for line in (rest .. "\r\n"):gmatch("([^\r\n]+)\r\n") do
        local k, v = line:match("^([^:]+):%s*(.*)$")
        if k then headers[k:lower()] = v end
    end

    -- Read body if Content-Length set.
    local cl = tonumber(headers["content-length"] or "0") or 0
    if cl > MAX_BODY then return nil, "body too large" end
    while #body < cl do
        local chunk, err, partial = sock:receive(cl - #body)
        if chunk then
            body = body .. chunk
        elseif partial and #partial > 0 then
            body = body .. partial
        end
        -- Same as the header loop: if the partial completed the body, don't
        -- park on a socket that will never become readable again.
        if #body >= cl then break end
        if err == "timeout" then
            if socket.gettime() > deadline then return nil, "body read deadline" end
            reactor:wait_readable(sock)
        elseif err == "closed" then
            return nil, "peer closed mid-body"
        elseif err then
            return nil, err
        end
    end

    return { method = method, path = path, headers = headers, body = body }
end

local function write_response(reactor, sock, status, content_type, body)
    body = body or ""
    local response = string.format(
        "HTTP/1.1 %d %s\r\n" ..
        "Content-Type: %s\r\n" ..
        "Content-Length: %d\r\n" ..
        "Connection: close\r\n" ..
        "Cache-Control: no-store\r\n" ..
        "\r\n%s",
        status, STATUS[status] or "Status", content_type, #body, body)
    reactor:send_all(sock, response, socket.gettime() + WRITE_DEADLINE)
end

local function json_response(status, tbl)
    return status, "application/json", json.encode(tbl) .. "\n"
end

local function err_response(status, msg)
    return json_response(status, { error = msg })
end

local function home(req, pool)
    return 200, "text/html; charset=utf-8", [[<!doctype html>
<meta charset="utf-8">
<title>moonmq-gateway</title>
<style>body{font:14px/1.5 system-ui,sans-serif;max-width:48em;margin:3em auto;padding:0 1em}code{background:#eee;padding:.1em .3em;border-radius:3px}</style>
<h1>moonmq HTTP gateway</h1>
<table>
<tr><td><code>GET /topics</code></td><td>list topics</td></tr>
<tr><td><code>POST /topics</code></td><td><code>{"name":"...","partitions":N}</code></td></tr>
<tr><td><code>POST /topics/{t}/records</code></td><td><code>{"key":"...","value":"..."}</code></td></tr>
<tr><td><code>POST /topics/{t}/records/fetch</code></td><td><code>{"group":"...","max":N}</code></td></tr>
<tr><td><code>POST /topics/{t}/commits</code></td><td><code>{"group":"...","partition":N,"offset":N}</code></td></tr>
<tr><td><code>GET /healthz</code></td><td>liveness</td></tr>
</table>
]]
end

local function healthz(req, pool)
    return 200, "text/plain", "ok\n"
end
 
local function list_topics(req, pool)
    local topics, err = pool:with(function(c) return c:list_topics() end)
    if not topics then return err_response(502, err or "list failed") end
    return json_response(200, topics)
end

local function create_topic(req, pool)
    local data, _, jerr = json.decode(req.body)
    if not data then return err_response(400, "bad json: " .. tostring(jerr)) end
    if type(data.name) ~= "string" or type(data.partitions) ~= "number" then
        return err_response(400, "require {name: string, partitions: number}")
    end

    local _, err = pool:with(function(c)
        return c:create_topic(data.name, math.floor(data.partitions))
    end)
    if err then return err_response(502, err) end
    return json_response(201, { name = data.name, partitions = math.floor(data.partitions) })
end

local function produce(req, pool, topic)
    local data, _, jerr = json.decode(req.body)
    if not data then return err_response(400, "bad json: " .. tostring(jerr)) end
    if type(data.key) ~= "string" or type(data.value) ~= "string" then
        return err_response(400, "require {key: string, value: string}")
    end

    local ack, err = pool:with(function(c)
        return c:produce(topic, data.key, data.value)
    end)
    if not ack then return err_response(502, err or "produce failed") end
    return json_response(200, { partition = ack.partition, offset = ack.offset })
end

local function fetch(req, pool, topic)
    local data, _, jerr = json.decode(req.body)
    if not data then return err_response(400, "bad json: " .. tostring(jerr)) end
    if type(data.group) ~= "string" then
        return err_response(400, "require {group: string, max?: number}")
    end
    local max_records = data.max and math.floor(data.max) or 100
 
    local records, err = pool:with(function(c)
        return c:fetch(topic, data.group, max_records)
    end)
    if not records then return err_response(502, err or "fetch failed") end
    return json_response(200, records)
end

local function commit(req, pool, topic)
    local data, _, jerr = json.decode(req.body)
    if not data then return err_response(400, "bad json: " .. tostring(jerr)) end
    if type(data.group) ~= "string"
       or type(data.partition) ~= "number"
       or type(data.offset)    ~= "number" then
        return err_response(400, "require {group, partition, offset}")
    end

    -- Fetch first to ensure the pool's client subscribes to topic+group.
    -- We can't share commit state across clients in the pool — each
    -- needs its own consumer context. For now this is a best-effort
    -- single-shot commit on a fresh subscription.
    local _, err = pool:with(function(c)
        local _, ferr = c:fetch(topic, data.group, 0)  -- 0 records, just establishes group
        if ferr then return nil, ferr end
        return c:commit(topic, math.floor(data.partition), math.floor(data.offset))
    end)
    if err then return err_response(502, err) end
    return json_response(200, { committed = true })
end

local ROUTES = {
    { method = "GET",  pattern = "^/$",                                handler = home          },
    { method = "GET",  pattern = "^/healthz$",                         handler = healthz       },
    { method = "GET",  pattern = "^/topics$",                          handler = list_topics   },
    { method = "POST", pattern = "^/topics$",                          handler = create_topic  },
    { method = "POST", pattern = "^/topics/([^/]+)/records$",          handler = produce       },
    { method = "POST", pattern = "^/topics/([^/]+)/records/fetch$",    handler = fetch         },
    { method = "POST", pattern = "^/topics/([^/]+)/commits$",          handler = commit        },
}

local function route(req, pool)
    -- Strip query string for matching.
    local path = req.path:gsub("%?.*", "")

    -- Method-not-allowed vs not-found: try matching just by pattern
    -- first; if we hit any pattern but wrong method, return 405.
    local found_pattern = false
    for _, r in ipairs(ROUTES) do
        local caps = { path:match(r.pattern) }
        if #caps > 0 or path:match(r.pattern) then
            found_pattern = true
            if r.method == req.method then
                if #caps == 0 then caps = {} end
                return r.handler(req, pool, table.unpack(caps))
            end
        end
    end
    if found_pattern then return err_response(405, "method not allowed") end
    return err_response(404, "not found: " .. path)
end

local Gateway = {}
Gateway.__index = Gateway
 
function Gateway.new(opts)
    return setmetatable({
        reactor = Reactor.new(),
        host    = opts.host or "0.0.0.0",
        port    = opts.port or 8080,
        pool_opts = opts.pool_opts,  -- forwarded to Pool.new
    }, Gateway)
end

function Gateway:_handle(sock, peer)
    local req, rerr = read_http_request(self.reactor, sock)
    if not req then
        pcall(function() sock:close() end)
        return
    end
 
    local ok, status, ctype, body = pcall(route, req, self.pool)
    if not ok then
        status, ctype, body = 500, "application/json",
            json.encode({ error = "handler crashed: " .. tostring(status) }) .. "\n"
    end
 
    pcall(function()
        write_response(self.reactor, sock, status, ctype, body)
        sock:close()
    end)
end

function Gateway:start()
    self.pool_opts.reactor = self.reactor
    self.pool = Pool.new(self.pool_opts)
 
    local _, lerr = self.reactor:listen(self.host, self.port,
        function(sock, peer, _ip) self:_handle(sock, peer) end)
    if lerr then return nil, lerr end
 
    io.stderr:write(string.format(
        "[gateway] listening on %s:%d → moonmq %s:%d (pool max=%d)\n",
        self.host, self.port,
        self.pool_opts.host, self.pool_opts.port, self.pool_opts.max or 8))
 
    self.reactor:run()
    return true
end

local function script_is_main()
    return arg and arg[0] and arg[0]:match("moonmq%-gateway")
end

if script_is_main() then
    local cfg = Config.load() or {}
    local gcfg = cfg.Gateway or {}
    local scfg = cfg.Server  or {}
    local acfg = cfg.Auth    or {}
 
    local gw = Gateway.new({
        host = gcfg.Host or "0.0.0.0",
        port = gcfg.Port or 8080,
        pool_opts = {
            max            = gcfg.PoolMax or 8,
            host           = gcfg.MoonMQHost or "127.0.0.1",
            port           = gcfg.MoonMQPort or scfg.Port or 9092,
            username       = acfg.Username,
            password       = gcfg.MoonMQPassword,   -- plaintext for client side
            client_name    = "moonmq-gateway",
            client_version = "0.1.0",
            timeout        = gcfg.MoonMQTimeout or 30,
        },
    })
 
    local ok, err = gw:start()
    if not ok then
        io.stderr:write("[gateway] failed to start: " .. tostring(err) .. "\n")
        os.exit(1)
    end
end

return Gateway
