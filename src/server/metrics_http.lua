-- Minimal HTTP endpoint for scraping metrics. Lives on its own port,
-- shares the main server's reactor (one event loop, no extra threads).
--
-- Routes:
--   GET /metrics  → Prometheus text exposition
--   GET /stats    → JSON broker snapshot (human/operator readable)
--   GET /health   → "ok\n" (liveness probe)
--   GET /         → tiny HTML index
--   anything else → 404
--
-- This is HTTP/1.1 with Connection: close. No keep-alive, no chunked
-- encoding, no compression. Prometheus scrapers don't need any of that,
-- and the smaller surface keeps the parser honest.
--
-- Security: there's no auth. The metrics expose connection counts,
-- byte counts, and close-reason histograms — useful to attackers if
-- they reach this port. Bind to localhost or firewall the port if it
-- isn't on a trusted network.

local socket  = require("socket")
local metrics = require("src.server.metrics")
local json    = require("dkjson")
local log     = require("src.log.logger").get("metrics")
 
local M = {}
M.__index = M

local READ_DEADLINE  = 5    -- max seconds to read the request
local WRITE_DEADLINE = 5    -- max seconds to write the response
local MAX_REQUEST    = 8192 -- bound on request size — headers only, we don't read bodies

local INDEX_HTML = [[<!doctype html>
<meta charset="utf-8">
<title>moonmq</title>
<style>body{font:14px/1.4 system-ui,sans-serif;max-width:40em;margin:3em auto;padding:0 1em}code{background:#eee;padding:.1em .3em;border-radius:3px}</style>
<h1>moonmq</h1>
<ul>
  <li><a href="/metrics"><code>/metrics</code></a> — Prometheus exposition</li>
  <li><a href="/stats"><code>/stats</code></a> — JSON broker snapshot</li>
  <li><a href="/health"><code>/health</code></a> — liveness probe</li>
</ul>
]]

-- opts.server: the Server instance, used to render /stats. Optional —
-- when nil, /stats responds 503. Passing it through is the cleanest way
-- to avoid making metrics_http reach back through globals.
function M.new(opts)
    return setmetatable({
        reactor = assert(opts.reactor, "reactor required"),
        host    = opts.host or "127.0.0.1",  -- safe default
        port    = opts.port or 9090,         -- Prometheus convention
        server  = opts.server,               -- for /stats snapshot
    }, M)
end

-- Read up to \r\n\r\n. We only need the headers — metrics endpoint has
-- no request body to consume. Returns the raw header bytes or nil,err.
local function read_request(reactor, sock, deadline)
    sock:settimeout(0)
    local buf = ""
    while #buf < MAX_REQUEST do
        -- Top-of-loop deadline check so we can't spin forever on a
        -- peer that keeps returning (nil, nil, "") or never yields any
        -- bytes. This is the pre-auth path, so be ungenerous.
        if socket.gettime() > deadline then
            return nil, "read deadline"
        end

        local chunk, err, partial = sock:receive(MAX_REQUEST - #buf)
        local progressed = false
        if chunk then
            buf = buf .. chunk
            progressed = #chunk > 0
        elseif partial and #partial > 0 then
            buf = buf .. partial
            progressed = true
        end

        local idx = buf:find("\r\n\r\n", 1, true)
        if idx then return buf:sub(1, idx + 3) end

        if err == "timeout" then
            reactor:wait_readable(sock)
        elseif err == "closed" then
            return nil, "peer closed"
        elseif err then
            return nil, err
        elseif not progressed then
            -- No chunk, no partial, no err: nothing to wait on. Yield
            -- to the reactor for a tick so we don't burn CPU.
            reactor:sleep(0.05)
        end
    end
    return nil, "request too large"
end

local function parse_request_line(line)
    return line:match("^(%S+)%s+(%S+)%s+(%S+)")
end

local STATUS_TEXT = {
    [200] = "OK",
    [400] = "Bad Request",
    [404] = "Not Found",
    [405] = "Method Not Allowed",
    [500] = "Internal Server Error",
    [503] = "Service Unavailable",
}

local function write_response(reactor, sock, status, content_type, body)
    local text = STATUS_TEXT[status] or "Status"
    local response = string.format(
        "HTTP/1.1 %d %s\r\n" ..
        "Content-Type: %s\r\n" ..
        "Content-Length: %d\r\n" ..
        "Connection: close\r\n" ..
        "Cache-Control: no-store\r\n" ..
        "\r\n%s",
        status, text, content_type, #body, body)
    reactor:send_all(sock, response, socket.gettime() + WRITE_DEADLINE)
end

local PROM_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

function M:_handle(sock, peer)
    metrics.inc("moonmq_metrics_http_requests_total")
    local deadline = socket.gettime() + READ_DEADLINE
 
    local req, err = read_request(self.reactor, sock, deadline)
    if not req then
        pcall(function() sock:close() end)
        return
    end
 
    local first_line = req:match("^([^\r\n]+)")
    local method, path, _version = parse_request_line(first_line or "")
    if not method then
        pcall(function()
            write_response(self.reactor, sock, 400, "text/plain", "bad request\n")
            sock:close()
        end)
        return
    end

    -- Strip query string — we ignore it but it's legal in the path.
    path = path:gsub("%?.*", "")

    local status, ctype, body

    if method ~= "GET" then
        status, ctype, body = 405, "text/plain", "GET only\n"
    elseif path == "/metrics" then
        local ok, rendered = pcall(metrics.render_prometheus)
        if ok then
            status, ctype, body = 200, PROM_CONTENT_TYPE, rendered
        else
            status, ctype, body = 500, "text/plain",
                "metrics render failed: " .. tostring(rendered) .. "\n"
        end
    elseif path == "/stats" then
        if not self.server then
            status, ctype, body = 503, "text/plain",
                "stats unavailable: server snapshot not wired\n"
        else
            local ok, snap = pcall(self.server.snapshot, self.server)
            if not ok then
                status, ctype, body = 500, "text/plain",
                    "snapshot failed: " .. tostring(snap) .. "\n"
            else
                -- dkjson with indent=true yields a human-readable shape.
                -- Operators read this; Prometheus does not.
                local rendered = json.encode(snap, { indent = true })
                status, ctype, body = 200,
                    "application/json; charset=utf-8",
                    rendered .. "\n"
            end
        end
    elseif path == "/health" then
        status, ctype, body = 200, "text/plain", "ok\n"
    elseif path == "/" then
        status, ctype, body = 200, "text/html; charset=utf-8", INDEX_HTML
    else
        status, ctype, body = 404, "text/plain", "not found\n"
    end

    pcall(function()
        write_response(self.reactor, sock, status, ctype, body)
        sock:close()
    end)
end

function M:start()
    local _, lerr = self.reactor:listen(self.host, self.port,
        function(sock, peer, _ip) self:_handle(sock, peer) end)
    if lerr then
        log:error("listen failed on %s:%d: %s", self.host, self.port, lerr)
        return nil, lerr
    end
    log:info("listening on %s:%d (GET /metrics)", self.host, self.port)
    return true
end

return M
