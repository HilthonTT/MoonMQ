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
-- Security: authentication is OPTIONAL and off by default, because these
-- endpoints have always been open and a broker that starts refusing its own
-- scraper on upgrade is a worse outcome than one that keeps its documented
-- posture. What the port exposes is not nothing — connection counts, per-topic
-- record rates, close-reason histograms, and, via /stats, every topic name and
-- its size on disk — so `Server.MetricsAuth` turns on one or both of:
--
--   Bearer token   Authorization: Bearer <token>, compared in constant time.
--                  The right choice for a scraper: no KDF, no user store, and
--                  the token can be rotated without touching the user list.
--   Basic auth     Authorization: Basic base64(user:pass), verified against
--                  the user store; the principal must hold cluster:describe.
--                  Repeat scrapes are cheap — the authenticator's success
--                  cache short-circuits the PBKDF2 — and a brute-force run
--                  against this port trips the same per-IP lockout the broker
--                  port uses.
--
-- /health stays open either way: it reveals nothing beyond "the process is
-- listening", and a liveness probe that needs a credential is a liveness
-- probe that fails for the wrong reasons.
--
-- Without either configured, bind to localhost or firewall the port.

local socket   = require("socket")
local metrics  = require("src.metrics")
local json     = require("dkjson")
local ct       = require("src.core.ct")
local b64      = require("src.core.base64")
local acl_m    = require("src.server.acl")
local http_kit = require("src.server.http_kit")
local log      = require("src.log.logger").get("metrics")

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
        -- { token = "...", basic = true } — nil leaves the endpoints open.
        auth          = opts.auth,
        authenticator = opts.authenticator,  -- user store, for Basic
        -- TLS for this listener (src/io/tls.lua). Separate from the client
        -- port's: a scrape endpoint on loopback often does not need it while
        -- the public port does — and Basic credentials over plaintext would
        -- otherwise hand the scraper's password to anyone on the path.
        tls           = opts.tls,
    }, M)
end

-- Paths that never require a credential. A liveness probe is not a data leak.
local PUBLIC_PATHS = { ["/health"] = true }

-- Returns (true) when the request may proceed, or (false, reason) when it may
-- not. Reason is for the log only — the response says nothing beyond 401, so a
-- prober cannot tell "wrong token" from "unknown user" from "no permission".
function M:_authorized(headers, ip)
    local cfg = self.auth
    if not cfg then return true end

    local header = http_kit.header(headers, "Authorization")
    if not header then return false, "no Authorization header" end

    local scheme, value = header:match("^(%S+)%s+(%S+)%s*$")
    if not scheme then return false, "malformed Authorization header" end
    scheme = scheme:lower()

    if scheme == "bearer" then
        if not cfg.token then return false, "bearer not enabled" end
        if ct.equal(value, cfg.token) then return true end
        return false, "bad token"
    end

    if scheme == "basic" then
        if not cfg.basic then return false, "basic not enabled" end
        if not self.authenticator then
            return false, "basic requires a configured user store"
        end

        local decoded, derr = b64.decode(value)
        if not decoded then return false, derr end
        local user, pass = decoded:match("^([^:]*):(.*)$")
        if not user then return false, "malformed basic credentials" end

        local ok, verr, principal =
            self.authenticator:verify(user, pass, ip or "?")
        if not ok then return false, verr or "invalid credentials" end

        -- Authentication is not authorization: reading broker-wide metrics is
        -- a cluster-level view, so it takes a cluster-level grant. A tenant
        -- with topic ACLs alone has no business reading everyone's counters.
        if principal and not principal.acl:authorized(
                acl_m.RES_CLUSTER, nil, acl_m.OP_DESCRIBE) then
            return false, "principal lacks cluster:describe"
        end
        return true
    end

    return false, "unsupported authentication scheme"
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

        -- park(), not wait_readable(): a TLS read can be waiting on
        -- writability (see Reactor:park).
        if reactor.would_block and reactor.would_block(err) then
            reactor:park(sock, err, "read")
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
    [401] = "Unauthorized",
    [404] = "Not Found",
    [405] = "Method Not Allowed",
    [500] = "Internal Server Error",
    [503] = "Service Unavailable",
}

local function write_response(reactor, sock, status, content_type, body, extra)
    local text = STATUS_TEXT[status] or "Status"
    local response = string.format(
        "HTTP/1.1 %d %s\r\n" ..
        "Content-Type: %s\r\n" ..
        "Content-Length: %d\r\n" ..
        "Connection: close\r\n" ..
        "Cache-Control: no-store\r\n" ..
        "%s\r\n%s",
        status, text, content_type, #body, extra or "", body)
    reactor:send_all(sock, response, socket.gettime() + WRITE_DEADLINE)
end

local PROM_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

function M:_handle(sock, peer, ip)
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

    if self.auth and not PUBLIC_PATHS[path] then
        local ok, reason = self:_authorized(req, ip)
        if not ok then
            metrics.inc("moonmq_metrics_http_unauthorized_total")
            log:warn("metrics request from %s refused: %s",
                peer or "?", reason or "unauthorized")
            -- Advertise Basic only when it is actually enabled, so a browser
            -- is not prompted for a password the broker will never accept.
            local challenge = self.auth.basic
                and "WWW-Authenticate: Basic realm=\"moonmq\"\r\n"
                or "WWW-Authenticate: Bearer realm=\"moonmq\"\r\n"
            pcall(function()
                write_response(self.reactor, sock, 401, "text/plain",
                    "unauthorized\n", challenge)
                sock:close()
            end)
            return
        end
    end

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
        function(sock, peer, ip) self:_handle(sock, peer, ip) end,
        { tls = self.tls })
    if lerr then
        log:error("listen failed on %s:%d: %s", self.host, self.port, lerr)
        return nil, lerr
    end

    local mode = "OPEN"
    if self.auth then
        local schemes = {}
        if self.auth.token then schemes[#schemes + 1] = "bearer" end
        if self.auth.basic then schemes[#schemes + 1] = "basic" end
        mode = table.concat(schemes, "+")
    end
    log:info("listening on %s:%d (GET /metrics, auth=%s)",
        self.host, self.port, mode)

    -- The bind address is the whole defence when nothing is configured, so say
    -- so when it is not a loopback one.
    if not self.auth and self.host ~= "127.0.0.1" and self.host ~= "localhost"
       and self.host ~= "::1" then
        log:warn("metrics endpoints are UNAUTHENTICATED on %s:%d and the bind "
            .. "address is not loopback: /stats lists every topic name and its "
            .. "size on disk. Set Server.MetricsAuth, or bind to 127.0.0.1.",
            self.host, self.port)
    end
    return true
end

return M
