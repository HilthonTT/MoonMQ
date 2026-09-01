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

local READ_DEADLINE  = 5
local WRITE_DEADLINE = 5

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

function M.new(opts)
    return setmetatable({
        reactor = assert(opts.reactor, "reactor required"),
        host    = opts.host or "127.0.0.1",
        port    = opts.port or 9090,
        server  = opts.server,
        auth          = opts.auth,
        authenticator = opts.authenticator,
        tls           = opts.tls,
    }, M)
end

local PUBLIC_PATHS = { ["/health"] = true }

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

        if principal and not principal.acl:authorized(
                acl_m.RES_CLUSTER, nil, acl_m.OP_DESCRIBE) then
            return false, "principal lacks cluster:describe"
        end
        return true
    end

    return false, "unsupported authentication scheme"
end

local function write_response(reactor, sock, status, content_type, body, extra)
    http_kit.respond(reactor, sock, status, content_type, body,
        { deadline = WRITE_DEADLINE, no_store = true, extra = extra })
end

local PROM_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

function M:_handle(sock, peer, ip)
    metrics.inc("moonmq_metrics_http_requests_total")
    local deadline = socket.gettime() + READ_DEADLINE

    local req = http_kit.read_headers(self.reactor, sock, deadline)
    if not req then
        pcall(function() sock:close() end)
        return
    end

    local method, path = http_kit.request_line(req)
    if not method then
        pcall(function()
            write_response(self.reactor, sock, 400, "text/plain", "bad request\n")
            sock:close()
        end)
        return
    end

    if self.auth and not PUBLIC_PATHS[path] then
        local ok, reason = self:_authorized(req, ip)
        if not ok then
            metrics.inc("moonmq_metrics_http_unauthorized_total")
            log:warn("metrics request from %s refused: %s",
                peer or "?", reason or "unauthorized")
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
