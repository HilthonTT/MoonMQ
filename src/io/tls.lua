-- TLS parameters and socket wrapping, shared by every listener and every
-- client in the tree.
--
-- Until this existed, everything MoonMQ spoke was plaintext: the client
-- protocol, the `/replicate` follower endpoint, the `/cluster/*` peer
-- endpoints, and `/metrics`. SCRAM had already taken the password off the
-- wire, but record payloads, topic names, group names and the cluster token
-- were all readable by anyone on the path. This is the other half.
--
-- **luasec is optional.** A broker without it boots exactly as before and
-- refuses only the configurations that ask for TLS — the same posture the
-- codebase takes for lua-zlib and luaossl. What it will not do is quietly
-- fall back to plaintext on a listener the operator asked to encrypt.
--
-- Configuration is per listener, because the four of them have genuinely
-- different threat models (a public client port, a private replication port,
-- a scrape endpoint) and an operator may well encrypt one and not the next:
--
--     "Tls": {
--       "Enabled": true,
--       "CertFile": "/etc/moonmq/server.crt",
--       "KeyFile":  "/etc/moonmq/server.key",
--       "CaFile":   "/etc/moonmq/ca.crt",   -- verify peers against this
--       "Verify":   "none" | "peer" | "required",
--       "Protocol": "any",                  -- luasec protocol selector
--       "Ciphers":  "...",
--       "HandshakeTimeout": 10
--     }
--
-- Everything is validated at load: a missing certificate, an unreadable key,
-- or `Verify: "required"` with no CA stops the broker at boot naming the
-- listener. A TLS config that silently does not apply is the failure mode
-- this is written to prevent.

local log = require("src.log.logger").get("tls")

local M = {}

local ssl_ok, ssl = pcall(require, "ssl")
if not ssl_ok then ssl = nil end

-- Whether this host can do TLS at all. Read by the config layer to turn a
-- missing rock into one clear boot error instead of a stack trace.
M.available = ssl ~= nil

function M.version()
    return ssl and (ssl._VERSION or "unknown") or nil
end

-- Modern floor. "any" plus these options means TLS 1.2+ in practice: it lets
-- OpenSSL negotiate the best version both ends know (1.3 when available)
-- while refusing everything with a published break. Pinning "tlsv1_2"
-- instead would exclude 1.3, which is the opposite of the intent.
local DEFAULT_OPTIONS  = { "all", "no_sslv2", "no_sslv3", "no_tlsv1", "no_tlsv1_1" }
local DEFAULT_PROTOCOL = "any"

-- A handshake is the one part of connection setup a peer can stall without
-- having sent anything meaningful, so it gets its own deadline rather than
-- inheriting the (longer) idle one.
local DEFAULT_HANDSHAKE_TIMEOUT = 10

M.DEFAULT_HANDSHAKE_TIMEOUT = DEFAULT_HANDSHAKE_TIMEOUT

local function readable(path)
    local f = io.open(path, "rb")
    if not f then return false end
    f:close()
    return true
end

-- Where the OS keeps its CA bundle, so a client that says `Tls = true` (or
-- gives no CaFile) can still verify against the system trust store instead of
-- failing to start. luasec has no built-in default: without an explicit
-- cafile or capath it verifies against nothing and rejects everything.
local SYSTEM_CA_FILES = {
    "/etc/ssl/certs/ca-certificates.crt",   -- Debian, Ubuntu, Alpine
    "/etc/pki/tls/certs/ca-bundle.crt",     -- RHEL, Fedora
    "/etc/ssl/cert.pem",                    -- BSD, macOS (Homebrew OpenSSL)
}

local function system_ca_file()
    for _, path in ipairs(SYSTEM_CA_FILES) do
        if readable(path) then return path end
    end
    return nil
end

local function copy(list)
    local out = {}
    for i = 1, #list do out[i] = list[i] end
    return out
end

-- Normalize a config block into luasec params. Returns:
--   (nil, nil)   TLS not requested for this listener — carry on in plaintext
--   (nil, err)   requested but unusable; the caller should refuse to start
--   (cfg, nil)   { params = <luasec params>, handshake_timeout = n }
--
-- `where` names the listener in error messages ("Server.Tls", "Cluster.Tls"),
-- because "certificate file not found" is useless when four listeners can
-- each configure one.
-- First present key wins. Config comes from appsettings.json (PascalCase) but
-- the Lua client takes the same block inline, where lowercase reads better;
-- accepting both costs one lookup and removes a whole class of "why is my TLS
-- config being ignored".
local function field(block, ...)
    for i = 1, select("#", ...) do
        local key = select(i, ...)
        if block[key] ~= nil then return block[key] end
    end
    return nil
end

local function build(block, where, mode)
    -- `Tls = true` is the shorthand for "encrypt, with the defaults".
    if block == true then block = { Enabled = true } end
    if type(block) ~= "table" then return nil, nil end
    -- Enabled defaults to true when the block exists at all: writing out a
    -- cert path and getting plaintext because a flag was missing is a trap.
    if field(block, "Enabled", "enabled") == false then return nil, nil end

    -- NB: no `M.available` check here. Validating a block is a pure function
    -- of the block plus the files it names, and it stays that way so a host
    -- without luasec still reports the REAL problem ("cannot read CertFile")
    -- rather than masking every mistake behind the missing rock. Whether this
    -- build can actually speak TLS is a separate question, asked by whoever
    -- is about to open the socket — see M.require_available.
    local params = {
        mode     = mode,
        protocol = field(block, "Protocol", "protocol") or DEFAULT_PROTOCOL,
        options  = copy(DEFAULT_OPTIONS),
    }

    local cert = field(block, "CertFile", "certfile", "Certificate", "certificate")
    local key  = field(block, "KeyFile", "keyfile", "Key", "key")
    local ca   = field(block, "CaFile", "cafile", "CaCert", "cacert")
    local capath = field(block, "CaPath", "capath")

    if mode == "server" then
        if not cert or cert == "" or not key or key == "" then
            return nil, string.format(
                "%s: a TLS listener needs both CertFile and KeyFile", where)
        end
    end

    if cert and cert ~= "" then
        if not readable(cert) then
            return nil, string.format("%s: cannot read CertFile %q", where, cert)
        end
        params.certificate = cert
    end
    if key and key ~= "" then
        if not readable(key) then
            return nil, string.format("%s: cannot read KeyFile %q", where, key)
        end
        params.key = key
    end
    if ca and ca ~= "" then
        if not readable(ca) then
            return nil, string.format("%s: cannot read CaFile %q", where, ca)
        end
        params.cafile = ca
    end

    -- A directory of hashed CA certificates — how a client trusts the system
    -- trust store (/etc/ssl/certs) rather than one pinned CA file.
    if capath and capath ~= "" then
        params.capath = capath
    end

    local ciphers = field(block, "Ciphers", "ciphers")
    if ciphers and ciphers ~= "" then
        params.ciphers = ciphers
    end

    -- Verify semantics, spelled the same on both sides:
    --   "none"     accept any peer certificate (a server's default: clients
    --              usually have none)
    --   "peer"     validate one if offered
    --   "required" validate one and refuse the connection without it (mTLS)
    -- `Insecure = true` is the explicit, greppable way to turn verification
    -- off — for a self-signed development broker, and nowhere else.
    local verify = field(block, "Verify", "verify")
    if verify == nil and field(block, "Insecure", "insecure") == true then
        verify = "none"
    end
    verify = tostring(verify or (mode == "client" and "peer" or "none")):lower()
    if verify == "none" then
        params.verify = { "none" }
    elseif verify == "peer" then
        params.verify = { "peer" }
    elseif verify == "required" then
        params.verify = { "peer", "fail_if_no_peer_cert" }
    else
        return nil, string.format(
            "%s: Verify must be none, peer or required (got %q)", where, verify)
    end

    if verify ~= "none" and not params.cafile and not params.capath then
        -- A CLIENT with no CA named falls back to the system trust store,
        -- which is what "verify this public certificate" means everywhere
        -- else. A LISTENER does not: `Verify=required` there means mTLS
        -- against a private CA, and quietly accepting every client signed by
        -- any public CA on the machine would be a serious surprise.
        local system = mode == "client" and system_ca_file() or nil
        if system then
            params.cafile = system
        else
            -- Without a CA there is nothing to validate against, and luasec
            -- would fail every handshake with an opaque "certificate verify
            -- failed".
            return nil, string.format(
                "%s: Verify=%q needs a CaFile (or CaPath) to validate against",
                where, verify)
        end
    end

    local timeout = field(block, "HandshakeTimeout", "handshake_timeout")
                    or DEFAULT_HANDSHAKE_TIMEOUT
    if type(timeout) ~= "number" or timeout <= 0 then
        return nil, string.format("%s: HandshakeTimeout must be a positive number", where)
    end

    return {
        params            = params,
        handshake_timeout = timeout,
        verify            = verify,
        -- The name to send in SNI and to check the certificate against. Only
        -- needed when it differs from the host being dialled (a broker reached
        -- by IP, or through a load balancer).
        server_name       = field(block, "ServerName", "server_name"),
    }, nil
end

function M.server_config(block, where)
    return build(block, where or "Tls", "server")
end

function M.client_config(block, where)
    return build(block, where or "Tls", "client")
end

-- Ask, at the point of use, whether this build can actually speak TLS.
--
-- Kept apart from the config functions on purpose: a config that fails to
-- validate is the operator's mistake and should say so, while a missing rock
-- is the machine's and should say *that*. Call this once a config has come
-- back non-nil and you are about to bind or dial with it — a listener the
-- operator asked to encrypt must refuse to start, never fall back to
-- plaintext.
--
-- Returns true, or (nil, err).
function M.require_available(where)
    if M.available then return true end
    return nil, string.format(
        "%s: TLS is configured but luasec is not installed "
        .. "(luarocks install luasec)", where or "Tls")
end

-- Wrap an already-connected socket. Does NOT handshake: that has to be driven
-- by whoever owns the scheduler (Reactor:tls_handshake for the broker, a
-- bounded blocking loop for the client), because a handshake can block in
-- either direction and must not stall the event loop.
--
-- NB: luasec takes ownership of the underlying socket, and the original
-- object must not be used again — read anything you need off it (peer name,
-- descriptor) BEFORE wrapping.
function M.wrap(sock, params)
    if not ssl then return nil, "luasec not installed" end
    return ssl.wrap(sock, params)
end

-- Server Name Indication, best-effort: it lets a peer terminating several
-- names on one address pick the right certificate, and it is what a client
-- verifying a hostname must send. Older luasec builds may not expose it.
function M.set_sni(sock, host)
    if type(sock.sni) == "function" and type(host) == "string" and host ~= "" then
        pcall(sock.sni, sock, host)
    end
end

-- Hostname verification. luasec validates the certificate CHAIN during the
-- handshake, but it does not check that the name on the certificate is the
-- host we dialled — a valid certificate for any host would otherwise pass,
-- which defeats the point of verification against a public CA.
function M.check_hostname(sock, host)
    if type(sock.getpeercertificate) ~= "function" then return true end
    local ok, cert = pcall(sock.getpeercertificate, sock)
    if not ok or not cert then
        return false, "peer presented no certificate"
    end
    if type(cert.validat) ~= "function" then return true end
    -- luasec exposes OpenSSL's own name matching via extensions/subject; the
    -- portable check here is setverifyext + a subject/SAN scan.
    --
    -- The three buckets are kept apart rather than concatenated, because RFC
    -- 6125 matches each against a different kind of host: an IP literal only
    -- ever against an iPAddress SAN, a name against dNSName, and the CN only
    -- as a legacy fallback when there is no dNSName at all.
    local dns_names, ip_names, common_names = {}, {}, {}

    local function collect(into, value)
        if type(value) == "string" then
            into[#into + 1] = value
        elseif type(value) == "table" then
            for _, entry in ipairs(value) do into[#into + 1] = tostring(entry) end
        end
    end

    local ok_ext, extensions = pcall(cert.extensions, cert)
    if ok_ext and type(extensions) == "table" then
        local san = extensions["2.5.29.17"]   -- subjectAltName
        if type(san) == "table" then
            collect(dns_names, san.dNSName)
            -- luasec renders these as text ("127.0.0.1", "::1"), which is the
            -- form a peer address is configured in. Without this bucket a
            -- broker reached by IP — how a cluster peer or a replica is
            -- normally addressed — rejects its own correctly-issued
            -- certificate, because an IP never appears as a dNSName.
            collect(ip_names, san.iPAddress)
        end
    end
    local ok_sub, subject = pcall(cert.subject, cert)
    if ok_sub and type(subject) == "table" then
        for _, entry in ipairs(subject) do
            if entry.name == "commonName" or entry.oid == "2.5.4.3" then
                common_names[#common_names + 1] = tostring(entry.value)
            end
        end
    end

    -- IPv4 dotted quad, or anything with a colon (IPv6). A host that is an
    -- address is never matched by a name or a wildcard: "*.example.com" must
    -- not stand in for 10.0.0.1, and neither must a CN that happens to read
    -- like one.
    local is_ip = host:find(":", 1, true) ~= nil
                  or host:match("^%d+%.%d+%.%d+%.%d+$") ~= nil

    local candidates
    if is_ip then
        candidates = ip_names
    elseif #dns_names > 0 then
        candidates = dns_names
    else
        candidates = common_names
    end

    local target = host:lower()
    for _, raw_name in ipairs(candidates) do
        local name = raw_name:lower()
        if name == target then return true end
        if not is_ip then
            -- One level of wildcard, the only form worth honouring:
            -- *.example.com matches a.example.com but not a.b.example.com or
            -- example.com.
            local suffix = name:match("^%*(%.[^%.]+%..+)$")
            if suffix and #target > #suffix then
                local head = target:sub(1, #target - #suffix)
                if target:sub(-#suffix) == suffix and not head:find("%.") then
                    return true
                end
            end
        end
    end

    -- Report everything the certificate carries, not just the bucket that was
    -- consulted: "presented: localhost" when you dialled an IP is the clue
    -- that the certificate needs an IP SAN, and that is the whole point of
    -- the message.
    local presented = {}
    for _, n in ipairs(dns_names)    do presented[#presented + 1] = n end
    for _, n in ipairs(ip_names)     do presented[#presented + 1] = n end
    for _, n in ipairs(common_names) do presented[#presented + 1] = n end

    return false, string.format("certificate is not valid for %q (presented: %s)",
        host, #presented > 0 and table.concat(presented, ", ") or "no names")
end

-- Connect-side handshake for a caller with no event loop — the Lua client in
-- its default blocking mode, and anything else holding one socket.
--
-- The socket stays in timeout mode, so OpenSSL blocks inside the handshake
-- rather than spinning here; the loop exists for the cases where luasec still
-- reports a want-state, and it is bounded by `deadline` so a peer that
-- completes TCP and then stops cannot hang the caller forever.
--
-- Returns (wrapped_socket, nil) or (nil, err). On failure the underlying
-- socket has been closed.
function M.connect_handshake(sock, cfg, host, timeout)
    local wrapped, werr = M.wrap(sock, cfg.params)
    if not wrapped then
        pcall(function() sock:close() end)
        return nil, werr or "tls wrap failed"
    end

    local server_name = cfg.server_name or host
    M.set_sni(wrapped, server_name)
    wrapped:settimeout(timeout or cfg.handshake_timeout)

    local socket_m = require("socket")
    local deadline = socket_m.gettime() + (cfg.handshake_timeout or DEFAULT_HANDSHAKE_TIMEOUT)

    while true do
        local ok, herr = wrapped:dohandshake()
        if ok then break end

        if herr ~= "timeout" and herr ~= "wantread" and herr ~= "wantwrite" then
            pcall(function() wrapped:close() end)
            return nil, string.format("tls handshake failed: %s", tostring(herr))
        end
        if socket_m.gettime() > deadline then
            pcall(function() wrapped:close() end)
            return nil, "tls handshake deadline exceeded"
        end
    end

    -- luasec validated the chain; it did not check that the name on the
    -- certificate is the host we asked for. Without this, any certificate
    -- signed by the trusted CA — including one issued to an attacker for a
    -- name they do control — would be accepted.
    if cfg.verify ~= "none" then
        local vok, verr = M.check_hostname(wrapped, server_name)
        if not vok then
            pcall(function() wrapped:close() end)
            return nil, verr
        end
    end

    return wrapped, nil
end

-- A LuaSocket `create` function that yields TLS connections, for the two
-- inter-broker HTTP clients (src/cluster/peer.lua, src/server/replica.lua).
--
-- Modelled on luasec's own ssl.https, with one deliberate difference: the
-- per-request timeout is honoured. ssl.https ignores the caller's timeout and
-- hard-codes its module-level 60s, which on a reactor thread is a very long
-- time to be blocked by an unreachable peer.
function M.http_create(params, timeout, host_override)
    local socket = require("socket")
    return function()
        local conn = { sock = socket.tcp() }
        if not conn.sock then return nil, "socket.tcp failed" end
        conn.sock:settimeout(timeout)

        function conn:settimeout()
            return self.sock:settimeout(timeout)
        end

        -- Defined BEFORE connect, not left to the forwarding loop below.
        -- socket.http wraps everything from settimeout onwards in a finalizer
        -- that calls close() on this object, so on a refused connection or a
        -- failed handshake — the common case for an unreachable peer — the
        -- forwarding loop has not run yet and close would be nil. That turns
        -- "connection refused" into a raised "attempt to call a nil value
        -- (method 'close')", and leaks the descriptor on the way out.
        -- `self.sock` is rebound by connect, so this reaches the wrapped
        -- socket once there is one.
        function conn:close()
            return self.sock:close()
        end

        function conn:connect(host, port)
            local ok, err = self.sock:connect(host, port)
            if not ok then return nil, err end

            local wrapped, werr = M.wrap(self.sock, params)
            if not wrapped then return nil, werr end
            self.sock = wrapped
            M.set_sni(self.sock, host_override or host)
            self.sock:settimeout(timeout)

            local hok, herr = self.sock:dohandshake()
            if not hok then return nil, herr end

            if params.verify and params.verify[1] ~= "none" then
                local vok, verr = M.check_hostname(self.sock, host_override or host)
                if not vok then return nil, verr end
            end

            -- Forward every method to the wrapped socket, so LuaSocket's HTTP
            -- layer sees an ordinary connection from here on.
            local mt = getmetatable(self.sock).__index
            for name, method in pairs(mt) do
                if type(method) == "function" and name ~= "connect"
                   and name ~= "settimeout" and name ~= "close" then
                    self[name] = function(s, ...) return method(s.sock, ...) end
                end
            end
            return 1
        end

        return conn
    end
end

-- One line at boot per listener, so "is this port encrypted?" is answerable
-- from the log rather than by reading config.
function M.describe(cfg)
    if not cfg then return "plaintext" end
    return string.format("TLS (%s, verify=%s)",
        cfg.params.protocol, cfg.verify)
end

function M.log_unavailable()
    if not M.available then
        log:debug("luasec not installed; TLS unavailable")
    end
end

return M
