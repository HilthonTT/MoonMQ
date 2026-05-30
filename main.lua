local Server = require("src.server.server")
local Config = require("src.server.config")
local auth_m = require("src.server.auth")
local Log    = require("src.log.logger")

local log = Log.get("main")

local function is_main(_arg, ...)
    local n_arg = _arg and #_arg or 0
    if n_arg == select("#", ...) then
        for i = 1, n_arg do
            if _arg[i] ~= select(i, ...) then return false end
        end
        return true
    end
    return false
end

local function build_authenticator(cfg)
    local ac = cfg.Auth
    if not ac or not ac.Username then
        log:warn("Auth.Username missing, server is OPEN")
        return nil
    end

    local opts = {
        username       = ac.Username,
        max_failures   = ac.MaxFailures,
        failure_window = ac.FailureWindow,
        ban_duration   = ac.BanDuration,
    }

    if ac.PasswordHash and ac.PasswordHash ~= "" then
        opts.password_hash = ac.PasswordHash
    elseif ac.Password and ac.Password ~= "" then
        if ac.Password == "CHANGE_ME" then
            log:warn("default password in use; replace Auth.Password")
        end
        opts.password = ac.Password
    else
        log:warn("no credential configured, server is OPEN")
        return nil
    end

    return auth_m.static_authenticator(opts)
end

if is_main(arg, ...) then
    local cfg, cerr = Config.load()
    if not cfg then
        log:error("config: %s", cerr)
        os.exit(1)
    end

    Log.configure({ level = Config.get(cfg, "Logging.Level", "INFO") })

    local s = cfg.Server or {}
    local srv = assert(Server.new({
        data_dir               = s.DataDir or "./data_server",
        host                   = s.Host or "0.0.0.0",
        port                   = s.Port or 9092,
        max_connections        = s.MaxConnections,
        max_connections_per_ip = s.MaxConnectionsPerIP,
        max_frame              = s.MaxFrameSize,
        idle_deadline          = s.IdleDeadline,
        handshake_deadline     = s.HandshakeDeadline,
        metrics_host =  "127.0.0.1",
        metrics_port = 9090,
        authenticator          = build_authenticator(cfg),
    }))

    log:info("env=%s host=%s port=%d data_dir=%s",
        cfg._environment, srv.host, srv.port, s.DataDir or "./data_server")
    srv:start()
end