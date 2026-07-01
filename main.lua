local Server = require("src.server.server")
local Config = require("src.server.config")
local auth_m = require("src.server.auth")
local version_m = require("src.core.version")
local Log    = require("src.log.logger")
local repl = require("src.repl.repl")

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

local function run_cli_command(argv)
    local cmd = argv and argv[1]

    if cmd == "--repl" then
        repl()
        return true
    end

    if cmd == "version" or cmd == "--version" or cmd == "-v" then
        local sub = argv[2]
        if sub == "--json" then
            print(version_m.GetVersionJSON())
        elseif sub == "--short" then
            print(version_m.GetVersionInfo():Short())
        else
            print(version_m.GetVersionInfo():String())
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
    if run_cli_command(arg) then
        os.exit(0)
    end

    local cfg, cerr = Config.load()
    if not cfg then
        log:error("config: %s", cerr)
        os.exit(1)
    end

    -- Empty string from JSON means "no file" (cleaner than nil/missing
    -- key distinction); Logger.configure interprets that as stderr-only.
    local log_file = Config.get(cfg, "Logging.File", "")
    if log_file == "" then log_file = nil end
    Log.configure({
        level         = Config.get(cfg, "Logging.Level", "INFO"),
        file_path     = log_file,
        log_to_stderr = Config.get(cfg, "Logging.LogToStderr", true),
    })

    local s = cfg.Server or {}
    local gc = s.GroupCommit or {}
    local srv = assert(Server.new({
        data_dir               = s.DataDir or "./data_server",
        -- Storage engine for topics that don't pin one themselves: "segmented"
        -- (default) or "commitlog". Per-topic config sidecars still win.
        default_backend        = s.StorageBackend,
        host                   = s.Host or "0.0.0.0",
        port                   = s.Port or 9092,
        max_connections        = s.MaxConnections,
        max_connections_per_ip = s.MaxConnectionsPerIP,
        max_frame              = s.MaxFrameSize,
        idle_deadline          = s.IdleDeadline,
        pre_auth_read_deadline = s.PreAuthReadDeadline,
        handshake_deadline     = s.HandshakeDeadline,
        max_topics             = s.MaxTopics,
        max_list_topics        = s.MaxListTopics,
        metrics_host           = s.MetricsHost or "127.0.0.1",
        metrics_port           = s.MetricsPort or 9090,
        authenticator          = build_authenticator(cfg),
        -- LingerMs is the JSON unit (Kafka-conventional); convert to seconds
        -- at the boundary. Server.new accepts seconds so all internal math
        -- stays in one unit (socket.gettime() is seconds).
        group_commit_linger_s    = gc.LingerMs and gc.LingerMs / 1000 or nil,
        group_commit_max_waiters = gc.MaxWaiters,
    }))

    log:info("env=%s host=%s port=%d data_dir=%s",
        cfg._environment, srv.host, srv.port, s.DataDir or "./data_server")
    srv:start()
end