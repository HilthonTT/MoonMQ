local Server = require("src.server.server")
local Config = require("src.server.config")
local auth_m = require("src.server.auth")
local users_m = require("src.server.users")
local quota_m = require("src.server.quota")
local tls_m = require("src.io.tls")
local version_m = require("src.core.version")
local Log    = require("src.log.logger")
local repl = require("src.repl.repl")

local log = Log.get("main")

local function is_main(argv, ...)
    local n_arg = argv and #argv or 0
    if n_arg == select("#", ...) then
        for i = 1, n_arg do
            if argv[i] ~= select(i, ...) then return false end
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

-- Build the user store, the authenticator over it, and the quota manager that
-- reads per-user limits from the same parsed config.
--
-- A configuration problem here is FATAL rather than a warning that degrades to
-- an open broker: an unparseable ACL or a bad credential means the operator's
-- intent is unknown, and guessing at it is how a broker ends up serving
-- everyone. The one exception is "no credentials at all", which stays the
-- documented OPEN mode.
--
-- Returns (authenticator, quotas). Either may be nil.
local function build_auth(cfg)
    local ac = cfg.Auth or {}

    if ac.Password == "CHANGE_ME" then
        log:warn("default password in use; replace Auth.Password")
    end

    local store, serr = users_m.load(ac)
    if serr then
        log:error("Auth config: %s", serr)
        os.exit(1)
    end
    if not store then
        log:warn("no credentials configured, server is OPEN "
            .. "(any client may produce, consume, and delete any topic)")
        return nil, nil
    end

    local authenticator = auth_m.authenticator({
        store          = store,
        max_failures   = ac.MaxFailures,
        failure_window = ac.FailureWindow,
        ban_duration   = ac.BanDuration,
    })
    log:info("auth: %d user(s): %s", store:count(), store:describe())

    -- Quotas: a default applying to every principal, per-user overrides drawn
    -- from the user entries themselves, and per-topic ceilings that apply to
    -- whoever touches that topic.
    local qc = ac.Quotas or {}
    local default_spec, derr = quota_m.spec(qc.Default)
    if derr then
        log:error("Auth.Quotas.Default: %s", derr)
        os.exit(1)
    end

    local topic_specs = {}
    for name, block in pairs(qc.Topics or {}) do
        local spec, terr = quota_m.spec(block)
        if terr then
            log:error("Auth.Quotas.Topics.%s: %s", name, terr)
            os.exit(1)
        end
        topic_specs[name] = spec
    end

    local user_specs = store:quota_specs()

    local quotas
    if default_spec or next(topic_specs) ~= nil or next(user_specs) ~= nil then
        quotas = quota_m.new({
            default       = default_spec,
            users         = user_specs,
            topics        = topic_specs,
            burst_seconds = qc.BurstSeconds,
        })
        local n_users, n_topics = 0, 0
        for _ in pairs(user_specs)  do n_users  = n_users  + 1 end
        for _ in pairs(topic_specs) do n_topics = n_topics + 1 end
        log:info("quotas enabled (default=%s, %d user override(s), %d topic rule(s))",
            default_spec and "yes" or "no", n_users, n_topics)
    end

    return authenticator, quotas
end

-- Resolve one TLS block into (server_config, client_config).
--
-- Fatal on error, like the rest of the security config: an unreadable
-- certificate or a Verify with nothing to verify against means the operator
-- asked for encryption and would not be getting it. Falling back to plaintext
-- there is the one outcome nobody wants.
--
-- `mode` selects which halves to build: a listener needs only the server side,
-- an inter-broker link needs both (its own listener and the client that dials
-- its peers), and both are built from ONE config block so a cluster cannot be
-- encrypted in one direction only.
local function build_tls(block, where, mode)
    local server_cfg, client_cfg

    if mode ~= "client" then
        local cfg, err = tls_m.server_config(block, where)
        if err then
            log:error("%s", err)
            os.exit(1)
        end
        server_cfg = cfg
    end

    if mode ~= "server" then
        local cfg, err = tls_m.client_config(block, where)
        if err then
            log:error("%s", err)
            os.exit(1)
        end
        client_cfg = cfg
    end

    -- The block validated, but a broker built without luasec cannot honour
    -- it. Same fatal treatment: the alternative is a port the operator
    -- believes is encrypted and isn't.
    if server_cfg or client_cfg then
        local ok, err = tls_m.require_available(where)
        if not ok then
            log:error("%s", err)
            os.exit(1)
        end
    end

    return server_cfg, client_cfg
end

-- Metrics-endpoint credentials. The token may come from the environment so it
-- does not have to live in a file that ships with the repo.
local function build_metrics_auth(cfg)
    local mc = Config.get(cfg, "Server.MetricsAuth", nil)
    local token = os.getenv("MOONMQ_METRICS_TOKEN")
    if type(mc) ~= "table" and not token then return nil end
    mc = type(mc) == "table" and mc or {}

    if mc.Token and mc.Token ~= "" then token = mc.Token end
    local basic = mc.Basic == true

    if not token and not basic then return nil end
    return { token = token, basic = basic }
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

    local authenticator, quotas = build_auth(cfg)

    -- Replication config (single-leader, static). Peers are the followers a
    -- leader ships to; map JSON {Id, Address} to the {id, address} the server
    -- expects. Absent/Enabled=false leaves replication off.
    local rep = s.Replication or {}
    local peers = {}
    for _, p in ipairs(rep.Peers or {}) do
        peers[#peers + 1] = { id = p.Id, address = p.Address }
    end
    -- One Replication.Tls block yields both halves: the follower's /replicate
    -- listener and the leader's client to it.
    local rep_server_tls, rep_client_tls = build_tls(rep.Tls, "Replication.Tls")
    local replication = {
        enabled        = rep.Enabled or false,
        replica_id     = rep.ReplicaId or 1,
        role           = rep.Role or "leader",
        replicate_host = rep.ReplicateHost or "127.0.0.1",
        replicate_port = rep.ReplicatePort,
        peers          = peers,
        lag_max        = rep.LagMax,
        ack_timeout    = rep.AckTimeout,
        server_tls     = rep_server_tls,
        tls            = rep_client_tls,
    }

    -- Cluster config (static membership + partition ownership; see
    -- docs/cluster.md). Absent → single-broker, zero overhead.
    local cluster = nil
    local cl = s.Cluster
    if cl and cl.Enabled ~= false and cl.BrokerId then
        local cluster_peers = {}
        for _, p in ipairs(cl.Peers or {}) do
            cluster_peers[#cluster_peers + 1] =
                { id = p.Id, address = p.Address, token = p.Token }
        end
        local cl_server_tls, cl_client_tls = build_tls(cl.Tls, "Cluster.Tls")
        cluster = {
            broker_id    = cl.BrokerId,
            host         = cl.Host or "127.0.0.1",
            port         = cl.Port,
            peers        = cluster_peers,
            token        = cl.Token,
            peer_timeout = cl.PeerTimeout,
            batch_bytes  = cl.BatchBytes,
            server_tls   = cl_server_tls,
            tls          = cl_client_tls,
        }
    end

    -- Autobalancer loop (needs Cluster; run on ONE broker per cluster).
    local autobalance = nil
    local ab = s.Autobalance
    if ab and ab.Enabled ~= false and cluster then
        autobalance = {
            interval_s             = ab.IntervalSeconds,
            dry_run                = ab.DryRun,
            window                 = ab.Window,
            min_valid              = ab.MinValid,
            percentile             = ab.Percentile,
            max_actions_per_detect = ab.MaxActionsPerDetect,
        }
    end

    local srv = assert(Server.new({
        acks                   = s.Acks,   -- "none" | "leader" | "all" (default leader)
        replication            = replication,
        cluster                = cluster,
        autobalance            = autobalance,
        -- Dead-letter queue tuning (nil = defaults: ".dlq", 3 deliveries).
        dlq                    = s.Dlq and {
            suffix         = s.Dlq.Suffix,
            max_deliveries = s.Dlq.MaxDeliveries,
        } or nil,
        data_dir               = s.DataDir or "./data_server",
        -- Storage engine for topics that don't pin one themselves: "segmented"
        -- (default) or "commitlog". Per-topic config sidecars still win.
        default_backend        = s.StorageBackend,
        host                   = s.Host or "0.0.0.0",
        port                   = s.Port or 9092,
        max_connections        = s.MaxConnections,
        max_connections_per_ip = s.MaxConnectionsPerIP,
        -- Descriptors reserved for non-connection use (listeners, metrics,
        -- replication, one open log + timeindex per partition). Raise it on a
        -- broker with many partitions; see Server.new.
        fd_reserve             = s.FdReserve,
        max_frame              = s.MaxFrameSize,
        max_pending_bytes      = s.MaxPendingBytes,
        send_deadline          = s.SendDeadline,
        idle_deadline          = s.IdleDeadline,
        pre_auth_read_deadline = s.PreAuthReadDeadline,
        handshake_deadline     = s.HandshakeDeadline,
        heartbeat_interval       = s.HeartbeatInterval,
        heartbeat_miss_threshold = s.HeartbeatMissThreshold,
        max_topics             = s.MaxTopics,
        max_list_topics        = s.MaxListTopics,
        -- Idle durable-producer expiry (seconds; 0 disables). Defaults to
        -- one day, matching Kafka's producer.id.expiration.ms.
        producer_expiry_s              = s.ProducerExpirySeconds,
        producer_expiry_check_interval = s.ProducerExpiryCheckIntervalSeconds,
        metrics_host           = s.MetricsHost or "127.0.0.1",
        metrics_port           = s.MetricsPort or 9090,
        authenticator          = authenticator,
        -- Per-user / per-topic quotas, and the credential gate on the metrics
        -- listener. Both nil unless configured; see docs/security.md.
        quotas                 = quotas,
        metrics_auth           = build_metrics_auth(cfg),
        -- TLS, per listener. Absent blocks leave that port in plaintext, which
        -- is what every existing deployment gets on upgrade.
        tls                    = (build_tls(s.Tls, "Server.Tls", "server")),
        metrics_tls            = (build_tls(s.MetricsTls, "Server.MetricsTls", "server")),
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