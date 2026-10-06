-- Turns the loaded appsettings tree into the options table Server.new takes.
-- Every builder returns (value, err) rather than exiting, so main.lua owns
-- the one exit path and each section can be tested on its own.
local Config  = require("src.server.config")
local auth_m  = require("src.server.auth")
local users_m = require("src.server.users")
local quota_m = require("src.server.quota")
local tls_m   = require("src.io.tls")
local log     = require("src.log.logger").get("main")

local M = {}

M.DEFAULT_DATA_DIR = "./data_server"

local function seconds(ms)
    return ms and ms / 1000 or nil
end

-- The timing keys Replication.Failover and Cluster.Raft share. An absent
-- ElectionTimeoutMaxMs is 1.6x ElectionTimeoutMs, so a lone minimum still
-- leaves the randomised window elections need.
local function consensus_timing(block)
    local max_ms = block.ElectionTimeoutMaxMs
                   or (block.ElectionTimeoutMs and block.ElectionTimeoutMs * 1.6)
    return {
        election_min    = seconds(block.ElectionTimeoutMs),
        election_max    = seconds(max_ms),
        heartbeat_s     = seconds(block.HeartbeatMs),
        rpc_timeout     = seconds(block.RpcTimeoutMs),
        commit_wait     = block.CommitTimeoutSeconds,
        max_log_entries = block.MaxLogEntries,
    }
end
M.consensus_timing = consensus_timing

-- An optional section, or nil when it is absent or `Enabled: false`.
-- `true` is shorthand for a section with every default, as for TLS blocks.
local function section(block)
    if block == true then return {} end
    if type(block) == "table" and block.Enabled ~= false then return block end
    return nil
end

-- mode "server" builds only the listener half, "client" only the dialling
-- half, nil both. Built configs are registered for SIGHUP reload.
function M.tls(block, where, mode)
    local server_cfg, client_cfg

    if mode ~= "client" then
        local cfg, err = tls_m.server_config(block, where)
        if err then return nil, nil, err end
        server_cfg = cfg
    end
    if mode ~= "server" then
        local cfg, err = tls_m.client_config(block, where)
        if err then return nil, nil, err end
        client_cfg = cfg
    end

    if server_cfg or client_cfg then
        local ok, err = tls_m.require_available(where)
        if not ok then return nil, nil, err end
    end

    return tls_m.register(server_cfg), tls_m.register(client_cfg), nil
end

local function quotas(qc, store)
    local default_spec, derr = quota_m.spec(qc.Default)
    if derr then return nil, "Auth.Quotas.Default: " .. derr end

    local topic_specs, n_topics = {}, 0
    for name, block in pairs(qc.Topics or {}) do
        local spec, terr = quota_m.spec(block)
        if terr then return nil, string.format("Auth.Quotas.Topics.%s: %s", name, terr) end
        topic_specs[name] = spec
        n_topics = n_topics + 1
    end

    local user_specs, n_users = store:quota_specs(), 0
    for _ in pairs(user_specs) do n_users = n_users + 1 end

    if not default_spec and n_topics == 0 and n_users == 0 then return nil, nil end

    log:info("quotas enabled (default=%s, %d user override(s), %d topic rule(s))",
        default_spec and "yes" or "no", n_users, n_topics)
    return quota_m.new({
        default       = default_spec,
        users         = user_specs,
        topics        = topic_specs,
        burst_seconds = qc.BurstSeconds,
    }), nil
end

-- Returns authenticator, quotas, err. No credentials at all is the
-- documented OPEN mode: nil, nil, nil with a warning.
function M.auth(cfg)
    local ac = cfg.Auth or {}

    if ac.Password == "CHANGE_ME" then
        log:warn("default password in use; replace Auth.Password")
    end

    local store, serr = users_m.load(ac)
    if serr then return nil, nil, "Auth config: " .. serr end
    if not store then
        log:warn("no credentials configured, server is OPEN "
            .. "(any client may produce, consume, and delete any topic)")
        return nil, nil, nil
    end

    local authenticator = auth_m.authenticator({
        store          = store,
        max_failures   = ac.MaxFailures,
        failure_window = ac.FailureWindow,
        ban_duration   = ac.BanDuration,
    })
    log:info("auth: %d user(s): %s", store:count(), store:describe())

    local q, qerr = quotas(ac.Quotas or {}, store)
    if qerr then return nil, nil, qerr end
    return authenticator, q, nil
end

function M.metrics_auth(cfg, getenv)
    getenv = getenv or os.getenv
    local mc = Config.get(cfg, "Server.MetricsAuth", nil)
    local token = getenv("MOONMQ_METRICS_TOKEN")
    if type(mc) ~= "table" and not token then return nil end
    mc = type(mc) == "table" and mc or {}

    if mc.Token and mc.Token ~= "" then token = mc.Token end
    local basic = mc.Basic == true

    if not token and not basic then return nil end
    return { token = token, basic = basic }
end

function M.replication(rep)
    rep = rep or {}

    local peers = {}
    for _, p in ipairs(rep.Peers or {}) do
        peers[#peers + 1] = { id = p.Id, address = p.Address, client_address = p.ClientAddress }
    end

    local server_tls, client_tls, terr = M.tls(rep.Tls, "Replication.Tls")
    if terr then return nil, terr end

    local failover
    local fo = section(rep.Failover)
    if fo then
        failover = consensus_timing(fo)
        failover.isr_lag_s       = fo.IsrLagSeconds
        failover.min_isr         = fo.MinInsyncReplicas
        failover.max_fetch_wait  = seconds(fo.MaxFetchWaitMs)
        failover.max_fetch_bytes = fo.MaxFetchBytes
    end

    return {
        enabled        = rep.Enabled or false,
        replica_id     = rep.ReplicaId or 1,
        role           = rep.Role or "leader",
        replicate_host = rep.ReplicateHost or "127.0.0.1",
        replicate_port = rep.ReplicatePort,
        peers          = peers,
        lag_max        = rep.LagMax,
        ack_timeout    = rep.AckTimeout,
        server_tls     = server_tls,
        tls            = client_tls,
        failover       = failover,
        token          = rep.Token,
        client_address = rep.ClientAddress,
    }, nil
end

-- nil (no error) when clustering is off or has no BrokerId.
function M.cluster(cl)
    cl = section(cl)
    if not cl or not cl.BrokerId then return nil, nil end

    local peers = {}
    for _, p in ipairs(cl.Peers or {}) do
        peers[#peers + 1] = { id = p.Id, address = p.Address, token = p.Token }
    end

    local server_tls, client_tls, terr = M.tls(cl.Tls, "Cluster.Tls")
    if terr then return nil, terr end

    local raft = section(cl.Raft)
    return {
        broker_id    = cl.BrokerId,
        host         = cl.Host or "127.0.0.1",
        port         = cl.Port,
        peers        = peers,
        token        = cl.Token,
        peer_timeout = cl.PeerTimeout,
        batch_bytes  = cl.BatchBytes,
        server_tls   = server_tls,
        tls          = client_tls,
        raft         = raft and consensus_timing(raft) or nil,
    }, nil
end

-- The balance loop only means something in a cluster.
function M.autobalance(ab, cluster)
    ab = section(ab)
    if not ab or not cluster then return nil end
    return {
        interval_s             = ab.IntervalSeconds,
        dry_run                = ab.DryRun,
        window                 = ab.Window,
        min_valid              = ab.MinValid,
        percentile             = ab.Percentile,
        max_actions_per_detect = ab.MaxActionsPerDetect,
    }
end

function M.dlq(d)
    if not d then return nil end
    return { suffix = d.Suffix, max_deliveries = d.MaxDeliveries }
end

-- The full Server.new options table, or nil and the first error.
function M.from_config(cfg, getenv)
    local s  = cfg.Server or {}
    local gc = s.GroupCommit or {}

    local authenticator, q, aerr = M.auth(cfg)
    if aerr then return nil, aerr end

    local replication, rerr = M.replication(s.Replication)
    if rerr then return nil, rerr end

    local cluster, cerr = M.cluster(s.Cluster)
    if cerr then return nil, cerr end

    local tls, _, terr = M.tls(s.Tls, "Server.Tls", "server")
    if terr then return nil, terr end
    local metrics_tls, _, mterr = M.tls(s.MetricsTls, "Server.MetricsTls", "server")
    if mterr then return nil, mterr end

    return {
        acks                   = s.Acks,
        replication            = replication,
        cluster                = cluster,
        autobalance            = M.autobalance(s.Autobalance, cluster),
        dlq                    = M.dlq(s.Dlq),
        data_dir               = s.DataDir or M.DEFAULT_DATA_DIR,
        default_backend        = s.StorageBackend,
        host                   = s.Host or "0.0.0.0",
        port                   = s.Port or 9092,
        max_connections        = s.MaxConnections,
        max_connections_per_ip = s.MaxConnectionsPerIP,
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
        producer_expiry_s              = s.ProducerExpirySeconds,
        producer_expiry_check_interval = s.ProducerExpiryCheckIntervalSeconds,
        metrics_host           = s.MetricsHost or "127.0.0.1",
        metrics_port           = s.MetricsPort or 9090,
        authenticator          = authenticator,
        quotas                 = q,
        metrics_auth           = M.metrics_auth(cfg, getenv),
        tls                    = tls,
        metrics_tls            = metrics_tls,
        group_commit_linger_s    = seconds(gc.LingerMs),
        group_commit_max_waiters = gc.MaxWaiters,
    }, nil
end

return M
