local options = require("src.server.options")
local auth    = require("src.server.auth")

local function no_env() return nil end

local function cred(password)
    return auth.hash_password(password, { iterations = 1000, format = auth.FORMAT_SCRAM })
end

describe("server options from config", function()
    it("fills the documented defaults for an empty config", function()
        local opts = assert(options.from_config({}, no_env))
        assert.are.equal("0.0.0.0", opts.host)
        assert.are.equal(9092, opts.port)
        assert.are.equal(options.DEFAULT_DATA_DIR, opts.data_dir)
        assert.are.equal("127.0.0.1", opts.metrics_host)
        assert.are.equal(9090, opts.metrics_port)
        assert.is_nil(opts.authenticator)
        assert.is_nil(opts.cluster)
        assert.is_nil(opts.autobalance)
        assert.is_nil(opts.tls)
        assert.is_false(opts.replication.enabled)
        assert.are.equal(1, opts.replication.replica_id)
    end)

    it("passes server keys through and converts milliseconds", function()
        local opts = assert(options.from_config({ Server = {
            Host = "10.0.0.1", Port = 9192, DataDir = "/var/moonmq", MaxTopics = 5,
            GroupCommit = { LingerMs = 4, MaxWaiters = 64 },
            Dlq = { Suffix = ".dead", MaxDeliveries = 3 },
        } }, no_env))
        assert.are.equal("10.0.0.1", opts.host)
        assert.are.equal(9192, opts.port)
        assert.are.equal("/var/moonmq", opts.data_dir)
        assert.are.equal(5, opts.max_topics)
        assert.are.equal(0.004, opts.group_commit_linger_s)
        assert.are.equal(64, opts.group_commit_max_waiters)
        assert.are.same({ suffix = ".dead", max_deliveries = 3 }, opts.dlq)
    end)

    it("builds an authenticator and quotas from Auth", function()
        local opts = assert(options.from_config({ Auth = {
            Users = { { Username = "admin", PasswordHash = cred("pw"), Superuser = true } },
            Quotas = { Default = { RequestsPerSec = 100 } },
        } }, no_env))
        assert.is_truthy(opts.authenticator)
        assert.is_truthy(opts.quotas)
    end)

    it("returns auth and quota errors instead of exiting", function()
        local opts, err = options.from_config({ Auth = {
            Users = { { Username = "admin", PasswordHash = "garbage" } } } }, no_env)
        assert.is_nil(opts)
        assert.is_truthy(err:find("Auth config"))

        opts, err = options.from_config({ Auth = {
            Users = { { Username = "admin", PasswordHash = cred("pw") } },
            Quotas = { Topics = { orders = { Nonsense = 1 } } },
        } }, no_env)
        assert.is_nil(opts)
        assert.is_truthy(err:find("Auth.Quotas.Topics.orders"))
    end)

    it("returns a TLS error instead of exiting", function()
        local opts, err = options.from_config({ Server = {
            Tls = { CertFile = "/nonexistent.pem", KeyFile = "/nonexistent.key" } } }, no_env)
        assert.is_nil(opts)
        assert.is_truthy(err:find("Server.Tls"))
    end)
end)

describe("consensus timing", function()
    it("converts milliseconds and widens a lone election minimum by 1.6x", function()
        local t = options.consensus_timing({
            ElectionTimeoutMs = 1000, HeartbeatMs = 150, RpcTimeoutMs = 800,
            CommitTimeoutSeconds = 5, MaxLogEntries = 100,
        })
        assert.are.equal(1.0, t.election_min)
        assert.are.equal(1.6, t.election_max)
        assert.are.equal(0.15, t.heartbeat_s)
        assert.are.equal(0.8, t.rpc_timeout)
        assert.are.equal(5, t.commit_wait)
        assert.are.equal(100, t.max_log_entries)
    end)

    it("prefers an explicit maximum", function()
        local t = options.consensus_timing({ ElectionTimeoutMs = 1000, ElectionTimeoutMaxMs = 3000 })
        assert.are.equal(3.0, t.election_max)
    end)

    it("leaves everything unset for an empty block", function()
        assert.are.same({}, options.consensus_timing({}))
    end)
end)

describe("replication options", function()
    it("maps peers and failover settings", function()
        local rep = assert(options.replication({
            Enabled = true, ReplicaId = 2, Role = "follower", ReplicatePort = 9095,
            Peers = { { Id = 1, Address = "10.0.0.1:9095", ClientAddress = "10.0.0.1:9092" } },
            Failover = { ElectionTimeoutMs = 1000, IsrLagSeconds = 10,
                         MinInsyncReplicas = 2, MaxFetchWaitMs = 500 },
        }))
        assert.is_true(rep.enabled)
        assert.are.equal(2, rep.replica_id)
        assert.are.same({ { id = 1, address = "10.0.0.1:9095",
                            client_address = "10.0.0.1:9092" } }, rep.peers)
        assert.are.equal(1.0, rep.failover.election_min)
        assert.are.equal(10, rep.failover.isr_lag_s)
        assert.are.equal(2, rep.failover.min_isr)
        assert.are.equal(0.5, rep.failover.max_fetch_wait)
    end)

    it("treats Failover = true as enabled with defaults, and Enabled = false as off", function()
        assert.are.same({}, assert(options.replication({ Failover = true })).failover)
        assert.is_nil(assert(options.replication({ Failover = { Enabled = false } })).failover)
    end)
end)

describe("cluster and autobalance options", function()
    it("needs a BrokerId to enable clustering", function()
        assert.is_nil((options.cluster({ Port = 9095 })))
        assert.is_nil((options.cluster({ BrokerId = "b1", Enabled = false })))
        assert.is_nil((options.cluster(nil)))
    end)

    it("maps peers and the optional Raft block", function()
        local cl = assert(options.cluster({
            BrokerId = "b1", Port = 9095, Token = "t",
            Peers = { { Id = "b2", Address = "10.0.0.2:9095" } },
            Raft = { ElectionTimeoutMs = 500 },
        }))
        assert.are.equal("b1", cl.broker_id)
        assert.are.equal("127.0.0.1", cl.host)
        assert.are.same({ { id = "b2", address = "10.0.0.2:9095" } }, cl.peers)
        assert.are.equal(0.5, cl.raft.election_min)
        assert.are.equal(0.8, cl.raft.election_max)

        assert.is_nil(assert(options.cluster({ BrokerId = "b1" })).raft)
    end)

    it("only balances inside a cluster", function()
        local ab = { IntervalSeconds = 60, DryRun = true }
        assert.is_nil(options.autobalance(ab, nil))
        local built = assert(options.autobalance(ab, { broker_id = "b1" }))
        assert.are.equal(60, built.interval_s)
        assert.is_true(built.dry_run)
        assert.is_nil(options.autobalance({ Enabled = false }, { broker_id = "b1" }))
    end)
end)

describe("metrics auth options", function()
    it("is off with no block and no environment token", function()
        assert.is_nil(options.metrics_auth({}, no_env))
    end)

    it("takes the token from the environment, the config winning when both are set", function()
        local env = function(k) return k == "MOONMQ_METRICS_TOKEN" and "from-env" or nil end
        assert.are.same({ token = "from-env", basic = false }, options.metrics_auth({}, env))
        assert.are.same({ token = "from-cfg", basic = false }, options.metrics_auth(
            { Server = { MetricsAuth = { Token = "from-cfg" } } }, env))
    end)

    it("supports Basic on its own", function()
        assert.are.same({ basic = true }, options.metrics_auth(
            { Server = { MetricsAuth = { Basic = true } } }, no_env))
        assert.is_nil(options.metrics_auth({ Server = { MetricsAuth = {} } }, no_env))
    end)
end)
