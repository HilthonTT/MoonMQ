local json            = require("dkjson")
local http            = require("socket.http")
local Reactor         = require("src.server.reactor")
local ClusterServer   = require("src.cluster.cluster_server")
local ControllerFence = require("src.cluster.controller_fence")
local Assignments     = require("src.cluster.assignments")
local Peer            = require("src.cluster.peer")
local RaftStore       = require("src.cluster.raft.store")
local RaftNode        = require("src.cluster.raft.node")
local RaftFence       = require("src.cluster.raft.fence")
local RaftService     = require("src.cluster.raft.service")
local ReplicaServer   = require("src.server.replica_server")
local replica_m       = require("src.server.replica")
local httpk           = require("src.server.http_kit")
local options         = require("src.server.options")
local Server          = require("src.server.server")
local brk_m           = require("src.broker")
local os_utils        = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_security_cluster_test"
                                      or "/tmp/moonmq_security_cluster_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function mkdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('mkdir "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("mkdir -p '%s'", path))
    end
end

local function mock_sock(request)
    local pos, out = 1, {}
    local sock = { _out = out, closed = false }
    sock.settimeout = function() end
    sock.close = function() sock.closed = true end
    sock.receive = function(_, n)
        if pos > #request then return nil, "closed" end
        local chunk = request:sub(pos, pos + n - 1)
        pos = pos + #chunk
        return chunk
    end
    sock.send = function(_, data, i, j)
        i, j = i or 1, j or #data
        out[#out + 1] = data:sub(i, j)
        return j
    end
    return sock
end

local function status_of(sock)
    return tonumber(table.concat(sock._out):match("^HTTP/1%.1 (%d+)"))
end

local function post(path, body, headers)
    local lines = { "POST " .. path .. " HTTP/1.1\r\n" }
    for k, v in pairs(headers or {}) do lines[#lines + 1] = k .. ": " .. v .. "\r\n" end
    lines[#lines + 1] = "Content-Length: " .. #body .. "\r\n\r\n"
    lines[#lines + 1] = body
    return table.concat(lines)
end

local function memory_store()
    return {
        state = RaftStore.empty_state(),
        save = function(self, state) self.state = state; return true end,
    }
end

local function intercept_http(fn)
    local real = http.request
    local captured
    http.request = function(req)
        captured = req
        req.sink('{"offset":1,"loads":{}}')
        req.sink(nil)
        return 1, 200, {}, "HTTP/1.1 200 OK"
    end
    local ok, err = pcall(fn)
    http.request = real
    assert(ok, err)
    return captured
end

describe("static replication endpoint token", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    local function new_server(token)
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("t", 1))
        return ReplicaServer.new({
            reactor = Reactor.new(), broker = broker, port = 0, token = token,
        }), broker
    end

    it("rejects POST /replicate without the token before touching the log", function()
        local rs, broker = new_server("s3cret")
        local before = broker.topic_manager.topics["t"].partitions[1].offset
        local sock = mock_sock(post("/replicate", string.rep("x", 64),
            { ["X-Topic"] = "t", ["X-Partition"] = "1" }))
        rs:_handle(sock)
        assert.are.equal(401, status_of(sock))
        assert.is_true(sock.closed)
        assert.are.equal(before, broker.topic_manager.topics["t"].partitions[1].offset)
    end)

    it("rejects a wrong token and passes a right one through", function()
        local rs = new_server("s3cret")
        local bad = mock_sock(post("/replicate", "x",
            { ["X-Topic"] = "t", ["X-Partition"] = "1", ["X-Cluster-Token"] = "nope" }))
        rs:_handle(bad)
        assert.are.equal(401, status_of(bad))

        local good = mock_sock(post("/replicate", "x",
            { ["X-Topic"] = "t", ["X-Partition"] = "1", ["X-Cluster-Token"] = "s3cret" }))
        rs:_handle(good)
        assert.are.equal(500, status_of(good))
    end)

    it("refuses an empty token at construction", function()
        assert.has_error(function() new_server("") end)
    end)

    it("sends the token from ReplicaClient with redirects off and a capped sink", function()
        local client = replica_m.ReplicaClient.new("127.0.0.1:1", 1, { token = "s3cret" })
        local req = intercept_http(function() client:send("t", 1, 1, "payload") end)
        assert.are.equal("s3cret", req.headers["X-Cluster-Token"])
        assert.is_false(req.redirect)
        local ok, err = req.sink(string.rep("x", httpk.MAX_RESPONSE + 1))
        assert.is_nil(ok)
        assert.matches("exceeds", err)
    end)

    it("wires Replication.Token into the static replicator clients", function()
        local srv = assert(Server.new({
            host = "127.0.0.1", port = 0, data_dir = BASE_DIR,
            replication = {
                enabled = true, role = "leader", token = "s3cret",
                peers = { { id = 2, address = "127.0.0.1:1" } },
            },
        }))
        assert.are.equal("s3cret", srv.replicator.followers[1].client.token)
    end)
end)

describe("handler exceptions", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("cluster server answers 500 and closes the socket", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local cs = ClusterServer.new({
            reactor = Reactor.new(), broker = broker, broker_id = "b1", port = 0,
            assignments = assert(Assignments.new(BASE_DIR, "b1")),
        })
        cs._loads = function() error("boom") end
        local sock = mock_sock("GET /cluster/loads HTTP/1.1\r\n\r\n")
        cs:_handle(sock)
        assert.are.equal(500, status_of(sock))
        assert.is_true(sock.closed)
    end)

    it("replica server answers 500 and closes the socket", function()
        local broker = assert(brk_m.Broker.new(BASE_DIR))
        local rs = ReplicaServer.new({ reactor = Reactor.new(), broker = broker, port = 0 })
        rs._apply = function() error("boom") end
        local sock = mock_sock(post("/replicate", "x",
            { ["X-Topic"] = "t", ["X-Partition"] = "1" }))
        rs:_handle(sock)
        assert.are.equal(500, status_of(sock))
        assert.is_true(sock.closed)
    end)
end)

describe("cluster endpoint input validation", function()
    local cs, assignments, broker
    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("t", 1))
        assignments = assert(Assignments.new(BASE_DIR, "b1"))
        cs = ClusterServer.new({
            reactor = Reactor.new(), broker = broker, broker_id = "b1", port = 0,
            assignments = assignments, token = "s3cret",
        })
    end)
    after_each(function() rmdir(BASE_DIR) end)

    local function call(path, value)
        local sock = mock_sock(post(path, json.encode(value), { ["X-Cluster-Token"] = "s3cret" }))
        cs:_handle(sock)
        return status_of(sock), table.concat(sock._out)
    end

    it("rejects a fractional, out-of-range or badly named owner assignment", function()
        assert.are.equal(400, (call("/cluster/owner", { topic = "t", partition = 5.0, owner = "b2" })))
        assert.are.equal(400, (call("/cluster/owner", { topic = "t", partition = 1025, owner = "b2" })))
        assert.are.equal(400, (call("/cluster/owner", { topic = "../x", partition = 1, owner = "b2" })))
        assert.are.equal(400, (call("/cluster/owner", { topic = "t", partition = 1, owner = "" })))
        assert.are.same({}, assignments:entries())
        assert.are.equal(200, (call("/cluster/owner", { topic = "t", partition = 2, owner = "b2" })))
        assert.are.equal("b2", assignments:owner("t", 2))
    end)

    it("rejects txn/enroll with an empty txn id instead of asserting", function()
        assert.are.equal(400, (call("/cluster/txn/enroll",
            { txn = "", topic = "t", partition = 1, first_offset = 0 })))
        assert.are.equal(400, (call("/cluster/txn/enroll",
            { txn = "x", topic = "t", partition = 1, first_offset = 0.5 })))
        assert.are.equal(200, (call("/cluster/txn/enroll",
            { txn = "x", topic = "t", partition = 1, first_offset = 3 })))
    end)

    it("rejects non-integer txn/resolve markers", function()
        assert.are.equal(400, (call("/cluster/txn/resolve",
            { txn = "x", topic = "t", partition = 1, aborted = true, pid = "p", first = 0, upto = 1 })))
        assert.are.equal(400, (call("/cluster/txn/resolve",
            { txn = "x", topic = "t", partition = 1.5 })))
    end)

    it("rejects non-integer offsets and partitions", function()
        assert.are.equal(400, (call("/cluster/offsets",
            { topic = "t", partition = 1, offsets = { g = 1e300 } })))
        assert.are.equal(400, (call("/cluster/offsets",
            { topic = "t", partition = 0.5, offsets = { g = 1 } })))
        assert.are.equal(400, (call("/cluster/ensure", { topic = "t", partitions = 2.5 })))
    end)

    it("rejects a 1e300 controller claim and keeps the fence usable", function()
        local status, body = call("/cluster/controller/claim", { epoch = 1e300, broker_id = "b2" })
        assert.are.equal(200, status)
        assert.matches('"accepted":false', body)
        assert.are.equal(0, (cs.fence:highest()))
        assert.are.equal(1, (cs.fence:claim("b1")))
    end)
end)

describe("Assignments validation", function()
    before_each(function() rmdir(BASE_DIR); mkdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("refuses fractional partitions and skips them on replace", function()
        local a = assert(Assignments.new(BASE_DIR, "b1"))
        assert.is_nil((a:set_owner("t", 5.5, "b2")))
        assert.is_nil((a:set_owner("t", 5.0, "b2")))
        assert.is_nil((a:set_owner("bad/topic", 1, "b2")))
        assert(a:replace({
            { topic = "t", partition = 2.0, owner = "b2" },
            { topic = "t", partition = 3, owner = "b3" },
        }))
        assert.are.same({ { topic = "t", partition = 3, owner = "b3" } }, a:entries())
        assert(a:set_owner("t", 4, "b2"))
    end)
end)

describe("controller epoch bounds", function()
    before_each(function() rmdir(BASE_DIR); mkdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("rejects non-integer and oversized epochs", function()
        local f = assert(ControllerFence.new(BASE_DIR))
        assert.is_nil((f:observe(1e300, "x")))
        assert.is_nil((f:observe(2.0, "x")))
        assert.is_nil((f:observe(1 << 53, "x")))
        assert.is_nil((f:observe(math.huge, "x")))
        assert.are.equal(0, (f:highest()))
        assert(f:observe(3, "x"))
    end)

    it("refuses to load a persisted non-integer epoch", function()
        local fh = assert(io.open(BASE_DIR .. "/" .. ControllerFence.FILE_NAME, "wb"))
        fh:write('{"epoch": 1e300, "claimant": "x"}')
        fh:close()
        local f, err = ControllerFence.new(BASE_DIR)
        assert.is_nil(f)
        assert.matches("integer", err)
    end)

    it("raft fence rejects non-integer epochs", function()
        local node = RaftNode.new({ id = "a", peers = { "a" }, store = memory_store() })
        local fence = RaftFence.new(node)
        assert.is_nil((fence:observe(1e300, nil)))
        assert.is_nil((fence:observe(0.5, nil)))
    end)
end)

describe("raft RPC numeric validation", function()
    local function follower()
        local node = RaftNode.new({
            id = "b", peers = { "a", "b", "c" }, store = memory_store(),
            random = function() return 0 end,
        })
        assert(node:handle_append({
            term = 1, leader_id = "a", prev_log_index = 0, prev_log_term = 0,
            leader_commit = 2,
            entries = {
                { term = 1, index = 1, kind = "owner", data = {} },
                { term = 1, index = 2, kind = "owner", data = {} },
                { term = 1, index = 3, kind = "owner", data = {} },
            },
        }))
        return node
    end

    it("rejects infinite or fractional terms on vote", function()
        local node = follower()
        assert.is_nil((node:handle_vote({ term = math.huge, candidate_id = "c",
            last_log_index = 9, last_log_term = 9 })))
        assert.is_nil((node:handle_vote({ term = 2.5, candidate_id = "c",
            last_log_index = 9, last_log_term = 9 })))
        assert.are.equal(1, node:term())
    end)

    it("rejects a fractional prev_log_index without truncating", function()
        local node = follower()
        assert.is_nil((node:handle_append({ term = 1, leader_id = "a",
            prev_log_index = 0.5, prev_log_term = 7, leader_commit = 0, entries = {} })))
        assert.are.equal(3, node:last_index())
    end)

    it("rejects malformed entries", function()
        local node = follower()
        assert.is_nil((node:handle_append({ term = 1, leader_id = "a",
            prev_log_index = 3, prev_log_term = 1, leader_commit = 0,
            entries = { { term = 1.5, kind = "owner" } } })))
        assert.is_nil((node:handle_append({ term = 1, leader_id = "a",
            prev_log_index = 3, prev_log_term = 1, leader_commit = 0,
            entries = { { term = 5, kind = "owner" } } })))
        assert.is_nil((node:handle_append({ term = 1, leader_id = "a",
            prev_log_index = 3, prev_log_term = 1, leader_commit = 0,
            entries = { { term = 1, index = 9, kind = "owner" } } })))
        assert.are.equal(3, node:last_index())
    end)

    it("never truncates at or below the commit index", function()
        local node = follower()
        assert.are.equal(2, node.commit_index)
        assert.is_nil((node:handle_append({ term = 2, leader_id = "c",
            prev_log_index = 2, prev_log_term = 2, leader_commit = 0, entries = {} })))
        assert.is_nil((node:handle_append({ term = 2, leader_id = "c",
            prev_log_index = 1, prev_log_term = 1, leader_commit = 0,
            entries = { { term = 2, kind = "owner" } } })))
        assert.are.equal(3, node:last_index())
        assert.are.equal(1, node:term_at(2))

        local reply = assert(node:handle_append({ term = 2, leader_id = "c",
            prev_log_index = 3, prev_log_term = 2, leader_commit = 0, entries = {} }))
        assert.is_false(reply.success)
        assert.are.equal(2, node:last_index())
    end)

    it("rejects a snapshot with non-integer indexes", function()
        local node = follower()
        assert.is_nil((node:handle_snapshot({ term = 1, leader_id = "a",
            last_index = 1e300, last_term = 1, state = {} })))
        assert.is_nil((node:handle_snapshot({ term = 1, leader_id = "a",
            last_index = 10, last_term = 0.5, state = {} })))
        assert.are.equal(3, node:last_index())
    end)

    it("ignores a peer reply carrying a non-integer term", function()
        local node = follower()
        local svc = RaftService.new({ node = node, reactor = Reactor.new(), addresses = {} })
        assert.is_true(svc:_observe_term({ term = math.huge }))
        assert.is_true(svc:_observe_term({ term = 3.5 }))
        assert.are.equal(1, node:term())
    end)
end)

describe("cluster peer HTTP client", function()
    it("disables redirects and caps the response body", function()
        local peer = Peer.new("b2", "127.0.0.1:1", { token = "s3cret" })
        local req = intercept_http(function() peer:loads() end)
        assert.is_false(req.redirect)
        assert.are.equal("s3cret", req.headers["X-Cluster-Token"])
        assert.is_truthy(req.sink(string.rep("x", 1024)))
        local ok, err = req.sink(string.rep("x", 8 * 1024 * 1024))
        assert.is_nil(ok)
        assert.matches("exceeds", err)
    end)
end)

describe("token configuration", function()
    it("rejects empty Replication.Token", function()
        local r, err = options.replication({ Token = "" })
        assert.is_nil(r)
        assert.matches("Replication.Token", err)
    end)

    it("rejects empty Cluster.Token and empty per-peer tokens", function()
        local c, err = options.cluster({ BrokerId = "b1", Token = "" })
        assert.is_nil(c)
        assert.matches("Cluster.Token", err)
        c, err = options.cluster({ BrokerId = "b1", Token = "t",
            Peers = { { Id = "b2", Address = "h:1", Token = "" } } })
        assert.is_nil(c)
        assert.matches("Peers%[1%]%.Token", err)
    end)

    it("requires Cluster.Token whenever a per-peer token is set", function()
        local c, err = options.cluster({ BrokerId = "b1",
            Peers = { { Id = "b2", Address = "h:1", Token = "p" } } })
        assert.is_nil(c)
        assert.matches("requires Cluster.Token", err)
        assert(options.cluster({ BrokerId = "b1", Token = "t",
            Peers = { { Id = "b2", Address = "h:1", Token = "p" } } }))
    end)
end)
