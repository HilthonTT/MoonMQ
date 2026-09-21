local Reactor  = require("src.server.reactor")
local Server   = require("src.server.server")
local Client   = require("src.client")
local proto    = require("src.wire.protocol")
local uuid     = require("src.core.uuid")
local os_utils = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\lua_replication_server_test"
                                      or "/tmp/lua_replication_server_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function failover_opts()
    return {
        host = "127.0.0.1", port = 0, data_dir = BASE_DIR,
        replication = {
            enabled        = true,
            replica_id     = 1,
            role           = "follower",
            replicate_port = 19531,
            client_address = "127.0.0.1:19631",
            peers = {
                { id = 2, address = "127.0.0.1:19532", client_address = "127.0.0.1:19632" },
                { id = 3, address = "127.0.0.1:19533", client_address = "127.0.0.1:19633" },
            },
            failover = {},
        },
    }
end

describe("server with replication failover", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    it("refuses to combine failover with cluster placement", function()
        local opts = failover_opts()
        opts.cluster = { broker_id = "b1", port = 19541 }
        local srv, err = Server.new(opts)
        assert.is_nil(srv)
        assert.matches("cannot be combined", err)
    end)

    it("answers every request on a follower with ERR_NOT_LEADER and the leader's address",
    function()
        local srv = assert(Server.new(failover_opts()))
        assert.is_truthy(srv.replication_group)
        assert.are.equal(srv.replication_group, srv.replicator)

        local sent, closed = {}, {}
        srv.reactor.send_all = function(_, _, frame) sent[#sent + 1] = frame; return true end
        local conn = { sock = {}, close = function(_, reason) closed[#closed + 1] = reason end }

        local function refusal()
            local frame = table.remove(sent, 1)
            local op, _, payload = proto.parse_frame(frame:sub(5))
            assert.are.equal(proto.OP_ERROR, op)
            return proto.decode_error(payload)
        end

        srv:dispatch(conn, proto.OP_HELLO, uuid.bytes(), "")
        local e = refusal()
        assert.are.equal(proto.ERR_NOT_LEADER, e.code)
        assert.matches("no leader elected yet", e.message)
        assert.are.equal("not_leader", closed[1])

        srv.replication_group.state.leader = "3"
        srv:dispatch(conn, proto.OP_PRODUCE, uuid.bytes(), "")
        e = refusal()
        assert.are.equal(proto.ERR_NOT_LEADER, e.code)
        assert.matches("leader=127.0.0.1:19633", e.message, 1, true)
    end)

    it("fences local writes on every partition until the replica leads", function()
        local srv = assert(Server.new(failover_opts()))
        local message = require("src.record.message")
        local ok, err = srv.broker:commit_offset("g", "orders", 1, 5)
        assert.is_falsy(ok)
        assert.matches("read%-only", tostring(err))

        local topic = assert(srv.broker:create_topic("orders", 1))
        local off, werr = topic.partitions[1]:write_message(message.Message.new("k", "v", 1))
        assert.is_nil(off)
        assert.matches("read%-only", werr)
    end)
end)

describe("client leader discovery", function()
    local function frame_server(reactor, port, respond)
        assert(reactor:listen("127.0.0.1", port, function(sock)
            local len = reactor:read_exact(sock, 4, nil)
            if not len then return end
            local body = reactor:read_exact(sock, string.unpack(">I4", len), nil)
            local _, correl = proto.parse_frame(body)
            reactor:send_all(sock, respond(correl), nil)
            reactor:sleep(0.2)
            pcall(function() sock:close() end)
        end))
    end

    it("follows the leader hint from a follower and tries the next bootstrap host", function()
        local reactor = Reactor.new()
        local hits = { follower = 0, leader = 0 }

        frame_server(reactor, 19551, function(correl)
            hits.follower = hits.follower + 1
            return proto.encode_error(correl, proto.ERR_NOT_LEADER,
                "not the leader; leader=127.0.0.1:19552")
        end)
        frame_server(reactor, 19552, function(correl)
            hits.leader = hits.leader + 1
            return proto.encode_welcome(correl, proto.PROTOCOL_VERSION)
        end)
        frame_server(reactor, 19553, function(correl)
            return proto.encode_error(correl, proto.ERR_NOT_LEADER,
                "not the leader; no leader elected yet")
        end)

        local redirected, bootstrapped, redirect_err, bootstrap_err
        reactor:spawn(function()
            local c
            c, redirect_err = Client.new({ host = "127.0.0.1", port = 19551, reactor = reactor })
            redirected = c ~= nil
            if c then c:close() end

            c, bootstrap_err = Client.new({
                hosts = { "127.0.0.1:19553", "127.0.0.1:19552" }, reactor = reactor,
            })
            bootstrapped = c ~= nil
            if c then c:close() end
            reactor:stop()
        end)
        reactor:spawn(function()
            reactor:sleep(10)
            reactor:stop()
        end)
        reactor:run()
        reactor:shutdown()

        assert.is_true(redirected, redirect_err)
        assert.is_true(bootstrapped, bootstrap_err)
        assert.are.equal(1, hits.follower)
        assert.are.equal(2, hits.leader)
    end)
end)
