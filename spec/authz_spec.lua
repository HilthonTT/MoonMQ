-- End-to-end authorization and quota enforcement, at the handler layer.
--
-- The unit tests for src/server/acl.lua prove the ACL answers correctly. This
-- file proves the handlers ASK — which is the failure mode that matters, since
-- a gate nobody calls tests green in every other file. Every handler that
-- names a topic or a group gets a case here for both outcomes, and the
-- filtered listings get one for each direction.

local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local producer_m = require("src.broker.producer")
local handlers   = require("src.server.handlers")
local acl_m      = require("src.server.acl")
local quota_m    = require("src.server.quota")
local metrics_http = require("src.server.metrics_http")
local b64        = require("src.core.base64")
local auth       = require("src.server.auth")
local users_m    = require("src.server.users")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_authz_test"
                                      or "/tmp/moonmq_authz_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function unframe(frame) return proto.parse_frame(frame:sub(5)) end

-- A connection carrying a principal, which is all the gates read from it.
local function conn_with(acl_rules, quota)
    local principal
    if acl_rules ~= "open" then
        principal = {
            username = "tenant",
            acl      = acl_rules == "all" and acl_m.ALLOW_ALL
                       or assert(acl_m.new(acl_rules)),
            quota    = quota,
        }
    end
    return {
        id_short      = "test",
        username      = principal and principal.username or nil,
        principal     = principal,
        subscriptions = {},
        sent          = {},
        send = function(self, frame)
            self.sent[#self.sent + 1] = frame
            return true
        end,
    }
end

-- op of the LAST frame a connection sent, and its payload.
local function last_reply(conn)
    local op, _, payload = unframe(conn.sent[#conn.sent])
    return op, payload
end

local function reply(conn)
    assert(#conn.sent >= 1, "handler sent no reply")
    local op, _, payload = unframe(conn.sent[1])
    return op, payload
end

local function expect_denied(conn)
    local op, payload = reply(conn)
    assert(op == proto.OP_ERROR,
        string.format("expected ERROR, got 0x%02x", op))
    local e = assert(proto.decode_error(payload))
    assert(e.code == proto.ERR_NOT_AUTHORIZED,
        string.format("expected NOT_AUTHORIZED, got code %d (%s)", e.code, e.message))
    return e
end

local function expect_not_denied(conn)
    local op, payload = reply(conn)
    if op == proto.OP_ERROR then
        local e = assert(proto.decode_error(payload))
        assert(e.code ~= proto.ERR_NOT_AUTHORIZED,
            "expected the request to pass authorization, got: " .. e.message)
    end
    return op, payload
end

describe("handler authorization", function()

    local broker, server

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders.eu", 1))
        assert(broker:create_topic("billing", 1))
        server = {
            broker          = broker,
            producer        = producer_m.Producer.new(broker, 0),
            max_topics      = 100,
            max_list_topics = 100,
            push_batch      = 10,
            reactor         = { sleep = function() end },
            coordinator     = {
                list             = function() return {} end,
                apply_assignment = function() end,
            },
        }
    end)

    after_each(function() rmdir(BASE_DIR) end)

    -- The tenant may read and write orders.*, and describe it; nothing else.
    local TENANT = {
        { Resource = "topic", Name = "orders.*",
          Operations = { "read", "write", "describe" } },
        { Resource = "group", Name = "orders-*", Operations = { "read", "describe" } },
    }

    local function call(handler, conn, frame)
        local _, _, payload = unframe(frame)
        handler(server, conn, uuid.bytes(), payload)
        return conn
    end

    describe("produce", function()
        it("allows a write to a permitted topic", function()
            local conn = call(handlers.produce, conn_with(TENANT),
                proto.encode_produce(uuid.bytes(), "orders.eu", "k", "v"))
            expect_not_denied(conn)
        end)

        it("refuses a write to another tenant's topic", function()
            local conn = call(handlers.produce, conn_with(TENANT),
                proto.encode_produce(uuid.bytes(), "billing", "k", "v"))
            local e = expect_denied(conn)
            -- The message names the operation and resource, so the operator
            -- knows which rule is missing.
            assert.is_truthy(e.message:find("write"))
            assert.is_truthy(e.message:find("billing"))
        end)

        it("refuses a read-only principal", function()
            local conn = call(handlers.produce, conn_with({
                { Resource = "topic", Name = "orders.*", Operations = { "read" } },
            }), proto.encode_produce(uuid.bytes(), "orders.eu", "k", "v"))
            expect_denied(conn)
        end)

        it("refuses a batch to a forbidden topic", function()
            local conn = call(handlers.produce_batch, conn_with(TENANT),
                proto.encode_produce_batch(uuid.bytes(), "billing",
                    { { key = "k", value = "v" } }))
            expect_denied(conn)
        end)

        it("allows everything in OPEN mode (no principal)", function()
            local conn = call(handlers.produce, conn_with("open"),
                proto.encode_produce(uuid.bytes(), "billing", "k", "v"))
            expect_not_denied(conn)
        end)
    end)

    describe("consume", function()
        it("refuses a fetch from a forbidden topic", function()
            local conn = call(handlers.fetch, conn_with(TENANT),
                proto.encode_fetch(uuid.bytes(), "billing", "orders-eu", 10))
            expect_denied(conn)
        end)

        it("refuses a fetch through a forbidden group", function()
            -- The topic is allowed; borrowing another tenant's group to read it
            -- would let this principal wreck that group's committed position.
            local conn = call(handlers.fetch, conn_with(TENANT),
                proto.encode_fetch(uuid.bytes(), "orders.eu", "someone-else", 10))
            local e = expect_denied(conn)
            assert.is_truthy(e.message:find("group"))
        end)

        it("allows a permitted topic and group", function()
            local conn = call(handlers.fetch, conn_with(TENANT),
                proto.encode_fetch(uuid.bytes(), "orders.eu", "orders-eu", 10))
            expect_not_denied(conn)
        end)

        it("refuses a subscribe to a forbidden topic", function()
            local conn = call(handlers.subscribe, conn_with(TENANT),
                proto.encode_subscribe(uuid.bytes(), "billing", "orders-eu"))
            expect_denied(conn)
        end)

        it("refuses a join_group naming a forbidden topic", function()
            -- The assignment alone would hand this member the topic's records.
            local conn = call(handlers.join_group, conn_with(TENANT),
                proto.encode_join_group(uuid.bytes(), "orders-eu", "", { "billing" }))
            expect_denied(conn)
        end)

        it("refuses a join_group for a forbidden group", function()
            local conn = call(handlers.join_group, conn_with(TENANT),
                proto.encode_join_group(uuid.bytes(), "other-group", "", { "orders.eu" }))
            expect_denied(conn)
        end)
    end)

    describe("topic administration", function()
        it("refuses delete without topic:delete, even on a readable topic", function()
            local conn = call(handlers.delete_topic, conn_with(TENANT),
                proto.encode_delete_topic(uuid.bytes(), "orders.eu"))
            expect_denied(conn)
            -- And the topic is still there.
            assert.is_truthy(broker.topic_manager.topics["orders.eu"])
        end)

        it("allows delete with the grant", function()
            local conn = call(handlers.delete_topic, conn_with({
                { Resource = "topic", Name = "orders.*", Operations = { "delete" } },
            }), proto.encode_delete_topic(uuid.bytes(), "orders.eu"))
            expect_not_denied(conn)
            assert.is_nil(broker.topic_manager.topics["orders.eu"])
        end)

        it("refuses alter without topic:alter", function()
            local conn = call(handlers.alter_topic_config, conn_with(TENANT),
                proto.encode_alter_topic_config(uuid.bytes(), "orders.eu",
                    { retention = 60 }))
            expect_denied(conn)
        end)

        it("refuses describe of a forbidden topic", function()
            local conn = call(handlers.describe_topic, conn_with(TENANT),
                proto.encode_describe_topic(uuid.bytes(), "billing"))
            expect_denied(conn)
        end)

        it("refuses list_offsets on a forbidden topic", function()
            local conn = call(handlers.list_offsets, conn_with(TENANT),
                proto.encode_list_offsets(uuid.bytes(), "billing"))
            expect_denied(conn)
        end)

        it("accepts create under a prefix grant and refuses outside it", function()
            local rules = {
                { Resource = "topic", Name = "orders.*", Operations = { "create" } },
            }
            local ok_conn = call(handlers.create_topic, conn_with(rules),
                proto.encode_create_topic(uuid.bytes(), "orders.us", 1))
            expect_not_denied(ok_conn)

            local bad_conn = call(handlers.create_topic, conn_with(rules),
                proto.encode_create_topic(uuid.bytes(), "payroll", 1))
            expect_denied(bad_conn)
            assert.is_nil(broker.topic_manager.topics["payroll"])
        end)

        it("accepts create via a blanket cluster:create grant", function()
            local conn = call(handlers.create_topic, conn_with({
                { Resource = "cluster", Operations = { "create" } },
            }), proto.encode_create_topic(uuid.bytes(), "anything", 1))
            expect_not_denied(conn)
        end)
    end)

    describe("listings are filtered, not refused", function()
        it("shows only the topics the principal may describe", function()
            local conn = call(handlers.list_topics, conn_with(TENANT),
                proto.encode_list_topics(uuid.bytes()))
            local op, payload = reply(conn)
            assert.are.equal(proto.OP_TOPIC_LIST, op)
            assert.are.same({ "orders.eu" }, assert(proto.decode_topic_list(payload)))
        end)

        it("shows everything in OPEN mode", function()
            local conn = call(handlers.list_topics, conn_with("open"),
                proto.encode_list_topics(uuid.bytes()))
            local _, payload = reply(conn)
            local names = assert(proto.decode_topic_list(payload))
            assert.are.equal(2, #names)
        end)

        it("filters group listings the same way", function()
            server.coordinator = {
                list = function()
                    return {
                        { group_id = "orders-eu", state = "stable", member_count = 1 },
                        { group_id = "billing-eu", state = "empty", member_count = 0 },
                    }
                end,
            }
            local conn = call(handlers.list_groups, conn_with(TENANT),
                proto.encode_list_groups(uuid.bytes()))
            local _, payload = reply(conn)
            local groups = assert(proto.decode_group_list(payload))
            assert.are.equal(1, #groups)
            assert.are.equal("orders-eu", groups[1].group_id)
        end)
    end)

    describe("superusers", function()
        it("bypass every check", function()
            local conn = call(handlers.produce, conn_with("all"),
                proto.encode_produce(uuid.bytes(), "billing", "k", "v"))
            expect_not_denied(conn)

            local conn2 = call(handlers.delete_topic, conn_with("all"),
                proto.encode_delete_topic(uuid.bytes(), "billing"))
            expect_not_denied(conn2)
        end)
    end)
end)

describe("handler quota enforcement", function()

    local broker, server, clock

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
        clock = { now = 1000 }
        server = {
            broker          = broker,
            producer        = producer_m.Producer.new(broker, 0),
            max_topics      = 100,
            max_list_topics = 100,
            push_batch      = 10,
            reactor         = { sleep = function() end },
            coordinator     = { apply_assignment = function() end },
            quotas = quota_m.new({
                default       = assert(quota_m.spec({ ProduceRecordsPerSec = 3 })),
                burst_seconds = 1,
                now_fn        = function() return clock.now end,
            }),
        }
    end)

    after_each(function() rmdir(BASE_DIR) end)

    local function produce(conn)
        local _, _, payload = unframe(
            proto.encode_produce(uuid.bytes(), "orders", "k", "v"))
        handlers.produce(server, conn, uuid.bytes(), payload)
        return conn
    end

    it("throttles a principal past its record rate", function()
        local conn = conn_with("all")
        for _ = 1, 3 do
            produce(conn)
        end
        for _, frame in ipairs(conn.sent) do
            local op = unframe(frame)
            assert.are.equal(proto.OP_PRODUCE_ACK, op)
        end

        produce(conn)
        local op, payload = last_reply(conn)
        assert.are.equal(proto.OP_ERROR, op)
        local e = assert(proto.decode_error(payload))
        assert.are.equal(proto.ERR_RATE_LIMITED, e.code)
        -- The client is told how long to wait, not merely that it failed.
        assert.is_truthy(e.message:find("retry in"))
        assert.is_truthy(e.message:find("user"))
    end)

    it("lets the bucket refill", function()
        local conn = conn_with("all")
        for _ = 1, 3 do produce(conn) end
        produce(conn)
        assert.are.equal(proto.OP_ERROR, (last_reply(conn)))

        clock.now = clock.now + 1
        produce(conn)
        assert.are.equal(proto.OP_PRODUCE_ACK, (last_reply(conn)))
    end)

    it("meters per principal, not per connection", function()
        -- Two connections, one user: opening a second socket must not double
        -- the budget. This is the whole reason quotas exist alongside the
        -- per-connection rate limiter.
        local c1, c2 = conn_with("all"), conn_with("all")
        for _ = 1, 3 do produce(c1) end
        produce(c2)
        local op, payload = last_reply(c2)
        assert.are.equal(proto.OP_ERROR, op)
        assert.are.equal(proto.ERR_RATE_LIMITED,
            assert(proto.decode_error(payload)).code)
    end)

    it("refuses a fetch from a consumer carrying delivery debt", function()
        -- Delivery is metered after the fact, so an oversized batch leaves the
        -- bucket in debt; the next FETCH is what pays for it.
        server.quotas = quota_m.new({
            default       = assert(quota_m.spec({ FetchRecordsPerSec = 2 })),
            burst_seconds = 1,
            now_fn        = function() return clock.now end,
        })
        local writer = conn_with("all")
        for _ = 1, 6 do produce(writer) end

        local conn = conn_with("all")
        local function fetch()
            local _, _, payload = unframe(
                proto.encode_fetch(uuid.bytes(), "orders", "g", 6))
            handlers.fetch(server, conn, uuid.bytes(), payload)
        end

        fetch()   -- delivers up to 6 records against a 2-record bucket
        assert.are.equal(proto.OP_OK, (last_reply(conn)))

        fetch()   -- the debt is now owed
        local op, payload = last_reply(conn)
        assert.are.equal(proto.OP_ERROR, op)
        assert.are.equal(proto.ERR_RATE_LIMITED,
            assert(proto.decode_error(payload)).code)

        -- An empty poll must not be billed: after enough time the bucket is
        -- back in credit and the fetch goes through even with no records left.
        clock.now = clock.now + 10
        fetch()
        assert.are.equal(proto.OP_OK, (last_reply(conn)))
    end)

    it("does not meter an unauthenticated connection", function()
        local conn = conn_with("open")
        for _ = 1, 20 do produce(conn) end
        assert.are.equal(proto.OP_PRODUCE_ACK, (last_reply(conn)))
    end)
end)

describe("metrics endpoint authentication", function()

    local ITER = 1000

    local function endpoint(auth_cfg, store)
        return metrics_http.new({
            reactor = {},
            auth    = auth_cfg,
            authenticator = store and auth.authenticator({ store = store }) or nil,
        })
    end

    local function headers(value)
        return "GET /metrics HTTP/1.1\r\nHost: x\r\n"
            .. (value and ("Authorization: " .. value .. "\r\n") or "")
            .. "\r\n"
    end

    it("allows everything when no auth is configured", function()
        assert.is_true(endpoint(nil):_authorized(headers(), "1.2.3.4"))
    end)

    it("accepts a matching bearer token and refuses everything else", function()
        local e = endpoint({ token = "s3cret-token" })
        assert.is_true(e:_authorized(headers("Bearer s3cret-token"), "1.2.3.4"))
        assert.is_false((e:_authorized(headers("Bearer wrong"), "1.2.3.4")))
        assert.is_false((e:_authorized(headers(), "1.2.3.4")))
        assert.is_false((e:_authorized(headers("Basic YWRtaW46YWRtaW4="), "1.2.3.4")))
        -- A prefix of the token must not pass: the compare is over the whole
        -- string, in constant time.
        assert.is_false((e:_authorized(headers("Bearer s3cret"), "1.2.3.4")))
    end)

    it("accepts basic credentials from the user store", function()
        local store = assert(users_m.load({
            Users = {
                { Username = "scraper", PasswordHash =
                    auth.hash_password("pw", { iterations = ITER }),
                  Acls = { { Resource = "cluster", Operations = { "describe" } } } },
            },
        }))
        local e = endpoint({ basic = true }, store)
        assert.is_true(e:_authorized(
            headers("Basic " .. b64.encode("scraper:pw")), "1.2.3.4"))
        assert.is_false((e:_authorized(
            headers("Basic " .. b64.encode("scraper:wrong")), "1.2.3.4")))
        assert.is_false((e:_authorized(
            headers("Basic " .. b64.encode("ghost:pw")), "1.2.3.4")))
    end)

    it("requires cluster:describe, not merely a valid password", function()
        -- A tenant with topic ACLs has no business reading broker-wide
        -- counters, or the topic names /stats lists.
        local store = assert(users_m.load({
            Users = {
                { Username = "tenant", PasswordHash =
                    auth.hash_password("pw", { iterations = ITER }),
                  Acls = { { Resource = "topic", Name = "orders.*",
                             Operations = { "read" } } } },
            },
        }))
        local e = endpoint({ basic = true }, store)
        local ok, reason = e:_authorized(
            headers("Basic " .. b64.encode("tenant:pw")), "1.2.3.4")
        assert.is_false(ok)
        assert.is_truthy(reason:find("cluster:describe"))
    end)

    it("refuses basic when only bearer is enabled, and vice versa", function()
        local store = assert(users_m.load({
            Users = { { Username = "u", PasswordHash =
                auth.hash_password("pw", { iterations = ITER }), Superuser = true } },
        }))
        local bearer_only = endpoint({ token = "t" }, store)
        assert.is_false((bearer_only:_authorized(
            headers("Basic " .. b64.encode("u:pw")), "1.2.3.4")))

        local basic_only = endpoint({ basic = true }, store)
        assert.is_false((basic_only:_authorized(headers("Bearer t"), "1.2.3.4")))
    end)

    it("rejects a malformed or unknown scheme", function()
        local e = endpoint({ token = "t" })
        assert.is_false((e:_authorized(headers("Bearer"), "1.2.3.4")))
        assert.is_false((e:_authorized(headers("Digest abc"), "1.2.3.4")))
        assert.is_false((e:_authorized(headers("Basic !!!not-base64"), "1.2.3.4")))
    end)
end)
