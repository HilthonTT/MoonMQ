local proto      = require("src.wire.protocol")
local uuid       = require("src.core.uuid")
local brk_m      = require("src.broker")
local producer_m = require("src.broker.producer")
local msg_m      = require("src.record.message")
local txn_m      = require("src.broker.txn_coordinator")
local handlers   = require("src.server.handlers")
local acl_m      = require("src.server.acl")
local options    = require("src.server.options")
local auth       = require("src.server.auth")
local Server     = require("src.server.server")
local GroupCoordinator = require("src.server.group_coordinator")
local TopicManager = require("src.storage.topic_manager")
local os_utils   = require("src.core.os")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_security_client_test"
                                      or "/tmp/moonmq_security_client_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function unframe(frame) return proto.parse_frame(frame:sub(5)) end

local function principal_for(rules)
    if rules == "open" then return nil end
    return {
        username = "tenant",
        acl      = rules == "all" and acl_m.ALLOW_ALL or assert(acl_m.new(rules)),
    }
end

local function conn_with(rules)
    local principal = principal_for(rules)
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

local function first_error(conn)
    for _, frame in ipairs(conn.sent) do
        local op, _, payload = unframe(frame)
        if op == proto.OP_ERROR then return assert(proto.decode_error(payload)) end
    end
    return nil
end

local function ops(conn)
    local out = {}
    for i, frame in ipairs(conn.sent) do
        local op, _, payload = unframe(frame)
        out[i] = { op = op, payload = payload }
    end
    return out
end

local function delivered(conn)
    local topics = {}
    for _, f in ipairs(ops(conn)) do
        if f.op == proto.OP_RECORD then
            local r = assert(proto.decode_record(f.payload))
            topics[#topics + 1] = r.topic
        end
    end
    return topics
end

local function append(broker, topic_name, n)
    local part = assert(broker:get_topic(topic_name)).partitions[1]
    local offsets = {}
    for i = 1, n do
        offsets[i] = part.offset
        assert(part:write_message(msg_m.Message.new("k", "v" .. i, 1)))
    end
    offsets[n + 1] = part.offset
    return offsets
end

describe("client security hardening", function()
    local broker, server

    local function call(handler, conn, frame)
        local _, _, payload = unframe(frame)
        handler(server, conn, uuid.bytes(), payload)
        return conn
    end

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR, { dlq = { max_deliveries = 1 } }))
        assert(broker:create_topic("orders", 1))
        assert(broker:create_topic("billing", 1))
        server = {
            broker            = broker,
            producer          = producer_m.Producer.new(broker, 0),
            max_topics        = 100,
            max_list_topics   = 100,
            max_fetch_records = 1000,
            push_batch        = 10,
            reactor           = { sleep = function() end },
            coordinator       = {
                list             = function() return {} end,
                apply_assignment = function() end,
            },
        }
    end)

    after_each(function()
        broker:close()
        rmdir(BASE_DIR)
    end)

    describe("internal topics", function()
        for _, mode in ipairs({ "all", "open" }) do
            it("refuses produce, fetch and list_offsets on __ topics (" .. mode .. ")", function()
                local c1 = call(handlers.produce, conn_with(mode),
                    proto.encode_produce(uuid.bytes(), "__consumer_offsets", "k", "v"))
                assert.are.equal(proto.ERR_TOPIC_FORBIDDEN, first_error(c1).code)

                local c2 = call(handlers.fetch, conn_with(mode),
                    proto.encode_fetch(uuid.bytes(), "__transaction_state", "g", 10))
                assert.are.equal(proto.ERR_TOPIC_FORBIDDEN, first_error(c2).code)
                assert.is_nil(c2.consumer)

                local c3 = call(handlers.list_offsets, conn_with(mode),
                    proto.encode_list_offsets(uuid.bytes(), "__producer_state"))
                assert.are.equal(proto.ERR_TOPIC_FORBIDDEN, first_error(c3).code)

                local c4 = call(handlers.join_group, conn_with(mode),
                    proto.encode_join_group(uuid.bytes(), "g", "", { "__dlq_attempts" }))
                assert.are.equal(proto.ERR_TOPIC_FORBIDDEN, first_error(c4).code)
            end)
        end

        it("never reports an internal topic as permitted", function()
            local conn = conn_with("all")
            assert.is_false(handlers.permitted(conn, acl_m.RES_TOPIC,
                "__consumer_offsets", acl_m.OP_READ))
            assert.is_true(handlers.permitted(conn, acl_m.RES_TOPIC,
                "orders", acl_m.OP_READ))
        end)

        it("still reports create of a reserved name as a bad frame", function()
            local conn = call(handlers.create_topic, conn_with("open"),
                proto.encode_create_topic(uuid.bytes(), "__evil", 1))
            assert.are.equal(proto.ERR_BAD_FRAME, first_error(conn).code)
        end)
    end)

    describe("INIT_PRODUCER_ID authorization", function()
        it("parses a transactional_id ACL rule", function()
            local acl = assert(acl_m.new({
                { Resource = "transactional_id", Name = "app-*", Operations = { "write" } },
            }))
            assert.is_true(acl:authorized("transactional_id", "app-1", "write"))
            assert.is_false(acl:authorized("transactional_id", "other", "write"))
            local _, err = acl_m.new({
                { Resource = "transactional_id", Name = "x", Operations = { "read" } },
            })
            assert.is_truthy(err:find("not valid", 1, true))
        end)

        it("refuses a producer name the principal may not write", function()
            local conn = call(handlers.init_producer_id, conn_with({
                { Resource = "topic", Name = "*", Operations = "*" },
            }), proto.encode_init_producer_id(uuid.bytes(), "victim-txn"))
            assert.are.equal(proto.ERR_NOT_AUTHORIZED, first_error(conn).code)
            assert.is_nil(conn.pid)
            assert.is_nil(broker.producer_state:pid_for("victim-txn"))
        end)

        it("allows a permitted name, a superuser and OPEN mode", function()
            for _, rules in ipairs({
                { { Resource = "transactional_id", Name = "victim-*", Operations = { "write" } } },
                "all", "open",
            }) do
                local conn = call(handlers.init_producer_id, conn_with(rules),
                    proto.encode_init_producer_id(uuid.bytes(), "victim-txn"))
                assert.is_nil(first_error(conn))
                assert.is_not_nil(conn.pid)
            end
        end)

        it("still lets anyone take an ephemeral producer id", function()
            local conn = call(handlers.init_producer_id, conn_with({}),
                proto.encode_init_producer_id(uuid.bytes(), ""))
            assert.is_nil(first_error(conn))
            assert.is_not_nil(conn.pid)
        end)
    end)

    describe("FETCH max_records", function()
        it("is clamped to the server limit", function()
            local offsets = append(broker, "orders", 20)
            server.max_fetch_records = 5
            local conn = call(handlers.fetch, conn_with("open"),
                proto.encode_fetch(uuid.bytes(), "orders", "g", 4000000000))
            assert.are.equal(5, #delivered(conn))
            assert.are.equal(offsets[6], broker:fetch_offset("g", "orders", 1))
        end)

        it("delivers nothing for max_records 0", function()
            append(broker, "orders", 3)
            local conn = call(handlers.fetch, conn_with("open"),
                proto.encode_fetch(uuid.bytes(), "orders", "g", 0))
            assert.are.equal(0, #delivered(conn))
            local all = ops(conn)
            assert.are.equal(proto.OP_OK, all[#all].op)
        end)

        it("reads Server.MaxFetchRecords and Server.TransactionTimeout", function()
            local opts = assert(options.from_config({}, function() return nil end))
            assert.are.equal(1000, opts.max_fetch_records)
            assert.are.equal(900, opts.transaction_timeout_s)

            opts = assert(options.from_config({ Server = {
                MaxFetchRecords = 50, TransactionTimeout = 30 } }, function() return nil end))
            assert.are.equal(50, opts.max_fetch_records)
            assert.are.equal(30, opts.transaction_timeout_s)

            local _, err = options.from_config({ Server = { MaxFetchRecords = 0 } },
                function() return nil end)
            assert.is_truthy(err:find("MaxFetchRecords", 1, true))
            _, err = options.from_config({ Server = { TransactionTimeout = -1 } },
                function() return nil end)
            assert.is_truthy(err:find("TransactionTimeout", 1, true))
        end)
    end)

    describe("NACK dead-lettering", function()
        it("drops a record of a dead-letter topic instead of nesting X.dlq.dlq", function()
            assert(broker:create_topic("orders.dlq", 1))
            local offsets = append(broker, "orders.dlq", 1)
            local res = assert(broker.dlq:record_failure("g", "orders.dlq", 1, 0, "bad"))
            assert.is_true(res.dead_lettered)
            assert.is_true(res.dropped)
            assert.is_nil(res.dlq_topic)
            assert.are.equal(offsets[2], res.next_offset)
            assert.is_nil(broker.topic_manager.topics["orders.dlq.dlq"])
        end)

        it("refuses to auto-create a dead-letter topic past max_topics", function()
            append(broker, "orders", 1)
            local current = #broker:list_topics()
            local res, err, code = broker.dlq:record_failure("g", "orders", 1, 0, "bad",
                { max_topics = current })
            assert.is_nil(res)
            assert.are.equal("limit", code)
            assert.is_truthy(err:find("topic limit", 1, true))
            assert.is_nil(broker.topic_manager.topics["orders.dlq"])
        end)

        it("refuses an internal source topic", function()
            local res, _, code = broker.dlq:record_failure("g", "__consumer_offsets", 1, 0, "x")
            assert.is_nil(res)
            assert.are.equal("forbidden", code)
        end)

        it("needs CREATE to auto-create the dead-letter topic over the wire", function()
            append(broker, "orders", 1)
            local conn = conn_with({
                { Resource = "topic", Name = "orders", Operations = { "read" } },
                { Resource = "group", Name = "g", Operations = { "read" } },
            })
            call(handlers.fetch, conn, proto.encode_fetch(uuid.bytes(), "orders", "g", 10))
            conn.sent = {}
            call(handlers.nack, conn, proto.encode_nack(uuid.bytes(), "orders", 1, 0, "bad"))
            assert.are.equal(proto.ERR_NOT_AUTHORIZED, first_error(conn).code)
            assert.is_nil(broker.topic_manager.topics["orders.dlq"])

            conn.principal = principal_for({
                { Resource = "topic", Name = "orders*", Operations = { "read", "create" } },
                { Resource = "group", Name = "g", Operations = { "read" } },
            })
            conn.sent = {}
            call(handlers.nack, conn, proto.encode_nack(uuid.bytes(), "orders", 1, 0, "bad"))
            assert.is_nil(first_error(conn))
            assert.is_not_nil(broker.topic_manager.topics["orders.dlq"])
        end)

        it("returns ERR_RATE_LIMITED when the topic limit blocks the dead-letter topic", function()
            append(broker, "orders", 1)
            server.max_topics = #broker:list_topics()
            local conn = conn_with("open")
            call(handlers.fetch, conn, proto.encode_fetch(uuid.bytes(), "orders", "g", 10))
            conn.sent = {}
            call(handlers.nack, conn, proto.encode_nack(uuid.bytes(), "orders", 1, 0, "bad"))
            assert.are.equal(proto.ERR_RATE_LIMITED, first_error(conn).code)
        end)
    end)

    describe("ACL revocation", function()
        local READ_BOTH = {
            { Resource = "topic", Name = "*", Operations = { "read" } },
            { Resource = "group", Name = "g", Operations = { "read" } },
        }
        local READ_BILLING = {
            { Resource = "topic", Name = "billing", Operations = { "read" } },
            { Resource = "group", Name = "g", Operations = { "read" } },
        }

        it("stops fetch delivering a topic whose read was revoked", function()
            local conn = conn_with(READ_BOTH)
            call(handlers.fetch, conn, proto.encode_fetch(uuid.bytes(), "orders", "g", 10))
            assert.is_true(conn.subscriptions.orders)

            append(broker, "orders", 3)
            append(broker, "billing", 2)
            conn.principal = principal_for(READ_BILLING)
            conn.sent = {}
            call(handlers.fetch, conn, proto.encode_fetch(uuid.bytes(), "billing", "g", 10))

            local topics = delivered(conn)
            assert.are.equal(2, #topics)
            for _, t in ipairs(topics) do assert.are.equal("billing", t) end
            assert.is_nil(conn.subscriptions.orders)
            assert.is_nil(conn.consumer.offsets.orders)
            local committed = broker:fetch_offset("g", "orders", 1)
            assert.is_true(committed == nil or committed == 0)
        end)

        it("reports the first subscription or group no longer readable", function()
            local conn = conn_with(READ_BOTH)
            call(handlers.fetch, conn, proto.encode_fetch(uuid.bytes(), "orders", "g", 10))
            assert.is_nil(handlers.revoked_read(conn))
            conn.principal = principal_for(READ_BILLING)
            assert.are.equal('topic "orders"', handlers.revoked_read(conn))
            conn.principal = principal_for({
                { Resource = "topic", Name = "*", Operations = { "read" } },
            })
            assert.are.equal('group "g"', handlers.revoked_read(conn))
        end)

        it("closes connections on reload whose subscriptions became unreadable", function()
            local cred = auth.hash_password("pw", { iterations = 1000 })
            local function cfg(app_acls)
                return { Auth = { Users = {
                    { Username = "admin", PasswordHash = cred, Superuser = true },
                    { Username = "app", PasswordHash = cred, Acls = app_acls },
                } } }
            end
            local ref = { cfg = cfg(READ_BILLING) }
            local authenticator = assert(options.auth(cfg(READ_BOTH)))
            local srv = setmetatable({
                authenticator     = authenticator,
                connections_by_id = {},
                auth_loader       = options.auth_loader(function() return ref.cfg end),
            }, Server)

            local function fake(id, subs)
                local c = { id = id, id_short = id, username = "app",
                            principal = authenticator:principal("app"),
                            subscriptions = subs }
                function c:close(reason, code, msg)
                    self.closed = { reason = reason, code = code, msg = msg }
                end
                return c
            end
            local reader = fake("c1", { orders = true })
            local keeper = fake("c2", { billing = true })
            srv.connections_by_id = { c1 = reader, c2 = keeper }

            local ok, updated, dropped = srv:reload_auth()
            assert.is_true(ok)
            assert.are.equal(1, updated)
            assert.are.equal(1, dropped)
            assert.are.equal(proto.ERR_NOT_AUTHORIZED, reader.closed.code)
            assert.is_nil(keeper.closed)
        end)
    end)

    describe("group membership", function()
        it("leaves the old member before a connection rejoins under a new member id", function()
            server.coordinator = GroupCoordinator.new(broker, { max_groups = 10 })
            local conn = conn_with("open")
            call(handlers.join_group, conn,
                proto.encode_join_group(uuid.bytes(), "g", "m1", { "orders" }))
            call(handlers.join_group, conn,
                proto.encode_join_group(uuid.bytes(), "g", "m2", { "orders" }))
            call(handlers.join_group, conn,
                proto.encode_join_group(uuid.bytes(), "g", "m3", { "orders" }))
            assert.is_nil(first_error(conn))

            local members = {}
            for id in pairs(server.coordinator.groups.g.members) do
                members[#members + 1] = id
            end
            assert.are.same({ "m3" }, members)
            assert.are.equal("m3", conn.member_id)
        end)
    end)

    describe("case-insensitive topic names", function()
        it("refuses a name that differs only in case", function()
            local _, err = broker:create_topic("ORDERS", 1)
            assert.is_truthy(err:find("differ only in case", 1, true))

            local conn = call(handlers.create_topic, conn_with("open"),
                proto.encode_create_topic(uuid.bytes(), "Billing", 1))
            local e = first_error(conn)
            assert.is_truthy(e.message:find("differ only in case", 1, true))
            assert.is_nil(broker.topic_manager.topics.Billing)
        end)

        it("reports the clashing name from the topic manager", function()
            local tm = TopicManager.new(BASE_DIR .. "_tm")
            tm.topics["Orders"] = {}
            assert.are.equal("Orders", tm:case_conflict("orders"))
            assert.is_nil(tm:case_conflict("Orders"))
            assert.is_nil(tm:case_conflict("billing"))
        end)

        it("still loads a pre-existing case clash during recovery", function()
            local dir = BASE_DIR .. "_recover"
            rmdir(dir)
            local tm = TopicManager.new(dir)
            assert.is_truthy(tm:create_topic("Audit", 1))
            local _, err = tm:create_topic("audit", 1)
            assert.is_truthy(err:find("differ only in case", 1, true))
            assert.is_truthy(tm:create_topic("audit", 1, nil, true))
            for _, t in pairs(tm.topics) do
                for _, p in ipairs(t.partitions) do
                    if p.close then p:close() end
                end
            end
            rmdir(dir)
        end)
    end)

    describe("internal error messages", function()
        local SECRET = "/srv/secret/data_server/orders/partition-1/00000.log"

        it("hides storage paths from a failed produce", function()
            server.producer = { produce = function()
                return -1, -1, "failed to write message: open " .. SECRET .. ": No space left"
            end }
            local conn = call(handlers.produce, conn_with("open"),
                proto.encode_produce(uuid.bytes(), "orders", "k", "v"))
            local e = first_error(conn)
            assert.are.equal(proto.ERR_INTERNAL, e.code)
            assert.is_nil(e.message:find("secret", 1, true))
        end)

        it("hides storage paths from a failed create_topic", function()
            local real = broker.create_topic
            broker.create_topic = function() return nil, "mkdir " .. SECRET .. ": denied" end
            local conn = call(handlers.create_topic, conn_with("open"),
                proto.encode_create_topic(uuid.bytes(), "fresh", 1))
            broker.create_topic = real
            local e = first_error(conn)
            assert.are.equal(proto.ERR_INTERNAL, e.code)
            assert.is_nil(e.message:find("secret", 1, true))
        end)

        it("hides storage paths from a failed nack", function()
            append(broker, "orders", 1)
            local conn = conn_with("open")
            call(handlers.fetch, conn, proto.encode_fetch(uuid.bytes(), "orders", "g", 10))
            local real = broker.dlq.record_failure
            broker.dlq.record_failure = function() return nil, "write " .. SECRET end
            conn.sent = {}
            call(handlers.nack, conn, proto.encode_nack(uuid.bytes(), "orders", 1, 0, "x"))
            broker.dlq.record_failure = real
            local e = first_error(conn)
            assert.are.equal(proto.ERR_INTERNAL, e.code)
            assert.is_nil(e.message:find("secret", 1, true))
        end)
    end)

    describe("transaction timeout", function()
        local function open_txn(name)
            assert(broker:create_topic("out", 1))
            local pid, epoch = assert(broker.producer_state:get_or_create_producer(name))
            assert(broker.transactions:begin(name, pid, epoch))
            assert(broker.transactions:add_partition(name, pid, epoch, "out", 1))
            local part = broker.topic_manager.topics.out.partitions[1]
            assert(part:write_message(
                msg_m.Message.new("k", "v", 1, msg_m.ATTR_TXN, pid, epoch)))
            return pid, epoch
        end

        it("aborts an ONGOING transaction older than the timeout", function()
            open_txn("slow")
            local txns = broker.transactions
            local started = txns:current("slow").started_at
            assert.is_number(started)
            assert.are.equal(0, txns:lso("out", 1))

            assert.are.same({}, txns:expire(900, started + 899 * 1000))
            assert.are.equal(txn_m.STATES.ONGOING, txns:current("slow").state)

            assert.are.same({ "slow" }, txns:expire(900, started + 900 * 1000))
            assert.are.equal(txn_m.STATES.COMPLETE_ABORT, txns:current("slow").state)
            assert.is_nil(txns:lso("out", 1))
            assert.is_true(txns:is_aborted("out", 1,
                txns:current("slow").pid, txns:current("slow").epoch, 0))
        end)

        it("retries a transaction stuck in PREPARE_ABORT after recovery", function()
            open_txn("stuck")
            local txns = broker.transactions
            local t = txns:current("stuck")
            t.state = txn_m.STATES.PREPARE_ABORT
            t.started_at = 0
            assert.are.same({ "stuck" }, txns:expire(1, 1000))
            assert.are.equal(txn_m.STATES.COMPLETE_ABORT, t.state)
        end)

        it("stamps recovered transactions with the recovery time", function()
            broker:close()
            broker = assert(brk_m.Broker.new(BASE_DIR,
                { transactions = { defer_recovery = true } }))
            open_txn("again")
            broker:close()
            broker = assert(brk_m.Broker.new(BASE_DIR,
                { transactions = { defer_recovery = true } }))
            local t = broker.transactions:current("again")
            assert.are.equal(txn_m.STATES.ONGOING, t.state)
            assert.is_number(t.started_at)
            assert.are.same({}, broker.transactions:expire(1, t.started_at + 5000))
            assert(broker.transactions:recover())
            assert.are.equal(txn_m.STATES.COMPLETE_ABORT, t.state)
        end)

        it("is swept by the server tick, which clears the session's in_txn", function()
            open_txn("swept")
            local conn = { producer_name = "swept", in_txn = true }
            local srv = setmetatable({
                broker                = broker,
                transaction_timeout_s = 1,
                connections_by_id     = { c1 = conn },
            }, Server)
            local started = broker.transactions:current("swept").started_at
            assert.are.same({}, srv:expire_transactions(started))
            assert.is_true(conn.in_txn)
            assert.are.same({ "swept" }, srv:expire_transactions(started + 1000))
            assert.is_false(conn.in_txn)
        end)
    end)
end)
