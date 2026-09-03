-- Regression tests for the bugs fixed in the 2026-09 audit. Each block names
-- the module and the failure it guards against; see the commit message for
-- the full list.
local socket    = require("socket")
local json      = require("dkjson")
local os_utils  = require("src.core.os")
local fs_m      = require("src.io.fs")
local config    = require("src.server.config")
local auth      = require("src.server.auth")
local Reactor   = require("src.server.reactor")
local metrics   = require("src.metrics")
local commitlog = require("src.commitlog")
local message   = require("src.record.message")
local Topic     = require("src.storage.topic")
local seg_m     = require("src.storage.segmentation")
local Router    = require("src.cluster.router")
local brk_m     = require("src.broker")
local txn_m     = require("src.broker.txn_coordinator")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_bugfix_audit"
                                      or "/tmp/moonmq_bugfix_audit"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function write_file(path, content)
    local f = assert(io.open(path, "wb"))
    f:write(content)
    f:close()
end

describe("audit regressions", function()
    before_each(function() rmdir(BASE_DIR); fs_m.mkdir(BASE_DIR) end)
    after_each(function() rmdir(BASE_DIR) end)

    describe("config.deep_merge", function()
        local base = {
            Server  = { Port = 9092, Host = "0.0.0.0" },
            Auth    = { Users = { { Name = "a" }, { Name = "b" } } },
            Cluster = { Peers = { { Id = "p1" } } },
        }

        it("keeps base lists when the overlay only touches other keys", function()
            write_file(BASE_DIR .. "/appsettings.json", json.encode(base))
            write_file(BASE_DIR .. "/appsettings.Test.json",
                json.encode({ Server = { Port = 9999 } }))
            local cfg = assert(config.load({ dir = BASE_DIR, environment = "Test" }))
            assert.are.equal(9999, cfg.Server.Port)
            assert.are.equal("0.0.0.0", cfg.Server.Host)
            assert.are.equal(2, #cfg.Auth.Users)
            assert.are.equal(1, #cfg.Cluster.Peers)
        end)

        it("keeps base lists under an empty overlay", function()
            write_file(BASE_DIR .. "/appsettings.json", json.encode(base))
            write_file(BASE_DIR .. "/appsettings.Test.json", "{}")
            local cfg = assert(config.load({ dir = BASE_DIR, environment = "Test" }))
            assert.are.equal(2, #cfg.Auth.Users)
        end)

        it("lets an overlay list replace a base list wholesale", function()
            write_file(BASE_DIR .. "/appsettings.json", json.encode(base))
            write_file(BASE_DIR .. "/appsettings.Test.json",
                json.encode({ Auth = { Users = { { Name = "only" } } } }))
            local cfg = assert(config.load({ dir = BASE_DIR, environment = "Test" }))
            assert.are.equal(1, #cfg.Auth.Users)
            assert.are.equal("only", cfg.Auth.Users[1].Name)
        end)
    end)

    describe("auth failure tracking", function()
        local function make_auth()
            local user = { username = "u", parsed = { iterations = 1000 } }
            local store = {
                get          = function(_, n) return n == "u" and user or nil end,
                count        = function() return 1 end,
                names_sorted = function() return { "u" } end,
                quota_specs  = function() return {} end,
                describe     = function() return "u" end,
            }
            return auth.authenticator({
                store = store, max_failures = 3, failure_window = 10, ban_duration = 15,
            })
        end

        it("does not lift an active ban when a failure lands after the window", function()
            local a = make_auth()
            local t0 = 1000
            a:_record_failure("10.0.0.9", t0)
            a:_record_failure("10.0.0.9", t0 + 1)
            a:_record_failure("10.0.0.9", t0 + 2)
            assert.is_true(a.failures["10.0.0.9"].banned_until > t0 + 2)

            -- A stray failure after the window would previously reset the record.
            a:_record_failure("10.0.0.9", t0 + 20)
            local rec = a.failures["10.0.0.9"]
            assert.is_not_nil(rec.banned_until)
            assert.is_true(rec.banned_until > t0 + 20)
        end)
    end)

    describe("reactor deadlines", function()
        it("wakes a parked read on its deadline instead of stranding it", function()
            local r = Reactor.new()
            local listener = assert(socket.bind("127.0.0.1", 0))
            local _, port = listener:getsockname()
            local client = assert(socket.tcp())
            client:settimeout(1)
            assert(client:connect("127.0.0.1", tonumber(port)))
            local server_side = assert(listener:accept())

            local result, err
            local t_start = socket.gettime()
            r:spawn(function()
                result, err = r:read_exact(client, 10, socket.gettime() + 0.2)
                r:stop()
            end)
            r:spawn(function() r:sleep(3); r:stop() end)  -- safety net
            r:run()

            assert.is_nil(result)
            assert.are.equal("deadline exceeded", err)
            assert.is_true(socket.gettime() - t_start < 2.5, "deadline was not enforced")
            assert.is_nil(r.read_waiters[client], "waiter entry must be cleaned up")
            local timers = 0
            for _ in pairs(r.timer_waiters) do timers = timers + 1 end
            assert.are.equal(1, timers, "only the safety-net sleeper may remain")

            server_side:close(); client:close(); listener:close()
        end)

        it("still returns data that arrives before the deadline", function()
            local r = Reactor.new()
            local listener = assert(socket.bind("127.0.0.1", 0))
            local _, port = listener:getsockname()
            local client = assert(socket.tcp())
            client:settimeout(1)
            assert(client:connect("127.0.0.1", tonumber(port)))
            local server_side = assert(listener:accept())

            local result
            r:spawn(function()
                result = r:read_exact(client, 5, socket.gettime() + 2)
                r:stop()
            end)
            r:spawn(function() r:sleep(0.05); server_side:send("hello") end)
            r:run()

            assert.are.equal("hello", result)
            assert.is_nil(next(r.timer_waiters))
            server_side:close(); client:close(); listener:close()
        end)

        it("release() wakes coroutines parked on a closed socket", function()
            local r = Reactor.new()
            local listener = assert(socket.bind("127.0.0.1", 0))
            local _, port = listener:getsockname()
            local client = assert(socket.tcp())
            client:settimeout(1)
            assert(client:connect("127.0.0.1", tonumber(port)))
            local server_side = assert(listener:accept())

            local finished = false
            r:spawn(function()
                r:read_exact(client, 10)
                finished = true
                r:stop()
            end)
            r:spawn(function()
                r:sleep(0.05)
                client:close()
                r:release(client)
            end)
            r:spawn(function() r:sleep(3); r:stop() end)
            r:run()

            assert.is_true(finished, "reader parked on a closed socket must be woken")
            assert.is_nil(r.read_waiters[client])
            server_side:close(); listener:close()
        end)
    end)

    describe("metrics exposition", function()
        it("emits histogram buckets in ascending le order with +Inf last", function()
            metrics.observe("audit_hist_order_seconds", 0.3)
            local text = metrics.render_prometheus()
            local les = {}
            for le in text:gmatch('audit_hist_order_seconds_bucket{le="([^"]+)"}') do
                les[#les + 1] = le
            end
            assert.is_true(#les > 2)
            assert.are.equal("+Inf", les[#les])
            for i = 1, #les - 2 do
                assert.is_true(tonumber(les[i]) < tonumber(les[i + 1]))
            end
        end)

        it("treats an empty label set like no labels", function()
            metrics.inc("audit_empty_labels_total", 1, {})
            metrics.inc("audit_empty_labels_total", 1)
            assert.are.equal(2, metrics.counters["audit_empty_labels_total"])
            assert.is_nil(metrics.counters["audit_empty_labels_total{}"])
        end)
    end)

    describe("fs.join_path", function()
        it("keeps the filesystem root", function()
            if os_utils.IS_WINDOWS then return end
            assert.are.equal("/tmp/x", fs_m.join_path("/", "tmp", "x"))
            assert.are.equal("/tmp/x", fs_m.join_path("/tmp/", "x"))
        end)
    end)

    describe("commitlog delete cleaner", function()
        local function new_log(max_seg, max_log)
            local path = fs_m.join_path(BASE_DIR, "log")
            local options = commitlog.Options.new(path, max_seg, max_log, "delete")
            local l, err = commitlog.CommitLog.new(options)
            assert(not err, err)
            return l
        end

        it("keeps the segment that straddles the byte budget", function()
            local l = new_log(1, 10)
            assert(l:append_message(message.Message.new("k", "v0", 1)))
            assert(l:append_message(message.Message.new("k", "v1", 2)))
            -- The just-sealed segment (offset 0) exceeds the budget on its
            -- own; it must still survive the roll.
            assert.are.equal(0, l:oldest_offset())
            local got, _, rerr = l:read_at(0)
            assert.is_nil(rerr)
            assert.are.equal("v0", got.value)

            assert(l:append_message(message.Message.new("k", "v2", 3)))
            -- Now two sealed segments; the newer one alone is over budget,
            -- so the oldest is retired.
            assert.are.equal(1, l:oldest_offset())
            l:close()
        end)
    end)

    describe("segmented partition torn tail", function()
        it("does not leave overlapping segments after a partial write + clean close", function()
            local t = Topic.new("t")
            local dir = fs_m.join_path(BASE_DIR, "seg")
            local p = assert(seg_m.SegmentedPartition.new(t, 1, dir, {}))
            assert(p:write_message(message.Message.new("k1", "v1", 1)))
            -- Simulate a torn write left by a failed file:write.
            p.active_segment.file:seek("end")
            p.active_segment.file:write("GARBAGE!!")
            p.active_segment.file:flush()
            local o2 = assert(p:write_message(message.Message.new("k2", "v2", 2)))
            p:close()

            local p2 = assert(seg_m.SegmentedPartition.new(t, 1, dir, {}))
            for i = 2, #p2.segments do
                local prev, cur = p2.segments[i - 1], p2.segments[i]
                assert.is_true(prev.base_offset + prev.bytes_written <= cur.base_offset,
                    "segments overlap after reopen")
            end
            local m, _, err = p2:read_message(o2)
            assert.is_nil(err)
            assert.are.equal("v2", m.value)
            p2:close()
        end)
    end)

    describe("cluster router", function()
        local function fake_peer(reply)
            return { id = "B", append = function() return table.unpack(reply) end }
        end

        it("acks the owner's reported first_offset", function()
            local router = Router.new({ assignments = {}, peers = {}, self_id = "A" })
            local msg = message.Message.new("k", "v", 1)
            local off = assert(router:forward(fake_peer({ 500, 123 }), "t", 1, msg))
            assert.are.equal(123, off)
        end)

        it("falls back to LEO arithmetic for peers without first_offset", function()
            local router = Router.new({ assignments = {}, peers = {}, self_id = "A" })
            local msg = message.Message.new("k", "v", 1)
            local bytes = assert(message.serialize_message(msg))
            local off = assert(router:forward(fake_peer({ 500 }), "t", 1, msg))
            assert.are.equal(500 - #bytes, off)
        end)
    end)

    describe("transaction recovery deferral", function()
        it("waits for recover() when defer_recovery is set", function()
            local dir = fs_m.join_path(BASE_DIR, "txn")
            do
                local broker = assert(brk_m.Broker.new(dir))
                assert(broker:create_topic("out", 1))
                local pid, epoch = assert(broker.producer_state:get_or_create_producer("txn-d"))
                assert(broker.transactions:begin("txn-d", pid, epoch))
                assert(broker.transactions:add_partition("txn-d", pid, epoch, "out", 1))
                for _, p in ipairs(broker.topic_manager.topics[txn_m.STATE_TOPIC].partitions) do
                    if p.sync then p:sync() end
                end
            end

            local broker2 = assert(brk_m.Broker.new(dir, {
                transactions = { defer_recovery = true },
            }))
            assert.is_true(broker2.transactions.recovery_pending)
            assert.are.equal(txn_m.STATES.ONGOING,
                broker2.transactions:current("txn-d").state)

            assert(broker2.transactions:recover())
            assert.is_false(broker2.transactions.recovery_pending)
            assert.are.equal(txn_m.STATES.COMPLETE_ABORT,
                broker2.transactions:current("txn-d").state)
            -- Idempotent.
            assert(broker2.transactions:recover())
        end)
    end)
end)
