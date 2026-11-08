local users_m  = require("src.server.users")
local auth     = require("src.server.auth")
local quota_m  = require("src.server.quota")
local options  = require("src.server.options")
local Server   = require("src.server.server")
local metrics  = require("src.metrics")

local ITER = 1000
local function cred(password)
    return auth.hash_password(password, { iterations = ITER })
end

local function orders_acl(ops)
    return { { Resource = "topic", Name = "orders", Operations = ops } }
end

local function auth_cfg(users, quotas)
    return { Auth = { Users = users, Quotas = quotas } }
end

local function fake_conn(id, principal)
    local conn = { id = id, id_short = id, username = principal and principal.username,
                   principal = principal }
    function conn:close(reason, code, msg)
        self.closed = { reason = reason, code = code, msg = msg }
    end
    return conn
end

local function server_with(cfg, next_cfg_ref)
    local authenticator, quotas = assert(options.auth(cfg))
    local srv = setmetatable({
        authenticator     = authenticator,
        quotas            = quotas,
        connections_by_id = {},
        auth_loader       = options.auth_loader(function() return next_cfg_ref.cfg end),
    }, Server)
    return srv
end

describe("Auth:replace_store", function()
    it("swaps users, bumps the generation and drops cached credentials", function()
        local store1 = assert(users_m.load({ Users = {
            { Username = "app", PasswordHash = cred("old"), Superuser = true } } }))
        local a = auth.authenticator({ store = store1 })
        assert.is_true((a:verify("app", "old", "1.1.1.1")))

        local store2 = assert(users_m.load({ Users = {
            { Username = "app", PasswordHash = cred("new"), Superuser = true } } }))
        local gen = a.generation
        a:replace_store(store2)
        assert.are.equal(gen + 1, a.generation)
        assert.is_false((a:verify("app", "old", "1.1.1.1")))
        assert.is_true((a:verify("app", "new", "1.1.1.1")))
    end)

    it("refuses a password login whose store changed mid-derivation", function()
        local store = assert(users_m.load({ Users = {
            { Username = "app", PasswordHash = cred("pw"), Superuser = true } } }))
        local a = auth.authenticator({ store = store })
        local real = auth.verify_password
        auth.verify_password = function(...)
            a:replace_store(store)
            return real(...)
        end
        local ok, err = a:verify("app", "pw", "1.1.1.1")
        auth.verify_password = real
        assert.is_false(ok)
        assert.matches("reloaded", err)
        assert.is_false((a:is_banned("1.1.1.1")))
        assert.is_true((a:verify("app", "pw", "1.1.1.1")))
    end)
end)

describe("options.reload_auth", function()
    it("builds a store and quotas from config", function()
        local store, q = assert(options.reload_auth(auth_cfg(
            { { Username = "app", PasswordHash = cred("x"), Superuser = true,
                Quota = { ProduceBytesPerSec = 10 } } })))
        assert.are.equal(1, store:count())
        assert.is_table(q)
    end)

    it("returns an error for a bad config and nothing for an empty one", function()
        local store, q, err = options.reload_auth(auth_cfg({ { Username = "" } }))
        assert.is_nil(store); assert.is_nil(q)
        assert.matches("Username", err)
        store, q, err = options.reload_auth({})
        assert.is_nil(store); assert.is_nil(q); assert.is_nil(err)
    end)

    it("auth_loader reports a config load failure", function()
        local _, _, err = options.auth_loader(function() return nil, "bad json" end)()
        assert.matches("bad json", err)
    end)
end)

describe("Server:reload_auth", function()
    before_each(function()
        metrics.counters["moonmq_auth_reloads_total"] = nil
        metrics.counters["moonmq_auth_reload_failures_total"] = nil
    end)

    it("applies new ACLs to live connections and closes removed users", function()
        local ref = {}
        local srv = server_with(auth_cfg({
            { Username = "admin", PasswordHash = cred("a"), Superuser = true },
            { Username = "app",   PasswordHash = cred("b"), Acls = orders_acl({ "read" }) },
            { Username = "gone",  PasswordHash = cred("c"), Acls = orders_acl({ "read" }) },
        }), ref)

        local app  = fake_conn("c1", srv.authenticator:principal("app"))
        local gone = fake_conn("c2", srv.authenticator:principal("gone"))
        local anon = fake_conn("c3", nil)
        srv.connections_by_id = { c1 = app, c2 = gone, c3 = anon }
        assert.is_false(app.principal.acl:authorized("topic", "orders", "write"))

        ref.cfg = auth_cfg({
            { Username = "admin", PasswordHash = cred("a"), Superuser = true },
            { Username = "app",   PasswordHash = cred("b"), Acls = orders_acl({ "read", "write" }) },
        })
        local ok, updated, dropped = srv:reload_auth()
        assert.is_true(ok)
        assert.are.equal(1, updated)
        assert.are.equal(1, dropped)
        assert.is_true(app.principal.acl:authorized("topic", "orders", "write"))
        assert.is_nil(app.closed)
        assert.are.equal("user removed", gone.closed.msg)
        assert.is_nil(anon.closed)
        assert.is_nil(srv.authenticator:principal("gone"))
        assert.are.equal(1, metrics.counters["moonmq_auth_reloads_total"])
    end)

    it("keeps the running users when the new config is invalid", function()
        local ref = {}
        local srv = server_with(auth_cfg({
            { Username = "admin", PasswordHash = cred("a"), Superuser = true } }), ref)
        local store = srv.authenticator.store

        ref.cfg = auth_cfg({ { Username = "admin" } })
        local ok, err = srv:reload_auth()
        assert.is_nil(ok)
        assert.matches("PasswordHash", err)
        assert.are.equal(store, srv.authenticator.store)
        assert.are.equal(1, metrics.counters["moonmq_auth_reload_failures_total"])
    end)

    it("refuses to drop every user, and refuses to enable auth on an OPEN broker", function()
        local ref = {}
        local srv = server_with(auth_cfg({
            { Username = "admin", PasswordHash = cred("a"), Superuser = true } }), ref)
        ref.cfg = {}
        local ok, err = srv:reload_auth()
        assert.is_nil(ok)
        assert.matches("OPEN", err)

        local open = setmetatable({
            connections_by_id = {},
            auth_loader = options.auth_loader(function()
                return auth_cfg({ { Username = "admin", PasswordHash = cred("a") } })
            end),
        }, Server)
        ok, err = open:reload_auth()
        assert.is_nil(ok)
        assert.matches("restart", err)
    end)

    it("swaps quota specs in place, and adds or clears them", function()
        local ref = {}
        local srv = server_with(auth_cfg(
            { { Username = "admin", PasswordHash = cred("a"), Superuser = true } },
            { Default = { ProduceBytesPerSec = 100 } }), ref)
        local manager = srv.quotas
        assert.is_table(manager.default)

        ref.cfg = auth_cfg(
            { { Username = "admin", PasswordHash = cred("a"), Superuser = true } },
            { Default = { ProduceBytesPerSec = 5 } })
        assert.is_true((srv:reload_auth()))
        assert.are.equal(manager, srv.quotas)
        assert.are.equal(5, manager.default[quota_m.DIM_PRODUCE_BYTES])

        ref.cfg = auth_cfg({ { Username = "admin", PasswordHash = cred("a"), Superuser = true } })
        assert.is_true((srv:reload_auth()))
        assert.is_true(manager:is_empty())
    end)
end)
