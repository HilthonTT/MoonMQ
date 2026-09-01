local users_m = require("src.server.users")
local auth    = require("src.server.auth")
local acl_m   = require("src.server.acl")
local quota_m = require("src.server.quota")

local ITER = 1000
local function cred(password, format)
    return auth.hash_password(password, { iterations = ITER, format = format })
end

describe("user store loading", function()

    it("returns no store when nothing is configured (OPEN mode)", function()
        assert.is_nil((users_m.load({})))
        assert.is_nil((users_m.load(nil)))
    end)

    it("accepts the legacy single-user form as a superuser", function()
        local store = assert(users_m.load({
            Username = "admin", PasswordHash = cred("hunter2"),
        }))
        assert.are.equal(1, store:count())
        local user = store:get("admin")
        assert.is_true(user.superuser)
        assert.is_true(user.acl:authorized("topic", "anything", "delete"))
    end)

    it("ignores a legacy username with no credential", function()
        assert.is_nil((users_m.load({ Username = "admin" })))
    end)

    it("loads a list of users with their own ACLs and quotas", function()
        local store = assert(users_m.load({
            Users = {
                { Username = "admin", PasswordHash = cred("a"), Superuser = true },
                { Username = "orders", PasswordHash = cred("b"),
                  Acls = {
                      { Resource = "topic", Name = "orders.*",
                        Operations = { "read", "write" } },
                  },
                  Quota = { ProduceRecordsPerSec = 100 } },
            },
        }))
        assert.are.equal(2, store:count())
        assert.are.same({ "admin", "orders" }, store:names_sorted())

        local orders = store:get("orders")
        assert.is_false(orders.superuser)
        assert.is_true(orders.acl:authorized("topic", "orders.eu", "write"))
        assert.is_false(orders.acl:authorized("topic", "billing", "read"))
        assert.are.equal(100, orders.quota[quota_m.DIM_PRODUCE_RECORDS])

        assert.are.same({ orders = orders.quota }, store:quota_specs())
    end)

    it("supports both credential formats", function()
        local store = assert(users_m.load({
            Users = {
                { Username = "old", PasswordHash = cred("a") },
                { Username = "new", PasswordHash = cred("a", auth.FORMAT_SCRAM) },
            },
        }))
        assert.are.equal(auth.FORMAT_PBKDF2, store:get("old").parsed.kind)
        assert.are.equal(auth.FORMAT_SCRAM,  store:get("new").parsed.kind)
    end)

    it("fails the load on a duplicate username", function()
        local store, err = users_m.load({
            Username = "admin", PasswordHash = cred("a"),
            Users = { { Username = "admin", PasswordHash = cred("b") } },
        })
        assert.is_nil(store)
        assert.is_truthy(err:find("duplicate user"))
    end)

    it("fails the load on a bad ACL rule, naming the user", function()
        local store, err = users_m.load({
            Users = { { Username = "orders", PasswordHash = cred("a"),
                        Acls = { { Resource = "topic", Name = "t",
                                   Operations = { "obliterate" } } } } },
        })
        assert.is_nil(store)
        assert.is_truthy(err:find("orders"))
        assert.is_truthy(err:find("obliterate"))
    end)

    it("fails the load on an unparseable credential", function()
        local store, err = users_m.load({
            Users = { { Username = "orders", PasswordHash = "not-a-hash" } },
        })
        assert.is_nil(store)
        assert.is_truthy(err:find("orders"))
    end)

    it("fails the load on a missing credential or username", function()
        assert.is_nil((users_m.load({ Users = { { Username = "orders" } } })))
        assert.is_nil((users_m.load({ Users = { { PasswordHash = cred("a") } } })))
    end)

    it("fails the load on an unknown quota key", function()
        local store, err = users_m.load({
            Users = { { Username = "orders", PasswordHash = cred("a"),
                        Quota = { ProduceRecords = 5 } } },
        })
        assert.is_nil(store)
        assert.is_truthy(err:find("unknown quota key"))
    end)

    it("rejects a non-object entry with a message, not a stack trace", function()
        local store, err = users_m.load({ Users = { 42 } })
        assert.is_nil(store)
        assert.is_truthy(err:find("must be an object"))
    end)

    it("rejects control characters in a username", function()
        assert.is_nil((users_m.load({
            Users = { { Username = "or\0ders", PasswordHash = cred("a") } },
        })))
    end)

    it("describes itself without leaking credentials", function()
        local store = assert(users_m.load({
            Users = {
                { Username = "admin", PasswordHash = cred("a"), Superuser = true },
                { Username = "orders", PasswordHash = cred("b", auth.FORMAT_SCRAM) },
            },
        }))
        local text = store:describe()
        assert.are.equal("admin[pbkdf2,superuser] orders[scram]", text)
        assert.is_nil(text:find("%$"), "no credential material in the description")
    end)
end)

describe("multi-user authenticator", function()

    local store, a

    before_each(function()
        store = assert(users_m.load({
            Users = {
                { Username = "admin", PasswordHash = cred("admin-pw"), Superuser = true },
                { Username = "orders", PasswordHash = cred("orders-pw", auth.FORMAT_SCRAM),
                  Acls = {
                      { Resource = "topic", Name = "orders.*",
                        Operations = { "read", "write" } },
                  } },
            },
        }))
        a = auth.authenticator({ store = store })
    end)

    it("verifies each user against its own credential", function()
        local ok, _, principal = a:verify("orders", "orders-pw", "10.0.0.1")
        assert.is_true(ok)
        assert.are.equal("orders", principal.username)
        assert.is_false(principal.superuser)
        assert.is_true(principal.acl:authorized("topic", "orders.eu", "write"))

        assert.is_true((a:verify("admin", "admin-pw", "10.0.0.1")))
    end)

    it("does not accept one user's password for another", function()
        assert.is_false((a:verify("orders", "admin-pw", "10.0.0.1")))
        assert.is_false((a:verify("admin", "orders-pw", "10.0.0.1")))
    end)

    it("refuses an unknown user with the same error as a wrong password", function()
        local _, unknown_err = a:verify("ghost", "orders-pw", "10.0.0.1")
        local _, wrong_err   = a:verify("orders", "wrong", "10.0.0.2")
        assert.are.equal(wrong_err, unknown_err)
        assert.are.equal("invalid credentials", unknown_err)
    end)

    it("returns no principal on failure", function()
        local ok, _, principal = a:verify("orders", "wrong", "10.0.0.1")
        assert.is_false(ok)
        assert.is_nil(principal)
    end)

    it("bans an IP after repeated failures, across users", function()
        local banned = auth.authenticator({ store = store, max_failures = 3 })
        assert.is_false((banned:verify("a", "x", "10.0.0.9")))
        assert.is_false((banned:verify("b", "x", "10.0.0.9")))
        assert.is_false((banned:verify("c", "x", "10.0.0.9")))
        assert.is_true(banned:is_banned("10.0.0.9"))

        local ok, err = banned:verify("orders", "orders-pw", "10.0.0.9")
        assert.is_false(ok)
        assert.is_truthy(err:find("banned"))
        assert.is_true((banned:verify("orders", "orders-pw", "10.0.0.10")))
    end)

    it("caches a successful credential per user", function()
        assert.is_true((a:verify("orders", "orders-pw", "10.0.0.1")))
        assert.is_true((a:verify("admin", "admin-pw", "10.0.0.1")))
        assert.is_truthy(a.cred_cache["orders"])
        assert.is_truthy(a.cred_cache["admin"])
        assert.is_true((a:verify("orders", "orders-pw", "10.0.0.1")))
    end)

    it("exposes the principal by name without a credential check", function()
        assert.are.equal("orders", a:principal("orders").username)
        assert.is_nil(a:principal("ghost"))
    end)
end)

describe("scram credential lookup", function()

    local a

    before_each(function()
        a = auth.authenticator({ store = assert(users_m.load({
            Users = { { Username = "orders", PasswordHash = cred("pw") } },
        })) })
    end)

    it("returns the real salt and keys for a known user", function()
        local credential, principal = a:scram_credential("orders")
        assert.are.equal("orders", principal.username)
        assert.are.equal(ITER, credential.iterations)
        assert.are.equal(32, #credential.stored_key)
        assert.are.equal(32, #credential.server_key)
    end)

    it("returns a decoy for an unknown user, with no principal", function()
        local credential, principal = a:scram_credential("ghost")
        assert.is_nil(principal)
        assert.are.equal(32, #credential.stored_key)
        assert.is_true(#credential.salt > 0)
    end)

    it("keeps the decoy salt stable per username and distinct between them", function()
        local ghost1 = a:scram_credential("ghost")
        local ghost2 = a:scram_credential("ghost")
        local other  = a:scram_credential("phantom")
        assert.are.equal(ghost1.salt, ghost2.salt)
        assert.are.equal(ghost1.iterations, ghost2.iterations)
        assert.are_not.equal(ghost1.salt, other.salt)
    end)

    it("gives the decoy the same iteration count as the real credentials", function()
        assert.are.equal(a:scram_credential("orders").iterations,
                         a:scram_credential("ghost").iterations)
    end)
end)

describe("single-user compatibility shim", function()

    it("still builds from a password hash and authenticates", function()
        local a = auth.static_authenticator({
            username = "admin", password_hash = cred("hunter2"),
        })
        local ok, _, principal = a:verify("admin", "hunter2", "1.2.3.4")
        assert.is_true(ok)
        assert.is_true(principal.superuser)
        assert.is_true(principal.acl:authorized("cluster", nil, "alter"))
        assert.is_false((a:verify("admin", "wrong", "1.2.3.4")))
        assert.is_false((a:verify("nobody", "hunter2", "1.2.3.4")))
    end)

    it("keeps its historical fields", function()
        local hash = cred("hunter2")
        local a = auth.static_authenticator({ username = "admin", password_hash = hash })
        assert.are.equal("admin", a.username)
        assert.are.equal(hash, a.password_hash)
    end)

    it("rejects an invalid stored hash at construction", function()
        assert.has_error(function()
            auth.static_authenticator({ username = "admin", password_hash = "nope" })
        end)
    end)

    it("is usable for SCRAM too", function()
        local a = auth.static_authenticator({
            username = "admin", password_hash = cred("hunter2", auth.FORMAT_SCRAM),
        })
        local credential, principal = a:scram_credential("admin")
        assert.are.equal("admin", principal.username)
        assert.are.equal(32, #credential.stored_key)
    end)
end)

describe("acl integration", function()

    it("gives a superuser everything and an empty-ACL user nothing", function()
        local store = assert(users_m.load({
            Users = {
                { Username = "root", PasswordHash = cred("a"), Superuser = true },
                { Username = "mute", PasswordHash = cred("a") },
            },
        }))
        assert.is_true(store:get("root").acl:authorized("topic", "x", "delete"))
        assert.is_false(store:get("mute").acl:authorized("topic", "x", "read"))
        assert.is_true(store:get("mute").acl:is_empty())
        assert.are.equal(acl_m.ALLOW_ALL, store:get("root").acl)
    end)
end)
