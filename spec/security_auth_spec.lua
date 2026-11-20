local brk_m        = require("src.broker")
local topic_config = require("src.storage.topic_config")
local fs_m         = require("src.io.fs")
local os_utils     = require("src.core.os")
local scram        = require("src.server.scram")
local auth         = require("src.server.auth")
local users_m      = require("src.server.users")
local rng          = require("src.core.rng")
local pbkdf2       = require("src.core.pbkdf2")
local b64          = require("src.core.base64")
local render       = require("src.repl.sql.render")

local BASE_DIR = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_security_auth_test"
                                      or "/tmp/moonmq_security_auth_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

describe("ALTER_TOPIC_CONFIG validation", function()
    local broker

    before_each(function()
        rmdir(BASE_DIR)
        broker = assert(brk_m.Broker.new(BASE_DIR))
        assert(broker:create_topic("orders", 1))
    end)

    after_each(function() rmdir(BASE_DIR) end)

    local function saved()
        return assert(topic_config.load(fs_m.join_path(BASE_DIR, "orders")))
    end

    it("refuses a string value that smuggles extra lines into the sidecar", function()
        local ok, err = broker:alter_topic_config("orders",
            { cleanup_policy = "delete\nbackend=commitlog\nmax_segment_size=99999999999" })
        assert.is_nil(ok)
        assert.is_truthy(err:find("control"))
        assert.is_nil(saved().backend)
        assert.is_nil(saved().max_segment_size)
    end)

    it("accepts only delete or compact as the cleanup policy", function()
        assert.is_nil((broker:alter_topic_config("orders", { cleanup_policy = "shred" })))
        assert.truthy(broker:alter_topic_config("orders", { cleanup_policy = "compact" }))
        assert.are.equal("compact", saved().cleanup_policy)
        assert.truthy(broker:alter_topic_config("orders", { cleanup_policy = "delete" }))
        assert.are.equal("delete", saved().cleanup_policy)
    end)

    it("refuses inf and nan", function()
        for _, v in ipairs({ "inf", "-inf", "nan", math.huge, 0/0, "1e400" }) do
            local ok, err = broker:alter_topic_config("orders", { retention = v })
            assert.is_nil(ok, tostring(v))
            assert.is_truthy(err)
        end
        assert.is_nil(saved().retention)
    end)

    it("caps max_segment_size at the commitlog index limit", function()
        local limit = 0xFFFFFFFF - 64 * 1024 * 1024
        assert.is_nil((broker:alter_topic_config("orders",
            { max_segment_size = limit + 1 })))
        assert.truthy(broker:alter_topic_config("orders", { max_segment_size = limit }))
        assert.are.equal(limit, saved().max_segment_size)
    end)

    it("caps other numbers at 2^53", function()
        assert.is_nil((broker:alter_topic_config("orders", { retention = 2 ^ 54 })))
        assert.truthy(broker:alter_topic_config("orders", { max_log_bytes = 2 ^ 40 }))
    end)

    it("still restarts cleanly after a rejected alter", function()
        broker:alter_topic_config("orders", { cleanup_policy = "x\nmax_segment_size=1e30" })
        broker:alter_topic_config("orders", { max_segment_size = 2 ^ 40 })
        broker:close()
        local reopened = assert(brk_m.Broker.new(BASE_DIR))
        assert.truthy(reopened:get_topic("orders"))
    end)
end)

describe("topic_config.save", function()
    after_each(function() rmdir(BASE_DIR) end)

    it("refuses non-finite numbers", function()
        for _, v in ipairs({ math.huge, -math.huge, 0/0 }) do
            local ok, err = topic_config.save(BASE_DIR, { retention = v })
            assert.is_false(ok)
            assert.is_truthy(err:find("finite"))
        end
    end)

    it("refuses strings containing control characters", function()
        for _, v in ipairs({ "delete\nbackend=x", "a\rb", "a\0b", "\27[31m" }) do
            local ok, err = topic_config.save(BASE_DIR, { cleanup_policy = v })
            assert.is_false(ok)
            assert.is_truthy(err:find("control"))
        end
    end)
end)

describe("SCRAM client iteration bounds", function()
    local NONCE = "clientnonce"

    local function server_first(iterations, salt_b64)
        return string.format("r=%sXYZ,s=%s,i=%s", NONCE, salt_b64 or "c2FsdHNhbHQ=",
            tostring(iterations))
    end

    it("rejects an iteration count below 4096 by default", function()
        local ok, err = scram.parse_server_first(server_first(1), NONCE)
        assert.is_nil(ok)
        assert.is_truthy(err:find("minimum"))
        assert.is_nil((scram.parse_server_first(server_first(4095), NONCE)))
        assert.truthy(scram.parse_server_first(server_first(4096), NONCE))
    end)

    it("rejects an iteration count above the pbkdf2 maximum", function()
        local over = pbkdf2.MAX_PBKDF2_ITERATIONS + 1
        local ok, err = scram.parse_server_first(server_first(over), NONCE)
        assert.is_nil(ok)
        assert.is_truthy(err:find("maximum"))
        assert.truthy(scram.parse_server_first(
            server_first(pbkdf2.MAX_PBKDF2_ITERATIONS), NONCE))
    end)

    it("rejects an empty salt", function()
        local ok, err = scram.parse_server_first(server_first(4096, ""), NONCE)
        assert.is_nil(ok)
        assert.is_truthy(err:find("salt"))
    end)

    it("lets the caller lower the minimum", function()
        assert.truthy(scram.parse_server_first(server_first(1000), NONCE,
            { min_iterations = 1000 }))
    end)

    it("accepts the shipped appsettings credential's iteration count", function()
        local salt = b64.encode("0123456789abcdef")
        assert.truthy(scram.parse_server_first(server_first(10000, salt), NONCE))
    end)
end)

describe("auth ban duration", function()
    local function store()
        local user = { username = "u", parsed = { iterations = 1000 } }
        return {
            get          = function(_, n) return n == "u" and user or nil end,
            count        = function() return 1 end,
            names_sorted = function() return { "u" } end,
            quota_specs  = function() return {} end,
            describe     = function() return "u" end,
        }
    end

    it("treats BanDuration as seconds", function()
        local a = auth.authenticator({ store = store(), max_failures = 1,
            failure_window = 60, ban_duration = 900 })
        a:_record_failure("10.0.0.1", 1000)
        assert.are.equal(1900, a.failures["10.0.0.1"].banned_until)
    end)

    it("defaults to 900 seconds", function()
        local a = auth.authenticator({ store = store(), max_failures = 1 })
        assert.are.equal(900, a.ban_duration)
    end)
end)

describe("decoy iteration count", function()
    local function hash(iter)
        return auth.hash_password("pw", { iterations = iter, format = auth.FORMAT_SCRAM })
    end

    local a
    before_each(function()
        a = auth.authenticator({ store = assert(users_m.load({
            Users = {
                { Username = "low",  PasswordHash = hash(1000) },
                { Username = "mid",  PasswordHash = hash(1500) },
                { Username = "high", PasswordHash = hash(2000) },
            },
        })) })
    end)

    it("is stable per unknown username", function()
        for i = 1, 20 do
            local name = "ghost" .. i
            assert.are.equal(a:scram_credential(name).iterations,
                             a:scram_credential(name).iterations)
        end
    end)

    it("is always one of the configured iteration counts", function()
        local allowed = { [1000] = true, [1500] = true, [2000] = true }
        local seen = {}
        for i = 1, 200 do
            local it_count = a:scram_credential("ghost" .. i).iterations
            assert.is_true(allowed[it_count] == true, tostring(it_count))
            seen[it_count] = true
        end
        assert.is_true(seen[1000] and seen[1500] and seen[2000],
            "every configured count should be used by some unknown name")
    end)

    it("uses the same per-name count for the password path decoy", function()
        for i = 1, 20 do
            local name = "ghost" .. i
            assert.are.equal(a:scram_credential(name).iterations,
                             a:_decoy_for(name).iterations)
        end
    end)

    it("follows a store reload", function()
        a:replace_store(assert(users_m.load({
            Users = { { Username = "only", PasswordHash = hash(1234) } },
        })))
        assert.are.equal(1234, a:scram_credential("ghost").iterations)
    end)
end)

describe("rng", function()
    local saved

    before_each(function() saved = rng._sources end)
    after_each(function() rng._sources = saved end)

    it("returns bytes from a working source", function()
        assert.are.equal(32, #rng.bytes(32))
    end)

    it("falls through to the next CSPRNG source", function()
        rng._sources = {
            { name = "broken", fn = function() return nil, "nope" end },
            { name = "fixed",  fn = function(n) return string.rep("A", n) end },
        }
        assert.are.equal("AAAA", rng.bytes(4))
    end)

    it("fails closed when no CSPRNG source works", function()
        rng._sources = {
            { name = "broken", fn = function() return nil, "nope" end },
        }
        assert.has_error(function() rng.bytes(16) end)
        rng._sources = {}
        assert.has_error(function() rng.bytes(16) end)
    end)

    it("offers openssl.rand as a source when luaossl is installed", function()
        if not pcall(require, "openssl.rand") then return end
        local found = false
        for _, s in ipairs(saved) do
            if s.name == "openssl.rand" then
                found = true
                assert.are.equal(24, #s.fn(24))
            end
        end
        assert.is_true(found)
    end)
end)

describe("REPL table rendering", function()
    it("escapes terminal control sequences in cells", function()
        local out = render.table({ "key" }, { { "a\27]0;pwned\7b\27[2J" } })
        out = out:match("[^\n]*pwned[^\n]*")
        assert.is_nil(out:find("\27", 1, true))
        assert.is_nil(out:find("\7", 1, true))
        assert.is_truthy(out:find("\\x1b]0;pwned\\x07b\\x1b[2J", 1, true))
    end)

    it("escapes UTF-8 encoded C1 controls", function()
        local line = render.one_line("x\194\155y")
        assert.are.equal("x\\u009by", line)
    end)

    it("still flattens newlines to spaces", function()
        assert.are.equal("a b  c", render.one_line("a\nb\r\nc"))
    end)
end)
