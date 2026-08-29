-- The user store: every principal the broker will accept, with its
-- credential, its ACL, and its quota.
--
-- Before this, `Auth` held exactly one username and one hash, and the answer
-- to "which tenants share this broker" was "one, or everybody pretending to
-- be it". A store turns the broker multi-tenant: N users, each with its own
-- credential and its own authority.
--
-- Config shape (appsettings.json):
--
--     "Auth": {
--       "Users": [
--         { "Username": "orders-svc",
--           "PasswordHash": "scram-sha-256$600000$...",
--           "Acls": [ { "Resource": "topic", "Name": "orders.*",
--                       "Operations": ["read","write","describe"] },
--                     { "Resource": "group", "Name": "orders-*",
--                       "Operations": ["read","describe"] } ],
--           "Quota": { "ProduceRecordsPerSec": 5000 } },
--         { "Username": "admin", "PasswordHash": "...", "Superuser": true }
--       ]
--     }
--
-- The single-user form (`Auth.Username` + `Auth.PasswordHash`) still works and
-- still means what it always meant — one superuser — so an existing config
-- keeps behaving identically after upgrading.
--
-- Everything is validated at load: a bad ACL rule, an unparseable credential,
-- a duplicate username, or an unknown quota key stops the broker at boot with
-- the offending entry named. Security config that fails open because of a
-- typo is the failure mode this is written to prevent.

local acl_m   = require("src.server.acl")
local quota_m = require("src.server.quota")
local auth_m  = require("src.server.auth")
local log     = require("src.log.logger").get("auth")

local M = {}

local Store = {}
Store.__index = Store

local function build_user(spec, index)
    -- Type-check BEFORE indexing: a Users list holding a bare number would
    -- otherwise raise "attempt to index a number value" out of a config
    -- validator, which is a stack trace where an error message belongs.
    if type(spec) ~= "table" then
        return nil, string.format("user #%d: entry must be an object", index)
    end

    local where = spec.Username and string.format("user %q", spec.Username)
                  or string.format("user #%d", index)

    local username = spec.Username or spec.username
    if type(username) ~= "string" or username == "" then
        return nil, string.format("%s: Username is required", where)
    end
    -- The SCRAM grammar gives "," and "=" a meaning inside the username field
    -- (they are escaped on the wire); a NUL would truncate comparisons. Reject
    -- control characters outright rather than discovering them mid-handshake.
    if username:find("[%c]") then
        return nil, string.format("%s: Username contains control characters", where)
    end

    local stored = spec.PasswordHash or spec.password_hash
    if stored == "" then stored = nil end

    if not stored then
        local plain = spec.Password or spec.password
        if type(plain) ~= "string" or plain == "" then
            return nil, string.format(
                "%s: PasswordHash (or Password) is required", where)
        end
        -- Hashing at boot costs one derivation per plaintext user. It is the
        -- documented convenience path; the warning is what nudges operators to
        -- the hashed form before the user list gets long.
        log:warn("hashing plaintext password for %q on startup. Generate a "
            .. "stored credential instead: lua bin/moonmq-hash.lua <password> --scram",
            username)
        stored = auth_m.hash_password(plain)
    end

    local parsed, perr = auth_m.parse_credential(stored)
    if not parsed then
        return nil, string.format("%s: %s", where, perr)
    end

    local superuser = spec.Superuser == true or spec.superuser == true

    local user_acl
    if superuser then
        user_acl = acl_m.ALLOW_ALL
    else
        local built, aerr = acl_m.new(spec.Acls or spec.acls)
        if not built then
            return nil, string.format("%s: %s", where, aerr)
        end
        user_acl = built
    end

    local quota, qerr = quota_m.spec(spec.Quota or spec.quota)
    if qerr then
        return nil, string.format("%s: %s", where, qerr)
    end

    return {
        username   = username,
        credential = stored,
        parsed     = parsed,
        acl        = user_acl,
        quota      = quota,
        superuser  = superuser,
    }, nil
end

-- Build a store from the `Auth` config block. Returns (store, nil) or
-- (nil, err). A config with neither Users nor Username yields (nil, nil):
-- no store, which the caller reads as "broker is OPEN" exactly as before.
function M.load(auth_cfg)
    if type(auth_cfg) ~= "table" then return nil, nil end

    local specs = {}

    -- Legacy single-user form first, so an explicit Users entry for the same
    -- name is a duplicate-name error rather than a silent shadowing.
    if type(auth_cfg.Username) == "string" and auth_cfg.Username ~= "" then
        local has_credential = (auth_cfg.PasswordHash and auth_cfg.PasswordHash ~= "")
                            or (auth_cfg.Password and auth_cfg.Password ~= "")
        if has_credential then
            specs[#specs + 1] = {
                Username     = auth_cfg.Username,
                PasswordHash = auth_cfg.PasswordHash,
                Password     = auth_cfg.Password,
                -- The historical contract: the configured account may do
                -- anything. Narrowing it on upgrade would break deployments
                -- silently, which is worse than a permissive default that the
                -- operator can now tighten explicitly.
                Superuser    = auth_cfg.Superuser ~= false,
                Acls         = auth_cfg.Acls,
                Quota        = auth_cfg.Quota,
            }
        else
            log:warn("Auth.Username is set but no credential is configured; "
                .. "ignoring it")
        end
    end

    for _, spec in ipairs(auth_cfg.Users or {}) do
        specs[#specs + 1] = spec
    end

    if #specs == 0 then return nil, nil end

    local by_name = {}
    local names   = {}
    for i, spec in ipairs(specs) do
        local user, err = build_user(spec, i)
        if not user then return nil, err end
        if by_name[user.username] then
            return nil, string.format("duplicate user %q", user.username)
        end
        by_name[user.username] = user
        names[#names + 1] = user.username
    end
    table.sort(names)

    -- A user that can authenticate but is authorized for nothing is almost
    -- always a forgotten Acls block. Default-deny means it fails on its first
    -- request with a puzzling error; say so at boot, where it is fixable.
    for _, name in ipairs(names) do
        local user = by_name[name]
        if not user.superuser and user.acl:is_empty() then
            log:warn("user %q has no ACL rules and is not a superuser: it can "
                .. "authenticate but every request will be denied", name)
        end
    end

    return setmetatable({ by_name = by_name, names = names }, Store), nil
end

function M.single(username, credential, opts)
    opts = opts or {}
    return M.load({
        Username     = username,
        PasswordHash = credential,
        Superuser    = opts.superuser ~= false,
        Acls         = opts.acls,
        Quota        = opts.quota,
    })
end

function Store:get(username)
    if type(username) ~= "string" then return nil end
    return self.by_name[username]
end

function Store:count()
    return #self.names
end

function Store:names_sorted()
    return self.names
end

-- Per-user quota specs, in the shape src/server/quota.lua wants. Kept here so
-- the quota manager is built from the same parsed config as the ACLs and
-- cannot drift from it.
function Store:quota_specs()
    local out = {}
    for _, name in ipairs(self.names) do
        local q = self.by_name[name].quota
        if q then out[name] = q end
    end
    return out
end

-- One line per user for the boot log: who exists, what they may do, and
-- whether the credential is the SCRAM-capable kind. Never includes the
-- credential itself.
function Store:describe()
    local out = {}
    for _, name in ipairs(self.names) do
        local user = self.by_name[name]
        out[#out + 1] = string.format("%s[%s%s]", name,
            user.parsed.kind,
            user.superuser and ",superuser" or "")
    end
    return table.concat(out, " ")
end

return M
