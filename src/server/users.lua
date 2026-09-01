local acl_m   = require("src.server.acl")
local quota_m = require("src.server.quota")
local auth_m  = require("src.server.auth")
local log     = require("src.log.logger").get("auth")

local M = {}

local Store = {}
Store.__index = Store

local function build_user(spec, index)
    if type(spec) ~= "table" then
        return nil, string.format("user #%d: entry must be an object", index)
    end

    local where = spec.Username and string.format("user %q", spec.Username)
                  or string.format("user #%d", index)

    local username = spec.Username or spec.username
    if type(username) ~= "string" or username == "" then
        return nil, string.format("%s: Username is required", where)
    end
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

function M.load(auth_cfg)
    if type(auth_cfg) ~= "table" then return nil, nil end

    local specs = {}

    if type(auth_cfg.Username) == "string" and auth_cfg.Username ~= "" then
        local has_credential = (auth_cfg.PasswordHash and auth_cfg.PasswordHash ~= "")
                            or (auth_cfg.Password and auth_cfg.Password ~= "")
        if has_credential then
            specs[#specs + 1] = {
                Username     = auth_cfg.Username,
                PasswordHash = auth_cfg.PasswordHash,
                Password     = auth_cfg.Password,
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

function Store:quota_specs()
    local out = {}
    for _, name in ipairs(self.names) do
        local q = self.by_name[name].quota
        if q then out[name] = q end
    end
    return out
end

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
