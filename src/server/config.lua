local json = require("dkjson")

local M = {}

local function read_file(path)
    local f, err = io.open(path, "rb")
    if not f then return nil end
    local content = f:read("*a")
    f:close()
    return content
end

local function deep_copy(v)
    if type(v) ~= "table" then return v end
    local out = {}
    for k, x in pairs(v) do out[k] = deep_copy(x) end
    return out
end

local function deep_merge(base, override)
    if type(override) ~= "table" then return override end
    if type(base) ~= "table" then return deep_copy(override) end

    -- A list in the overlay replaces the base value wholesale (merging
    -- arrays element-wise is never what a config author means). An *empty*
    -- or object-shaped overlay must not erase a base list, though: an overlay
    -- of `{}` (or one that only touches unrelated keys) has to leave
    -- Auth.Users / Cluster.Peers / ... intact.
    if override[1] ~= nil then
        return deep_copy(override)
    end
    if base[1] ~= nil and next(override) == nil then
        return deep_copy(base)
    end

    local result = {}
    for k, v in pairs(base) do
        result[k] = deep_copy(v)
    end

    for k, v in pairs(override) do
        if type(v) == "table" and type(result[k]) == "table" then
            result[k] = deep_merge(result[k], v)
        else
            result[k] = v
        end
    end
    return result
end

local function load_json(path, required)
    local content, ferr = read_file(path)
    if not content then
        if required then
            return nil, string.format("could not read %s: %s", path, ferr)
        end
        return nil, nil
    end

    local parsed, _, perr = json.decode(content)
    if not parsed then
        return nil, string.format("failed to parse %s: %s", path, perr)
    end
    return parsed, nil
end

function M.load(opts)
    opts = opts or {}
    local dir = opts.dir or "."
    local env = opts.environment
        or os.getenv("MOONMQ_ENVIRONMENT")
        or "Development"

    local base_path = dir .. "/appsettings.json"
    local overlay_path = dir .. "/appsettings." .. env .. ".json"

    local base, berr = load_json(base_path, true)
    if not base then return nil, berr end

    local overlay, oerr = load_json(overlay_path, false)
    if oerr then return nil, oerr end

    local merged = overlay and deep_merge(base, overlay) or base
    merged._environment = env
    return merged, nil
end

function M.get(cfg, path, default)
    local cur = cfg
    for part in path:gmatch("[^.]+") do
        if type(cur) ~= "table" then return default end
        cur = cur[part]
        if cur == nil then return default end
    end
    return cur
end


return M