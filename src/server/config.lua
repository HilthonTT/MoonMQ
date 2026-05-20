local json = require("dkjson")

local M = {}

local function read_file(path)
    local f, err = io.open(path, "rb")
    if not f then return nil end
    local content = f:read("*a")
    f:close()
    return content
end

local function deep_merge(base, override)
    if type(override) ~= "table" then return override end
    if type(base) ~= "table" then return deep_merge({}, override) end

    -- Treat as array (replace wholesale) if it looks like one.
    if override[1] ~= nil then
        local copy = {}
        for i = 1, #override do copy[i] = override[i] end
        return copy
    end

    local result = {}
    for k, v in pairs(base) do
        result[k] = (type(v) == "table") and deep_merge(v, {}) or v
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