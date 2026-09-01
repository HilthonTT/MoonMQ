local fs_m    = require("src.io.fs")
local io_sync = require("src.io.io_sync")

local FILE_NAME = "topic.config"

local NUMBER_KEYS = {
    "max_segment_size",
    "retention",
    "cleaner_interval",
    "max_log_bytes",
}

local STRING_KEYS = {
    "backend",
    "cleanup_policy",
}

local function in_list(list, key)
    for i = 1, #list do
        if list[i] == key then return true end
    end
    return false
end

local function is_number_key(key) return in_list(NUMBER_KEYS, key) end
local function is_string_key(key) return in_list(STRING_KEYS, key) end

local function load(topic_dir)
    assert(type(topic_dir) == "string", "topic_dir must be a string")

    local path = fs_m.join_path(topic_dir, FILE_NAME)
    local f = io.open(path, "rb")
    if not f then
        return {}, nil
    end

    local body = f:read("*a") or ""
    f:close()

    local opts = {}
    local lineno = 0
    for line in (body .. "\n"):gmatch("([^\n]*)\n") do
        lineno = lineno + 1
        local stripped = line:gsub("\r$", ""):match("^%s*(.-)%s*$")
        if stripped ~= "" and stripped:sub(1, 1) ~= "#" then
            local key, val = stripped:match("^([%w_]+)%s*=%s*(.+)$")
            if not key then
                return nil, string.format(
                    "%s: malformed line %d: %q", path, lineno, line)
            end
            if is_number_key(key) then
                local n = tonumber(val)
                if not n then
                    return nil, string.format(
                        "%s: non-numeric value for %s on line %d: %q",
                        path, key, lineno, val)
                end
                opts[key] = n
            elseif is_string_key(key) then
                opts[key] = val
            end
        end
    end

    return opts, nil
end

local function save(topic_dir, opts)
    assert(type(topic_dir) == "string", "topic_dir must be a string")
    assert(type(opts) == "table", "opts must be a table")

    local lines = {}
    for i = 1, #NUMBER_KEYS do
        local key = NUMBER_KEYS[i]
        local val = opts[key]
        if val ~= nil then
            if type(val) ~= "number" then
                return false, string.format(
                    "opts.%s must be a number, got %s", key, type(val))
            end
            lines[#lines + 1] = string.format("%s=%.f", key, val)
        end
    end
    for i = 1, #STRING_KEYS do
        local key = STRING_KEYS[i]
        local val = opts[key]
        if val ~= nil then
            if type(val) ~= "string" then
                return false, string.format(
                    "opts.%s must be a string, got %s", key, type(val))
            end
            lines[#lines + 1] = string.format("%s=%s", key, val)
        end
    end

    local path = fs_m.join_path(topic_dir, FILE_NAME)
    local tmp  = path .. ".tmp"

    local f, ferr = io.open(tmp, "wb")
    if not f then return false, ferr end
    f:write(table.concat(lines, "\n"))
    f:write("\n")
    f:flush()
    f:close()

    return io_sync.atomic_rename(tmp, path)
end

return {
    load = load,
    save = save,
    FILE_NAME = FILE_NAME,
}
