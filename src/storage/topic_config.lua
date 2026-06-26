-- Per-topic config sidecar.
--
-- Persists the three SegmentedPartition opts that vary per topic
-- (max_segment_size, retention, cleaner_interval) so they survive a
-- broker restart. Without this, Broker:load_topics rebuilds every
-- partition with the module defaults and any per-topic tuning the
-- caller passed to create_topic is silently lost.
--
-- Format is line-based key=value with numeric values only:
--
--   max_segment_size=1073741824
--   retention=604800
--   cleaner_interval=3600
--
-- Whitespace around `=` is tolerated; blank lines and lines starting
-- with `#` are skipped. Unknown keys are ignored on read (forward-
-- compat) and never written. Lives at <topic_dir>/topic.config.

local fs_m    = require("src.io.fs")
local io_sync = require("src.io.io_sync")

local FILE_NAME = "topic.config"

-- Keys we recognise. Anything else in the file is dropped on read; any
-- extra keys handed to save() are ignored. Kept as an array (not a set)
-- so save() writes in a stable order — easier to diff.
local KNOWN_KEYS = {
    "max_segment_size",
    "retention",
    "cleaner_interval",
}

local function is_known(key)
    for i = 1, #KNOWN_KEYS do
        if KNOWN_KEYS[i] == key then return true end
    end
    return false
end

-- Returns the table of opts loaded from the sidecar. An absent file
-- returns an empty table (callers treat that as "use defaults"); a
-- present-but-malformed file returns (nil, err).
local function load(topic_dir)
    assert(type(topic_dir) == "string", "topic_dir must be a string")

    local path = fs_m.join_path(topic_dir, FILE_NAME)
    local f = io.open(path, "rb")
    if not f then
        -- No sidecar — topic predates this feature, or was created via
        -- a path that doesn't write one. Use defaults.
        return {}, nil
    end

    local body = f:read("*a") or ""
    f:close()

    local opts = {}
    local lineno = 0
    for line in (body .. "\n"):gmatch("([^\n]*)\n") do
        lineno = lineno + 1
        -- Strip CR (CRLF working trees per team memory) and surrounding
        -- whitespace before classifying the line.
        local stripped = line:gsub("\r$", ""):match("^%s*(.-)%s*$")
        if stripped ~= "" and stripped:sub(1, 1) ~= "#" then
            local key, val = stripped:match("^([%w_]+)%s*=%s*(.+)$")
            if not key then
                return nil, string.format(
                    "%s: malformed line %d: %q", path, lineno, line)
            end
            if is_known(key) then
                local n = tonumber(val)
                if not n then
                    return nil, string.format(
                        "%s: non-numeric value for %s on line %d: %q",
                        path, key, lineno, val)
                end
                opts[key] = n
            end
            -- Unknown key: silently ignored (forward-compat).
        end
    end

    return opts, nil
end

-- Writes the sidecar atomically (tmp + rename). Only the three known
-- keys are emitted, in KNOWN_KEYS order. Non-numeric values are
-- rejected; nil values are skipped (so partial opts work and the
-- defaults path covers the rest on load).
local function save(topic_dir, opts)
    assert(type(topic_dir) == "string", "topic_dir must be a string")
    assert(type(opts) == "table", "opts must be a table")

    local lines = {}
    for i = 1, #KNOWN_KEYS do
        local key = KNOWN_KEYS[i]
        local val = opts[key]
        if val ~= nil then
            if type(val) ~= "number" then
                return false, string.format(
                    "opts.%s must be a number, got %s", key, type(val))
            end
            -- %d would truncate doubles silently; %.f keeps a stable
            -- integer-looking form for the integer values we ship and
            -- still round-trips through tonumber on load.
            lines[#lines + 1] = string.format("%s=%.f", key, val)
        end
    end

    local path = fs_m.join_path(topic_dir, FILE_NAME)
    local tmp  = path .. ".tmp"

    local f, ferr = io.open(tmp, "wb")
    if not f then return false, ferr end
    -- Trailing newline matches the parser, which expects "\n" after the
    -- last entry. Writing with no entries (all opts nil) yields just
    -- the newline — load() treats that as empty opts.
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
