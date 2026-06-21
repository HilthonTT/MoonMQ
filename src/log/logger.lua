-- Tiny leveled logger.
--
--   local Log = require("src.log.logger")
--   Log.configure({                                  -- once at startup
--       level         = "INFO",
--       file_path     = "/var/log/moonmq.log",       -- nil = no file
--       log_to_stderr = true,                        -- default true
--   })
--   local log = Log.get("server")
--   log:info("listening on %s:%d", host, port)
--   log:warn("no authenticator configured, allowing")
--   log:error("conn=%s start failed: %s", id, err)
--
-- Output line: "2026-05-30T12:34:56.789Z [INFO ] [server] message\n"
--
-- One process-global level + (optionally tee'd) sinks. Until Log.configure
-- is called the default (INFO -> stderr) applies, matching the codebase's
-- de-facto behavior before this module existed.
--
-- File output uses line-buffered I/O (setvbuf "line") so a crash loses at
-- most one partial line. No rotation: pair with logrotate(8) using
-- `copytruncate` (we hold the FD open across rotation; logrotate copies
-- and zero-truncates, which we don't need to re-open for).
--
-- If file open fails at configure() time we fall back to stderr-only and
-- emit a one-time WARN — startup never aborts because of a misconfigured
-- log path.

local socket = require("socket")

local M = {}

local LEVELS = {
    DEBUG = 10,
    INFO  = 20,
    WARN  = 30,
    ERROR = 40,
}

-- Padded to 5 chars so columns align.
local LEVEL_LABEL = {
    [10] = "DEBUG",
    [20] = "INFO",
    [30] = "WARN",
    [40] = "ERROR",
}

local state = {
    level = LEVELS.INFO,
    -- `sinks` is a list of io-like handles. emit() writes to all of them
    -- in order. The default is a single stderr sink, which preserves the
    -- pre-existing behavior for anyone who never calls configure().
    sinks = { io.stderr },
}

local function parse_level(s)
    if type(s) ~= "string" then return nil end
    return LEVELS[s:upper()]
end

local function now_iso()
    local t = socket.gettime()
    local secs = math.floor(t)
    local ms = math.floor((t - secs) * 1000)
    return string.format("%s.%03dZ", os.date("!%Y-%m-%dT%H:%M:%S", secs), ms)
end

local function emit(component, level_num, fmt, ...)
    assert(type(component) == "string", "component must be a string")
    assert(type(level_num) == "number", "level_num must be a number")
    assert(type(fmt) == "string", "fmt must be a string")

    if level_num < state.level then return end
    local msg
    if select("#", ...) > 0 then
        local ok, formatted = pcall(string.format, fmt, ...)
        msg = ok and formatted or (fmt .. " (logger: bad format args)")
    else
        msg = fmt
    end
    local line = string.format(
        "%s [%s] [%s] %s\n",
        now_iso(), LEVEL_LABEL[level_num], component, msg)
    for i = 1, #state.sinks do
        -- pcall: never let a broken sink (closed file, EBADF, full disk)
        -- crash an in-flight request. We'd rather lose a log line than
        -- abort a produce/fetch.
        pcall(state.sinks[i].write, state.sinks[i], line)
    end
end

-- Public: one-time process init from config.
-- opts.level:         "DEBUG"|"INFO"|"WARN"|"ERROR" (case-insensitive).
--                     Unknown falls back to INFO and emits a one-time WARN.
-- opts.sink:          legacy single-sink override. Replaces stderr.
-- opts.file_path:     if set (non-nil, non-empty), append-open and tee.
-- opts.log_to_stderr: bool. When false AND file_path opened, suppress
--                     stderr. Default true.
function M.configure(opts)
    opts = opts or {}
    if opts.level ~= nil then
        local lv = parse_level(opts.level)
        if lv then
            state.level = lv
        else
            state.level = LEVELS.INFO
            emit("logger", LEVELS.WARN,
                "unknown level %q, falling back to INFO",
                tostring(opts.level))
        end
    end

    -- Reset sinks. opts.sink (legacy) wins; otherwise rebuild from
    -- log_to_stderr + file_path. Always at least one sink so emit() can
    -- never silently swallow logs.
    if opts.sink ~= nil then
        state.sinks = { opts.sink }
        return
    end

    local sinks = {}
    local log_to_stderr = opts.log_to_stderr
    if log_to_stderr == nil then log_to_stderr = true end

    if opts.file_path and opts.file_path ~= "" then
        local f, ferr = io.open(opts.file_path, "a")
        if f then
            -- Line-buffered so a crash loses at most one partial line.
            -- Also lets `tail -f` see new lines without an explicit flush.
            local ok = pcall(f.setvbuf, f, "line")
            if not ok then pcall(f.setvbuf, f, "no") end
            sinks[#sinks + 1] = f
        else
            -- Forced to stderr even if the user disabled it — we
            -- absolutely must surface the open failure somewhere.
            log_to_stderr = true
            emit("logger", LEVELS.WARN,
                "could not open log file %q: %s — falling back to stderr",
                tostring(opts.file_path), tostring(ferr))
        end
    end

    if log_to_stderr or #sinks == 0 then
        sinks[#sinks + 1] = io.stderr
    end

    state.sinks = sinks
end

local Logger = {}
Logger.__index = Logger

function Logger:debug(fmt, ...) emit(self.component, LEVELS.DEBUG, fmt, ...) end
function Logger:info(fmt, ...)  emit(self.component, LEVELS.INFO,  fmt, ...) end
function Logger:warn(fmt, ...)  emit(self.component, LEVELS.WARN,  fmt, ...) end
function Logger:error(fmt, ...) emit(self.component, LEVELS.ERROR, fmt, ...) end

-- Public: get a component-scoped logger. Cheap; cached per name.
local cache = {}
function M.get(component)
    local existing = cache[component]
    if existing then return existing end
    local l = setmetatable({ component = component }, Logger)
    cache[component] = l
    return l
end

-- Exposed for tests / introspection.
M.LEVELS = LEVELS

return M
