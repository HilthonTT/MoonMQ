-- Tiny leveled logger.
--
--   local Log = require("src.log.logger")
--   Log.configure({ level = "INFO", sink = io.stderr })  -- once at startup
--   local log = Log.get("server")
--   log:info("listening on %s:%d", host, port)
--   log:warn("no authenticator configured, allowing")
--   log:error("conn=%s start failed: %s", id, err)
--
-- Output line: "2026-05-30T12:34:56.789Z [INFO ] [server] message\n"
--
-- One process-global level + sink. Until Log.configure is called the
-- default (INFO -> stderr) applies, which matches the codebase's
-- de-facto behavior before this module existed.

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
    sink  = io.stderr,
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
    state.sink:write(string.format(
        "%s [%s] [%s] %s\n",
        now_iso(), LEVEL_LABEL[level_num], component, msg))
end

-- Public: one-time process init from config.
-- opts.level: "DEBUG"|"INFO"|"WARN"|"ERROR" (case-insensitive). Unknown
--   falls back to INFO and emits a one-time WARN.
-- opts.sink:  any io-like handle with :write. Defaults to io.stderr.
function M.configure(opts)
    opts = opts or {}
    if opts.sink ~= nil then state.sink = opts.sink end
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
