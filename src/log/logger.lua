local socket = require("socket")

local M = {}

local LEVELS = {
    DEBUG = 10,
    INFO  = 20,
    WARN  = 30,
    ERROR = 40,
}

local LEVEL_LABEL = {
    [10] = "DEBUG",
    [20] = "INFO",
    [30] = "WARN",
    [40] = "ERROR",
}

local state = {
    level = LEVELS.INFO,
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

local CTRL_ESCAPES = { ["\r"] = "\\r", ["\n"] = "\\n", ["\t"] = "\\t" }
local function sanitize(msg)
    return (msg:gsub("%c", function(c)
        return CTRL_ESCAPES[c] or string.format("\\x%02x", c:byte())
    end))
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
        now_iso(), LEVEL_LABEL[level_num], component, sanitize(msg))
    for i = 1, #state.sinks do
        pcall(state.sinks[i].write, state.sinks[i], line)
    end
end

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
            local ok = pcall(f.setvbuf, f, "line")
            if not ok then pcall(f.setvbuf, f, "no") end
            sinks[#sinks + 1] = f
        else
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

local cache = {}
function M.get(component)
    local existing = cache[component]
    if existing then return existing end
    local l = setmetatable({ component = component }, Logger)
    cache[component] = l
    return l
end

M.LEVELS = LEVELS

return M
