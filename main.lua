local Server    = require("src.server.server")
local Config    = require("src.server.config")
local options_m = require("src.server.options")
local version_m = require("src.core.version")
local Log       = require("src.log.logger")
local repl      = require("src.repl.repl")

local log = Log.get("main")

-- True when run as `lua main.lua …` rather than required as a module.
local function is_main(argv, ...)
    local n_arg = argv and #argv or 0
    if n_arg ~= select("#", ...) then return false end
    for i = 1, n_arg do
        if argv[i] ~= select(i, ...) then return false end
    end
    return true
end

local function print_version(argv)
    local sub = argv[2]
    if sub == "--json" then
        print(version_m.GetVersionJSON())
    elseif sub == "--short" then
        print(version_m.GetVersionInfo():Short())
    else
        print(version_m.GetVersionInfo():String())
    end
end

local COMMANDS = {
    ["--repl"]    = function() repl() end,
    ["version"]   = print_version,
    ["--version"] = print_version,
    ["-v"]        = print_version,
}

local function configure_logging(cfg)
    local log_file = Config.get(cfg, "Logging.File", "")
    Log.configure({
        level         = Config.get(cfg, "Logging.Level", "INFO"),
        file_path     = log_file ~= "" and log_file or nil,
        log_to_stderr = Config.get(cfg, "Logging.LogToStderr", true),
    })
end

-- Loads the config and starts the broker; returns only on a startup
-- failure, with the reason.
local function run_broker()
    local cfg, cerr = Config.load()
    if not cfg then return "config: " .. tostring(cerr) end
    configure_logging(cfg)

    local opts, oerr = options_m.from_config(cfg)
    if not opts then return oerr end

    local srv, serr = Server.new(opts)
    if not srv then return "server: " .. tostring(serr) end

    log:info("env=%s host=%s port=%d data_dir=%s",
        cfg._environment, srv.host, srv.port, opts.data_dir)
    local started, err = srv:start()
    if not started then return "server: " .. tostring(err) end
    return nil
end

if is_main(arg, ...) then
    local command = arg[1] and COMMANDS[arg[1]]
    if command then
        command(arg)
        os.exit(0)
    end

    local err = run_broker()
    if err then
        log:error("%s", err)
        os.exit(1)
    end
end
