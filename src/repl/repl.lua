local sql = require("src.repl.sql.repl")

local function make_editor_reader()
    local ok_prompt, LuaPrompt = pcall(require, "src.repl.luaprompt")
    local ok_conf,   conf      = pcall(require, "src.repl.conf")
    if not (ok_prompt and ok_conf) then
        return nil
    end

    local cdo = require("src.repl.do")
    local history = {}
    pcall(function() history = cdo.loadHistory() or {} end)

    local quit_requested = false

    return function(promptstr)
        local code = LuaPrompt {
            prompt      = promptstr,
            parsing     = false,
            history     = history,
            tokenColors = conf.syntaxColors,
            help        = {},
            quit        = function() quit_requested = true end,
        }:ask()

        if quit_requested then
            return nil
        end

        if code and code ~= "" and (not history[1] or history[1] ~= code) then
            table.insert(history, 1, code)
            pcall(cdo.appendToHistory, code)
        end

        return code or ""
    end
end

local function fallback_reader(promptstr)
    io.write(promptstr)
    io.flush()
    return io.read("*l")
end

return function(opts)
    opts = opts or {}
    local reader = make_editor_reader() or fallback_reader

    sql.run {
        read_line    = reader,
        autoconnect  = opts.autoconnect ~= false,
        session_opts = opts.session_opts,
    }
end
