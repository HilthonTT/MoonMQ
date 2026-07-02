-- Entry point for MoonMQ's interactive console.
--
-- The console speaks a small SQL-like language (see src/repl/sql/*) rather than
-- raw Lua: CREATE TOPIC, LIST TOPICS, PRODUCE, FETCH, consumer-group commands,
-- and so on. This module only wires terminal I/O to the loop in
-- src/repl/sql/repl.lua; the language itself lives in the sql/ submodules.

local sql = require("src.repl.sql.repl")

-- Build a line reader backed by the sirocco line-editor (history, editing,
-- completion) when it's available, falling back to a plain io.read loop
-- otherwise (non-TTY, missing deps). Returns a reader function compatible with
-- sql.run's read_line, or nil to signal "use the fallback".
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
            -- Not Lua, so don't validate/parse input as Lua while typing.
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
    return io.read("*l") -- nil at EOF
end

return function(opts)
    opts = opts or {}
    local reader = make_editor_reader() or fallback_reader

    sql.run {
        read_line    = reader,
        -- Try the local broker on startup; harmless hint if nothing is there.
        autoconnect  = opts.autoconnect ~= false,
        session_opts = opts.session_opts,
    }
end
