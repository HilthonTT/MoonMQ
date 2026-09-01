local Lexer   = require("src.repl.sql.lexer")
local Parser  = require("src.repl.sql.parser")
local Session = require("src.repl.sql.executor")
local render  = require("src.repl.sql.render")
local help    = require("src.repl.sql.help")

local paint = render.paint

local M = {}

local PROMPT      = "mq> "
local CONTINUATION = "..> "

local function last_semicolon(tokens)
    local found
    for _, t in ipairs(tokens) do
        if t.kind == "semicolon" then found = t end
    end
    return found
end

local function print_error(err, src, out)
    out(paint("red", "error: " .. (err.message or "syntax error")) .. "\n")
    if err.line and src then
        local lines = {}
        for line in (src .. "\n"):gmatch("(.-)\n") do lines[#lines + 1] = line end
        local ln = lines[err.line]
        if ln then
            out("  " .. ln .. "\n")
            out("  " .. string.rep(" ", math.max(0, (err.col or 1) - 1)) .. paint("red", "^") .. "\n")
        end
    end
end

local function print_help(topic, out)
    if topic then
        local entry = help[topic]
        if not entry then
            out(paint("yellow", "no help for '" .. topic .. "'") .. "\n")
            return
        end
        out(paint("cyan", entry.usage) .. "\n  " .. entry.blurb .. "\n")
        return
    end
    out(paint("cyan", "MoonMQ console commands:") .. "\n")
    for _, name in ipairs(help._order) do
        local entry = help[name]
        if entry then
            out(string.format("  %-11s %s\n", name:upper(), entry.blurb))
        end
    end
    out("\nType HELP <command>; for syntax. Statements end with ';'.\n")
end

local function print_result(res, out)
    if not res.ok then
        out(paint("red", "error: " .. tostring(res.error)) .. "\n")
        return
    end
    if res.kind == "message" then
        out(paint("green", "OK") .. " " .. res.message .. "\n")
    elseif res.kind == "rows" then
        if #res.rows == 0 then
            out(paint("dim", "(no rows)") .. (res.note and ("  " .. res.note) or "") .. "\n")
        else
            out(render.table(res.columns, res.rows) .. "\n")
            if res.note then out(paint("dim", "(" .. res.note .. ")") .. "\n") end
        end
    elseif res.kind == "help" then
        print_help(res.topic, out)
    end
end

local function banner(out)
    out(paint("magenta", "MoonMQ SQL console") .. " — type HELP; for commands, EXIT; to leave.\n")
end

local function run_chunk(session, chunk, out)
    local statements, err = Parser.parse(chunk)
    if not statements then
        print_error(err, chunk, out)
        return false
    end
    for _, stmt in ipairs(statements) do
        local res = session:execute(stmt)
        if res.ok and res.kind == "quit" then
            return true
        end
        print_result(res, out)
    end
    return false
end

function M.run(opts)
    opts = opts or {}
    local read_line = assert(opts.read_line, "read_line is required")
    local out = opts.out or function(s) io.write(s) end
    local session = Session.new(opts.session_opts)

    banner(out)

    if opts.autoconnect then
        local res = session:connect({})
        if res.ok then
            print_result(res, out)
        else
            out(paint("dim", string.format(
                "not connected (no broker at %s:%d). Run CONNECT ...; to connect.",
                session.host, session.port)) .. "\n")
        end
    end

    local pending = ""
    while true do
        local is_cont = pending:match("%S") ~= nil
        local line = read_line(is_cont and CONTINUATION or PROMPT, is_cont)
        if line == nil then
            out("\n")
            break
        end

        pending = pending .. line .. "\n"

        local tokens, lerr = Lexer.tokenize(pending)
        if not tokens then
            if lerr.message == "unterminated string literal" then
            else
                print_error(lerr, pending, out)
                pending = ""
            end
        else
            local semi = last_semicolon(tokens)
            if semi then
                local runnable = pending:sub(1, semi.pos)
                local rest     = pending:sub(semi.pos + 1)
                if run_chunk(session, runnable, out) then
                    break
                end
                pending = rest:match("%S") and rest or ""
            end
        end
    end

    if session:is_connected() then session.client:close() end
end

M.PROMPT = PROMPT
M.CONTINUATION = CONTINUATION

return M
