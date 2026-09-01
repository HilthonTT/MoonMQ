local Lexer = {}

local KEYWORDS = {
    connect = true, disconnect = true,
    create = true, list = true, show = true,
    topic = true, topics = true, group = true, groups = true,
    partitions = true, partition = true,
    produce = true, into = true, key = true, value = true,
    fetch = true, from = true, limit = true,
    subscribe = true, to = true, timeout = true,
    commit = true, offset = true,
    join = true, leave = true, heartbeat = true, ["on"] = true,
    host = true, port = true, user = true, password = true,
    help = true, exit = true, quit = true,
}

Lexer.KEYWORDS = KEYWORDS

local function is_space(c)
    return c == " " or c == "\t" or c == "\r" or c == "\n"
end

local function is_digit(c)
    return c >= "0" and c <= "9"
end

local function is_ident_start(c)
    return (c >= "A" and c <= "Z")
        or (c >= "a" and c <= "z")
        or is_digit(c)
        or c == "_"
end

local function is_ident_part(c)
    return is_ident_start(c) or c == "." or c == "-"
end

function Lexer.tokenize(src)
    local tokens = {}
    local i, n = 1, #src
    local line, col = 1, 1

    local function err(message, at_pos, at_col)
        return nil, {
            message = message,
            pos     = at_pos or i,
            line    = line,
            col     = at_col or col,
        }
    end

    local function advance()
        local c = src:sub(i, i)
        if c == "\n" then
            line = line + 1
            col = 1
        else
            col = col + 1
        end
        i = i + 1
        return c
    end

    local function push(kind, value, text, start_pos, start_col)
        tokens[#tokens + 1] = {
            kind  = kind,
            value = value,
            text  = text or value,
            pos   = start_pos,
            line  = line,
            col   = start_col,
        }
    end

    while i <= n do
        local c = src:sub(i, i)

        if is_space(c) then
            advance()

        elseif c == "-" and src:sub(i + 1, i + 1) == "-" then
            while i <= n and src:sub(i, i) ~= "\n" do advance() end

        elseif c == ";" then
            local sc = col
            advance()
            push("semicolon", ";", ";", i - 1, sc)

        elseif c == "," then
            local sc = col
            advance()
            push("comma", ",", ",", i - 1, sc)

        elseif c == "*" then
            local sc = col
            advance()
            push("star", "*", "*", i - 1, sc)

        elseif c == "'" or c == '"' then
            local quote = c
            local start_pos, start_col = i, col
            advance()
            local parts = {}
            local closed = false
            while i <= n do
                local ch = src:sub(i, i)
                if ch == quote then
                    if src:sub(i + 1, i + 1) == quote then
                        parts[#parts + 1] = quote
                        advance(); advance()
                    else
                        advance()
                        closed = true
                        break
                    end
                elseif ch == "\n" then
                    return err("unterminated string literal", start_pos, start_col)
                else
                    parts[#parts + 1] = ch
                    advance()
                end
            end
            if not closed then
                return err("unterminated string literal", start_pos, start_col)
            end
            push("string", table.concat(parts), nil, start_pos, start_col)

        elseif is_digit(c) then
            local start_pos, start_col = i, col
            local buf = {}
            while i <= n and is_digit(src:sub(i, i)) do
                buf[#buf + 1] = advance()
            end
            if i <= n and is_ident_part(src:sub(i, i)) then
                while i <= n and is_ident_part(src:sub(i, i)) do
                    buf[#buf + 1] = advance()
                end
                push("ident", table.concat(buf), nil, start_pos, start_col)
            else
                local text = table.concat(buf)
                push("number", tonumber(text), text, start_pos, start_col)
            end

        elseif is_ident_start(c) then
            local start_pos, start_col = i, col
            local buf = {}
            while i <= n and is_ident_part(src:sub(i, i)) do
                buf[#buf + 1] = advance()
            end
            local text = table.concat(buf)
            local lowered = text:lower()
            if KEYWORDS[lowered] then
                push("keyword", lowered, text, start_pos, start_col)
            else
                push("ident", text, text, start_pos, start_col)
            end

        else
            return err(string.format("unexpected character %q", c), i, col)
        end
    end

    push("eof", "<eof>", "<eof>", i, col)
    return tokens
end

return Lexer
