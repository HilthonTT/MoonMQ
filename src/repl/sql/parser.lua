-- Recursive-descent parser for MoonMQ's SQL-like console language (MQL).
--
-- Consumes the token stream from lexer.lua and produces one AST node per
-- statement. A node is a plain table with a `type` field plus statement-
-- specific fields; the executor switches on `type`. Every statement must end
-- with a semicolon, which lets the REPL treat a bare `;` as "run now" and keep
-- reading continuation lines until it sees one.
--
-- parse_all(tokens) -> (statements, nil) | (nil, err)
--   statements : array of AST nodes
--   err        : { message, line, col, pos }

local Lexer = require("src.repl.sql.lexer")

local Parser = {}
Parser.__index = Parser

local function new(tokens)
    return setmetatable({ tokens = tokens, pos = 1 }, Parser)
end

function Parser:peek(ahead)
    return self.tokens[self.pos + (ahead or 0)]
end

function Parser:next()
    local tok = self.tokens[self.pos]
    self.pos = self.pos + 1
    return tok
end

function Parser:fail(message, tok)
    tok = tok or self:peek()
    return nil, {
        message = message,
        line    = tok and tok.line,
        col     = tok and tok.col,
        pos     = tok and tok.pos,
    }
end

-- Is the next token the keyword `kw`?
function Parser:at_kw(kw)
    local tok = self:peek()
    return tok and tok.kind == "keyword" and tok.value == kw
end

-- Consume the next token if it is keyword `kw`; return true when it did.
function Parser:accept_kw(kw)
    if self:at_kw(kw) then
        self:next()
        return true
    end
    return false
end

-- A name is either a bare identifier or a quoted string (so topics/groups with
-- unusual characters can still be addressed). Numbers are also accepted as
-- names because a purely numeric topic name is legal.
function Parser:expect_name(what)
    local tok = self:peek()
    if tok and (tok.kind == "ident" or tok.kind == "string") then
        self:next()
        return tok.value
    end
    if tok and tok.kind == "number" then
        self:next()
        return tok.text
    end
    return self:fail(string.format("expected %s", what or "a name"))
end

function Parser:expect_string(what)
    local tok = self:peek()
    if tok and tok.kind == "string" then
        self:next()
        return tok.value
    end
    -- Allow a bare identifier where a string is expected; it's friendlier at a
    -- console (KEY foo rather than forcing KEY 'foo').
    if tok and tok.kind == "ident" then
        self:next()
        return tok.value
    end
    return self:fail(string.format("expected %s (a quoted string)", what or "a value"))
end

function Parser:expect_number(what)
    local tok = self:peek()
    if tok and tok.kind == "number" then
        self:next()
        return tok.value
    end
    return self:fail(string.format("expected %s (a number)", what or "a number"))
end

-- Parse a comma-separated list of names, e.g. `orders, payments, audit`.
function Parser:name_list(what)
    local names = {}
    repeat
        local name, err = self:expect_name(what)
        if not name then return nil, err end
        names[#names + 1] = name
    until not (self:peek() and self:peek().kind == "comma" and self:next())
    return names
end

-- ---------------------------------------------------------------------------
-- Statement productions. Each assumes the leading keyword has NOT yet been
-- consumed and returns (node, nil) | (nil, err).
-- ---------------------------------------------------------------------------

function Parser:parse_connect()
    self:next() -- CONNECT
    self:accept_kw("to") -- optional filler
    local node = { type = "connect" }

    -- Optional bare host as the first token: CONNECT 'localhost' ...
    local tok = self:peek()
    if tok and (tok.kind == "string" or tok.kind == "ident") and not Lexer.KEYWORDS[tok.value] then
        node.host = tok.value
        self:next()
    end

    -- Then any mix of HOST/PORT/USER/PASSWORD clauses.
    while true do
        if self:accept_kw("host") then
            local v, e = self:expect_string("a host"); if not v then return nil, e end
            node.host = v
        elseif self:accept_kw("port") then
            local v, e = self:expect_number("a port"); if not v then return nil, e end
            node.port = v
        elseif self:accept_kw("user") then
            local v, e = self:expect_string("a username"); if not v then return nil, e end
            node.user = v
        elseif self:accept_kw("password") then
            local v, e = self:expect_string("a password"); if not v then return nil, e end
            node.password = v
        else
            break
        end
    end
    return node
end

function Parser:parse_create()
    self:next() -- CREATE
    if self:accept_kw("topic") then
        local name, e = self:expect_name("a topic name")
        if not name then return nil, e end
        local node = { type = "create_topic", name = name, partitions = 1 }
        if self:accept_kw("partitions") then
            local p, pe = self:expect_number("a partition count")
            if not p then return nil, pe end
            node.partitions = p
        end
        return node
    elseif self:accept_kw("group") then
        local name, e = self:expect_name("a group name")
        if not name then return nil, e end
        -- CREATE GROUP g SUBSCRIBE t1, t2  (aliases: ON / TO)
        if not (self:accept_kw("subscribe") or self:accept_kw("on") or self:accept_kw("to")) then
            return self:fail("expected SUBSCRIBE <topic[, ...]>")
        end
        local topics, te = self:name_list("a topic name")
        if not topics then return nil, te end
        return { type = "join_group", group = name, topics = topics }
    end
    return self:fail("expected TOPIC or GROUP after CREATE")
end

function Parser:parse_list()
    self:next() -- LIST / SHOW
    if self:accept_kw("topics") or self:accept_kw("topic") then
        return { type = "list_topics" }
    elseif self:accept_kw("group") or self:accept_kw("groups") then
        return { type = "show_group" }
    end
    return self:fail("expected TOPICS or GROUP")
end

function Parser:parse_produce()
    self:next() -- PRODUCE
    if not self:accept_kw("into") then
        return self:fail("expected INTO <topic>")
    end
    local topic, e = self:expect_name("a topic name")
    if not topic then return nil, e end

    local node = { type = "produce", topic = topic, key = "", value = nil }
    while true do
        if self:accept_kw("key") then
            local v, ke = self:expect_string("a key"); if not v then return nil, ke end
            node.key = v
        elseif self:accept_kw("value") then
            local v, ve = self:expect_string("a value"); if not v then return nil, ve end
            node.value = v
        else
            break
        end
    end
    if node.value == nil then
        return self:fail("PRODUCE requires VALUE '<payload>'")
    end
    return node
end

function Parser:parse_fetch()
    self:next() -- FETCH
    if not self:accept_kw("from") then
        return self:fail("expected FROM <topic>")
    end
    local topic, e = self:expect_name("a topic name")
    if not topic then return nil, e end

    local node = { type = "fetch", topic = topic, group = nil, limit = 100 }
    if self:accept_kw("group") then
        local g, ge = self:expect_name("a group name"); if not g then return nil, ge end
        node.group = g
    end
    if self:accept_kw("limit") then
        local l, le = self:expect_number("a limit"); if not l then return nil, le end
        node.limit = l
    end
    return node
end

function Parser:parse_subscribe()
    self:next() -- SUBSCRIBE
    self:accept_kw("to") -- optional
    local topic, e = self:expect_name("a topic name")
    if not topic then return nil, e end

    local node = { type = "subscribe", topic = topic, group = nil, timeout = 5 }
    if self:accept_kw("group") then
        local g, ge = self:expect_name("a group name"); if not g then return nil, ge end
        node.group = g
    end
    if self:accept_kw("timeout") then
        local t, te = self:expect_number("a timeout"); if not t then return nil, te end
        node.timeout = t
    end
    if self:accept_kw("limit") then
        local l, le = self:expect_number("a limit"); if not l then return nil, le end
        node.limit = l
    end
    return node
end

function Parser:parse_commit()
    self:next() -- COMMIT
    local topic, e = self:expect_name("a topic name")
    if not topic then return nil, e end
    if not self:accept_kw("partition") then
        return self:fail("expected PARTITION <n>")
    end
    local part, pe = self:expect_number("a partition"); if not part then return nil, pe end
    if not self:accept_kw("offset") then
        return self:fail("expected OFFSET <n>")
    end
    local off, oe = self:expect_number("an offset"); if not off then return nil, oe end
    return { type = "commit", topic = topic, partition = part, offset = off }
end

function Parser:parse_join()
    self:next() -- JOIN
    if not self:accept_kw("group") then
        return self:fail("expected GROUP after JOIN")
    end
    local name, e = self:expect_name("a group name")
    if not name then return nil, e end
    if not (self:accept_kw("subscribe") or self:accept_kw("on") or self:accept_kw("to")) then
        return self:fail("expected SUBSCRIBE <topic[, ...]>")
    end
    local topics, te = self:name_list("a topic name")
    if not topics then return nil, te end
    return { type = "join_group", group = name, topics = topics }
end

function Parser:parse_leave()
    self:next() -- LEAVE
    self:accept_kw("group") -- optional filler
    return { type = "leave_group" }
end

-- Dispatch one statement based on its leading keyword.
function Parser:parse_statement()
    local tok = self:peek()
    if not tok or tok.kind == "eof" then
        return self:fail("expected a statement")
    end
    if tok.kind ~= "keyword" then
        return self:fail(string.format("unexpected %s; a statement starts with a command word", tok.text))
    end

    local kw = tok.value
    if kw == "connect" then
        return self:parse_connect()
    elseif kw == "disconnect" then
        self:next(); return { type = "disconnect" }
    elseif kw == "create" then
        return self:parse_create()
    elseif kw == "list" or kw == "show" then
        return self:parse_list()
    elseif kw == "produce" then
        return self:parse_produce()
    elseif kw == "fetch" then
        return self:parse_fetch()
    elseif kw == "subscribe" then
        return self:parse_subscribe()
    elseif kw == "commit" then
        return self:parse_commit()
    elseif kw == "join" then
        return self:parse_join()
    elseif kw == "leave" then
        return self:parse_leave()
    elseif kw == "heartbeat" then
        self:next(); return { type = "heartbeat" }
    elseif kw == "help" then
        self:next()
        local t = self:peek()
        local topic
        if t and (t.kind == "keyword" or t.kind == "ident") then
            topic = t.value
            self:next()
        end
        return { type = "help", topic = topic }
    elseif kw == "exit" or kw == "quit" then
        self:next(); return { type = "quit" }
    end
    return self:fail(string.format("unknown command '%s'", kw:upper()))
end

-- Parse the whole token stream into a list of statements. Each statement must
-- be terminated by a semicolon.
function Parser.parse_all(tokens)
    local p = new(tokens)
    local statements = {}

    while true do
        local tok = p:peek()
        if not tok or tok.kind == "eof" then break end

        -- Tolerate stray semicolons (e.g. an empty `;;`).
        if tok.kind == "semicolon" then
            p:next()
        else
            local stmt, err = p:parse_statement()
            if not stmt then return nil, err end

            local term = p:peek()
            if not term or term.kind ~= "semicolon" then
                return p:fail("expected ';' to end the statement", term)
            end
            p:next() -- consume ';'
            statements[#statements + 1] = stmt
        end
    end

    return statements
end

-- Convenience: lex + parse a source string in one call.
function Parser.parse(src)
    local tokens, lerr = Lexer.tokenize(src)
    if not tokens then return nil, lerr end
    return Parser.parse_all(tokens)
end

return Parser
