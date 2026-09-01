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

function Parser:at_kw(kw)
    local tok = self:peek()
    return tok and tok.kind == "keyword" and tok.value == kw
end

function Parser:accept_kw(kw)
    if self:at_kw(kw) then
        self:next()
        return true
    end
    return false
end

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

function Parser:name_list(what)
    local names = {}
    repeat
        local name, err = self:expect_name(what)
        if not name then return nil, err end
        names[#names + 1] = name
    until not (self:peek() and self:peek().kind == "comma" and self:next())
    return names
end


function Parser:parse_connect()
    self:next()
    self:accept_kw("to")
    local node = { type = "connect" }

    local tok = self:peek()
    if tok and (tok.kind == "string" or tok.kind == "ident") and not Lexer.KEYWORDS[tok.value] then
        node.host = tok.value
        self:next()
    end

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
    self:next()
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
    self:next()
    if self:accept_kw("topics") or self:accept_kw("topic") then
        return { type = "list_topics" }
    elseif self:accept_kw("group") or self:accept_kw("groups") then
        return { type = "show_group" }
    end
    return self:fail("expected TOPICS or GROUP")
end

function Parser:parse_produce()
    self:next()
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
    self:next()
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
    self:next()
    self:accept_kw("to")
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
    self:next()
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
    self:next()
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
    self:next()
    self:accept_kw("group")
    return { type = "leave_group" }
end

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

function Parser.parse_all(tokens)
    local p = new(tokens)
    local statements = {}

    while true do
        local tok = p:peek()
        if not tok or tok.kind == "eof" then break end

        if tok.kind == "semicolon" then
            p:next()
        else
            local stmt, err = p:parse_statement()
            if not stmt then return nil, err end

            local term = p:peek()
            if not term or term.kind ~= "semicolon" then
                return p:fail("expected ';' to end the statement", term)
            end
            p:next()
            statements[#statements + 1] = stmt
        end
    end

    return statements
end

function Parser.parse(src)
    local tokens, lerr = Lexer.tokenize(src)
    if not tokens then return nil, lerr end
    return Parser.parse_all(tokens)
end

return Parser
