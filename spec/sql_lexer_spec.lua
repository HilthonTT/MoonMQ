local Lexer = require("src.repl.sql.lexer")

local function kinds(src)
    local toks = assert(Lexer.tokenize(src))
    local out = {}
    for _, t in ipairs(toks) do
        if t.kind ~= "eof" then out[#out + 1] = { t.kind, t.value } end
    end
    return out
end

describe("sql lexer", function()
    it("lowercases keywords but preserves identifier case", function()
        local toks = kinds("CREATE Topic OrDeRs")
        assert.same({ "keyword", "create" }, toks[1])
        assert.same({ "keyword", "topic" }, toks[2])
        assert.same({ "ident", "OrDeRs" }, toks[3])
    end)

    it("distinguishes idents from keywords", function()
        local toks = kinds("fetch payments")
        assert.same({ "keyword", "fetch" }, toks[1])
        assert.same({ "ident", "payments" }, toks[2])
    end)

    it("accepts topic-name characters in identifiers", function()
        local toks = kinds("v1.0_test-2")
        assert.same({ "ident", "v1.0_test-2" }, toks[1])
    end)

    it("lexes a digit run followed by letters as an identifier", function()
        local toks = kinds("4orders")
        assert.same({ "ident", "4orders" }, toks[1])
    end)

    it("lexes bare integers as numbers", function()
        local toks = kinds("42")
        assert.same("number", toks[1][1])
        assert.same(42, toks[1][2])
    end)

    it("reads single- and double-quoted strings", function()
        assert.same({ "string", "hello world" }, kinds("'hello world'")[1])
        assert.same({ "string", "hello world" }, kinds('"hello world"')[1])
    end)

    it("treats a doubled quote as an escaped quote", function()
        assert.same({ "string", "it's" }, kinds("'it''s'")[1])
    end)

    it("keeps a semicolon inside a string as content", function()
        local toks = kinds("'a;b'")
        assert.same({ "string", "a;b" }, toks[1])
    end)

    it("skips -- line comments", function()
        local toks = kinds("list -- a comment\ntopics")
        assert.same({ "keyword", "list" }, toks[1])
        assert.same({ "keyword", "topics" }, toks[2])
        assert.is_nil(toks[3])
    end)

    it("emits punctuation tokens", function()
        local toks = kinds("a, b;")
        assert.same("comma", toks[2][1])
        assert.same("semicolon", toks[4][1])
    end)

    it("errors on an unterminated string", function()
        local toks, err = Lexer.tokenize("'oops")
        assert.is_nil(toks)
        assert.matches("unterminated", err.message)
    end)

    it("errors on an unexpected character with a position", function()
        local toks, err = Lexer.tokenize("list @topics")
        assert.is_nil(toks)
        assert.is_number(err.col)
        assert.equals(6, err.col)
    end)

    it("always terminates with an eof token", function()
        local toks = assert(Lexer.tokenize("list topics;"))
        assert.equals("eof", toks[#toks].kind)
    end)
end)
