local proto     = require("src.wire.protocol")
local message   = require("src.record.message")
local uuid      = require("src.core.uuid")
local Client    = require("src.client")
local verify    = require("src.storage.segment_verify")
local brk       = require("src.broker")
local segment_m = require("src.commitlog.segment")
local AbortIndex = require("src.broker.abort_index")
local replica_srv = require("src.server.replica_server")
local os_utils  = require("src.core.os")

local BASE_DIR = (os_utils.IS_WINDOWS and "C:/Temp/lua_bugfix2_test"
                                     or  "/tmp/lua_bugfix2_test")

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', (path:gsub("/", string.char(92)))))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function dripping_sock(stream, drip)
    return {
        pos = 1,
        settimeout = function() end,
        send = function(_, d) return #d end,
        close = function() end,
        receive = function(self, n)
            local avail = #stream - self.pos + 1
            if avail <= 0 then return nil, "timeout", "" end
            local take = math.min(n, drip, avail)
            local chunk = stream:sub(self.pos, self.pos + take - 1)
            self.pos = self.pos + take
            if take < n then return nil, "timeout", chunk end
            return chunk
        end,
    }
end

describe("client frame reassembly", function()
    it("reassembles a frame delivered in short reads instead of desyncing", function()
        local frame = proto.encode_record(uuid.ZERO, "orders", 2, 7, 0, "k", "payload-value")
        local c = setmetatable({ closed = false, timeout = 1, next_seq = {} }, Client)
        c.sock = dripping_sock(frame, 5)

        local op, _, payload
        for _ = 1, 100 do
            local o, _c, p, err = c:_read_frame()
            if o then op, payload = o, p break end
            assert.are.equal("timeout", err)
        end

        assert.are.equal(proto.OP_RECORD, op)
        local rec = c:_decode_record(payload)
        assert.is_not_nil(rec)
        assert.are.equal("orders", rec.topic)
        assert.are.equal(2, rec.partition)
        assert.are.equal(7, rec.offset)
        assert.are.equal("payload-value", rec.value)
    end)

    it("keeps the length prefix and body phases separate across timeouts", function()
        local frame = proto.encode_record(uuid.ZERO, "t", 1, 0, 0, "", "x")
        local c = setmetatable({ closed = false, timeout = 1, next_seq = {} }, Client)
        c.sock = dripping_sock(frame, 3)

        local op
        for _ = 1, 100 do
            local o = c:_read_frame()
            if o then op = o break end
        end
        assert.are.equal(proto.OP_RECORD, op)
    end)
end)

describe("segment_verify length-prefix bounds", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("truncates a near-2^63 length prefix instead of raising out of memory", function()
        os.execute(string.format("mkdir -p '%s'", BASE_DIR))
        local path = BASE_DIR .. "/corrupt.log"
        local f = assert(io.open(path, "wb"))
        f:write(string.pack(">I8", math.maxinteger))
        f:write(string.rep("x", 40))
        f:close()

        local ok, last_good, err = pcall(verify, path, 0)
        assert.is_true(ok)
        assert.is_nil(err)
        assert.are.equal(0, last_good)

        local g = assert(io.open(path, "rb"))
        assert.are.equal(0, g:seek("end"))
        g:close()
    end)
end)

describe("reserved internal topic prefix", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("hides __ topics from list_topics, so the cap must reject them up front", function()
        local b = assert(brk.Broker.new(BASE_DIR))
        assert.are.equal(0, #b:list_topics())
        assert(b:create_topic("__uncounted", 1))
        assert.are.equal(0, #b:list_topics())
        assert.is_not_nil(b.topic_manager.topics["__uncounted"])
    end)

    it("rejects a client-supplied __ name at the CREATE_TOPIC handler", function()
        local handlers = require("src.server.handlers")
        local sent = {}
        local conn = { send = function(_, f) sent[#sent + 1] = f end }
        local server = {
            broker     = { list_topics = function() return {} end,
                           create_topic = function()
                               error("create_topic must not be reached")
                           end },
            max_topics = 1024,
        }
        local correl = uuid.bytes()
        handlers.create_topic(server, conn, correl,
            proto.encode_create_topic(correl, "__evil", 1):sub(5 + 1 + 16))

        assert.are.equal(1, #sent)
        local op, _, payload = proto.parse_frame(sent[1]:sub(5))
        assert.are.equal(proto.OP_ERROR, op)
        local e = proto.decode_error(payload)
        assert.are.equal(proto.ERR_BAD_FRAME, e.code)
        assert.is_not_nil(e.message:find("reserved", 1, true))
    end)
end)

describe("commitlog build_index length-prefix bounds", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    it("treats a near-2^63 length prefix as a torn tail and trims it", function()
        os.execute(string.format("mkdir -p '%s'", BASE_DIR))
        local good = assert(message.serialize_message(
            message.Message.new("k", "v", 1)))
        local f = assert(io.open(BASE_DIR .. "/00000000000000000000.log", "wb"))
        f:write(good)
        f:write(string.pack(">I8", math.maxinteger))
        f:write(string.rep("x", 40))
        f:close()

        local ok, seg, err = pcall(segment_m.Segment.new, BASE_DIR, 0, 1024 * 1024)
        assert.is_true(ok)
        assert.is_nil(err)
        assert.is_not_nil(seg)
        assert.are.equal(1, seg.next_offset - seg.base_offset)
        assert.are.equal(#good, seg.position)
        local msg = seg:read_at(0)
        assert.are.equal("v", msg.value)
        seg:close()
    end)
end)

describe("abort index epoch handling", function()
    before_each(function() rmdir(BASE_DIR) end)
    after_each(function()  rmdir(BASE_DIR) end)

    local function new_index()
        os.execute(string.format("mkdir -p '%s'", BASE_DIR))
        local ix, err = AbortIndex.new(BASE_DIR)
        assert.is_nil(err)
        return ix
    end

    it("collapses a duplicate add instead of growing the file", function()
        local ix = new_index()
        assert(ix:add("orders", 1, 7, 0, 10, 20))
        assert(ix:add("orders", 1, 7, 0, 10, 20))
        assert(ix:add("orders", 1, 7, 0, 10, 20))
        assert.are.equal(1, #ix:entries("orders", 1))
    end)

    it("rejects a nil epoch at the boundary rather than storing an unmatchable entry", function()
        local ix = new_index()
        assert.has_error(function() ix:add("orders", 1, 7, nil, 10, 20) end)
        assert.are.equal(0, #ix:entries("orders", 1))
    end)

    it("still filters after reload when the sidecar carries a non-number epoch", function()
        local ix = new_index()
        assert(ix:add("orders", 1, 7, 0, 10, 20))
        assert.is_true(ix:is_aborted("orders", 1, 7, 0, 15))

        local json = require("dkjson")
        local p = BASE_DIR .. "/" .. AbortIndex.FILE_NAME
        local fh = assert(io.open(p, "wb"))
        fh:write(json.encode({ entries = { {
            topic = "orders", partition = 1, pid = 7,
            epoch = "0", first = 10, upto = 20,
        } } }))
        fh:close()

        local reloaded, err = AbortIndex.new(BASE_DIR)
        assert.is_nil(err)
        assert.are.equal(1, #reloaded:entries("orders", 1))
        assert.is_true(reloaded:is_aborted("orders", 1, 7, 0, 15))
        assert.is_false(reloaded:is_aborted("orders", 1, 7, 0, 25))
    end)
end)

describe("replica_server record bounds", function()
    it("rejects a near-2^63 length prefix as truncated", function()
        local payload = string.pack(">I8", math.maxinteger) .. string.rep("x", 40)
        local msg, err = replica_srv._apply({}, "t", 1, payload)
        assert.is_nil(msg)
        assert.are.equal("truncated record body", err)
    end)

    it("still accepts a well-formed record body", function()
        local good = assert(message.serialize_message(
            message.Message.new("k", "v", 1)))
        local _, err = replica_srv._apply(
            { broker = { get_topic = function() return nil, "nope" end } },
            "t", 1, good)
        assert.are.equal("topic: nope", err)
    end)
end)
