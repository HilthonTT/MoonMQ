local proto = require("src.server.protocol")
local uuid  = require("src.server.uuid")

-- Strip the u32 length prefix an encode_* frame carries and hand the body
-- to parse_frame, the same way the reader coroutine does on the wire.
local function unframe(frame)
    return proto.parse_frame(frame:sub(5))
end

describe("consumer-group wire protocol", function()
    local correl = uuid.bytes()

    it("round-trips JOIN_GROUP", function()
        local frame = proto.encode_join_group(correl, "billing", "m-7",
            { "orders", "payments" })
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_JOIN_GROUP, op)
        assert.are.equal(correl, c)

        local j = assert(proto.decode_join_group(payload))
        assert.are.equal("billing", j.group_id)
        assert.are.equal("m-7", j.member_id)
        assert.are.same({ "orders", "payments" }, j.topics)
    end)

    it("round-trips JOIN_GROUP with an empty member_id (first join)", function()
        local frame = proto.encode_join_group(correl, "g", "", { "t" })
        local _, _, payload = unframe(frame)
        local j = assert(proto.decode_join_group(payload))
        assert.are.equal("", j.member_id)
        assert.are.same({ "t" }, j.topics)
    end)

    it("round-trips GROUP_ASSIGNMENT", function()
        local assignment = { orders = { 1, 2 }, payments = { 3 } }
        local frame = proto.encode_group_assignment(correl, "m-7", assignment)
        local op, c, payload = unframe(frame)
        assert.are.equal(proto.OP_GROUP_ASSIGNMENT, op)
        assert.are.equal(correl, c)

        local res = assert(proto.decode_group_assignment(payload))
        assert.are.equal("m-7", res.member_id)
        assert.are.same({ 1, 2 }, res.assignment["orders"])
        assert.are.same({ 3 }, res.assignment["payments"])
    end)

    it("round-trips an empty GROUP_ASSIGNMENT", function()
        local frame = proto.encode_group_assignment(correl, "m-7", {})
        local _, _, payload = unframe(frame)
        local res = assert(proto.decode_group_assignment(payload))
        assert.are.equal("m-7", res.member_id)
        assert.are.same({}, res.assignment)
    end)

    it("round-trips LEAVE_GROUP and GROUP_HEARTBEAT (shared body)", function()
        local lf = proto.encode_leave_group(correl, "billing", "m-7")
        local lop, _, lpayload = unframe(lf)
        assert.are.equal(proto.OP_LEAVE_GROUP, lop)
        local l = assert(proto.decode_leave_group(lpayload))
        assert.are.equal("billing", l.group_id)
        assert.are.equal("m-7", l.member_id)

        local hf = proto.encode_group_heartbeat(correl, "billing", "m-7")
        local hop, _, hpayload = unframe(hf)
        assert.are.equal(proto.OP_GROUP_HEARTBEAT, hop)
        local h = assert(proto.decode_group_heartbeat(hpayload))
        assert.are.equal("billing", h.group_id)
        assert.are.equal("m-7", h.member_id)
    end)

    it("rejects a JOIN_GROUP claiming more topics than it carries", function()
        -- group_id="g", member_id="", topic_count=5, but zero topic strings.
        local body = proto.encode_string("g") .. proto.encode_string("")
            .. string.pack(">I4", 5)
        local res, err = proto.decode_join_group(body)
        assert.is_nil(res)
        assert.is_string(err)
    end)

    it("caps the JOIN_GROUP topic count", function()
        local body = proto.encode_string("g") .. proto.encode_string("")
            .. string.pack(">I4", proto.MAX_GROUP_TOPICS + 1)
        local res, err = proto.decode_join_group(body)
        assert.is_nil(res)
        assert.matches("too many topics", err)
    end)

    it("assigns distinct opcodes and error codes to the new ops", function()
        local ops = {
            proto.OP_JOIN_GROUP, proto.OP_LEAVE_GROUP,
            proto.OP_GROUP_HEARTBEAT, proto.OP_GROUP_ASSIGNMENT,
        }
        local seen = {}
        for _, op in ipairs(ops) do
            assert.is_nil(seen[op], "duplicate opcode")
            seen[op] = true
        end
        assert.are_not.equal(proto.ERR_GROUP_MEMBER_UNKNOWN, proto.ERR_GROUP_CONFLICT)
    end)
end)
