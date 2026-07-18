-- Controller epoch fencing (src/cluster/controller_fence.lua): the balance
-- loop is no longer "one controller by convention" — a superseded controller
-- is refused by peers and stops acting.

local json            = require("dkjson")
local brk_m           = require("src.broker")
local os_utils        = require("src.core.os")
local Assignments     = require("src.cluster.assignments")
local ClusterServer   = require("src.cluster.cluster_server")
local ControllerFence = require("src.cluster.controller_fence")
local BalanceLoop     = require("src.cluster.balance_loop")

local BASE = os_utils.IS_WINDOWS and "C:\\Temp\\moonmq_fence_test"
    or "/tmp/moonmq_fence_test"

local function rmdir(path)
    if os_utils.IS_WINDOWS then
        os.execute(string.format('rmdir /s /q "%s" 2>nul', path:gsub("/", "\\")))
    else
        os.execute(string.format("rm -rf '%s'", path))
    end
end

local function make_node(id)
    local dir = BASE .. "/" .. id
    local broker = assert(brk_m.Broker.new(dir))
    local assignments = assert(Assignments.new(dir, id))
    broker.cluster_assignments = assignments
    local fence = assert(ControllerFence.new(dir))
    local node = {
        id = id, dir = dir, broker = broker,
        assignments = assignments, fence = fence,
    }
    node.cluster_server = ClusterServer.new({
        reactor = {}, broker = broker, assignments = assignments,
        broker_id = id, port = 1, fence = fence,
    })
    return node
end

-- In-process Peer: only what the fencing paths need.
local function local_peer(node)
    local cs = node.cluster_server
    return {
        id = node.id,
        claim_controller = function(_, epoch, id)
            local status, out = cs:_claim(json.encode(
                { epoch = epoch, broker_id = id }))
            if status ~= 200 then return nil, tostring(out) end
            if out.accepted then return true, out.highest end
            return false, out.highest, out.reason
        end,
        set_controller = function(self_, epoch, id)
            self_.controller = epoch and { epoch = epoch, id = id } or nil
        end,
        loads = function(_)
            local status, out = cs:_loads()
            if status ~= 200 then return nil, tostring(out) end
            return out.loads
        end,
    }
end

describe("controller fence", function()
    before_each(function() rmdir(BASE) end)
    after_each(function() rmdir(BASE) end)

    it("claims monotonically and persists across reload", function()
        os.execute("mkdir -p '" .. BASE .. "/A'")
        local f = assert(ControllerFence.new(BASE .. "/A"))
        assert.are.equal(1, f:claim("A"))
        assert.are.equal(2, f:claim("A"))

        local reloaded = assert(ControllerFence.new(BASE .. "/A"))
        local epoch, claimant = reloaded:highest()
        assert.are.equal(2, epoch)
        assert.are.equal("A", claimant)
    end)

    it("observe accepts newer epochs, rejects stale and rival-equal ones", function()
        os.execute("mkdir -p '" .. BASE .. "/A'")
        local f = assert(ControllerFence.new(BASE .. "/A"))
        assert.is_true(f:observe(3, "B"))
        assert.is_true(f:observe(3, "B"))        -- same claimant, same epoch: ok
        local ok, err = f:observe(3, "C")        -- rival at same epoch: no
        assert.is_nil(ok)
        assert.matches("stale", err)
        local ok2, err2 = f:observe(2, "B")      -- older: no
        assert.is_nil(ok2)
        assert.matches("stale", err2)
        assert.is_true(f:observe(4, "C"))
    end)

    it("cluster server fences mutating requests with a stale epoch header", function()
        local n = make_node("A")
        assert.are.equal(2, n.fence:claim("A") and 2 or n.fence:highest())

        local function hdrs(epoch, id)
            return string.format(
                "POST /cluster/append HTTP/1.1\r\nX-Controller-Epoch: %d\r\nX-Controller-Id: %s\r\n\r\n",
                epoch, id)
        end
        -- fence is at epoch 2 (claimed above by A)
        local ok, err = n.cluster_server:_check_fence(hdrs(1, "B"))
        assert.is_nil(ok)
        assert.matches("stale", err)
        assert.is_true(n.cluster_server:_check_fence(hdrs(3, "B")))
        -- No headers at all: legacy request, passes.
        assert.is_true(n.cluster_server:_check_fence("POST /cluster/append HTTP/1.1\r\n\r\n"))
    end)

    it("a superseded balance loop stops acting; the newer one keeps going", function()
        local a, b = make_node("A"), make_node("B")
        a.peers = { B = local_peer(b) }
        b.peers = { A = local_peer(a) }

        local function loop_for(node)
            return BalanceLoop.new({
                broker = node.broker, assignments = node.assignments,
                peers = node.peers, self_id = node.id, fence = node.fence,
                dry_run = true, emit_metrics = false, min_valid = 1,
            })
        end

        local loop_a = loop_for(a)
        local actions_a, err_a = loop_a:tick()
        assert.is_nil(err_a)
        assert.is_table(actions_a)
        assert.are.equal(1, loop_a.controller_epoch)

        -- B starts its own loop: claims epoch 2 locally and announces to A.
        local loop_b = loop_for(b)
        local _, err_b = loop_b:tick()
        assert.is_nil(err_b)
        assert.are.equal(2, loop_b.controller_epoch)

        -- A's next pass re-announces epoch 1 to B, which knows epoch 2: fenced.
        local actions_a2, err_a2 = loop_a:tick()
        assert.are.equal(0, #actions_a2)
        assert.matches("superseded", err_a2)
        assert.is_true(loop_a.fenced)

        -- And it stays fenced without re-claiming.
        local _, err_a3 = loop_a:tick()
        assert.matches("superseded", err_a3)

        -- B is unaffected.
        local _, err_b2 = loop_b:tick()
        assert.is_nil(err_b2)
    end)
end)
