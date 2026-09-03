local msg_m = require("src.record.message")
local log   = require("src.log.logger").get("cluster_router")

local Router = {}
Router.__index = Router

function Router.new(opts)
    return setmetatable({
        assignments = assert(opts.assignments, "assignments required"),
        peers       = assert(opts.peers, "peers required"),
        self_id     = assert(opts.self_id, "self_id required"),
    }, Router)
end

function Router:route(topic_name, partition_id)
    local owner = self.assignments:owner(topic_name, partition_id)
    if owner == self.self_id then return nil end
    local peer = self.peers[owner]
    if not peer then
        return nil, string.format(
            "%s/partition-%d is owned by %s but no such peer is configured",
            topic_name, partition_id, owner)
    end
    return peer
end

function Router:forward(peer, topic_name, partition_id, msg)
    local bytes, serr = msg_m.serialize_message(msg)
    if not bytes then return nil, serr end

    local leo, first_or_err = peer:append(topic_name, partition_id, bytes, true)
    if not leo then
        return nil, string.format("forward to %s failed: %s", peer.id, first_or_err)
    end
    -- The owner reports where the record actually landed; fall back to the
    -- LEO arithmetic only for peers that predate first_offset.
    local offset = type(first_or_err) == "number" and first_or_err or (leo - #bytes)
    log:debug("forwarded %s/partition-%d -> %s (offset %d)",
        topic_name, partition_id, peer.id, offset)
    return offset
end

return Router
