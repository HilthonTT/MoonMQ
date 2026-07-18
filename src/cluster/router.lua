-- Produce router — the piece that makes partition ownership real on the
-- produce path. The Producer consults it after picking a partition: if the
-- ownership table says a peer owns it, the serialized record is forwarded to
-- that peer's cluster endpoint instead of being written locally. Clients keep
-- talking to whichever broker they connected to; the cluster routes for them.

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

-- Returns nil when (topic, partition) is served locally, otherwise the Peer
-- that owns it. An owner with no configured Peer is surfaced as an error —
-- silently writing locally would fork the partition into two histories.
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

-- Forward one Message to the owning peer. Returns (offset, nil) — the record's
-- offset on the OWNER's log — or (nil, err).
function Router:forward(peer, topic_name, partition_id, msg)
    local bytes, serr = msg_m.serialize_message(msg)
    if not bytes then return nil, serr end

    -- forwarded=true so the owner counts this as produce traffic (NW_IN);
    -- migration batches through the same endpoint deliberately don't.
    local leo, aerr = peer:append(topic_name, partition_id, bytes, true)
    if not leo then
        return nil, string.format("forward to %s failed: %s", peer.id, aerr)
    end
    -- The peer returns its LEO after the append; the record starts at
    -- LEO - #bytes, matching what a local write_message would have returned.
    log:debug("forwarded %s/partition-%d -> %s (offset %d)",
        topic_name, partition_id, peer.id, leo - #bytes)
    return leo - #bytes
end

return Router
