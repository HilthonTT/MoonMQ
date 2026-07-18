-- Reassigner — executes an autobalancer rebalance plan against the cluster.
-- This is the `execute(actions)` hook the AnomalyDetector wants: it turns each
-- MOVE/SWAP Action into an actual partition migration over the cluster
-- endpoint.
--
-- A MOVE of (topic, partition) from this broker to peer D runs:
--   1. ensure   D has the topic (created with the same partition count)
--   2. copy     stream the partition's records to D in batches
--   3. cutover  flip the local ownership entry to D — from this instant the
--               produce router forwards new records for this partition to D
--   4. drain    copy any records that landed locally during step 2 (the
--               copy loop yields between batches, so produces can interleave)
--   5. offsets  push every group's committed offset for the partition to D
--               (higher-wins there), so consumers resume in place
--   6. confirm  tell D it owns the partition (clears any stale entry there)
--
-- The cutover (step 3) is exact under the cooperative reactor: once the
-- ownership entry says D, no new local write can begin, so the drain loop
-- terminates at a stable LEO.
--
-- Boundaries (documented, not hidden):
--   * Only actions whose src is THIS broker are executable — we can't push
--     data we don't hold. Others are skipped with a warning; run the balancer
--     on the broker you want acting as controller.
--   * The destination partition must be empty: records keep their byte
--     offsets only when D appends from zero.
--   * Local data is left in place after a move — the ownership table is what
--     routes around it. Committed consumer offsets DO migrate (step 5); only
--     if the offsets push fails do groups re-consume from their configured
--     start on D.

local Action  = require("src.autobalancer.common.action")
local metrics = require("src.metrics")
local log     = require("src.log.logger").get("reassigner")

local ActionType = Action.ActionType

local DEFAULT_BATCH_BYTES = 256 * 1024

local Reassigner = {}
Reassigner.__index = Reassigner

-- opts:
--   broker       the local Broker
--   assignments  the cluster Assignments table
--   peers        map broker_id -> Peer (duck-typed; see src/cluster/peer.lua)
--   self_id      this broker's cluster id
--   reactor      optional; when present the copy loop yields between batches
--   batch_bytes  max bytes per append request (default 256 KiB)
function Reassigner.new(opts)
    return setmetatable({
        broker      = assert(opts.broker, "broker required"),
        assignments = assert(opts.assignments, "assignments required"),
        peers       = assert(opts.peers, "peers required"),
        self_id     = assert(opts.self_id, "self_id required"),
        reactor     = opts.reactor,
        batch_bytes = opts.batch_bytes or DEFAULT_BATCH_BYTES,
    }, Reassigner)
end

-- First readable offset of a partition. Retention may have cleaned early
-- segments, so this is the oldest surviving segment's base offset (0 for a
-- backend that doesn't expose segments).
local function start_offset(partition)
    local segs = partition.segments
    if type(segs) == "table" and segs[1] and type(segs[1].base_offset) == "number" then
        return segs[1].base_offset
    end
    return 0
end

-- Copy records in [from, partition.offset) to the peer in batches. Returns
-- (next_offset, nil) — the offset the copy stopped at — or (nil, err).
function Reassigner:_copy_range(peer, topic_name, partition, from)
    local msg_m = require("src.record.message")
    local off = from
    while off < partition.offset do
        -- Accumulate one batch.
        local batch, batch_bytes = {}, 0
        while off < partition.offset and batch_bytes < self.batch_bytes do
            local msg, next_offset, rerr = partition:read_message(off)
            if not msg then
                return nil, string.format("read %s/partition-%d @%d: %s",
                    topic_name, partition.id, off, tostring(rerr))
            end
            local bytes, serr = msg_m.serialize_message(msg)
            if not bytes then
                return nil, string.format("serialize @%d: %s", off, tostring(serr))
            end
            batch[#batch + 1] = bytes
            batch_bytes = batch_bytes + #bytes
            off = next_offset
        end

        local _, aerr = peer:append(topic_name, partition.id, table.concat(batch))
        if aerr then return nil, aerr end

        -- Yield so produce/fetch traffic interleaves with a long migration.
        -- New records that land here are picked up by the drain pass.
        if self.reactor then self.reactor:sleep(0) end
    end
    return off
end

-- Move one partition this broker owns to dest_id. Returns (true, nil) or
-- (nil, err).
function Reassigner:_move(topic_name, partition_id, dest_id)
    local peer = self.peers[dest_id]
    if not peer then
        return nil, string.format("unknown dest broker %q", tostring(dest_id))
    end
    if not self.assignments:owned_by_self(topic_name, partition_id) then
        return nil, string.format("%s/partition-%d is not owned by this broker",
            topic_name, partition_id)
    end

    local topic, terr = self.broker:get_topic(topic_name)
    if not topic then return nil, terr end
    local partition = topic.partitions[partition_id]
    if not partition then
        return nil, string.format("no local partition %s/partition-%d",
            topic_name, partition_id)
    end

    -- 1. ensure: same partition count so the id maps 1:1.
    local ok, err = peer:ensure_topic(topic_name, #topic.partitions)
    if not ok then return nil, err end

    -- Destination must be empty — byte offsets are only preserved when the
    -- dest appends from zero (see module comment).
    local dest_leo, derr = peer:leo(topic_name, partition_id)
    if not dest_leo then return nil, derr end
    if dest_leo ~= 0 then
        return nil, string.format(
            "dest %s already has %d bytes in %s/partition-%d; refusing to append a mix",
            dest_id, dest_leo, topic_name, partition_id)
    end

    local from = start_offset(partition)
    if from > 0 then
        log:warn("%s/partition-%d: head cleaned up to %d; dest offsets will start at 0",
            topic_name, partition_id, from)
    end

    -- 2. copy the bulk.
    local stop = metrics.timer("moonmq_reassign_duration_seconds",
        { topic = topic_name })
    local copied_to, cerr = self:_copy_range(peer, topic_name, partition, from)
    if not copied_to then stop(); return nil, cerr end

    -- 3. cutover: local ownership flips; produce routing now forwards to dest.
    local fok, ferr = self.assignments:set_owner(topic_name, partition_id, dest_id)
    if not fok then stop(); return nil, ferr end

    -- 4. drain the tail written during the copy. After the flip no new local
    -- write can begin, so partition.offset is now stable.
    local drained_to, drr = self:_copy_range(peer, topic_name, partition, copied_to)
    if not drained_to then
        -- Data exists locally but ownership already flipped — roll the flip
        -- back so the partition stays served (and produce keeps landing) here.
        self.assignments:set_owner(topic_name, partition_id, self.self_id)
        stop()
        return nil, string.format("tail drain failed (ownership rolled back): %s", drr)
    end

    -- 5. offsets: ship every group's committed offset for this partition to
    -- the new owner (higher-wins there), so consumers resume where they left
    -- off instead of restarting from their configured start position. Runs
    -- after the drain: consumers here stopped being served at the cutover, so
    -- the snapshot is complete (the COMMIT handler refuses commits for
    -- partitions this broker no longer serves). A push failure degrades to
    -- the old behaviour — data is already safe on dest — so it's loud but
    -- does not roll back the move.
    if self.broker.offsets and peer.push_offsets then
        local snapshot = self.broker.offsets:offsets_for_partition(
            topic_name, partition_id)
        if next(snapshot) ~= nil then
            local applied, oerr = peer:push_offsets(topic_name, partition_id, snapshot)
            if applied then
                log:info("migrated %d committed offset(s) for %s/partition-%d -> %s",
                    applied, topic_name, partition_id, dest_id)
            else
                log:error("offset migration for %s/partition-%d -> %s failed: %s "
                    .. "(groups resume from their configured start on dest)",
                    topic_name, partition_id, dest_id, tostring(oerr))
            end
        end
    end

    -- 6. confirm at dest.
    local cok, coerr = peer:set_owner(topic_name, partition_id, dest_id)
    stop()
    if not cok then
        log:warn("dest %s did not confirm ownership of %s/partition-%d: %s "
            .. "(routing here already forwards; dest treats unlisted partitions as its own)",
            dest_id, topic_name, partition_id, tostring(coerr))
    end

    metrics.inc("moonmq_reassign_partitions_total", 1, { dest = dest_id })
    metrics.inc("moonmq_reassign_bytes_total", drained_to - from, { dest = dest_id })
    log:info("moved %s/partition-%d -> %s (%d bytes)",
        topic_name, partition_id, dest_id, drained_to - from)
    return true
end

-- Execute a rebalance plan (array of Actions). Actions whose src is not this
-- broker are skipped (see module comment). Stops on the first failed move so
-- a broken cluster doesn't get a half-applied plan piled on top. Returns
-- (true, nil, skipped) or (nil, err, skipped).
function Reassigner:execute(actions)
    local skipped = {}
    for _, action in ipairs(actions) do
        if action.src_broker_id ~= self.self_id then
            skipped[#skipped + 1] = action
            log:warn("skipping non-local action: %s", action:pretty())
        elseif action.action_type == ActionType.MOVE then
            local ok, err = self:_move(
                action.src_topic.name, action.src_partition, action.dest_broker_id)
            if not ok then return nil, err, skipped end
        elseif action.action_type == ActionType.SWAP then
            -- A SWAP's second leg starts on the dest broker, which we can't
            -- drive from here — execute our half only.
            local ok, err = self:_move(
                action.src_topic.name, action.src_partition, action.dest_broker_id)
            if not ok then return nil, err, skipped end
            log:warn("SWAP executed as one-way MOVE (counterpart is on %s): %s",
                action.dest_broker_id, action:pretty())
        end
    end
    return true, nil, skipped
end

return Reassigner
