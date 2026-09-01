local message = require("src.record.message")
local socket  = require("socket")
local brk     = require("src.broker")
local util    = require("src.core.util")
local log     = require("src.log.logger").get("producer")
local time_m = require("src.core.time")

local AckMode = {
    AckNone     = 0,
    AckLeader   = 1,
    AckAll      = -1,
}

local ERR_ACKS_ALL_UNSUPPORTED =
    "acks=all is not supported: replication is fire-and-forget without HWM. " ..
    "Use acks=0 (None) or acks=1 (Leader)."

local function rand_intn(n)
    assert(type(n) == "number", "n must be a number")

    if n <= 0 then return 0 end
    return math.random(0, n-1)
end

local function fnv32a(key)
    local FNV_OFFSET_BASIS = 2166136261
    local FNV_PRIME        = 16777619

    local hash = FNV_OFFSET_BASIS

    for i = 1, #key do
        local byte = string.byte(key, i)
        hash = hash ~ byte
        hash = (hash * FNV_PRIME) & 0xFFFFFFFF
    end

    return hash
end

local function default_partitioner(key, numPartitions)
    assert(type(key) == "string", "key must be a string")
    assert(type(numPartitions) == "number", "numPartitions must be a number")

    if #key == 0 then
        return rand_intn(numPartitions)
    end

    local hash = fnv32a(key)
    return hash % numPartitions
end

local STICKY_MAX_RECORDS = 16
local STICKY_MAX_AGE_S   = 0.010

local function pick_partition(self, topic_name, key, num_partitions)
    if num_partitions <= 0 then return 0 end

    if key ~= nil and #key > 0 then
        return self.partitioner(key, num_partitions)
    end

    local s = self.sticky[topic_name]
    local now = socket.gettime()
    if s and s.num_partitions == num_partitions
       and s.count < STICKY_MAX_RECORDS
       and (now - s.started_at) < STICKY_MAX_AGE_S
    then
        s.count = s.count + 1
        return s.partition
    end

    local prev = s and s.partition or -1
    local p = rand_intn(num_partitions)
    if num_partitions > 1 and p == prev then
        p = (p + 1) % num_partitions
    end
    self.sticky[topic_name] = {
        partition       = p,
        num_partitions  = num_partitions,
        count           = 1,
        started_at      = now,
    }
    return p
end

local Producer = {}
Producer.__index = Producer

function Producer.new(broker, acks, opts)
    assert(getmetatable(broker) == brk.Broker, "broker must be a Broker instance")
    assert(type(acks) == "number", "acks must be a number")
    opts = opts or {}
    if acks == AckMode.AckAll then
        assert(opts.replicator, ERR_ACKS_ALL_UNSUPPORTED)
    end

    return setmetatable({
        broker = broker,
        acks = acks,
        replicator = opts.replicator,
        router = opts.router,
        partitioner = default_partitioner,
        sticky = {},
    }, Producer)
end

function Producer:produce(topic_name, msg, opts)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(getmetatable(msg) == message.Message, "msg must be a Message instance")

    if self.acks == AckMode.AckAll
        and not (self.replicator and self.replicator:enabled()) then
        return -1, -1, ERR_ACKS_ALL_UNSUPPORTED
    end

    local valid, vErr = util.validate_topic_name(topic_name)
    if not valid then return -1, -1, vErr end

    if msg.timestamp == 0 then
        msg.timestamp = time_m.now_ms()
    end

    local topic, err = self.broker:get_topic(topic_name)
    if not topic then
        return -1, -1, string.format("failed to get topic: %s", err)
    end

    local num_partitions = #topic.partitions
    local partition_id   = pick_partition(self, topic_name, msg.key, num_partitions) + 1

    local partition = topic.partitions[partition_id]
    if not partition then
        return -1, -1, string.format("partition %d does not exist", partition_id)
    end

    if self.router then
        local peer, rerr = self.router:route(topic_name, partition_id)
        if rerr then return -1, -1, rerr end
        if peer then
            if opts and opts.pre_append then
                local rleo, lerr = peer:leo(topic_name, partition_id)
                if not rleo then
                    return -1, -1, string.format(
                        "leo of %s/partition-%d on peer %s: %s",
                        topic_name, partition_id, peer.id, tostring(lerr))
                end
                local pok, perr = opts.pre_append(topic_name, partition_id, nil,
                    { peer = peer, leo = rleo })
                if not pok then return -1, -1, perr end
            end
            local roffset, ferr = self.router:forward(peer, topic_name, partition_id, msg)
            if not roffset then return -1, -1, ferr end
            return partition_id, roffset, nil
        end
    end

    if opts and opts.pre_append then
        local pok, perr = opts.pre_append(topic_name, partition_id, partition)
        if not pok then return -1, -1, perr end
    end

    local offset, werr = partition:write_message(msg)
    if werr then
        return -1, -1, string.format("failed to write message: %s", werr)
    end

    if self.broker.traffic then
        self.broker.traffic:add_in(topic_name, partition_id, #msg.key + #msg.value)
    end

    local record_leo = partition.offset

    if self.replicator and self.replicator:enabled() then
        local bytes, berr = message.serialize_message(msg)
        if bytes then
            self.replicator:replicate(topic_name, partition_id, record_leo, bytes)
        else
            log:error("replication skipped: serialize failed: %s", tostring(berr))
        end
    end

    if self.acks == AckMode.AckLeader then
        local sok, serr = partition:request_sync()
        if not sok then
            return -1, -1, string.format("failed to sync partition: %s", serr)
        end
    elseif self.acks == AckMode.AckAll then
        local sok, serr = partition:request_sync()
        if not sok then
            return -1, -1, string.format("failed to sync partition: %s", serr)
        end
        local rok, rerr = self.replicator:wait_for(
            topic_name, partition_id, record_leo)
        if not rok then
            return -1, -1, rerr
        end
    end

    return partition_id, offset, nil
end

function Producer:produce_batch(topic_name, msgs, opts)
    assert(type(topic_name) == "string", "topic_name must be a string")
    assert(type(msgs) == "table" and #msgs > 0, "msgs must be a non-empty list")

    local acks = {}

    if self.acks == AckMode.AckAll
        and not (self.replicator and self.replicator:enabled()) then
        return acks, ERR_ACKS_ALL_UNSUPPORTED
    end

    local valid, vErr = util.validate_topic_name(topic_name)
    if not valid then return acks, vErr end

    local topic, err = self.broker:get_topic(topic_name)
    if not topic then
        return acks, string.format("failed to get topic: %s", err)
    end

    local num_partitions = #topic.partitions

    local touched, touched_order = {}, {}
    local enrolled = {}
    local batch_err

    for i = 1, #msgs do
        local msg = msgs[i]
        assert(getmetatable(msg) == message.Message, "msgs must contain Messages")
        if msg.timestamp == 0 then
            msg.timestamp = time_m.now_ms()
        end

        local partition_id = pick_partition(self, topic_name, msg.key, num_partitions) + 1
        local partition    = topic.partitions[partition_id]
        if not partition then
            batch_err = string.format("partition %d does not exist", partition_id)
            break
        end

        local peer
        if self.router then
            local rerr
            peer, rerr = self.router:route(topic_name, partition_id)
            if rerr then batch_err = rerr; break end
        end

        if peer then
            if opts and opts.pre_append and not enrolled[partition_id] then
                local rleo, lerr = peer:leo(topic_name, partition_id)
                if not rleo then
                    batch_err = string.format("leo of %s/partition-%d on peer %s: %s",
                        topic_name, partition_id, peer.id, tostring(lerr))
                    break
                end
                local pok, perr = opts.pre_append(topic_name, partition_id, nil,
                    { peer = peer, leo = rleo })
                if not pok then batch_err = perr; break end
                enrolled[partition_id] = true
            end
            local roffset, ferr = self.router:forward(peer, topic_name, partition_id, msg)
            if not roffset then batch_err = ferr; break end
            acks[#acks + 1] = { partition = partition_id, offset = roffset }
        else
            if opts and opts.pre_append and not enrolled[partition_id] then
                local pok, perr = opts.pre_append(topic_name, partition_id, partition)
                if not pok then batch_err = perr; break end
                enrolled[partition_id] = true
            end

            local offset, werr = partition:write_message(msg)
            if werr then
                batch_err = string.format("failed to write message: %s", werr)
                break
            end
            acks[#acks + 1] = { partition = partition_id, offset = offset }

            if self.broker.traffic then
                self.broker.traffic:add_in(topic_name, partition_id, #msg.key + #msg.value)
            end

            if not touched[partition_id] then
                touched_order[#touched_order + 1] = partition_id
                touched[partition_id] = { partition = partition }
            end
            touched[partition_id].leo = partition.offset

            if self.replicator and self.replicator:enabled() then
                local bytes, berr = message.serialize_message(msg)
                if bytes then
                    self.replicator:replicate(topic_name, partition_id,
                        partition.offset, bytes)
                else
                    log:error("replication skipped: serialize failed: %s", tostring(berr))
                end
            end
        end
    end

    if self.acks == AckMode.AckLeader or self.acks == AckMode.AckAll then
        for _, partition_id in ipairs(touched_order) do
            local sok, serr = touched[partition_id].partition:request_sync()
            if not sok then
                return acks, string.format("failed to sync partition %d: %s",
                    partition_id, tostring(serr))
            end
        end
    end

    if self.acks == AckMode.AckAll then
        for _, partition_id in ipairs(touched_order) do
            local rok, rerr = self.replicator:wait_for(
                topic_name, partition_id, touched[partition_id].leo)
            if not rok then return acks, rerr end
        end
    end

    return acks, batch_err
end

return {
    AckMode  = AckMode,
    Producer = Producer,
}
