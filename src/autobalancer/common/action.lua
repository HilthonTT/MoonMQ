local Topic = require("src.storage.topic")

local ActionType = {
    MOVE = 1,
    SWAP = 2,
}

local ACTION_TYPE_NAMES = {
    [ActionType.MOVE] = "MOVE",
    [ActionType.SWAP] = "SWAP",
}

local Action = {}
Action.__index = Action

-- src_partition / dest_partition identify *which* partition of the topic moves.
-- They are optional (default 0) so a caller balancing whole topics can omit
-- them, but a partition-level balancer (see src/autobalancer/model) threads them
-- through so a MOVE/SWAP names an exact topic-partition replica, mirroring
-- AutoMQ's Action-over-TopicPartition.
function Action.new(action_type, src_topic, src_broker_id, dest_broker_id, dest_topic,
        src_partition, dest_partition)
    assert(action_type == ActionType.MOVE or action_type == ActionType.SWAP,
        "action_type must be ActionType.MOVE or ActionType.SWAP")
    assert(getmetatable(src_topic) == Topic, "src_topic must be a Topic")
    assert(dest_topic == nil or getmetatable(dest_topic) == Topic,
        "dest_topic must be a Topic or nil")
    assert(type(src_broker_id) == "string", "src_broker_id must be a string")
    assert(type(dest_broker_id) == "string", "dest_broker_id must be a string")
    assert(action_type ~= ActionType.SWAP or dest_topic ~= nil,
        "SWAP requires a dest_topic")
    assert(src_partition == nil or type(src_partition) == "number",
        "src_partition must be a number or nil")
    assert(dest_partition == nil or type(dest_partition) == "number",
        "dest_partition must be a number or nil")

    return setmetatable({
        action_type    = action_type,
        src_topic      = src_topic,
        dest_topic     = dest_topic,   -- nil for MOVE
        src_broker_id  = src_broker_id,
        dest_broker_id = dest_broker_id,
        src_partition  = src_partition or 0,
        dest_partition = dest_partition or 0,
    }, Action)
end

-- The one mutable field, mirroring Java's setDestBrokerId. (You can also just
-- assign action.dest_broker_id directly; this exists to document the intent
-- that it's the only field meant to change after construction.)
function Action:set_dest_broker_id(dest_broker_id)
    assert(type(dest_broker_id) == "string", "dest_broker_id must be a string")
    self.dest_broker_id = dest_broker_id
end

-- Return the inverse action. For MOVE, the same topic moves back (broker roles
-- swapped, no dest topic). For SWAP, the two topics/brokers trade places.
function Action:undo()
    if self.action_type == ActionType.MOVE then
        return Action.new(
            self.action_type,
            self.src_topic,
            self.dest_broker_id,   -- was dest, now src
            self.src_broker_id,    -- was src, now dest
            nil,
            self.src_partition)    -- same partition moves back
    end

    -- SWAP
    return Action.new(
        self.action_type,
        self.dest_topic,           -- was dest, now src
        self.dest_broker_id,
        self.src_broker_id,
        self.src_topic,            -- was src, now dest
        self.dest_partition,       -- partitions trade with their topics
        self.src_partition)
end

function Action:equals(other)
    if self == other then return true end
    if getmetatable(other) ~= Action then return false end
    return self.action_type    == other.action_type
       and self.src_broker_id  == other.src_broker_id
       and self.dest_broker_id == other.dest_broker_id
       and self.src_topic      == other.src_topic
       and self.dest_topic     == other.dest_topic
       and self.src_partition  == other.src_partition
       and self.dest_partition == other.dest_partition
end

function Action:key()
    return table.concat({
        tostring(self.action_type),
        self.src_topic  and self.src_topic.name  or "",
        tostring(self.src_partition),
        self.dest_topic and self.dest_topic.name or "",
        tostring(self.dest_partition),
        self.src_broker_id,
        self.dest_broker_id,
    }, "|")
end

function Action:pretty()
    local src = self.src_topic and self.src_topic.name or "?"
    if self.action_type == ActionType.MOVE then
        return string.format("Action-MOVE: %s-%d@node-%s ---> node-%s",
            src, self.src_partition, self.src_broker_id, self.dest_broker_id)
    elseif self.action_type == ActionType.SWAP then
        local dst = self.dest_topic and self.dest_topic.name or "?"
        return string.format("Action-SWAP: %s-%d@node-%s <--> %s-%d@node-%s",
            src, self.src_partition, self.src_broker_id,
            dst, self.dest_partition, self.dest_broker_id)
    end
    return tostring(self)
end

function Action.__tostring(self)
    return string.format(
        "Action{src_topic=%s, dest_topic=%s, src_broker_id=%s, dest_broker_id=%s, type=%s}",
        self.src_topic  and self.src_topic.name  or "nil",
        self.dest_topic and self.dest_topic.name or "nil",
        self.src_broker_id,
        self.dest_broker_id,
        ACTION_TYPE_NAMES[self.action_type] or tostring(self.action_type))
end

Action.ActionType = ActionType

return Action