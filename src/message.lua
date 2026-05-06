-- ┌────────┬────────┬─────────────────┐
-- │ Length │ Header │ Message Payload │
-- │ (8B)   │ (var)  │      (var)      │
-- └────────┴────────┴─────────────────┘

local Message = {}
Message.__index = Message

function Message.new(key, value, timestamp)
    assert(type(key) == "string", "key must be a string")
    assert(type(value) == "string", "value must be a string")
    assert(type(timestamp) == "number", "timestamp must be a number")

    return setmetatable({
        key = key,
        value = value,
        timestamp = timestamp,
    }, Message)
end

local MessageHeader = {}
MessageHeader.__index = MessageHeader

function MessageHeader.new(key_size, timestamp)
    assert(type(key_size) == "number", "key_size must be a number")
    assert(type(timestamp) == "number", "timestamp must be a number")

    return setmetatable({
        key_size = key_size,
        timestamp = timestamp,
    }, MessageHeader)
end

--- Serializes a Message into its binary wire format.
--- Returns the full byte string (length prefix + header + key + value), or nil and an error.
local function serialize_message(msg)
    assert(getmetatable(msg) == Message, "msg must be a Message instance")

    local key_size = #msg.key

    -- Header: key_size (4B) + timestamp (8B)
    local header = string.pack(">I4I8", key_size, msg.timestamp)

    -- Total size covers header + key + value (not the length prefix itself)
    local total_size = #header + key_size + #msg.value

    -- Length prefix (8B)
    local size_prefix = string.pack(">I8", total_size)

    return size_prefix .. header .. msg.key .. msg.value, nil
end

local Pool = {}
Pool.__index = Pool

-- factory: () -> object, called when the pool is empty
-- reset:   (object) -> (), called before returning to the pool (optional)
-- max:     cap on retained objects (optional; default 1024)
function Pool.new(factory, reset, max)
    return setmetatable({
        items = {},
        count = 0,
        factory = factory,
        reset = reset,
        max = max or 1024,
    }, Pool)
end

function Pool:get()
    local n = self.count

    if n > 0 then
        local item = self.items[n]
        self.items[n] = nil
        self.count = n - 1
        return item
    end

    return self.factory()
end

function Pool:put(item)
    if self.count >= self.max then
        return -- drop on the floor; let GC handle it
    end

    if self.reset then self.reset(item) end
    self.count = self.count + 1
    self.items[self.count] = item
end

return {
    Message = Message,
    MessageHeader = MessageHeader,
    serialize_message = serialize_message,
    Pool = Pool,
}
