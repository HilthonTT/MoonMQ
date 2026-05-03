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

return {
    Message = Message,
    MessageHeader = MessageHeader,
}
