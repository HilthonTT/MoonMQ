local SingleValueSamples = {}

function SingleValueSamples.new(value, timestamp)
  assert(type(value) == "number", "value must be a number")

  return setmetatable({
    value = value,
  }, SingleValueSamples)
end

function SingleValueSamples:append(timestamp)
    assert(type(timestamp) == "number", "timestamp must be a number")
end

return SingleValueSamples