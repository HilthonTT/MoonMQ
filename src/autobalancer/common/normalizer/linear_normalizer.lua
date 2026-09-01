local LinearNormalizer = {}
LinearNormalizer.__index = LinearNormalizer

function LinearNormalizer.new(min, max)
  assert(type(min) == "number", "min must be a number")
  assert(type(max) == "number", "max must be a number")

  if max <= min then
    return nil, "max must be greater than min"
  end

  return setmetatable({
    min = min,
    max = max,
  }, LinearNormalizer)
end

function LinearNormalizer:normalize(value)
  assert(type(value) == "number", "value must be a number")

  return (value - self.min) / (self.max - self.min)
end

return LinearNormalizer