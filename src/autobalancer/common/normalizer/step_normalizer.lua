local LinearNormalizer = require("src.autobalancer.common.normalizer.linear_normalizer")

local StepNormalizer = {}
StepNormalizer.__index = StepNormalizer

function StepNormalizer.new(min, step, step_offset, step_value)
  assert(type(min) == "number", "min must be a number")
  assert(type(step) == "number", "step must be a number")
  assert(type(step_offset) == "number", "step_offset must be a number")
  assert(type(step_value) == "number", "step_value must be a number")

  if step_value < 0 or step_value > 1 then
    return nil, "step value must be in [0, 1]"
  end
  if step + step_offset <= 1 then
    return nil, "step + step_offset must be greater than 1"
  end

  local linear_normalizer, lerr = LinearNormalizer.new(min, step)
  if not linear_normalizer then return nil, lerr end

  return setmetatable({
    step = step,
    step_offset = step_offset,
    step_value = step_value,
    linear_normalizer = linear_normalizer,
  }, StepNormalizer)
end

local function delta(value, step, step_value, step_offset)
  assert(type(value) == "number", "value must be a number")
  assert(type(step) == "number", "step must be a number")
  assert(type(step_value) == "number", "step_value must be a number")
  assert(type(step_offset) == "number", "step_offset must be a number")


  return (1 - step_value) * (1 - 1 / (math.log(value) / math.log(step + step_offset)))
end

function StepNormalizer:normalize(value)
  assert(type(value) == "number", "value must be a number")

  if value <= self.step then
    return self.step_value * self.linear_normalizer:normalize(value)
  end
  return self.step_value + delta(value + self.step_offset, self.step, self.step_value, self.step_offset)
end

return StepNormalizer