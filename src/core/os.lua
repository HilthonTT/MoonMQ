local IS_WINDOWS   = package.config:sub(1,1) == "\\"

local M = {}

M.IS_WINDOWS = IS_WINDOWS

return M
