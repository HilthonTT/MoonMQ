local M = {}

M.NANOSECOND  = 0.000000001
M.MICROSECOND = 0.000001
M.MILLISECOND = 0.001
M.SECOND      = 1.0
M.MINUTE      = 60.0
M.HOUR        = 3600.0

local ok_socket, socket = pcall(require, "socket")

function M.now()
    if ok_socket then return socket.gettime() end
    return os.time()
end

function M.now_ms()
    return math.floor(M.now() * 1000)
end

return M
