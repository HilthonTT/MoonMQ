local rct = require("src.server.reactor")

local M = {}

function M.read_frame(reactor, sock, max_frame, deadline)
    assert(deadline == nil or type(deadline) == 'number',
        "deadline must be a number or nil")
    assert(getmetatable(reactor) == rct, "reactor must be a Reactor instance")

    local len_bytes, err = reactor:read_exact(sock, 4, deadline)
    if not len_bytes then return nil, err end

    local frame_len = string.unpack(">I4", len_bytes)

    if frame_len > max_frame then
        return nil, string.format("frame %d exceeds max %d", frame_len, max_frame)
    end
    if frame_len == 0 then
        return "", nil
    end

    local body, berr = reactor:read_exact(sock, frame_len, deadline)
    if not body then return nil, berr end

    return body, nil
end

return M