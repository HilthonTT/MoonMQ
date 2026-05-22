-- Length-prefixed message framer. Pure I/O concern: takes a byte stream,
-- yields complete frames; takes a payload, prefixes a u32 length.
--
-- Wire format:  [FrameLen:u32 BE] [FrameBody:FrameLen bytes]
--
-- Knows nothing about opcodes, correlation IDs, or message types. The
-- protocol module is layered on top.

local rct = require("src.server.reactor")

local M = {}

-- Reads one full frame off the socket. Returns (frame_body, nil) or
-- (nil, err).
--
-- The caller is responsible for bounding the read with `deadline` — for
-- handshake reads this should be the handshake deadline, for steady-
-- state reads the idle deadline.
function M.read_frame(reactor, sock, max_frame, deadline)
    assert(type(deadline) == 'number', "deadline must be a number")
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

-- Wraps a payload with a u32 BE length prefix.
function M.wrap_frame(payload)
    return string.pack(">I4", #payload) .. payload
end

return M