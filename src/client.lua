local socket = require("socket")

local Client = {}
Client.__index = Client

function Client.new(host, port)
    local tcp = assert(socket.tcp())
    tcp:connect(host, port)
    tcp:settimeout(10)

    return setmetatable({
        tcp = tcp,
    }, Client)
end

function Client:receive()
    while true do
        local s, status, partial = self.tcp:receive("*l")
        if s then
            print("Received:", s)
        elseif partial and #partial > 0 then
            print("Partial:", partial)
        end

        if status == "closed" then
            print("Status: closed")
            break
        end
    end
end

function Client:send(data)
    local len = string.pack(">I4", #data)
    return self.tcp:send(len..data)
end

function Client:close()
    self.tcp:close()
end

return Client