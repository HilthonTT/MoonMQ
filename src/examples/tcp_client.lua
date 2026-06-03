local Client = require("src.client")

local function is_main(_arg, ...)
    local n_arg = _arg and #_arg or 0
    if n_arg == select("#", ...) then
        for i = 1, n_arg do
            if _arg[i] ~= select(i, ...) then return false end
        end
        return true
    end
    return false
end

if is_main(arg, ...) then
    local client = Client.new("127.0.0.1", 9092)
    client:send("hello world from client")
    client:receive()
    client:close()
end
