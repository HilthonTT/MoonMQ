local Server = require("src.server.server")

local function is_main(_arg, ...)
    local n_arg = _arg and #_arg or 0;
    if n_arg == select("#", ...) then
        for i=1,n_arg do
            if _arg[i] ~= select(i, ...) then
                print(_arg[i], "does not match", (select(i, ...)))
                return false;
            end
        end
        return true;
    end
    return false;
end

if is_main(arg, ...) then
    local srv = assert(Server.new({
        data_dir = "./data_server",
        port = 9092,
    }))
    srv:start()
end
