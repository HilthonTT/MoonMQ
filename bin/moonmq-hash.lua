local auth = require("src.server.auth")

local args = {}
local scram_format = false
for i = 1, #arg do
    if arg[i] == "--scram" then
        scram_format = true
    else
        args[#args + 1] = arg[i]
    end
end

local password = args[1]
if not password or password == "" then
    io.stderr:write("usage: lua bin/moonmq-hash.lua <password> [iterations] [--scram]\n")
    os.exit(1)
end

local iterations = auth.DEFAULT_PBKDF2_ITERATIONS
if args[2] and args[2] ~= "" then
    iterations = tonumber(args[2])
    iterations = iterations and math.tointeger(iterations)
    if not iterations or iterations < 1 then
        io.stderr:write(string.format(
            "iterations must be a positive integer (got: %q)\n", tostring(args[2])))
        os.exit(1)
    end
    if iterations > auth.MAX_PBKDF2_ITERATIONS then
        io.stderr:write(string.format(
            "iterations must be at most %d (got: %d)\n",
            auth.MAX_PBKDF2_ITERATIONS, iterations))
        os.exit(1)
    end
end

print(auth.hash_password(password, {
    iterations = iterations,
    format     = scram_format and auth.FORMAT_SCRAM or auth.FORMAT_PBKDF2,
}))
