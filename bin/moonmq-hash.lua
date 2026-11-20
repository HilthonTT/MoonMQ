local auth = require("src.server.auth")

local USAGE = "usage: lua bin/moonmq-hash.lua [-|<password>] [iterations] [--scram]\n"
    .. "       with no password argument (or '-') the password is read from stdin\n"

local args = {}
local scram_format = false
local iterations_flag
local i = 1
while i <= #arg do
    local a = arg[i]
    if a == "--scram" then
        scram_format = true
    elseif a == "--iterations" then
        i = i + 1
        iterations_flag = arg[i] or ""
    elseif a:match("^%-%-iterations=") then
        iterations_flag = a:match("^%-%-iterations=(.*)$")
    elseif a == "-h" or a == "--help" then
        io.stderr:write(USAGE)
        os.exit(0)
    else
        args[#args + 1] = a
    end
    i = i + 1
end

local password
if args[1] == nil or args[1] == "-" then
    password = io.read("l")
    if password then password = password:gsub("\r$", "") end
else
    io.stderr:write("warning: passing the password as an argument exposes it "
        .. "in the process list and shell history; pipe it on stdin instead\n")
    password = args[1]
end

if not password or password == "" then
    io.stderr:write(USAGE)
    os.exit(1)
end

local iterations_arg = iterations_flag or args[2]
local iterations = auth.DEFAULT_PBKDF2_ITERATIONS
if iterations_arg and iterations_arg ~= "" then
    iterations = tonumber(iterations_arg)
    iterations = iterations and math.tointeger(iterations)
    if not iterations or iterations < 1 then
        io.stderr:write(string.format(
            "iterations must be a positive integer (got: %q)\n", tostring(iterations_arg)))
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
