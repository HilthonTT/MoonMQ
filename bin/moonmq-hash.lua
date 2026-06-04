-- Generate a PBKDF2-SHA256 password hash in the format MoonMQ expects
-- in `Auth.PasswordHash` (see appsettings.json). The output is a single
-- line — safe to copy directly into the JSON value.
--
-- Usage:
--     lua bin/moonmq-hash.lua <password> [iterations]
--     make hash PASSWORD=<password> [ITER=<iterations>]
--
-- `iterations` defaults to 10000 to match `DEFAULT_PBKDF2_ITERATIONS`
-- in src/server/auth.lua. Higher values slow auth verification
-- (acceptable on a typical login path) and proportionally raise the
-- cost of an offline brute-force.

local password = arg[1]
if not password or password == "" then
    io.stderr:write("usage: lua bin/moonmq-hash.lua <password> [iterations]\n")
    os.exit(1)
end

local iterations = 10000
if arg[2] and arg[2] ~= "" then
    iterations = tonumber(arg[2])
    if not iterations or iterations < 1 or iterations ~= math.floor(iterations) then
        io.stderr:write(string.format(
            "iterations must be a positive integer (got: %q)\n", tostring(arg[2])))
        os.exit(1)
    end
end

-- Reuse the broker's own hasher so the output is guaranteed to round-trip
-- through src/server/auth.lua's parse_hash + _verify_password.
local auth = require("src.server.auth")
print(auth.hash_password(password, { iterations = iterations }))
