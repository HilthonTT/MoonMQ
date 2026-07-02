-- Generate a PBKDF2-SHA256 password hash in the format MoonMQ expects
-- in `Auth.PasswordHash` (see appsettings.json). The output is a single
-- line — safe to copy directly into the JSON value.
--
-- Usage:
--     lua bin/moonmq-hash.lua <password> [iterations]
--     make hash PASSWORD=<password> [ITER=<iterations>]
--
-- `iterations` defaults to auth.DEFAULT_PBKDF2_ITERATIONS (see
-- src/server/auth.lua) so the tool and the broker never disagree. Higher
-- values slow auth verification (which runs inline on the reactor — keep
-- that in mind) and proportionally raise the cost of an offline brute-force.

-- Reuse the broker's own hasher so the output is guaranteed to round-trip
-- through src/server/auth.lua's parse_hash + _verify_password.
local auth = require("src.server.auth")

local password = arg[1]
if not password or password == "" then
    io.stderr:write("usage: lua bin/moonmq-hash.lua <password> [iterations]\n")
    os.exit(1)
end

local iterations = auth.DEFAULT_PBKDF2_ITERATIONS
if arg[2] and arg[2] ~= "" then
    iterations = tonumber(arg[2])
    if not iterations or iterations < 1 or iterations ~= math.floor(iterations) then
        io.stderr:write(string.format(
            "iterations must be a positive integer (got: %q)\n", tostring(arg[2])))
        os.exit(1)
    end
end

print(auth.hash_password(password, { iterations = iterations }))
