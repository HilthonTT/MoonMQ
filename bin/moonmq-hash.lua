-- Generate a stored credential in one of the two formats MoonMQ accepts in
-- `Auth.PasswordHash` / `Auth.Users[].PasswordHash` (see appsettings.json).
-- The output is a single line — safe to copy directly into the JSON value.
--
-- Usage:
--     lua bin/moonmq-hash.lua <password> [iterations] [--scram]
--     make hash PASSWORD=<password> [ITER=<iterations>] [SCRAM=1]
--
-- Formats:
--     (default)  pbkdf2-sha256$<iter>$<salt>$<hash>
--                Works with both the password (AUTH) and SCRAM mechanisms.
--                <hash> is SCRAM's SaltedPassword, so anyone who reads this
--                line can authenticate as the user.
--     --scram    scram-sha-256$<iter>$<salt>$<stored_key>$<server_key>
--                Also works with both mechanisms, but stores only the derived
--                verification keys — a leaked config no longer yields a
--                working proof. Prefer this for new credentials.
--
-- `iterations` defaults to pbkdf2.DEFAULT_PBKDF2_ITERATIONS (see
-- src/core/pbkdf2.lua) so the tool and the broker never disagree. Higher
-- values raise the cost of an offline brute-force. They also slow PASSWORD
-- verification, which runs inline on the broker's reactor — SCRAM logins are
-- unaffected, because the client does that derivation.

-- Reuse the broker's own hasher so the output is guaranteed to round-trip
-- through src/server/auth.lua's parse_credential + verify_password.
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
    if not iterations or iterations < 1 or iterations ~= math.floor(iterations) then
        io.stderr:write(string.format(
            "iterations must be a positive integer (got: %q)\n", tostring(args[2])))
        os.exit(1)
    end
    -- Mirror parse_credential's ceiling so this tool can't emit a credential
    -- the broker will reject at boot ("iterations exceeds maximum").
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
