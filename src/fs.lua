local IS_WINDOWS = package.config:sub(1,1) == "\\"

local function join_path(...)
    local sep   = IS_WINDOWS and "\\" or "/"
    local parts = {}
    for _, p in ipairs({ ... }) do
        p = p:gsub("[/\\]+$", "") -- strip trailing slashes (both kinds)
        if p ~= "" then
            parts[#parts+1] = p
        end
    end
    return table.concat(parts, sep)
end

local function mkdir(path)
    assert(type(path) == "string", "path must be a string")

    local cmd
    if IS_WINDOWS then
        -- Windows: mkdir creates intermediate dirs by default, errors if exists
        -- 2>nul suppresses "already exists" error
        cmd = ('mkdir "%s" 2>nul'):format(path:gsub("/", "\\"))
    else
        cmd = ("mkdir -p '%s'"):format(path)
    end

    os.execute(cmd)

    -- Verify the directory actually exists
    local f = io.open(join_path(path, ".test_probe"), "w")
    if not f then
        return false, ("failed to create directory: %s"):format(path)
    end
    f:close()
    os.remove(join_path(path, ".test_probe"))
    return true, nil
end

local function read_dir(dir)
    assert(type(dir) == "string", "dir must be a string")

    local cmd = IS_WINDOWS
        and ('dir /b "%s"'):format(dir)
        or  ("ls -1a '%s'"):format(dir)

    local pipe = io.popen(cmd)
    if not pipe then
        return nil, "failed to open pipe for read_dir"
    end

    local entries = {}
    for name in pipe:lines() do
        -- skip . and ..
        if name ~= "." and name ~= ".." then
            entries[#entries + 1] = name
        end
    end

    pipe:close()
    return entries, nil
end

--- Returns a list of file paths matching a glob pattern.
--- `dir`     - the directory to search in
--- `pattern` - a Lua pattern (e.g. "^partition%-.*%.log$")
local function glob(dir, pattern)
    assert(type(dir) == "string", "dir must be a string")
    assert(type(pattern) == "string", "pattern must be a string")

    local entries, err = read_dir(dir)
    if not entries then
        return nil, err
    end

    local matches = {}
    for _, name in ipairs(entries) do
        if name:match(pattern) then
            matches[#matches + 1] = join_path(dir, name)
        end
    end
    return matches, nil
end

--- Returns true if `path` is a directory.
local function is_dir(path)
    -- Attempt to open a probe file inside it; only works if it's a directory
    local probe = join_path(path, ".test_probe")
    local f = io.open(probe, "w")
    if f then
        f:close()
        os.remove(probe)
        return true
    end
    return false
end

return {
    IS_WINDOWS = IS_WINDOWS,
    join_path  = join_path,
    mkdir      = mkdir,
    read_dir   = read_dir,
    glob       = glob,
    is_dir     = is_dir,
}
