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

return {
    IS_WINDOWS = IS_WINDOWS,
    join_path  = join_path,
    mkdir      = mkdir,
}
