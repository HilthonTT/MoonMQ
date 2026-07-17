local M = {}

M.Version   = "dev"
M.GitCommit = "unknown"
M.BuildTime = "unknown"

local cached_os, cached_arch

local function detect_os()
	if os.getenv("OS") == "Windows_NT" or package.config:sub(1, 1) == "\\" then
		return "windows"
	end
	local ok, pipe = pcall(io.popen, "uname -s 2>/dev/null")
	if ok and pipe then
		local out = pipe:read("l")
		pipe:close()
		if out and out ~= "" then
			return out:lower()
		end
	end
	return "unknown"
end

local function detect_arch()
    local ok, pipe = pcall(io.popen, "uname -m 2>/dev/null")
    if ok and pipe then
		local out = pipe:read("l")
		pipe:close()
		if out and out ~= "" then
			return out
		end
	end
	return os.getenv("PROCESSOR_ARCHITECTURE") or "unknown"
end

local function json_escape(s)
	return (s:gsub('[%c\\"]', function(c)
		local escapes = {
			['"'] = '\\"', ['\\'] = '\\\\',
			['\b'] = '\\b', ['\f'] = '\\f',
			['\n'] = '\\n', ['\r'] = '\\r', ['\t'] = '\\t',
		}
		return escapes[c] or string.format('\\u%04x', c:byte())
	end))
end

local Info = {}
Info.__index = Info

local FIELD_ORDER = {
	{ "version" },
	{ "git_commit" },
	{ "build_time" },
	{ "lua_version" },
	{ "host_os" },
	{ "host_arch" },
	{ "build_flags", omitempty = true },
}

function Info:String()
	local lines = {
		string.format("MoonMQ version %s", self.version),
		string.format("  Commit:      %s", self.git_commit),
		string.format("  Lua version: %s", self.lua_version),
		string.format("  Platform:    %s", self.host_os),
	}
	return table.concat(lines, "\n")
end

function Info:Short()
	return self.version
end

function Info:JSON()
	local parts = {}
	for _, field in ipairs(FIELD_ORDER) do
		local name = field[1]
		local value = self[name]
		if not (field.omitempty and (value == nil or value == "")) then
			parts[#parts + 1] = string.format('  "%s": "%s"', name, json_escape(value or ""))
		end
	end
	return "{\n" .. table.concat(parts, ",\n") .. "\n}"
end

function M.GetVersionInfo()
	if not cached_os then cached_os = detect_os() end
	if not cached_arch then cached_arch = detect_arch() end

	return setmetatable({
		version     = M.Version,
		git_commit  = M.GitCommit,
		build_time  = M.BuildTime,
		lua_version = _VERSION,
		host_os     = cached_os,
		host_arch   = cached_arch,
		build_flags = "",
	}, Info)
end

function M.GetVersionString()
	return string.format("Version: %s, GitCommit: %s, BuildTime: %s",
		M.Version, M.GitCommit, M.BuildTime)
end

function M.GetVersionJSON()
	return M.GetVersionInfo():JSON()
end

function M.IsDev()
	return M.Version == "dev"
end

function M.IsDirty()
	local suffix = "-dirty"
	return M.Version:sub(-#suffix) == suffix
end

return M