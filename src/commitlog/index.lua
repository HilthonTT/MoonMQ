local io_sync = require("src.io.io_sync")

local OFFSET_WIDTH   = 4
local POSITION_WIDTH = 4
local ENTRY_WIDTH    = OFFSET_WIDTH + POSITION_WIDTH

local Index = {}
Index.__index = Index

function Index.new(path, base_offset)
    assert(type(path) == "string", "path must be a string")
    assert(type(base_offset) == "number", "base_offset must be a number")

    local file = io.open(path, "r+b")
    if not file then
        file = io.open(path, "w+b")
    end
    if not file then
        return nil, string.format("failed to open index %s", path)
    end

    local size = file:seek("end") or 0

    return setmetatable({
        file        = file,
        path        = path,
        base_offset = base_offset,
        position    = size,
    }, Index), nil
end

function Index:count()
    return math.floor(self.position // ENTRY_WIDTH)
end

function Index:write_entry(offset, position)
    assert(type(offset) == "number", "offset must be a number")
    assert(type(position) == "number", "position must be a number")

    local rel = offset - self.base_offset
    if rel < 0 then
        return string.format("offset %d below base_offset %d",
                             offset, self.base_offset)
    end

    local entry = string.pack(">I4I4", rel, position)
    self.file:seek("set", self.position)
    local ok, werr = self.file:write(entry)
    if not ok then
        return string.format("index write failed: %s", tostring(werr))
    end
    self.file:flush()

    self.position = self.position + ENTRY_WIDTH
    return nil
end

function Index:read_entry_at_log_offset(rel)
    assert(type(rel) == "number", "rel must be a number")
    if rel < 0 or rel >= self:count() then
        return nil, string.format("index offset %d out of range [0, %d)",
                                  rel, self:count())
    end

    self.file:seek("set", rel * ENTRY_WIDTH)
    local b = self.file:read(ENTRY_WIDTH)
    if not b or #b < ENTRY_WIDTH then
        return nil, "short index read"
    end

    local stored_rel, position = string.unpack(">I4I4", b)
    return self.base_offset + stored_rel, position
end

function Index:lookup(offset)
    assert(type(offset) == "number", "offset must be a number")
    local abs, position = self:read_entry_at_log_offset(offset - self.base_offset)
    if not abs then
        return nil, string.format("offset %d not in index", offset)
    end
    return position, nil
end

function Index:find_floor(offset)
    local n = self:count()
    local lo, hi = 0, n - 1
    local result
    while lo <= hi do
        local mid = (lo + hi) // 2
        local abs_off, position = self:read_entry_at_log_offset(mid)
        if not abs_off then break end
        if abs_off <= offset then
            result = position
            lo = mid + 1
        else
            hi = mid - 1
        end
    end
    return result
end

function Index:sanity_check()
    if self.position == 0 then
        return nil
    end
    if self.position % ENTRY_WIDTH ~= 0 then
        return "index corrupt: size is not a multiple of entry width"
    end
    local abs_off = self:read_entry_at_log_offset(self:count() - 1)
    if not abs_off or abs_off < self.base_offset then
        return "index corrupt: last entry offset below base"
    end
    return nil
end

function Index:truncate_entries(n)
    assert(type(n) == "number", "n must be a number")
    local want = n * ENTRY_WIDTH
    if want > self.position then
        return "bad truncate number"
    end
    local ok, terr = io_sync.truncate(self.file, want)
    if not ok then
        return string.format("index truncate failed: %s", tostring(terr))
    end
    self.position = want
    return nil
end

function Index:close()
    if self.file then
        self.file:flush()
        self.file:close()
        self.file = nil
    end
end

return {
    Index       = Index,
    ENTRY_WIDTH = ENTRY_WIDTH,
}
