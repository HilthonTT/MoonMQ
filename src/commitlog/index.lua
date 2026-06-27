-- Per-segment offset index, a faithful (if mmap-free) port of jocko's
-- commitlog/index.go.
--
-- The index is a dense array of fixed-width entries, one per message in the
-- owning segment, in offset order. Each entry maps a message's *relative*
-- offset (offset - base_offset) to the byte position where that message's
-- record starts inside the segment's .log file:
--
--   ┌────────────────────┬──────────────────────┐
--   │ rel_offset (u32 BE) │ position (u32 BE)    │
--   └────────────────────┴──────────────────────┘
--
-- Storing the *relative* offset keeps every entry to 8 bytes regardless of
-- how large the absolute offset grows, exactly as jocko does. Because the
-- index is dense (one entry per message) a lookup is a direct seek to
-- rel_offset * ENTRY_WIDTH — no binary search needed for the common path,
-- though find_floor() is provided for completeness.
--
-- jocko mmaps a pre-truncated file; Lua has no portable mmap, so we use plain
-- buffered file I/O. The file is opened "r+b" (random read/write) so seek-then-
-- write lands at an exact offset and truncate_entries can shrink it — "a+b"
-- would force every write to EOF and break rebuilds.

local io_sync = require("src.io.io_sync")

local OFFSET_WIDTH   = 4
local POSITION_WIDTH = 4
local ENTRY_WIDTH    = OFFSET_WIDTH + POSITION_WIDTH  -- 8

local Index = {}
Index.__index = Index

-- new opens (creating if absent) the index file at `path`. `base_offset` is
-- the owning segment's first offset, used to translate absolute <-> relative
-- offsets. Returns (index, nil) or (nil, err).
function Index.new(path, base_offset)
    assert(type(path) == "string", "path must be a string")
    assert(type(base_offset) == "number", "base_offset must be a number")

    -- "r+b" opens an existing file for random read/write without truncating.
    -- If it doesn't exist yet, create an empty one with "w+b" (only reached
    -- when the file is genuinely new, so nothing is lost).
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
        -- `position` is the number of *bytes* currently in use (a multiple of
        -- ENTRY_WIDTH). It is the write cursor and the logical length.
        position    = size,
    }, Index), nil
end

-- count returns the number of entries currently stored.
function Index:count()
    return math.floor(self.position // ENTRY_WIDTH)
end

-- write_entry appends one (offset, position) pair. The offset must be >=
-- base_offset. Returns nil on success, err string on failure.
function Index:write_entry(offset, position)
    assert(type(offset) == "number", "offset must be a number")
    assert(type(position) == "number", "position must be a number")

    local rel = offset - self.base_offset
    if rel < 0 then
        return string.format("offset %d below base_offset %d",
                             offset, self.base_offset)
    end

    local entry = string.pack(">I4I4", rel, position)
    -- Seek to the logical end before writing — with "r+b" the cursor is not
    -- pinned to EOF, so we position it explicitly. This also makes a write
    -- right after truncate_entries land in the freed region.
    self.file:seek("set", self.position)
    local ok, werr = self.file:write(entry)
    if not ok then
        return string.format("index write failed: %s", tostring(werr))
    end
    -- Flush so a crash recovery / cross-process reader sees the entry; the
    -- bytes are tiny and Lua's buffer would otherwise hoard them.
    self.file:flush()

    self.position = self.position + ENTRY_WIDTH
    return nil
end

-- read_entry_at_log_offset reads the entry at logical index `rel` (the
-- relative offset). Returns (absolute_offset, position) or (nil, err).
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

-- lookup returns the byte position of `offset`'s record within the segment's
-- .log, or (nil, err) if the offset isn't indexed. This is the hot path used
-- by CommitLog:read_at — a single seek + read.
function Index:lookup(offset)
    assert(type(offset) == "number", "offset must be a number")
    local _, position = self:read_entry_at_log_offset(offset - self.base_offset)
    if not position then
        return nil, string.format("offset %d not in index", offset)
    end
    return position, nil
end

-- find_floor returns the position of the greatest indexed offset that is <=
-- `offset` (binary search). Useful for sparse-style "seek to at-or-before"
-- queries; the dense index makes lookup() the usual choice.
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

-- sanity_check verifies the file length is a whole number of entries and that
-- the last entry's offset is not below base_offset. Returns nil if healthy,
-- err string otherwise. Mirrors jocko's Index.SanityCheck.
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

-- truncate_entries shrinks the index to the first `n` entries, physically
-- truncating the file so stale tail bytes can't be re-read. Returns nil on
-- success, err string otherwise.
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
