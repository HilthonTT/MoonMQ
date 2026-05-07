local Buffer = {}
Buffer.__index = Buffer

function Buffer.new()
    return setmetatable({
        chunks = {},
        n      = 0,        -- chunk count
        size   = 0,        -- total bytes
    }, Buffer)
end

function Buffer:write(s)
    self.n = self.n + 1
    self.chunks[self.n] = s
    self.size = self.size + #s
end

function Buffer:bytes()
    return table.concat(self.chunks, "", 1, self.n)
end

function Buffer:len()
    return self.size
end

function Buffer:reset()
    for i = 1, self.n do -- clear refs so chunk strings can be GC'd
        self.chunks[i] = nil
    end
    self.n = 0
    self.size = 0
end

return Buffer
