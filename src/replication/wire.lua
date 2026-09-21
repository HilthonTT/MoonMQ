local json = require("dkjson")

local M = {}

function M.encode_fetch(header, chunks)
    local head = json.encode(header)
    return string.pack(">I4", #head) .. head .. table.concat(chunks or {})
end

function M.decode_fetch(body)
    if type(body) ~= "string" or #body < 4 then
        return nil, "fetch response shorter than its header length"
    end
    local head_len = string.unpack(">I4", body)
    if head_len > #body - 4 then
        return nil, "fetch response header exceeds the body"
    end
    local header, _, err = json.decode(body:sub(5, 4 + head_len))
    if type(header) ~= "table" then
        return nil, string.format("fetch response header: %s", tostring(err))
    end

    local records = {}
    local pos = 5 + head_len
    for _, r in ipairs(type(header.records) == "table" and header.records or {}) do
        if type(r) ~= "table" or type(r[1]) ~= "string" or type(r[2]) ~= "number"
           or type(r[3]) ~= "number" or type(r[4]) ~= "number" or type(r[5]) ~= "number" then
            return nil, "malformed record descriptor"
        end
        local len = r[5]
        if len < 8 or pos + len - 1 > #body then
            return nil, "record descriptor overruns the body"
        end
        records[#records + 1] = {
            topic = r[1], partition = r[2], offset = r[3], epoch = r[4],
            bytes = body:sub(pos, pos + len - 1),
        }
        pos = pos + len
    end

    local resets = {}
    for _, r in ipairs(type(header.resets) == "table" and header.resets or {}) do
        if type(r) == "table" and type(r[1]) == "string" and type(r[2]) == "number"
           and type(r[3]) == "number" then
            resets[#resets + 1] = { topic = r[1], partition = r[2], start = r[3] }
        end
    end

    local errors = {}
    for _, r in ipairs(type(header.errors) == "table" and header.errors or {}) do
        if type(r) == "table" and type(r[1]) == "string" and type(r[2]) == "number" then
            errors[#errors + 1] = { topic = r[1], partition = r[2], reason = tostring(r[3]) }
        end
    end

    return {
        epoch   = header.epoch,
        digest  = header.digest,
        records = records,
        resets  = resets,
        errors  = errors,
    }
end

return M
