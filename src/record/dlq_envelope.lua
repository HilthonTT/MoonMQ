local M = {}

M.VERSION = 1

local FMT = ">Bs4I4I8I8s4I2s4s4"

function M.encode(meta)
    return string.pack(FMT, M.VERSION,
        meta.topic, meta.partition, meta.offset, meta.timestamp or 0,
        meta.group, meta.attempts, meta.reason or "", meta.value)
end

function M.decode(blob)
    if type(blob) ~= "string" or #blob < 1 then
        return nil, "empty dlq envelope"
    end
    local version = string.unpack(">B", blob, 1)
    if version ~= M.VERSION then
        return nil, string.format("unsupported dlq envelope version %d", version)
    end
    local ok, _, topic, partition, offset, timestamp, group, attempts, reason, value =
        pcall(string.unpack, FMT, blob)
    if not ok then
        return nil, "truncated dlq envelope"
    end
    return {
        topic     = topic,
        partition = partition,
        offset    = offset,
        timestamp = timestamp,
        group     = group,
        attempts  = attempts,
        reason    = reason,
        value     = value,
    }, nil
end

return M
