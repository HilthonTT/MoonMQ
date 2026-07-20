-- Dead-letter envelope — the value stored in a record on a dead-letter topic.
--
-- The v2 record format has no named-headers map (only the fixed attrs
-- bitfield), so the provenance a dead-lettered record must carry — where it
-- came from, which group gave up on it, why — is packed into the record's
-- VALUE instead. The record's KEY is the original key, untouched, so key
-- compaction and key-based partitioning keep working on the DLQ topic.
--
-- Layout (big-endian, strings u32-length-prefixed via string.pack "s4"):
--
--   u8  version (1)
--   s4  original topic
--   u32 original partition
--   u64 original offset
--   u64 original record timestamp (ms)
--   s4  consumer group that dead-lettered it
--   u16 delivery attempts consumed before giving up
--   s4  failure reason (client-supplied, may be empty)
--   s4  original value (decompressed plaintext)
--
-- The leading version byte is what lets this evolve: decode() rejects
-- versions it doesn't know rather than misparsing them.

local M = {}

M.VERSION = 1

local FMT = ">Bs4I4I8I8s4I2s4s4"

-- meta = { topic, partition, offset, timestamp, group, attempts, reason, value }
function M.encode(meta)
    return string.pack(FMT, M.VERSION,
        meta.topic, meta.partition, meta.offset, meta.timestamp or 0,
        meta.group, meta.attempts, meta.reason or "", meta.value)
end

-- decode returns ({ topic, partition, offset, timestamp, group, attempts,
-- reason, value }, nil) or (nil, err). Never raises on malformed input.
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
