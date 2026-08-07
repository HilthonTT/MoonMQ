-- The balanceable resource dimensions, mirroring AutoMQ's Resource enum. Each
-- broker and each topic-partition replica carries a load value per resource;
-- goals read those loads to decide what to move.
--
--   NW_IN           inbound (produce) byte-rate, bytes/sec
--   NW_OUT          outbound (consume + replication) byte-rate, bytes/sec
--   DISK            on-disk bytes held by the replica
--   PARTITION_COUNT count of replicas (each replica contributes exactly 1);
--                   the metric-free dimension that always has signal
local Resource = {
    NW_IN           = 1,
    NW_OUT          = 2,
    DISK            = 3,
    PARTITION_COUNT = 4,
}

local NAMES = {
    [Resource.NW_IN]           = "NW_IN",
    [Resource.NW_OUT]          = "NW_OUT",
    [Resource.DISK]            = "DISK",
    [Resource.PARTITION_COUNT] = "PARTITION_COUNT",
}

-- Stable iteration order (enum value ascending) so snapshots/metrics are
-- deterministic across runs.
local VALUES = { Resource.NW_IN, Resource.NW_OUT, Resource.DISK, Resource.PARTITION_COUNT }

function Resource.name(r)
    return NAMES[r] or ("UNKNOWN(" .. tostring(r) .. ")")
end

-- PARTITION_COUNT is synthesized (always 1 per replica) rather than sampled from
-- a metric, so callers that feed real measurements can skip it.
function Resource.is_measured(r)
    return r == Resource.NW_IN or r == Resource.NW_OUT or r == Resource.DISK
end

Resource.NAMES = NAMES
Resource.VALUES = VALUES

return Resource
