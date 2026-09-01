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

local VALUES = { Resource.NW_IN, Resource.NW_OUT, Resource.DISK, Resource.PARTITION_COUNT }

function Resource.name(r)
    return NAMES[r] or ("UNKNOWN(" .. tostring(r) .. ")")
end

function Resource.is_measured(r)
    return r == Resource.NW_IN or r == Resource.NW_OUT or r == Resource.DISK
end

Resource.NAMES = NAMES
Resource.VALUES = VALUES

return Resource
