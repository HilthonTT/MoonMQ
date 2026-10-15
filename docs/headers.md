# Record headers

Every record can carry an ordered list of named headers next to its key and
value. Use them for tracing IDs, content types and routing hints: metadata the
broker and consumers can read without parsing the payload. Duplicate names are
allowed and order is preserved, as in Kafka.

```lua
c:produce("orders", "key-1", payload, { ["trace-id"] = "abc123", ["content-type"] = "json" })

c:produce_batch("orders", {
    { key = "k1", value = "v1", headers = { { "retry", "1" }, { "retry", "2" } } },
    { key = "k2", value = "v2" },
})

for _, r in ipairs(assert(c:fetch("orders", "billing"))) do
    for _, h in ipairs(r.headers) do print(h.name, h.value) end
end
```

On produce, `headers` may be a map (encoded sorted by name) or a list of
`{ name, value }` pairs. Records from `fetch` and `next_record` always have
`headers` as a list of `{ name = , value = }`, which is empty when the record
has no headers.

## Limits

| Bound | Value |
| --- | --- |
| Headers per record | 64 |
| Name | 1–255 bytes |
| Value | up to 1 MiB (the frame size limit usually applies first) |

Header bytes count toward the produce and fetch byte quotas. Compression applies
to the value only, so headers are stored uncompressed.

## Encoding

One block format is used both on disk and on the wire:

```
u16 count | (u16 name_len | name | u32 value_len | value) * count
```

### On disk

Records with headers set `ATTR_HEADERS` (0x10) in the attrs byte. The record
header then ends with `u32 headers_len`, after the `pid`/`epoch` fields when
`ATTR_TXN` is also set. The payload becomes `headers | key | value`, so the
existing payload CRC covers the headers. A record without headers is written
byte for byte as before, so existing segments need no migration.

The flag is derived from the headers that are present each time a record is
serialized, so a record that is decoded and written again (by replication,
partition moves or compaction) keeps its headers.

### On the wire

| Frame | How headers travel |
| --- | --- |
| `PRODUCE`, `PRODUCE_IDEMPOTENT`, `RECORD` | Optional trailing block after the value; left out when there are no headers |
| `PRODUCE_BATCH` | `BATCH_FLAG_HEADERS` (0x02): every record is followed by a block |
| `FETCH` → `RECORD_BATCH` | The client sets `FETCH_FLAG_HEADERS` (0x02), and every batch entry is then followed by a block |

Decoders written before headers ignore trailing bytes, and a client that does
not set `FETCH_FLAG_HEADERS` gets the original `RECORD_BATCH` layout. Mixed
client versions keep working.

## Dead letters

A dead-lettered record keeps the original record's headers. Its provenance
still lives in the value envelope ([dlq.md](dlq.md)), so the envelope format is
unchanged.
