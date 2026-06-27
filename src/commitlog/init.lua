-- Public API for the commitlog subsystem — a segmented, append-only message
-- log modelled on jocko (https://github.com/travisjeffery/jocko).
--
-- Usage:
--   local commitlog = require("src.commitlog")
--   local message   = require("src.record.message")
--
--   local opts = commitlog.Options.new(
--       "/var/lib/moonmq/topic-a", -- path
--       64 * 1024 * 1024,          -- max_segment_bytes (0 -> 64 MiB default)
--       512 * 1024 * 1024,         -- max_log_bytes  (0/-1 -> retain all)
--       commitlog.CleanupPolicy.Delete)
--
--   local log, err = commitlog.CommitLog.new(opts)
--   local offset    = log:append_message(message.Message.new("k", "v", os.time()))
--   local msg, next = log:read_at(offset)
--   log:close()

local cl = require("src.commitlog.commitlog")

return {
    CommitLog       = cl.CommitLog,
    Options         = cl.Options,
    CleanupPolicy   = cl.CleanupPolicy,
    DeleteCleaner   = require("src.commitlog.delete_cleaner"),
    CompactCleaner  = require("src.commitlog.compact_cleaner"),
    Segment         = require("src.commitlog.segment").Segment,
    Index           = require("src.commitlog.index").Index,
}
