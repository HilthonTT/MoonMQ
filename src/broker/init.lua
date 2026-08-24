local fs_m       = require("src.io.fs")
local tpm_m      = require("src.storage.topic_manager")
local topic_config = require("src.storage.topic_config")
local util_m     = require("src.core.util")
local offmgr_m   = require("src.storage.offset_manager")
local prodstate_m = require("src.storage.producer_state")
local txn_m      = require("src.broker.txn_coordinator")
local dlq_m      = require("src.broker.dlq")
local traffic_m  = require("src.metrics.traffic")
local uuid = require("src.core.uuid")

local Broker = {}
Broker.__index = Broker

-- opts (optional):
--   default_backend  storage backend for topics that don't request one
--                    ("segmented" | "commitlog"; default "segmented"). Per-topic
--                    `backend` opts and persisted sidecars still take precedence.
function Broker.new(data_dir, opts)
    assert(type(data_dir) == "string", "data dir must be a string")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end
    opts = opts or {}

    local success, err = fs_m.mkdir(data_dir)
    if not success then return nil, err end

    local topic_manager = tpm_m.new(data_dir, {
        default_backend = opts.default_backend,
    })
    local broker = setmetatable({
        id = uuid.bytes(),
        topic_manager = topic_manager,
        -- Per-partition produce/consume byte counters, feeding the
        -- autobalancer's network goals (see src/metrics/traffic.lua).
        traffic = traffic_m.new(),
    }, Broker)

    local lerr = broker:load_topics()
    if lerr then return nil, lerr end

    -- Durable consumer offsets live in an internal __consumer_offsets topic.
    -- Build the manager after user topics are loaded (load_topics will have
    -- already recreated the internal topic from disk on a restart; the manager
    -- detects that and replays it rather than re-creating). opts.offsets is an
    -- optional { num_partitions = N } passed through to first-time creation.
    local offsets, oerr = offmgr_m.OffsetManager.new(topic_manager, opts.offsets)
    if not offsets then return nil, oerr end
    broker.offsets = offsets

    -- Durable producer state (PIDs, epochs, idempotent-produce memos) lives in
    -- another internal topic, __producer_state, built the same way. This is what
    -- lets idempotent/transactional producers survive a reconnect or restart.
    local prod_state, perr =
        prodstate_m.ProducerStateManager.new(topic_manager, opts.producer_state)
    if not prod_state then return nil, perr end

    broker.producer_state = prod_state

    -- Transaction coordinator (built LAST: it uses topic_manager, offsets, and
    -- producer_state during its own crash-recovery pass, which resolves any
    -- transaction left in flight by a previous crash).
    local txn, terr = txn_m.Coordinator.new(broker, opts.transactions)
    if not txn then return nil, terr end

    broker.transactions = txn

    -- Dead-letter queue manager (in-memory attempt counters + lazy
    -- <topic>.dlq creation; see src/broker/dlq.lua). opts.dlq is an optional
    -- { suffix, max_deliveries } from the server config.
    broker.dlq = dlq_m.DlqManager.new(broker, opts.dlq)

    return broker, nil
end

function Broker:load_topics()
    local baseDir = self.topic_manager.baseDir

    local entries, err = fs_m.read_dir(baseDir)
    if not entries then
        return err
    end

    for _, name in ipairs(entries) do
        local topic_dir = fs_m.join_path(baseDir, name)

        if fs_m.is_dir(topic_dir) then
            -- Skip names we'd reject anyway. Without this, a file/dir
            -- left behind by some other tool would crash load.
            local valid = util_m.validate_topic_name(name)
            if valid then
                -- Each partition lives under topic_dir/partition-<id>/ as
                -- a directory containing one or more <base_offset>.log
                -- segment files plus a recovery-checkpoint sidecar.
                local partition_dirs, gErr = fs_m.glob(topic_dir, "^partition%-(%d+)$")
                if not partition_dirs then
                    return string.format("failed to glob partitions for topic %s: %s", name, gErr)
                end

                -- Extract partition IDs from dir names; reject gaps.
                local ids = {}
                for _, dir_path in ipairs(partition_dirs) do
                    if fs_m.is_dir(dir_path) then
                        local basename = dir_path:match("[^/\\]+$") or dir_path
                        local id = tonumber(basename:match("^partition%-(%d+)$"))
                        if id then ids[#ids + 1] = id end
                    end
                end
                table.sort(ids)

                if #ids > 0 then
                    local max_id = ids[#ids]
                    -- Verify contiguous 1..max_id.
                    for i = 1, max_id do
                        if ids[i] ~= i then
                            return string.format("topic %s has non-contiguous partition dirs (missing partition %d)", name, i)
                        end
                    end

                    -- Restore per-topic config from the sidecar. Missing
                    -- file is normal for topics created before this
                    -- feature; load() returns {} which means "use
                    -- defaults" (status quo). Malformed file is loud —
                    -- we'd rather fail loudly than silently quote-unquote-fix.
                    local opts, oerr = topic_config.load(topic_dir)
                    if not opts then
                        return string.format(
                            "failed to load topic %s config: %s", name, oerr)
                    end

                    -- SegmentedPartition.new performs its own crash recovery
                    -- (verify_file with checkpoint/clean-shutdown protocol)
                    -- when it opens the partition dir, so the broker doesn't
                    -- need a separate recovery pass here.
                    local _, cErr = self.topic_manager:create_topic(name, max_id, opts)
                    if cErr then
                        return string.format("failed to load topic %s: %s", name, cErr)
                    end
                end
            end
        end
    end

    return nil
end

-- opts (optional): per-topic config forwarded to the TopicManager. See
-- src/storage/segmentation.lua SegmentedPartition.new for supported keys.
function Broker:create_topic(name, num_partitions, opts)
    assert(type(name) == "string", "name must be a string")
    assert(type(num_partitions) == "number", "num_partitions must be a number")
    if opts ~= nil then
        assert(type(opts) == "table", "opts must be a table or nil")
    end

    local topic, err = self.topic_manager:create_topic(name, num_partitions, opts)
    if topic and self._committer_factory then
        for _, p in ipairs(topic.partitions) do
            self._committer_factory(p)
        end
    end
    return topic, err
end

-- is_internal reports whether `name` is broker-owned state rather than user
-- data (__consumer_offsets, __producer_state). The `__` prefix is already the
-- convention Broker:list_topics and tick_cleaners filter on; this puts a name
-- on it so the admin paths refuse the same set.
function Broker.is_internal(name)
    return name:sub(1, 2) == "__"
end

-- delete_topic removes a topic and everything that referenced it.
--
-- Deleting the log is the easy half. The rest of this function is the state
-- that would otherwise dangle and quietly misbehave:
--
--   * consumer groups keep a subscribed-topic table and per-member
--     assignments, so a rebalance after deletion would assign partitions of a
--     log that no longer exists. Cleared via the coordinator (installed by the
--     Server; absent in bare-broker tests, hence the nil check).
--   * committed offsets in __consumer_offsets survive independently of the
--     topic. Recreating a topic of the same name would hand every previously
--     subscribed group a stale offset into an unrelated log, so they are
--     tombstoned.
--   * DLQ attempt counters are keyed by (group, topic, partition, offset) and
--     are in-memory only, so they die with the process; they are dropped here
--     anyway so a delete/recreate cycle inside one broker lifetime starts
--     clean.
--
-- Internal topics are refused: they are broker state, and removing them out
-- from under the OffsetManager or ProducerStateManager would corrupt a running
-- broker. Returns (true, nil) or (nil, err).
function Broker:delete_topic(name)
    assert(type(name) == "string", "name must be a string")

    if Broker.is_internal(name) then
        return nil, string.format("refusing to delete internal topic '%s'", name)
    end
    if not self.topic_manager.topics[name] then
        return nil, string.format("topic %s does not exist", name)
    end

    -- Drop the topic out of live consumer groups BEFORE the log goes away, so
    -- no rebalance can observe a half-deleted topic.
    if self.group_coordinator then
        self.group_coordinator:forget_topic(name)
    end

    local ok, err = self.topic_manager:delete_topic(name)
    if not ok then return nil, err end

    -- Past the point of no return: the log is gone. Any failure below leaves
    -- stale bookkeeping rather than a live topic, so report it without
    -- pretending the delete didn't happen.
    if self.offsets then
        local _, oerr = self.offsets:delete_topic_offsets(name)
        if oerr then
            return true, string.format(
                "topic deleted, but clearing its committed offsets failed: %s", oerr)
        end
    end

    if self.dlq and self.dlq.forget_topic then
        self.dlq:forget_topic(name)
    end

    return true, nil
end

-- describe_topic returns { name, num_partitions, config } where `config` is
-- the persisted sidecar (see topic_config.lua). Only keys actually set are
-- present -- an absent key means the partition default applies.
function Broker:describe_topic(name)
    assert(type(name) == "string", "name must be a string")

    local topic, terr = self:get_topic(name)
    if not topic then return nil, terr end

    local config, cerr = self.topic_manager:config(name)
    if not config then return nil, cerr end

    return {
        name           = name,
        num_partitions = #topic.partitions,
        config         = config,
    }, nil
end

-- Config keys ALTER_TOPIC_CONFIG accepts, and how to parse each one's wire
-- value (values arrive as strings; see encode_alter_topic_config).
--
-- `backend` is deliberately absent: it selects the storage engine, which is
-- baked into the on-disk layout at creation. Changing it on a topic with data
-- would mean reinterpreting existing segments under a different format.
local ALTERABLE = {
    max_segment_size = "number",
    retention        = "number",
    cleaner_interval = "number",
    max_log_bytes    = "number",
    cleanup_policy   = "string",
}

-- Which alterable keys are also LIVE fields on an open SegmentedPartition, so
-- a change takes effect without a restart. The rest are read at open time and
-- apply on the next one.
local LIVE_PARTITION_FIELDS = {
    max_segment_size = true,
    retention        = true,
    cleaner_interval = true,
}

-- alter_topic_config merges `changes` into the topic's persisted config and
-- applies what can be applied live.
--
-- The sidecar is the durable source of truth Broker:load_topics restores from,
-- so it is written first: if the process dies between the write and the
-- in-memory update, a restart converges on the new config. The reverse order
-- would lose the change entirely.
--
-- Returns (applied_table, nil) or (nil, err).
function Broker:alter_topic_config(name, changes)
    assert(type(name) == "string", "name must be a string")
    assert(type(changes) == "table", "changes must be a table")

    if Broker.is_internal(name) then
        return nil, string.format(
            "refusing to reconfigure internal topic '%s'", name)
    end

    local topic, terr = self:get_topic(name)
    if not topic then return nil, terr end

    local parsed = {}
    for key, raw in pairs(changes) do
        local kind = ALTERABLE[key]
        if not kind then
            return nil, string.format("unknown or immutable config key '%s'", key)
        end
        if kind == "number" then
            local n = tonumber(raw)
            if not n then
                return nil, string.format(
                    "config key '%s' needs a number, got %q", key, tostring(raw))
            end
            if n <= 0 then
                return nil, string.format(
                    "config key '%s' must be positive, got %s", key, tostring(n))
            end
            -- The sidecar writes numbers with %.f and reads them back with
            -- tonumber, so a fractional value would not round-trip.
            parsed[key] = math.floor(n)
        else
            parsed[key] = tostring(raw)
        end
    end

    if next(parsed) == nil then return {}, nil end

    local merged, cerr = self.topic_manager:config(name)
    if not merged then return nil, cerr end
    for k, v in pairs(parsed) do merged[k] = v end

    local ok, serr = self.topic_manager:set_config(name, merged)
    if not ok then return nil, serr end

    for key, value in pairs(parsed) do
        if LIVE_PARTITION_FIELDS[key] then
            for _, p in ipairs(topic.partitions) do
                if p[key] ~= nil then p[key] = value end
            end
        end
    end

    return parsed, nil
end

-- attach_committer_factory installs a callback `fn(partition)` that the
-- broker invokes on every partition — both those already loaded and any
-- created later via :create_topic. The Server calls this once at startup
-- with a closure that wires the partition to its reactor for group
-- commit. Kept separate from Broker.new so the broker module doesn't
-- need to know about the reactor.
function Broker:attach_committer_factory(fn)
    assert(type(fn) == "function", "factory must be a function")
    self._committer_factory = fn
    for _, topic in pairs(self.topic_manager.topics) do
        for _, p in ipairs(topic.partitions) do
            fn(p)
        end
    end
end

-- Inverse of attach_committer_factory: detach every partition's
-- committer (draining any in-flight waiters) and forget the factory so
-- future :create_topic calls don't re-attach. Used at shutdown so the
-- reactor doesn't get fresh sleeps queued while it's stopping.
function Broker:detach_committers()
    self._committer_factory = nil
    for _, topic in pairs(self.topic_manager.topics) do
        for _, p in ipairs(topic.partitions) do
            if p.detach_committer then p:detach_committer() end
        end
    end
end

-- tick_cleaners pumps every partition's retention/compaction cleaner one
-- step. SegmentedPartition runs its cleaner as a manually-driven coroutine
-- (tick_cleaner is a cheap no-op until the cleaner is due), so nothing ages
-- out segments unless something calls this on a loop — the Server does, from
-- a periodic reactor coroutine. Backends without a cleaner (CommitLogPartition
-- cleans synchronously on roll) expose a no-op tick_cleaner, so this is
-- uniform across backends. Returns the number of partitions that actually ran
-- a cleanup pass on this tick (for logging/metrics).
function Broker:tick_cleaners()
    local ran = 0
    for name, topic in pairs(self.topic_manager.topics) do
        -- Skip internal topics (e.g. __consumer_offsets). They are
        -- compaction-oriented (one live record per key), but the segmented
        -- backend only does *time-based* retention — so ticking their cleaner
        -- would delete old offset segments and an idle group could lose its
        -- last committed offset on restart. Leaving them untouched keeps the
        -- pre-existing safe behaviour (bounded only by real compaction, which
        -- is tracked separately) rather than trading a leak for data loss.
        if name:sub(1, 2) ~= "__" then
            for _, p in ipairs(topic.partitions) do
                if p.tick_cleaner and p:tick_cleaner() then
                    ran = ran + 1
                    -- Retention just ran: drop aborted-transaction index
                    -- entries whose whole range aged out with the segments.
                    if self.transactions and p.oldest_offset then
                        self.transactions.aborts:prune(name, p.id, p:oldest_offset())
                    end
                end
            end
        end
    end
    return ran
end

-- expire_idle_producers garbage-collects durable producer identities (and
-- their idempotent-produce memos) idle for at least max_idle_ms, via
-- ProducerStateManager:expire_idle. The broker adds the one veto only it can
-- check: a producer whose transaction is still unresolved is never expired
-- (its epoch is what fences the zombie session during txn recovery). Callers
-- (the Server) layer their own veto on top via opts.is_active — e.g. pids
-- bound to a live connection. Returns (expired_count, err?).
function Broker:expire_idle_producers(max_idle_ms, opts)
    opts = opts or {}
    local caller_active = opts.is_active
    return self.producer_state:expire_idle(max_idle_ms, {
        now_ms    = opts.now_ms,
        is_active = function(name, pid)
            -- transactional_id == producer_name (see txn_coordinator.lua).
            if self.transactions and self.transactions:has_unresolved(name) then
                return true
            end
            return caller_active ~= nil and caller_active(name, pid) or false
        end,
    })
end

-- serves_partition reports whether this broker currently serves reads/writes
-- for (topic, partition). Always true in a single-broker deployment;
-- cluster-aware once the Server installs the ownership table (see
-- Server.new / src/cluster/assignments.lua). Consumers use this to stop
-- reading the stale local copy of a partition that was reassigned away.
function Broker:serves_partition(topic_name, partition_id)
    local a = self.cluster_assignments
    if not a then return true end
    return a:owned_by_self(topic_name, partition_id)
end

function Broker:get_topic(name)
    assert(type(name) == "string", "name must be a string")

    local existing_topic = self.topic_manager.topics[name]
    if not existing_topic then
        return nil, string.format("topic %s does not exist", name)
    end

    return existing_topic, nil
end

function Broker:list_topics()
    local topics = {}
    for topic_name, _ in pairs(self.topic_manager.topics) do
        -- Hide internal topics (e.g. __consumer_offsets) from the public
        -- listing and from the topic-count cap that gates CREATE_TOPIC.
        if topic_name:sub(1, 2) ~= "__" then
            topics[#topics + 1] = topic_name
        end
    end
    return topics
end

-- commit_offset / fetch_offset delegate to the OffsetManager. They sit on the
-- Broker so the per-connection Consumer can reach durable storage through the
-- broker reference it already holds, without OffsetManager plumbing of its own.
function Broker:commit_offset(group, topic, partition, offset)
    return self.offsets:commit(group, topic, partition, offset)
end

function Broker:fetch_offset(group, topic, partition)
    return self.offsets:fetch(group, topic, partition)
end

return {
    Broker = Broker,
}
