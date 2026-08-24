-- Application-level command handlers, one per opcode. Extracted from
-- server.lua so the Server stays focused on the listener, capacity
-- accounting, and lifecycle; everything protocol-command-shaped lives here.
--
-- Every handler has the same signature:
--
--     handler(server, conn, correl, payload)
--
-- and is looked up through the BY_OP table by Server:dispatch. Handlers reach
-- broker state through `server` (producer, broker, reactor, coordinator) and
-- per-connection state through `conn`.

local proto      = require("src.wire.protocol")
local Connection = require("src.server.connection")
local uuid       = require("src.core.uuid")
local consumer_m = require("src.broker.consumer")
local msg_m      = require("src.record.message")
local compression = require("src.record.compression")
local metrics    = require("src.metrics")
local log        = require("src.log.logger").get("server")
local push_log   = require("src.log.logger").get("push")

local M = {}

-- Build the Message to append for a produce request, applying compression when
-- a codec is requested. The KEY is always stored plaintext (so commitlog
-- key-compaction still matches); only the value is compressed, and the codec is
-- recorded in the record's attrs byte so read paths can decompress. Returns
-- (Message, nil) or (nil, err) when the requested codec isn't available here.
-- txn (optional) = { pid, epoch }: mark the record as transactional data
-- (ATTR_TXN + producer session in the header) so read_committed consumers can
-- attribute it to its transaction.
local function build_stored_message(codec, key, value, txn)
    codec = codec or msg_m.CODEC_NONE
    local attrs = codec & msg_m.ATTR_CODEC_MASK
    local pid, epoch
    if txn then
        attrs = attrs | msg_m.ATTR_TXN
        pid, epoch = txn.pid, txn.epoch
    end
    if codec == msg_m.CODEC_NONE then
        return msg_m.Message.new(key, value, 0, attrs, pid, epoch)
    end
    if not compression.available(codec) then
        return nil, string.format("compression codec %s unavailable on this broker",
            compression.codec_name(codec))
    end
    local stored, cerr = compression.compress(codec, value)
    if not stored then
        return nil, string.format("compress (%s) failed: %s",
            compression.codec_name(codec), cerr)
    end
    return msg_m.Message.new(key, stored, 0, attrs, pid, epoch)
end

-- Map a coordinator error "code" hint to a wire error code.
local function txn_err_code(code)
    if code == "fenced" then return proto.ERR_PRODUCER_FENCED end
    if code == "state"  then return proto.ERR_INVALID_TXN_STATE end
    return proto.ERR_INTERNAL
end

function M.hello(server, conn, correl, payload)
    local h, err = proto.decode_hello(payload)
    if not h then
        conn:close(Connection.REASON_BAD_FRAME, proto.ERR_BAD_FRAME, err)
        return
    end
    if h.version ~= proto.PROTOCOL_VERSION then
        conn:close(Connection.REASON_BAD_PROTOCOL, proto.ERR_BAD_PROTOCOL,
            string.format("expected v%d got v%d", proto.PROTOCOL_VERSION, h.version))
        return
    end
    conn:transition_to(Connection.STATE_GREETED)
    conn:send(proto.encode_welcome(correl, proto.PROTOCOL_VERSION))
end

function M.identify_client(server, conn, correl, payload)
    local i, err = proto.decode_identify_client(payload)
    if not i then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if #i.name > 128 or #i.version > 64 then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            "name (max 128) or version (max 64) too long"))
        return
    end
    conn.client_name    = i.name
    conn.client_version = i.version
    log:info("conn=%s identified client=%s/%s",
        conn.id_short, conn.client_name, conn.client_version)
    conn:send(proto.encode_identify_ack(correl,
        proto.SERVER_NAME, proto.SERVER_VERSION))
end

function M.auth(server, conn, correl, payload)
    local a, err = proto.decode_auth(payload)
    if not a then
        conn:close(Connection.REASON_BAD_FRAME, proto.ERR_BAD_FRAME, err)
        return
    end

    if not server.authenticator then
        log:warn("no authenticator configured, allowing")
        conn.username = a.username
        conn:transition_to(Connection.STATE_AUTHENTICATED)
        conn:send(proto.encode_auth_ok(correl))
        return
    end

    -- Flag the derivation so the handshake watchdog doesn't count the broker's
    -- own PBKDF2 work against the peer (see Connection:run_handshake_watchdog).
    -- pcall so a throwing verify can't leave the flag stuck on, which would
    -- disable the watchdog for this connection.
    conn.auth_in_progress = true
    local called, ok, auth_err =
        pcall(server.authenticator.verify, server.authenticator,
              a.username, a.password, conn.ip)
    conn.auth_in_progress = false
    if not called then
        log:error("conn=%s auth verify failed: %s", conn.id_short, tostring(ok))
        conn:close(Connection.REASON_AUTH_FAILED, proto.ERR_INTERNAL, "auth error")
        return
    end
    if not ok then
        -- Don't log the supplied username — it's attacker-controlled.
        conn:close(Connection.REASON_AUTH_FAILED, proto.ERR_AUTH_FAILED,
            auth_err or "auth failed")
        return
    end

    conn.username = a.username
    conn:transition_to(Connection.STATE_AUTHENTICATED)
    conn:send(proto.encode_auth_ok(correl))
end

function M.produce(server, conn, correl, payload)
    if conn.rate_limiter and not conn.rate_limiter:take(1) then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            "produce rate exceeded"))
        return
    end

    local p, err = proto.decode_produce(payload)
    if not p then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    local msg, berr = build_stored_message(p.codec, p.key, p.value)
    if not msg then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, berr))
        return
    end
    local part_id, offset, werr = server.producer:produce(p.topic, msg)
    if werr then
        local code = werr:find("does not exist", 1, true)
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn:send(proto.encode_error(correl, code, werr))
        return
    end

    metrics.inc("moonmq_produce_records_total", 1, { topic = p.topic })

    conn:send(proto.encode_produce_ack(correl, part_id, offset))
end

-- Build every Message for a batch up front, so a codec that isn't available
-- here fails the whole batch before anything is appended rather than halfway
-- through. Returns (messages, nil) or (nil, err).
local function build_batch_messages(records, codec, txn)
    local msgs = {}
    for i = 1, #records do
        local r = records[i]
        local msg, berr = build_stored_message(codec, r.key, r.value, txn)
        if not msg then return nil, berr end
        msgs[i] = msg
    end
    return msgs, nil
end

-- Read the dedup state for (pid, topic) — the durable memo for a named
-- producer, per-connection state for an ephemeral one. Returns a table with
-- last_seq (-1 when there is none), and for a memo written by a batch, the
-- base_seq/acks that let an exact duplicate batch be replayed.
local function seq_state_for(conn, ps, topic, durable)
    if durable then
        local m = ps:lookup_memo(conn.pid, topic)
        -- Only THIS session's memo counts: a reconnect bumps the epoch and
        -- restarts sequences at 0 (KIP-360), so an older session's memo must
        -- not swallow the new session's first records as retries.
        if m and (m.epoch or 0) == conn.epoch then return m end
        return { last_seq = -1 }
    end
    return conn.seq_state[topic] or { last_seq = -1 }
end

-- PRODUCE_BATCH: N records for one topic in one frame, answered by one
-- PRODUCE_BATCH_ACK carrying N (partition, offset) pairs.
--
-- The batch is not atomic — it is N ordinary appends that share a frame, a
-- dispatch, and (via Producer:produce_batch) one fsync per partition touched.
-- A failure part-way through acks the durable prefix and reports the error;
-- the client resends the tail. Use transactions when you need all-or-nothing.
--
-- Idempotent batches (flags & IDEMPOTENT) extend the single-record contract:
-- record i carries sequence base_seq + i, so the per-(pid, topic) counter
-- advances by `count`. Dedup is at BATCH granularity — an exact resend of the
-- last batch replays its memoized acks, anything else that overlaps the
-- consumed sequence space is ERR_OUT_OF_ORDER_SEQUENCE.
function M.produce_batch(server, conn, correl, payload)
    local b, err = proto.decode_produce_batch(payload)
    if not b then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    local count = #b.records

    -- One token per record: a batch is N records' worth of work, and charging
    -- it as one would make PRODUCE_BATCH a trivial bypass of the limiter.
    if conn.rate_limiter and not conn.rate_limiter:take(count) then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            "produce rate exceeded"))
        return
    end

    -- Plain (non-idempotent) batch: append and ack.
    if not b.idempotent then
        local msgs, berr = build_batch_messages(b.records, b.codec)
        if not msgs then
            conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, berr))
            return
        end
        local acks, perr = server.producer:produce_batch(b.topic, msgs)
        if perr and #acks == 0 then
            local code = perr:find("does not exist", 1, true)
                and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
            conn:send(proto.encode_error(correl, code, perr))
            return
        end
        if #acks > 0 then
            metrics.inc("moonmq_produce_records_total", #acks, { topic = b.topic })
            metrics.inc("moonmq_produce_batches_total", 1, { topic = b.topic })
        end
        conn:send(proto.encode_produce_batch_ack(correl, acks,
            perr and proto.ERR_INTERNAL or 0, perr))
        return
    end

    -- ---- Idempotent batch ------------------------------------------------
    if not conn.pid then
        conn:send(proto.encode_error(correl, proto.ERR_NO_PRODUCER_ID,
            "INIT_PRODUCER_ID required before an idempotent PRODUCE_BATCH"))
        return
    end
    if b.pid ~= conn.pid then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            string.format("pid mismatch: frame=%d conn=%d", b.pid, conn.pid)))
        return
    end
    -- The acks of an idempotent batch are persisted so a duplicate can replay
    -- them; refuse a batch whose memo would be unreasonably large. Nothing has
    -- been appended at this point, so the client can simply split and resend.
    if count > proto.MAX_IDEMPOTENT_BATCH then
        conn:send(proto.encode_error(correl, proto.ERR_BATCH_TOO_LARGE,
            string.format("idempotent batch of %d exceeds the %d-record limit; split it",
                count, proto.MAX_IDEMPOTENT_BATCH)))
        return
    end

    local ps      = server.broker.producer_state
    local durable = conn.producer_name ~= nil

    if durable then
        local cur = ps:current_epoch(conn.pid)
        if cur ~= nil and b.epoch ~= cur then
            conn:send(proto.encode_error(correl, proto.ERR_PRODUCER_FENCED,
                string.format("producer fenced: frame epoch %d != current %d",
                    b.epoch, cur)))
            return
        end
    end

    local state    = seq_state_for(conn, ps, b.topic, durable)
    local last_seq = state.last_seq
    local base     = b.base_seq
    local last     = base + count - 1

    -- Exact duplicate of the previous batch: replay its acks, append nothing.
    -- Checked before the fresh case; the two can't both hold for count >= 1.
    if state.acks and state.base_seq == base and last_seq == last then
        conn:send(proto.encode_produce_batch_ack(correl, state.acks, 0, nil))
        return
    end
    if base ~= last_seq + 1 then
        conn:send(proto.encode_error(correl, proto.ERR_OUT_OF_ORDER_SEQUENCE,
            string.format("expected base seq %d, got %d..%d (pid=%d %s)",
                last_seq + 1, base, last, conn.pid, b.topic)))
        return
    end

    local in_txn = conn.in_txn and conn.producer_name ~= nil
    local msgs, berr = build_batch_messages(b.records, b.codec,
        in_txn and { pid = conn.pid, epoch = conn.epoch } or nil)
    if not msgs then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, berr))
        return
    end

    local produce_opts
    local enrol_err, enrol_code
    if in_txn then
        produce_opts = {
            pre_append = function(topic_name, partition_id, _partition, remote)
                local pok, perr, pcode = server.broker.transactions:add_partition(
                    conn.producer_name, conn.pid, conn.epoch, topic_name, partition_id,
                    remote)
                if not pok then
                    enrol_err, enrol_code = perr, pcode
                    return nil, "failed to enrol partition in txn: " .. tostring(perr)
                end
                return true
            end,
        }
    end

    local acks, perr = server.producer:produce_batch(b.topic, msgs, produce_opts)

    if perr and #acks == 0 then
        if enrol_err then
            conn:send(proto.encode_error(correl, txn_err_code(enrol_code), perr))
            return
        end
        local code = perr:find("does not exist", 1, true)
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn:send(proto.encode_error(correl, code, perr))
        return
    end

    -- Memoize what actually landed. On a partial append the memo describes the
    -- prefix, so the client's resend of the tail starts at last_seq + 1 and is
    -- treated as fresh rather than as an out-of-order duplicate.
    local applied  = #acks
    local last_ack = acks[applied]
    local memo     = { base_seq = base, acks = acks }
    if durable then
        local ok, rerr = ps:record_produce(conn.pid, b.topic, base + applied - 1,
            last_ack.offset, last_ack.partition, conn.epoch, memo)
        if not ok then
            -- The records are durable; only the replay memo failed, so a later
            -- retry could duplicate. Surface that rather than acking clean.
            conn:send(proto.encode_produce_batch_ack(correl, acks, proto.ERR_INTERNAL,
                "produced but failed to persist producer state: " .. tostring(rerr)))
            return
        end
    else
        conn.seq_state[b.topic] = {
            last_seq       = base + applied - 1,
            last_offset    = last_ack.offset,
            last_partition = last_ack.partition,
            base_seq       = base,
            acks           = acks,
        }
    end

    metrics.inc("moonmq_produce_records_total", applied, { topic = b.topic })
    metrics.inc("moonmq_idempotent_produce_total", applied, { topic = b.topic })
    metrics.inc("moonmq_produce_batches_total", 1, { topic = b.topic })

    conn:send(proto.encode_produce_batch_ack(correl, acks,
        perr and proto.ERR_INTERNAL or 0, perr))
end

-- INIT_PRODUCER_ID: assign a producer ID (+ epoch) to this connection.
--
-- Empty producer_name → an ephemeral, session-scoped PID (today's behaviour):
-- the PID and its sequence memos live only on this connection and vanish when
-- it closes. Epoch is always 0.
--
-- Non-empty producer_name → a DURABLE producer identity backed by
-- __producer_state: the same PID is returned across reconnects/restarts and the
-- epoch is bumped each session so a stale old session is fenced. The dedup memo
-- is persisted, so an idempotent retry survives a broker restart.
function M.init_producer_id(server, conn, correl, payload)
    local req, err = proto.decode_init_producer_id(payload)
    if not req then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    local ps = server.broker.producer_state

    if req.producer_name == "" then
        local pid = ps:allocate_ephemeral()
        conn.pid           = pid
        conn.epoch         = 0
        conn.producer_name = nil
        conn.seq_state     = {}
        log:info("conn=%s assigned ephemeral producer_id=%d", conn.id_short, pid)
        conn:send(proto.encode_producer_id(correl, pid, 0))
        return
    end

    local pid, epoch, gerr = ps:get_or_create_producer(req.producer_name)
    if not pid then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL,
            gerr or "producer id allocation failed"))
        return
    end
    conn.pid           = pid
    conn.epoch         = epoch
    conn.producer_name = req.producer_name
    conn.seq_state     = {}   -- durable producers dedup via the persisted memo
    log:info("conn=%s producer_id=%d epoch=%d name=%s",
        conn.id_short, pid, epoch, req.producer_name)
    conn:send(proto.encode_producer_id(correl, pid, epoch))
end

-- PRODUCE_IDEMPOTENT: like PRODUCE, but checks (PID, topic, seq) for
-- monotonicity. Dedup contract per (PID, topic):
--   * seq == last_seq + 1  → append, return offset, update memo.
--   * seq == last_seq      → idempotent retry. DON'T append; return the
--     original offset+partition. Without this the retry would duplicate.
--   * seq <  last_seq      → ERR_OUT_OF_ORDER_SEQUENCE (stale).
--   * seq >  last_seq + 1  → ERR_OUT_OF_ORDER_SEQUENCE (gap).
-- First record's seq must be 0 (last_seq starts at -1 implicitly).
--
-- Why per-(PID, topic) and not per-(PID, topic, partition)? Two reasons:
-- (1) it lets the client track ONE seq counter per topic without
-- needing to know the broker's partition count or hash function, and
-- (2) TCP guarantees per-connection in-order delivery, so a single
-- monotonic counter across partitions is consistent. The full
-- Kafka-style per-partition seq becomes necessary only when producers
-- batch across partitions in parallel, which we don't.
function M.produce_idempotent(server, conn, correl, payload)
    if conn.rate_limiter and not conn.rate_limiter:take(1) then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            "produce rate exceeded"))
        return
    end

    if not conn.pid then
        conn:send(proto.encode_error(correl, proto.ERR_NO_PRODUCER_ID,
            "INIT_PRODUCER_ID required before PRODUCE_IDEMPOTENT"))
        return
    end

    local p, err = proto.decode_produce_idempotent(payload)
    if not p then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    if p.pid ~= conn.pid then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            string.format("pid mismatch: frame=%d conn=%d",
                p.pid, conn.pid)))
        return
    end

    local ps       = server.broker.producer_state
    local durable  = conn.producer_name ~= nil

    -- Epoch fencing (durable producers only): a stale-epoch frame means a newer
    -- session has taken over this producer identity — reject the zombie.
    if durable then
        local cur = ps:current_epoch(conn.pid)
        if cur ~= nil and p.epoch ~= cur then
            conn:send(proto.encode_error(correl, proto.ERR_PRODUCER_FENCED,
                string.format("producer fenced: frame epoch %d != current %d",
                    p.epoch, cur)))
            return
        end
    end

    -- Dedup state comes from the DURABLE memo for named producers or from
    -- in-memory per-connection state for ephemeral producers. The durable memo
    -- only applies when it was written by THIS session (same epoch): a
    -- reconnect bumps the epoch and the client restarts its sequences at 0,
    -- so an old session's memo must not swallow the new session's first
    -- records as "retries" (KIP-360 semantics). Idempotent-retry protection
    -- therefore spans a session — including a broker restart mid-session —
    -- but not a producer reconnect, which starts a fresh sequence space.
    -- A memo left by PRODUCE_BATCH describes the batch's LAST record, so this
    -- path reads it exactly like a single-record memo: seq == last_seq replays
    -- that record's ack, and the next fresh seq is last_seq + 1.
    local state = seq_state_for(conn, ps, p.topic, durable)
    local last_seq, last_offset, last_partition =
        state.last_seq, state.last_offset, state.last_partition

    if p.seq == last_seq and last_offset ~= nil then
        -- Idempotent retry: replay the original ack without re-appending.
        conn:send(proto.encode_produce_ack(correl, last_partition, last_offset))
        return
    elseif p.seq < last_seq or p.seq > last_seq + 1 then
        conn:send(proto.encode_error(correl, proto.ERR_OUT_OF_ORDER_SEQUENCE,
            string.format("expected seq %d, got %d (pid=%d %s)",
                last_seq + 1, p.seq, conn.pid, p.topic)))
        return
    end

    -- seq == last_seq + 1: fresh record. Route through the normal
    -- producer so partitioning + group-commit fsync behave identically
    -- to the non-idempotent path. The (partition, offset) we get back
    -- is what we memo so a retry can replay the same ack.
    --
    -- Inside a transaction the record is marked transactional (pid/epoch in
    -- its header) and the chosen partition is enrolled with the coordinator
    -- BEFORE the append — the enrolment captures the partition's LEO as the
    -- transaction's first offset there, which is what bounds the LSO and the
    -- aborted-range filter for read_committed (see Coordinator:add_partition).
    local in_txn = conn.in_txn and conn.producer_name ~= nil
    local msg, berr = build_stored_message(p.codec, p.key, p.value,
        in_txn and { pid = conn.pid, epoch = conn.epoch } or nil)
    if not msg then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, berr))
        return
    end
    local produce_opts
    local enrol_err, enrol_code
    if in_txn then
        produce_opts = {
            pre_append = function(topic_name, partition_id, _partition, remote)
                local pok, perr, pcode = server.broker.transactions:add_partition(
                    conn.producer_name, conn.pid, conn.epoch, topic_name, partition_id,
                    remote)
                if not pok then
                    enrol_err, enrol_code = perr, pcode
                    return nil, "failed to enrol partition in txn: " .. tostring(perr)
                end
                return true
            end,
        }
    end
    local partition_id, offset, werr =
        server.producer:produce(p.topic, msg, produce_opts)
    if werr then
        if enrol_err then
            conn:send(proto.encode_error(correl, txn_err_code(enrol_code), werr))
            return
        end
        local code = werr:find("does not exist", 1, true)
            and proto.ERR_TOPIC_MISSING or proto.ERR_INTERNAL
        conn:send(proto.encode_error(correl, code, werr))
        return
    end

    if durable then
        -- Persist the memo so a retry after a restart replays this ack. The
        -- record is already durable; if persisting the memo fails, the
        -- idempotence guarantee for THIS record is degraded (a later retry could
        -- duplicate), so surface it rather than acking as if fully durable.
        local ok, rerr = ps:record_produce(conn.pid, p.topic, p.seq,
            offset, partition_id, conn.epoch)
        if not ok then
            conn:send(proto.encode_error(correl, proto.ERR_INTERNAL,
                "produced but failed to persist producer state: " .. tostring(rerr)))
            return
        end
    else
        conn.seq_state[p.topic] = {
            last_seq       = p.seq,
            last_offset    = offset,
            last_partition = partition_id,
        }
    end

    metrics.inc("moonmq_produce_records_total", 1, { topic = p.topic })
    metrics.inc("moonmq_idempotent_produce_total", 1, { topic = p.topic })

    conn:send(proto.encode_produce_ack(correl, partition_id, offset))
end

-- isolation is the wire byte (0 = read_uncommitted, 1 = read_committed) from
-- the FETCH/SUBSCRIBE frame that creates the consumer. It is fixed at creation:
-- a later frame asking for a different isolation on the same connection is an
-- error rather than a silent switch mid-stream.
local function ensure_consumer(conn, broker, group_id, isolation)
    local iso = (isolation == proto.ISOLATION_READ_COMMITTED)
        and "read_committed" or "read_uncommitted"
    if conn.consumer then
        if conn.consumer.group_id ~= group_id then
            return nil, string.format("group_id mismatch (already in group %s)",
                conn.consumer.group_id)
        end
        if conn.consumer.isolation ~= iso then
            return nil, string.format("isolation mismatch (connection is %s)",
                conn.consumer.isolation)
        end
        return conn.consumer
    end
    conn.consumer = consumer_m.Consumer.new(broker, group_id, { isolation = iso })
    return conn.consumer
end

-- Push-mode delivery loop, one coroutine per subscribed connection. Spawned
-- by M.subscribe; runs until the connection closes or delivery fails.
local function subscriber_loop(server, conn)
    while conn.state ~= Connection.STATE_CLOSED do
        -- Re-scope to the current assignment each pass so a rebalance (another
        -- member joining/leaving) takes effect on the next poll.
        server.coordinator:apply_assignment(conn)
        -- Drain up to push_batch records per pass rather than one per
        -- partition. Frames are unchanged (one RECORD per record, which is
        -- what every subscriber already expects) — what this saves is the
        -- push_interval sleep between records on a backlogged partition.
        local records, err = conn.consumer:poll({ max_records = server.push_batch })
        if err then
            push_log:error("conn=%s poll: %s", conn.id_short, err)
            return
        end
        if records and #records > 0 then
            for i = 1, #records do
                if conn.state == Connection.STATE_CLOSED then return end
                local r = records[i]
                local frame = proto.encode_record(uuid.ZERO,
                    r.topic, r.partition, r.offset, r.timestamp, r.key, r.value)
                if not conn:send(frame) then return end
                metrics.inc("moonmq_fetch_records_total", 1, { topic = r.topic })
                server.broker.traffic:add_out(r.topic, r.partition, #r.key + #r.value)
                -- Commit only after the record is accepted by the send layer.
                -- poll() already advanced the in-memory cursor to the next
                -- offset for this partition; persist that. If conn:send had
                -- failed we'd have returned above without committing, so the
                -- record is redelivered rather than silently lost — at-least-
                -- once on the push path. (send() enqueues; a peer that dies
                -- after enqueue but before transmit still redelivers on
                -- reconnect since the offset wasn't committed until here.)
                local adv = conn.consumer.offsets[r.topic]
                    and conn.consumer.offsets[r.topic][r.partition]
                if adv then
                    local cok, cerr =
                        conn.consumer:commit_offset(r.topic, r.partition, adv)
                    if not cok then
                        push_log:error("conn=%s commit: %s", conn.id_short, cerr)
                        return
                    end
                end
            end
        else
            server.reactor:sleep(server.push_interval)
        end
    end
end

function M.subscribe(server, conn, correl, payload)
    local s, err = proto.decode_subscribe(payload)
    if not s then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if conn.mode == "pull" then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "connection already in pull mode (used FETCH)"))
        return
    end
    local consumer, cerr = ensure_consumer(conn, server.broker, s.group_id, s.isolation)
    if not consumer then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL, cerr))
        return
    end
    local sok, serr = consumer:subscribe(s.topic)
    if not sok then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            serr or "subscribe failed"))
        return
    end
    conn.subscriptions[s.topic] = true
    conn.mode = "push"
    -- Push mode commits AFTER a record is handed to the send layer (see
    -- subscriber_loop), not inside poll(). Turn off poll()'s auto-commit so a
    -- record isn't marked consumed before we've even tried to deliver it.
    consumer.auto_commit = false
    conn:send(proto.encode_ok(correl))

    if not conn.subscriber_co then
        conn.subscriber_co = server.reactor:spawn(function()
            subscriber_loop(server, conn)
        end)
    end
end

function M.fetch(server, conn, correl, payload)
    local f, err = proto.decode_fetch(payload)
    if not f then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if conn.mode == "push" then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "connection already in push mode (used SUBSCRIBE)"))
        return
    end
    local consumer, cerr = ensure_consumer(conn, server.broker, f.group_id, f.isolation)
    if not consumer then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL, cerr))
        return
    end
    if not conn.subscriptions[f.topic] then
        local sok, sberr = consumer:subscribe(f.topic)
        if not sok then
            conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
                sberr or "subscribe failed"))
            return
        end
        conn.subscriptions[f.topic] = true
    end
    conn.mode = "pull"
    -- Pull mode commits AFTER a record is handed to the send layer (below),
    -- exactly like the push path. With poll()'s auto-commit on, a batch
    -- truncated by max_records (or dropped by a failed send) would have had
    -- its offsets durably committed anyway — permanently skipping the
    -- records that were never delivered.
    consumer.auto_commit = false

    -- Restrict to this member's assigned partitions (no-op unless joined).
    server.coordinator:apply_assignment(conn)

    -- max_records is the real batch size now: poll() spreads it across the
    -- partitions this member reads instead of stopping at one record each.
    local records, perr = consumer:poll({ max_records = f.max_records })
    if perr then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, perr))
        return
    end
    if records then
        local limit = math.min(#records, f.max_records or #records)

        -- Rewind the in-memory cursor for everything we are NOT sending, so
        -- the next FETCH re-reads it. Walk BACKWARDS: a partition can appear
        -- several times in one poll now, and the correct rewind target is its
        -- FIRST undelivered record (records are collected in read order, so
        -- the lowest offset is the last assignment a backwards walk makes).
        for i = #records, limit + 1, -1 do
            local r = records[i]
            consumer.offsets[r.topic][r.partition] = r.offset
        end

        -- Deliver. Batched clients get one RECORD_BATCH frame; everyone else
        -- gets the historical one-RECORD-frame-per-record stream.
        local delivered = limit
        if f.batched then
            local batch = {}
            for i = 1, limit do batch[i] = records[i] end
            if limit > 0 and not conn:send(proto.encode_record_batch(correl, batch)) then
                -- Nothing was delivered: rewind the whole batch so the next
                -- FETCH re-reads it, and skip the commits below.
                for i = limit, 1, -1 do
                    local r = records[i]
                    consumer.offsets[r.topic][r.partition] = r.offset
                end
                return
            end
            for i = 1, limit do
                local r = records[i]
                server.broker.traffic:add_out(r.topic, r.partition, #r.key + #r.value)
            end
        else
            for i = 1, limit do
                local r = records[i]
                local frame = proto.encode_record(correl,
                    r.topic, r.partition, r.offset, r.timestamp, r.key, r.value)
                if not conn:send(frame) then
                    -- Send layer refused (connection closing): rewind this
                    -- record and everything after it — nothing from here on
                    -- was delivered, so nothing from here on gets committed.
                    for j = limit, i, -1 do
                        local rr = records[j]
                        consumer.offsets[rr.topic][rr.partition] = rr.offset
                    end
                    delivered = i - 1
                    break
                end
                server.broker.traffic:add_out(r.topic, r.partition, #r.key + #r.value)
            end
        end

        -- Commit ONCE PER PARTITION, after delivery — at-least-once, same
        -- contract as before (commit strictly after the send layer accepted
        -- the record), but one durable offset write per partition instead of
        -- one per record.
        --
        -- The committed value is read from consumer.offsets AFTER every rewind
        -- above has been applied, which is what makes it exact: a partition
        -- with undelivered records has been rewound to the first of them, and
        -- a partition that was fully delivered still holds the cursor poll()
        -- left. Either way it names the first record this group has not been
        -- handed, so nothing is skipped and only delivered records are marked
        -- consumed.
        local advanced, order = {}, {}
        for i = 1, delivered do
            local r = records[i]
            local by_topic = advanced[r.topic]
            if not by_topic then
                by_topic = {}
                advanced[r.topic] = by_topic
            end
            if by_topic[r.partition] == nil then
                order[#order + 1] = { topic = r.topic, partition = r.partition }
            end
            by_topic[r.partition] = consumer.offsets[r.topic][r.partition]
        end
        for _, k in ipairs(order) do
            local cok, commit_err = consumer:commit_offset(
                k.topic, k.partition, advanced[k.topic][k.partition])
            if not cok then
                -- Logged, not fatal: the records were delivered, and an
                -- uncommitted offset only means redelivery.
                log:error("conn=%s fetch commit %s/partition-%d: %s",
                    conn.id_short, k.topic, k.partition, commit_err)
            end
        end

        if delivered > 0 then
            metrics.inc("moonmq_fetch_records_total", delivered, { topic = f.topic })
        end
    end
    conn:send(proto.encode_ok(correl))
end

function M.commit(server, conn, correl, payload)
    local c, err = proto.decode_commit(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if not conn.consumer then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "commit requires prior subscribe/fetch"))
        return
    end
    -- Bounds-check the partition against the topic before forwarding to
    -- the consumer. Without this, a client could commit to any u32
    -- partition id, and offset persistence (OffsetManager) would store a
    -- commit for a partition that doesn't exist.
    local topic, terr = server.broker:get_topic(c.topic)
    if not topic then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            terr or "topic missing"))
        return
    end
    if c.partition < 1 or c.partition > #topic.partitions then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            string.format("partition %d out of range (1..%d)",
                c.partition, #topic.partitions)))
        return
    end
    -- Refuse commits for a partition this broker no longer serves (moved to a
    -- peer). Accepting them would land offsets in the SOURCE broker's
    -- __consumer_offsets after the migration snapshot was pushed, silently
    -- diverging from the position the new owner tracks. The client should
    -- consume — and commit — on the partition's new owner.
    if not server.broker:serves_partition(c.topic, c.partition) then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            string.format("%s/partition-%d has moved to another broker; commit there",
                c.topic, c.partition)))
        return
    end
    -- Fence commits from a connection whose group membership has lapsed. A
    -- member the coordinator evicted (heartbeat timeout) or that left still
    -- holds a live socket; without this it could keep committing and clobber
    -- the offset the partition's new owner is advancing. Non-group commits
    -- (conn.group_id unset) are unaffected.
    if conn.group_id then
        if not server.coordinator:member_alive(conn.group_id, conn.member_id) then
            conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
                "group membership lapsed; rejoin before committing"))
            return
        end
    end
    local ok, cerr = conn.consumer:commit_offset(c.topic, c.partition, c.offset)
    if not ok then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL,
            cerr or "commit failed"))
        return
    end
    conn:send(proto.encode_ok(correl))
end

-- NACK: the consumer failed to process a delivered record. Below the
-- configured maximum the group's offset is rewound to the record so it is
-- redelivered; at the maximum the record moves to <topic>.dlq and the group
-- advances past it (see src/broker/dlq.lua). The validation gauntlet is the
-- same as COMMIT's — a NACK both rewinds and commits offsets, so everything
-- that could corrupt offset state there applies here too.
function M.nack(server, conn, correl, payload)
    local n, err = proto.decode_nack(payload)
    if not n then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if not conn.consumer then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            "nack requires prior subscribe/fetch"))
        return
    end
    local topic, terr = server.broker:get_topic(n.topic)
    if not topic then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            terr or "topic missing"))
        return
    end
    if n.partition < 1 or n.partition > #topic.partitions then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            string.format("partition %d out of range (1..%d)",
                n.partition, #topic.partitions)))
        return
    end
    if not server.broker:serves_partition(n.topic, n.partition) then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_PROTOCOL,
            string.format("%s/partition-%d has moved to another broker; nack there",
                n.topic, n.partition)))
        return
    end
    if conn.group_id then
        if not server.coordinator:member_alive(conn.group_id, conn.member_id) then
            conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
                "group membership lapsed; rejoin before nacking"))
            return
        end
    end

    local group = conn.consumer.group_id
    local res, nerr = server.broker.dlq:record_failure(
        group, n.topic, n.partition, n.offset, n.reason)
    if not res then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, nerr))
        return
    end
    metrics.inc("moonmq_nack_total", 1, { topic = n.topic })

    local offsets = conn.consumer.offsets[n.topic]
    local cur = offsets and offsets[n.partition]

    if res.dead_lettered then
        -- Advance the group past the poison record — in-memory cursor and
        -- durable offset both, so neither a live poll nor a restart
        -- redelivers it. next_offset comes from the DLQ manager's read
        -- (offsets are opaque per-backend cursors; offset+1 would be wrong
        -- on the segmented backend).
        if offsets and (cur == nil or cur < res.next_offset) then
            offsets[n.partition] = res.next_offset
        end
        local cok, cerr = conn.consumer:commit_offset(
            n.topic, n.partition, res.next_offset)
        if not cok then
            -- The record is already dead-lettered; a failed advance only
            -- means redelivery of a record whose replacement exists in the
            -- DLQ. Same log-not-fail contract as the fetch path's commit.
            log:error("conn=%s nack advance %s/partition-%d: %s",
                conn.id_short, n.topic, n.partition, cerr)
        end
        metrics.inc("moonmq_dlq_records_total", 1, { topic = n.topic })
        conn:send(proto.encode_nack_ack(correl, true, res.attempts, res.dlq_topic))
        return
    end

    -- Redelivery: rewind to the failed record. Delivery already committed
    -- the offset past it (both fetch and push commit after send), so without
    -- the rewind the record would never come back.
    if offsets and (cur == nil or cur > n.offset) then
        offsets[n.partition] = n.offset
    end
    local cok, cerr = conn.consumer:commit_offset(n.topic, n.partition, n.offset)
    if not cok then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL,
            cerr or "nack rewind failed"))
        return
    end
    conn:send(proto.encode_nack_ack(correl, false, res.attempts, nil))
end

function M.create_topic(server, conn, correl, payload)
    local c, err = proto.decode_create_topic(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if c.num_partitions < 1 or c.num_partitions > 1024 then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            "num_partitions out of range (1..1024)"))
        return
    end
    -- Topic-count cap. Without this a client (authenticated or not,
    -- depending on the auth config) can CREATE_TOPIC in a loop and
    -- exhaust the broker's in-memory topic table + descriptors. We
    -- check the current count against max_topics before allocating
    -- anything on disk.
    local current = #server.broker:list_topics()
    if current >= server.max_topics then
        conn:send(proto.encode_error(correl, proto.ERR_RATE_LIMITED,
            string.format("topic limit reached (%d)", server.max_topics)))
        return
    end
    local _, terr = server.broker:create_topic(c.name, c.num_partitions)
    if terr then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, terr))
        return
    end
    metrics.set("moonmq_topic_count", current + 1)
    conn:send(proto.encode_ok(correl))
end

function M.list_topics(server, conn, correl, _payload)
    -- Bound response size. Sorting by name first so the truncation is
    -- deterministic across calls (rather than depending on table-pair
    -- iteration order). For the common case the bound is irrelevant —
    -- it only kicks in if max_topics has been raised past max_list_topics.
    local all = server.broker:list_topics()
    table.sort(all)
    if #all > server.max_list_topics then
        local truncated = {}
        for i = 1, server.max_list_topics do truncated[i] = all[i] end
        all = truncated
    end
    conn:send(proto.encode_topic_list(correl, all))
end

-- LIST_OFFSETS: report the readable offset range of every partition of a
-- topic. `earliest` is whatever retention has left behind, `latest` the offset
-- the next append will get — both are in-memory counters on the partition
-- (`.offset` and `:oldest_offset()`, which the segmented and commitlog
-- backends both expose), so this handler does no disk I/O and needs no
-- backend-specific branch.
--
-- Two further bounds ride along when they mean something, each gated by its
-- flag bit in the reply:
--
--   * high_watermark — min LEO across in-sync followers. Observability only
--     today: the read path ceilings on the log end, not on this (see the note
--     at replicator.lua:114). With no followers configured every record is as
--     replicated as it will ever be, so the log end IS the watermark; with
--     followers configured but none in sync there is no minimum to report and
--     the flag goes clear.
--   * lso — the Last Stable Offset, which unlike the watermark is a real
--     ceiling: a read_committed consumer is never handed anything above it.
--     Lag for such a consumer is lso - committed, not latest - committed.
--     On a partition with no transaction in flight this equals the log end,
--     which is a known answer, not a missing one — see below.
function M.list_offsets(server, conn, correl, payload)
    local q, err = proto.decode_list_offsets(payload)
    if not q then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    local topic, terr = server.broker:get_topic(q.topic)
    if not topic then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            terr or "topic missing"))
        return
    end

    local replicator = server.replicator
    local txns       = server.broker.transactions

    local entries = {}
    for id, partition in ipairs(topic.partitions) do
        local latest = partition.offset

        local hwm = latest
        if replicator and replicator:enabled() then
            hwm = replicator:high_watermark(q.topic, id)
        end

        -- A nil from the coordinator is NOT "unknown": it means no unresolved
        -- transaction touches this partition, so the stable point is the log
        -- end (exactly what consumer.poll falls back to). Only a broker with
        -- no coordinator at all leaves this genuinely unanswerable.
        local lso
        if txns then lso = txns:lso(q.topic, id) or latest end

        entries[#entries + 1] = {
            partition      = id,
            earliest       = partition:oldest_offset(),
            latest         = latest,
            high_watermark = hwm,
            lso            = lso,
            local_leader   = server.broker:serves_partition(q.topic, id),
        }
    end

    -- TIMESTAMP mode: resolve, per partition, the earliest offset whose record
    -- timestamp is >= the query. Both storage backends implement
    -- offset_for_timestamp (the segmented one through its sparse .timeindex,
    -- the commitlog one by scanning), so there is no backend branch here.
    --
    -- A nil answer means no record qualifies -- the partition is empty, or the
    -- query is past every record. We report `latest` with the found bit clear,
    -- which is where such a record would land: a consumer that seeks there
    -- waits for new data instead of re-reading the whole log, which is the
    -- behaviour you want and what a sentinel would have to be translated into
    -- by every client anyway.
    local for_times
    if q.mode == proto.LIST_OFFSETS_MODE_TIMESTAMP then
        for_times = {}
        for i = 1, #entries do
            local e = entries[i]
            local off = topic.partitions[e.partition]:offset_for_timestamp(q.timestamp)
            for_times[i] = {
                partition = e.partition,
                offset    = off or e.latest,
                found     = off ~= nil,
            }
        end
    end

    conn:send(proto.encode_offsets(correl, entries, for_times))
end

-- DELETE_TOPIC: remove a topic, its log, and everything referencing it (live
-- group subscriptions, committed offsets, DLQ counters -- see
-- Broker:delete_topic). Internal topics are refused.
--
-- This is the one destructive admin op, and it is deliberately unconditional:
-- there is no "only if empty" guard, because a topic you want gone is usually
-- one with data in it. The protection that does exist is the internal-topic
-- refusal, which stops a client from deleting the broker's own state.
function M.delete_topic(server, conn, correl, payload)
    local c, err = proto.decode_delete_topic(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    if server.broker.is_internal(c.name) then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_FORBIDDEN,
            string.format("cannot delete internal topic '%s'", c.name)))
        return
    end
    if not server.broker.topic_manager.topics[c.name] then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            string.format("topic %s does not exist", c.name)))
        return
    end

    local ok, derr = server.broker:delete_topic(c.name)
    if not ok then
        conn:send(proto.encode_error(correl, proto.ERR_INTERNAL, derr))
        return
    end
    -- A non-nil err alongside ok=true means the log is gone but some
    -- bookkeeping cleanup failed. The delete happened, so this is a warning,
    -- not an error reply -- telling the client it failed would invite a retry
    -- against a topic that no longer exists.
    if derr then
        log:warn("delete_topic %s: %s", c.name, derr)
    end

    metrics.set("moonmq_topic_count", #server.broker:list_topics())
    log:info("topic '%s' deleted", c.name)
    conn:send(proto.encode_ok(correl))
end

-- DESCRIBE_TOPIC: partition count plus the persisted per-topic config. Only
-- keys actually set are returned; an absent key means the partition default is
-- in force (which is why we do not helpfully fill defaults in -- "unset" and
-- "set to the current default" behave differently the day the default moves).
function M.describe_topic(server, conn, correl, payload)
    local c, err = proto.decode_describe_topic(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    local desc, derr = server.broker:describe_topic(c.name)
    if not desc then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            derr or "topic missing"))
        return
    end

    conn:send(proto.encode_topic_description(
        correl, desc.name, desc.num_partitions, desc.config))
end

-- ALTER_TOPIC_CONFIG: merge config changes into the topic's sidecar and apply
-- the live-applicable ones to open partitions (see Broker:alter_topic_config).
function M.alter_topic_config(server, conn, correl, payload)
    local c, err = proto.decode_alter_topic_config(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    if server.broker.is_internal(c.name) then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_FORBIDDEN,
            string.format("cannot reconfigure internal topic '%s'", c.name)))
        return
    end
    if not server.broker.topic_manager.topics[c.name] then
        conn:send(proto.encode_error(correl, proto.ERR_TOPIC_MISSING,
            string.format("topic %s does not exist", c.name)))
        return
    end

    local applied, aerr = server.broker:alter_topic_config(c.name, c.config)
    if not applied then
        -- A rejected key or unparseable value is the client's mistake, not a
        -- broker failure, so it gets its own code rather than ERR_INTERNAL.
        conn:send(proto.encode_error(correl, proto.ERR_INVALID_CONFIG, aerr))
        return
    end

    local keys = {}
    for k in pairs(applied) do keys[#keys + 1] = k end
    table.sort(keys)
    if #keys > 0 then
        log:info("topic '%s' config altered: %s", c.name, table.concat(keys, ","))
    end
    conn:send(proto.encode_ok(correl))
end

-- LIST_GROUPS: every group this broker knows about, live or merely holding
-- committed offsets. See GroupCoordinator:list for the cluster caveat.
function M.list_groups(server, conn, correl, _payload)
    conn:send(proto.encode_group_list(correl, server.coordinator:list()))
end

-- DESCRIBE_GROUP: members, their assignments, and the group's durable
-- committed offsets. Pairing the offsets with LIST_OFFSETS' latest/lso is what
-- makes consumer lag computable without the broker defining it.
function M.describe_group(server, conn, correl, payload)
    local c, err = proto.decode_describe_group(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    local desc, derr = server.coordinator:describe(c.group_id)
    if not desc then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MISSING,
            derr or "group missing"))
        return
    end

    conn:send(proto.encode_group_description(correl, desc))
end

-- DELETE_GROUP: drop a group and tombstone its committed offsets. Refused
-- while it still has live members (see GroupCoordinator:delete).
function M.delete_group(server, conn, correl, payload)
    local c, err = proto.decode_delete_group(payload)
    if not c then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end

    local n, derr, not_empty = server.coordinator:delete(c.group_id)
    if not n then
        conn:send(proto.encode_error(correl,
            not_empty and proto.ERR_GROUP_NOT_EMPTY or proto.ERR_GROUP_MISSING,
            derr))
        return
    end

    log:info("group '%s' deleted (%d committed offset(s) cleared)", c.group_id, n)
    conn:send(proto.encode_ok(correl))
end

-- JOIN_GROUP: register this connection as a member of a group subscribing
-- to one or more topics, and reply with the partitions assigned to it.
-- An empty member_id means "first join, assign me one" — we use the
-- connection's short id so it's stable for this connection's lifetime.
function M.join_group(server, conn, correl, payload)
    local j, err = proto.decode_join_group(payload)
    if not j then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if #j.topics == 0 then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME,
            "join_group requires at least one topic"))
        return
    end
    -- One group membership per connection: a connection already bound to a
    -- different group must LEAVE_GROUP (or reconnect) before joining another.
    if conn.group_id and conn.group_id ~= j.group_id then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_CONFLICT,
            string.format("connection already in group %s", conn.group_id)))
        return
    end

    local member_id = j.member_id
    if member_id == "" then
        member_id = conn.member_id or conn.id_short
    end

    -- The coordinator routes: locally-coordinated groups join here; in
    -- cluster mode a group hashed to a peer broker is forwarded there
    -- (membership then spans the cluster — see group_coordinator.lua).
    local assignment, jerr, jcode =
        server.coordinator:join(j.group_id, member_id, j.topics)
    if not assignment then
        local code = proto.ERR_INTERNAL
        if jcode == "limit" then
            code = proto.ERR_RATE_LIMITED
        elseif jcode == "topic" then
            code = proto.ERR_TOPIC_MISSING
        end
        conn:send(proto.encode_error(correl, code, jerr or "join failed"))
        return
    end

    conn.group_id  = j.group_id
    conn.member_id = member_id
    -- If a Consumer already exists on this connection (a prior FETCH/SUBSCRIBE
    -- created one before the client joined), scope it to the new assignment now
    -- rather than waiting for the next poll.
    server.coordinator:apply_assignment(conn)
    log:info("conn=%s joined group=%s member=%s",
        conn.id_short, j.group_id, member_id)
    conn:send(proto.encode_group_assignment(correl, member_id, assignment))
end

-- LEAVE_GROUP: voluntary departure. Survivors rebalance; an emptied group
-- collapses back to its empty state (handled inside ConsumerGroup:leave).
function M.leave_group(server, conn, correl, payload)
    local l, err = proto.decode_leave_group(payload)
    if not l then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if conn.group_id ~= l.group_id then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
            "not a member of this group"))
        return
    end

    local ok = server.coordinator:leave(l.group_id, conn.member_id)
    if not ok then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
            "not a member of this group"))
        return
    end
    log:info("conn=%s left group=%s member=%s",
        conn.id_short, l.group_id, conn.member_id)
    conn.group_id  = nil
    conn.member_id = nil
    conn:send(proto.encode_ok(correl))
end

-- GROUP_HEARTBEAT: renew the member's lease so the reaper doesn't evict it.
-- A heartbeat for a member the coordinator has already reaped (or never
-- saw) returns GROUP_MEMBER_UNKNOWN, signalling the client to re-JOIN_GROUP.
function M.group_heartbeat(server, conn, correl, payload)
    local h, err = proto.decode_group_heartbeat(payload)
    if not h then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, err))
        return
    end
    if conn.group_id ~= h.group_id then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
            "not a member of this group"))
        return
    end

    -- Forwarded members refresh their cached assignment as a side effect
    -- (the coordinator's heartbeat response carries the current one), so a
    -- rebalance elsewhere in the cluster lands here within one heartbeat.
    local ok, herr = server.coordinator:heartbeat(h.group_id, conn.member_id)
    if not ok then
        conn:send(proto.encode_error(correl, proto.ERR_GROUP_MEMBER_UNKNOWN,
            herr or "unknown member"))
        return
    end
    conn:send(proto.encode_ok(correl))
end

-- BEGIN_TXN: start a transaction on this connection. Requires a durable
-- producer identity (a producer_name was given to INIT_PRODUCER_ID) so the
-- coordinator has a stable transactional_id + epoch to fence on.
function M.begin_txn(server, conn, correl, _payload)
    if not conn.producer_name then
        conn:send(proto.encode_error(correl, proto.ERR_INVALID_TXN_STATE,
            "transactions require a durable producer (producer_name in INIT_PRODUCER_ID)"))
        return
    end
    local ok, err, code = server.broker.transactions:begin(
        conn.producer_name, conn.pid, conn.epoch)
    if not ok then
        conn:send(proto.encode_error(correl, txn_err_code(code), err))
        return
    end
    conn.in_txn = true
    log:info("conn=%s began txn=%s epoch=%d", conn.id_short, conn.producer_name, conn.epoch)
    conn:send(proto.encode_ok(correl))
end

-- END_TXN: commit or abort the in-progress transaction. On commit the
-- coordinator writes COMMIT markers to every participant partition and applies
-- buffered offset commits atomically; on abort it writes ABORT markers and drops
-- the buffered offsets.
function M.end_txn(server, conn, correl, payload)
    if not conn.in_txn or not conn.producer_name then
        conn:send(proto.encode_error(correl, proto.ERR_INVALID_TXN_STATE,
            "no transaction in progress"))
        return
    end
    local e, derr = proto.decode_end_txn(payload)
    if not e then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, derr))
        return
    end
    local ok, err, code = server.broker.transactions:end_txn(
        conn.producer_name, conn.pid, conn.epoch, e.commit)
    if not ok then
        conn:send(proto.encode_error(correl, txn_err_code(code), err))
        return
    end
    conn.in_txn = false
    log:info("conn=%s %s txn=%s", conn.id_short,
        e.commit and "committed" or "aborted", conn.producer_name)
    conn:send(proto.encode_ok(correl))
end

-- TXN_OFFSET_COMMIT: buffer consumer offsets into the transaction. They are
-- persisted only when the txn commits — the consume-transform-produce pattern's
-- exactly-once handoff.
function M.txn_offset_commit(server, conn, correl, payload)
    if not conn.in_txn or not conn.producer_name then
        conn:send(proto.encode_error(correl, proto.ERR_INVALID_TXN_STATE,
            "no transaction in progress"))
        return
    end
    local t, derr = proto.decode_txn_offset_commit(payload)
    if not t then
        conn:send(proto.encode_error(correl, proto.ERR_BAD_FRAME, derr))
        return
    end
    local ok, err, code = server.broker.transactions:add_offsets(
        conn.producer_name, conn.pid, conn.epoch, t.group, t.offsets)
    if not ok then
        conn:send(proto.encode_error(correl, txn_err_code(code), err))
        return
    end
    conn:send(proto.encode_ok(correl))
end

function M.goodbye(_server, conn, _correl, _payload)
    conn:close(Connection.REASON_CLIENT_GOODBYE)
end

-- Opcode → handler. Server:dispatch resolves through this table; an opcode
-- absent here is answered with ERR_UNKNOWN_OP.
M.BY_OP = {
    [proto.OP_HELLO]              = M.hello,
    [proto.OP_AUTH]               = M.auth,
    [proto.OP_IDENTIFY_CLIENT]    = M.identify_client,
    [proto.OP_PRODUCE]            = M.produce,
    [proto.OP_PRODUCE_BATCH]      = M.produce_batch,
    [proto.OP_INIT_PRODUCER_ID]   = M.init_producer_id,
    [proto.OP_PRODUCE_IDEMPOTENT] = M.produce_idempotent,
    [proto.OP_SUBSCRIBE]          = M.subscribe,
    [proto.OP_FETCH]              = M.fetch,
    [proto.OP_COMMIT]             = M.commit,
    [proto.OP_NACK]               = M.nack,
    [proto.OP_CREATE_TOPIC]       = M.create_topic,
    [proto.OP_LIST_TOPICS]        = M.list_topics,
    [proto.OP_LIST_OFFSETS]       = M.list_offsets,
    [proto.OP_DELETE_TOPIC]       = M.delete_topic,
    [proto.OP_DESCRIBE_TOPIC]     = M.describe_topic,
    [proto.OP_ALTER_TOPIC_CONFIG] = M.alter_topic_config,
    [proto.OP_LIST_GROUPS]        = M.list_groups,
    [proto.OP_DESCRIBE_GROUP]     = M.describe_group,
    [proto.OP_DELETE_GROUP]       = M.delete_group,
    [proto.OP_JOIN_GROUP]         = M.join_group,
    [proto.OP_LEAVE_GROUP]        = M.leave_group,
    [proto.OP_GROUP_HEARTBEAT]    = M.group_heartbeat,
    [proto.OP_BEGIN_TXN]          = M.begin_txn,
    [proto.OP_END_TXN]            = M.end_txn,
    [proto.OP_TXN_OFFSET_COMMIT]  = M.txn_offset_commit,
    [proto.OP_GOODBYE]            = M.goodbye,
}

return M
