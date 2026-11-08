return {
    _order = {
        "connect", "disconnect", "create", "list",
        "describe", "alter", "drop",
        "produce", "fetch", "subscribe", "commit",
        "join", "leave", "heartbeat", "help", "exit",
    },

    connect = {
        usage = "CONNECT ['host'] [HOST 'host'] [PORT n] [USER 'u'] [PASSWORD 'p'];",
        blurb = "Open a connection to a broker. Defaults: host 127.0.0.1, port 9092, user admin, password admin.",
    },
    disconnect = {
        usage = "DISCONNECT;",
        blurb = "Close the current broker connection.",
    },
    create = {
        usage = "CREATE TOPIC <name> [PARTITIONS n];\n" ..
                "  CREATE GROUP <name> SUBSCRIBE <topic>[, <topic> ...];",
        blurb = "Create a partitioned topic, or create/join a consumer group.",
    },
    list = {
        usage = "LIST TOPICS;   (alias: SHOW TOPICS)\n" ..
                "  LIST GROUPS;   (alias: SHOW GROUPS) -- every group on the broker\n" ..
                "  SHOW GROUP;    -- this session's membership & assignment",
        blurb = "List topics or groups, or show this session's group membership.",
    },
    describe = {
        usage = "DESCRIBE TOPIC <name>;\n" ..
                "  DESCRIBE GROUP <name>;",
        blurb = "Show a topic's partitions, offsets and config, or a group's members, committed offsets and lag.",
    },
    alter = {
        usage = "ALTER TOPIC <name> SET <key> = <value>[, <key> = <value> ...];\n" ..
                "  ALTER TOPIC <name> PARTITIONS <total>;",
        blurb = "Change topic config (retention, max_segment_size, cleanup_policy, ...) or grow to a new partition total.",
    },
    drop = {
        usage = "DROP TOPIC <name>;\n" ..
                "  DROP GROUP <name>;",
        blurb = "Delete a topic and its data, or delete an empty group and its committed offsets.",
    },
    produce = {
        usage = "PRODUCE INTO <topic> [KEY '<key>'] VALUE '<payload>';",
        blurb = "Append one record to a topic. KEY defaults to the empty string.",
    },
    fetch = {
        usage = "FETCH FROM <topic> [GROUP <group>] [LIMIT n];",
        blurb = "Pull up to LIMIT records (default 100) from a topic.",
    },
    subscribe = {
        usage = "SUBSCRIBE [TO] <topic> [GROUP <group>] [TIMEOUT secs] [LIMIT n];",
        blurb = "Stream pushed records until TIMEOUT (default 5s) or LIMIT is hit.",
    },
    commit = {
        usage = "COMMIT <topic> PARTITION <n> OFFSET <n>;",
        blurb = "Commit a consumer-group offset for a topic partition.",
    },
    join = {
        usage = "JOIN GROUP <name> SUBSCRIBE <topic>[, <topic> ...];",
        blurb = "Join a consumer group and receive a partition assignment.",
    },
    leave = {
        usage = "LEAVE [GROUP];",
        blurb = "Leave the consumer group this session joined.",
    },
    heartbeat = {
        usage = "HEARTBEAT;",
        blurb = "Renew the joined group member's lease.",
    },
    help = {
        usage = "HELP [<command>];",
        blurb = "Show this overview, or detailed help for one command.",
    },
    exit = {
        usage = "EXIT;   (alias: QUIT)",
        blurb = "Leave the console.",
    },
}
