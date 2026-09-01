return {
    _order = {
        "connect", "disconnect", "create", "list",
        "produce", "fetch", "subscribe", "commit",
        "join", "leave", "heartbeat", "help", "exit",
    },

    connect = {
        usage = "CONNECT ['host'] [HOST 'host'] [PORT n] [USER 'u'] [PASSWORD 'p'];",
        blurb = "Open a connection to a broker. Defaults: host 127.0.0.1, port 9092, no auth.",
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
                "  SHOW GROUP;    -- current group membership & assignment",
        blurb = "List broker topics, or show this session's group membership.",
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
