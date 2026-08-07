# MQL — the interactive console

`lua5.4 main.lua --repl` opens a console that speaks a small **SQL-like
language** over TCP. It connects to `127.0.0.1:9092` with the shipped
`admin`/`admin` credentials (the broker requires an `AUTH` handshake even in
OPEN mode) and transparently reconnects if an idle socket drops.

Statements end with `;` and may span lines (the prompt switches to `..>`).
Command words are case-insensitive; topic and group names are not. Strings are
single- or double-quoted (`''` escapes a quote) and `-- ...` is a comment. The
language runs through a real lexer → parser → executor pipeline
(`src/repl/sql/`), so errors carry actual positions.

`HELP;` prints the same reference in-session; `HELP <command>;` prints one
entry.

## Statements

| Statement | Maps to (client) |
| --- | --- |
| `CONNECT ['host'] [HOST 'h'] [PORT n] [USER 'u'] [PASSWORD 'p'];` | `Client.new` |
| `DISCONNECT;` | `Client:close` |
| `CREATE TOPIC <name> [PARTITIONS n];` | `create_topic` |
| `LIST TOPICS;` (alias `SHOW TOPICS;`) | `list_topics` |
| `PRODUCE INTO <topic> [KEY '<k>'] VALUE '<payload>';` | `produce` |
| `FETCH FROM <topic> [GROUP <g>] [LIMIT n];` | `fetch` (LIMIT defaults to 100) |
| `SUBSCRIBE [TO] <topic> [GROUP <g>] [TIMEOUT secs] [LIMIT n];` | `subscribe`/`next_record` (TIMEOUT defaults to 5s) |
| `COMMIT <topic> PARTITION <n> OFFSET <n>;` | `commit` |
| `CREATE GROUP <name> SUBSCRIBE <topic>[, ...];` | `join_group` |
| `JOIN GROUP <name> SUBSCRIBE <topic>[, ...];` | `join_group` |
| `SHOW GROUP;` | current membership & assignment |
| `HEARTBEAT;` · `LEAVE [GROUP];` | `group_heartbeat` · `leave_group` |
| `HELP [<command>];` · `EXIT;` / `QUIT;` | — |

## A session

```sql
mq> CREATE TOPIC orders PARTITIONS 4;
OK topic 'orders' created (4 partitions)

mq> PRODUCE INTO orders KEY 'k1' VALUE 'hello world';
OK produced to orders → partition 2, offset 0

mq> FETCH FROM orders GROUP billing LIMIT 10;
+-----------+--------+-----+-------------+
| partition | offset | key | value       |
+-----------+--------+-----+-------------+
| 2         | 0      | k1  | hello world |
+-----------+--------+-----+-------------+
(1 record)

mq> COMMIT orders PARTITION 2 OFFSET 1;
OK committed orders[2] @ offset 1
```
