# Vole

*Vole* is Haitian Creole for *to fly*. We built this project so your data can do exactly that.

Vole is an in-memory data store that compiles to a single binary with no external dependencies. It speaks the RESP wire protocol, which means every existing client library already works with it -- Go, Python, Node, Java, Ruby, you name it. Just point your client at Vole and go.

But Vole isn't just another key-value store. We got tired of stitching together five different systems just to handle caching, queues, rate limiting, pub/sub, and time-series data. So we put all of it in one place.

## The problems we got tired of solving the hard way

**"I just want to query by value."** Key-value stores make you look things up by key. That's it. Need to find all users older than 18? You're on your own. Vole's `HSEARCH` lets you query hash fields directly: `HSEARCH user:* WHERE age > 18 AND status = active`.

**"My queue eats messages."** Pop-based queues throw messages away the moment a consumer picks them up. Consumer crashes? Message gone. Vole tracks every message through processing. If nobody acknowledges it, it goes back in the queue. After too many failures, it lands in a dead-letter queue where you can inspect it.

**"I need a rate limiter and I don't want to write Lua."** One command: `RATELIMIT api:user:123 100 60`. Done. Sliding window, returns remaining quota, retry-after, the works.

**"I need an HTTP endpoint for this."** Start Vole with `--http-addr :8080` and every operation is available as a JSON API. No client library required. `curl` works fine.

**"Keys just vanish and nobody knows."** When a key expires in most stores, it's just gone. Vole can fire a webhook or publish a pub/sub event when that happens, so your app can actually react to it.

**"I have staging and production data in the same instance."** Named namespaces. `NAMESPACE CREATE staging`, `NAMESPACE USE staging`. Keys are completely isolated. No more prefixing everything with `env:`.

## Documentation

- [Getting started](docs/getting-started.md) -- your first 10 minutes with Vole
- [Installation](docs/installation.md) -- building from source, Docker, systemd, upgrading
- [Configuration](docs/configuration.md) -- every flag explained, with examples and a production template

## Quick start

```bash
go build -o vole ./cmd/vole
go build -o vole-cli ./cmd/vole-cli

# Simplest possible start
vole

# With the HTTP API
vole --http-addr :8080

# With persistence that survives restarts
vole --appendonly --snapshot vole.snap --snapshot-interval 5m

# With TLS and a password
vole --tls-cert cert.pem --tls-key key.pem --requirepass mysecret

# With memory limits
vole --maxmemory 536870912 --maxmemory-policy allkeys-lru

# Or use a config file
cp vole.conf.example /etc/vole/vole.conf
vole --config /etc/vole/vole.conf
```

## CLI

```bash
vole-cli SET hello world
vole-cli GET hello

# Or drop into an interactive shell
vole-cli
127.0.0.1:7379> SET user:1 "John"
OK
127.0.0.1:7379> GET user:1
"John"
```

---

## What Vole can store

| Type | What it is |
|------|-----------|
| String | The basics. Binary-safe, optional TTL. |
| Hash | Field-value maps. Think of a row in a table. |
| List | Double-ended queue backed by a ring buffer. Push and pop from either end in O(1). |
| Set | Unique unordered strings. |
| Sorted Set | Unique strings ordered by score. The sorted order is maintained on every write, so range queries are fast. |
| Stream | Append-only log with consumer groups, acknowledgment, and blocking reads. |
| Bitmap | Bit-level operations on string values. |
| HyperLogLog | Probabilistic cardinality counting. Useful for "how many unique visitors" without storing every visitor. |
| JSON | Structured documents you can read and write with dot-path syntax like `$.address.city`. |
| Time-Series | Timestamped numeric samples with labels and downsampling (avg, min, max, sum, count). |

---

## The HTTP API

Start with `--http-addr :8080`. Everything comes back as JSON.

```bash
# Strings
curl -X PUT localhost:8080/api/v1/keys/mykey -d '{"value":"hello","ex":3600}'
curl localhost:8080/api/v1/keys/mykey
curl -X DELETE localhost:8080/api/v1/keys/mykey

# Hashes
curl -X PUT localhost:8080/api/v1/hash/user:1 -d '{"name":"John","age":"30"}'
curl localhost:8080/api/v1/hash/user:1

# Lists
curl -X POST localhost:8080/api/v1/list/tasks/push -d '{"values":["a","b"],"side":"left"}'
curl "localhost:8080/api/v1/list/tasks?start=0&stop=-1"

# Sets
curl -X POST localhost:8080/api/v1/set/tags -d '{"members":["go","database"]}'
curl localhost:8080/api/v1/set/tags

# Sorted sets
curl -X POST localhost:8080/api/v1/zset/scores \
  -d '{"members":[{"member":"alice","score":100}]}'

# Pub/sub
curl -X POST localhost:8080/api/v1/publish/events -d '{"message":"hello"}'

# Rate limiting -- returns 200 or 429 with standard rate limit headers
curl -X POST localhost:8080/api/v1/ratelimit/api:user:123 -d '{"max":100,"window":60}'

# Search hashes by field value
curl "localhost:8080/api/v1/search/hash?pattern=user:*&where=age>18&limit=50"

# Stream key changes in real time (Server-Sent Events)
curl -N localhost:8080/api/v1/events?patterns=__keyspace__:user:*

# Webhooks
curl -X POST localhost:8080/api/v1/webhooks \
  -d '{"pattern":"session:*","event":"expired","url":"https://example.com/hook"}'

# Prometheus metrics
curl localhost:8080/metrics

# Health check
curl localhost:8080/health
```

---

## Features worth knowing about

### Rate limiting

```
RATELIMIT api:user:123 100 60
```

One command. 100 requests per 60 seconds, sliding window. Returns four values: allowed (1/0), remaining requests, retry-after in milliseconds, and when the window resets.

```
RATELIMIT.PEEK api:user:123   -- check without consuming a request
RATELIMIT.RESET api:user:123  -- clear the counter
```

### JSON documents

```
JSON.SET user:1 $ '{"name":"John","age":30,"address":{"city":"NYC"}}'
JSON.GET user:1 $.address.city       -- "NYC"
JSON.NUMINCRBY user:1 $.age 1       -- 31
JSON.DEL user:1 $.address
JSON.TYPE user:1 $.name              -- "string"
JSON.KEYS user:1 $                   -- ["name", "age"]

JSON.SET items $ '["a","b"]'
JSON.ARRAPPEND items $ '"c"'         -- 3
JSON.ARRLEN items $                  -- 3
```

### Reliable queues

```
ENQUEUE tasks '{"type":"email","to":"user@example.com"}'
ENQUEUE tasks '{"type":"cleanup"}' DELAY 300

DEQUEUE tasks TIMEOUT 30
-- returns [message-id, body, retry-count]

QACK tasks <message-id>              -- done processing
QNACK tasks <message-id>             -- put it back, try again

QLEN tasks                           -- how many are waiting
QINFO tasks                          -- pending / processing / dead-letter counts
QDEAD tasks COUNT 10                 -- look at what failed
```

Messages that fail too many times end up in the dead-letter queue instead of disappearing.

### Time-series

```
TS.ADD metrics:cpu * 72.5 LABELS host=web1 region=us-east
TS.RANGE metrics:cpu - + COUNT 100
TS.GET metrics:cpu
TS.INFO metrics:cpu
TS.DOWNSAMPLE metrics:cpu metrics:cpu:hourly avg 0 9999999999999 3600000
```

Aggregations: `avg`, `sum`, `min`, `max`, `count`, `first`, `last`.

### Hash search

```
HSET user:1 name John age 25 status active
HSET user:2 name Jane age 32 status active
HSET user:3 name Bob age 17 status inactive

HSEARCH user:* WHERE age > 18 AND status = active LIMIT 0 50
```

Operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `CONTAINS`, `STARTSWITH`.

### Namespaces

```
NAMESPACE CREATE analytics
NAMESPACE USE analytics
SET pageview:home 42        -- only exists in "analytics"

NAMESPACE USE default
GET pageview:home           -- (nil)

NAMESPACE LIST
NAMESPACE DROP analytics
```

### Key tagging

```
TAG user:1 env=prod region=us-east tier=premium
TAGQUERY env=prod AND tier=premium LIMIT 100
TAGGET user:1
TAGDEL user:1 region
```

### Schema enforcement

```
SCHEMA.SET user:* name:string age:int email:email

HSET user:1 name John age 25 email john@example.com   -- fine
HSET user:2 name Jane age notanumber                   -- rejected
```

Types: `string`, `int`, `float`, `bool`, `email`, `required`.

### Scheduled keys

```
SET announcement "Big news!" AFTER 3600
SETDELAYED config:flag "enabled" 86400 EX 7200
```

The key is invisible until the delay passes. `GET`, `EXISTS`, `KEYS` all act as if it doesn't exist yet.

### Webhooks

```
WEBHOOK REGISTER session:* expired https://example.com/session-expired
WEBHOOK LIST
WEBHOOK UNREGISTER session:* expired https://example.com/session-expired
```

When a matching key expires (or is set/deleted, depending on the event), Vole sends an HTTP POST with the key name, event type, and timestamp.

### Real-time events

Vole publishes every key mutation to `__keyspace__:<key>` and `__keyevent__:<event>` channels. You can subscribe via RESP (`SUBSCRIBE`, `PSUBSCRIBE`) or via the HTTP SSE endpoint.

### Cron

```
CRON.ADD cleanup "0 */6 * * *" DEL temp:*
CRON.LIST
CRON.INFO cleanup
CRON.DEL cleanup
```

Standard 5-field cron syntax. The command runs inside the server -- no shell, no external process.

### Audit log

Disabled by default. When enabled, Vole records every write with the key, command, timestamp, and client address.

```
AUDIT.ENABLE
SET user:1 John
AUDIT user:1 COUNT 5
AUDIT.SEARCH user:* COUNT 50
AUDIT.DISABLE
```

### Scripting

```
EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 mykey myvalue
SCRIPT LOAD "return redis.call('GET', KEYS[1])"
EVALSHA <sha1> 1 mykey
```

Vole's script engine handles sequences of `redis.call()` with `KEYS[n]`/`ARGV[n]` substitution, variable assignment, and return values. It covers the patterns you'll see in practice. It is not a full Lua VM -- things like loops, coroutines, and `require` won't work.

---

## Persistence

Your data survives restarts. Two mechanisms, usable together or separately.

### AOF (Append-Only File)

Every write goes to a log file. Each entry has a CRC32 checksum, so corrupted entries are caught and skipped on replay.

| Fsync | What it does |
|-------|-------------|
| `always` | Flush to disk after every write. Safest. Slowest. |
| `everysec` | Flush once a second. Good default. |
| `no` | Let the OS decide. Fastest. |

```bash
vole --appendonly --appendfilename vole.aof --appendfsync everysec
```

### Snapshots

Full point-in-time dumps. All 10 data types are included.

```bash
vole --snapshot vole.snap --snapshot-interval 5m
```

```
SAVE        -- snapshot now (blocks)
BGSAVE      -- snapshot in background
LASTSAVE    -- when was the last one
```

### Both together

On startup: load snapshot, then replay AOF. After each snapshot, the AOF is truncated.

---

## Replication

Leader-follower. A follower connects, gets a full snapshot, then streams every write from the leader.

```bash
vole --addr :7379                            # leader
vole --addr :7380 --replicaof localhost:7379  # follower
```

```
REPLICAOF localhost 7379    -- start following at runtime
REPLICAOF NO ONE            -- stop following, become standalone
```

Followers reject writes with `READONLY`. Check `INFO` for replication status.

Worth knowing: replication is asynchronous. There is no automatic failover -- you promote a follower manually. If a follower reconnects, it gets a fresh snapshot (no partial resync).

---

## Cluster

16,384 hash slots distributed across nodes. Commands for keys on another node get a `MOVED` redirect -- your client follows it.

```bash
vole --addr :7379 --node-id node1 --peers "node2@host2:7380"
```

```
CLUSTER MEET host2:7380
CLUSTER FORGET <node-id>
CLUSTER NODES
CLUSTER INFO
CLUSTER KEYSLOT mykey
```

A background heartbeat pings peers every 5 seconds and tracks their state. Slot ownership rebalances when nodes join or leave.

Worth knowing: Vole does not forward commands on your behalf -- clients must handle `MOVED` redirects. There is no live data migration when slots move; only the ownership metadata changes.

---

## Security

```bash
# Require a password
vole --requirepass mysecret

# Encrypt everything with TLS
vole --tls-cert cert.pem --tls-key key.pem
```

TLS covers both the RESP protocol and the HTTP API. Minimum version is TLS 1.2.

---

## Observability

### Prometheus

`/metrics` on the HTTP port. Exposes commands processed, connections, keyspace hit/miss rate, memory usage, goroutine count, uptime.

### Slow queries

```
SLOWLOG GET 10
SLOWLOG LEN
SLOWLOG RESET
```

Threshold is 10ms. Logs the command, duration, and client address.

### Client tracking

```
CLIENT LIST
CLIENT ID
CLIENT SETNAME myapp
CLIENT GETNAME
CLIENT KILL ID 42
```

### General

```
INFO
DBSIZE
TIME
```

---

## Eviction

When `--maxmemory` is set and usage exceeds the limit:

| Policy | What happens |
|--------|-------------|
| `noeviction` | Writes fail with OOM error (default) |
| `allkeys-random` | Random keys get deleted |
| `volatile-random` | Random keys with a TTL get deleted |
| `allkeys-lru` | Least recently accessed keys get deleted (samples 5 keys per cycle) |

```bash
vole --maxmemory 536870912 --maxmemory-policy allkeys-lru
```

Also configurable at runtime via `CONFIG SET`.

---

## All 209 commands

### Strings

`GET` `SET` `MGET` `MSET` `MSETNX` `SETNX` `SETEX` `PSETEX` `GETSET` `GETEX` `GETDEL` `GETRANGE` `SETRANGE` `SUBSTR` `INCR` `INCRBY` `DECR` `DECRBY` `INCRBYFLOAT` `APPEND` `STRLEN`

### Hashes

`HSET` `HGET` `HGETALL` `HDEL` `HEXISTS` `HKEYS` `HVALS` `HLEN` `HINCRBY` `HSETNX` `HRANDFIELD`

### Lists

`LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE` `LLEN` `LINDEX` `LSET` `LINSERT` `LPOS` `LREM` `BLPOP` `BRPOP` `RPOPLPUSH` `LMOVE`

### Sets

`SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SRANDMEMBER` `SMOVE` `SPOP` `SINTER` `SINTERCARD` `SINTERSTORE` `SUNION` `SUNIONSTORE` `SDIFF` `SDIFFSTORE`

### Sorted Sets

`ZADD` `ZRANGE` `ZRANGEBYSCORE` `ZRANGEBYLEX` `ZREVRANGE` `ZREM` `ZSCORE` `ZCARD` `ZRANK` `ZREVRANK` `ZCOUNT` `ZPOPMIN` `ZPOPMAX` `ZINCRBY`

### HyperLogLog

`PFADD` `PFCOUNT` `PFMERGE`

### Streams

`XADD` `XRANGE` `XREAD` `XLEN` `XTRIM` `XINFO` `XGROUP CREATE` `XREADGROUP` `XACK` `XCLAIM` `XAUTOCLAIM` `XPENDING`

### Geo

`GEOADD` `GEOPOS` `GEODIST` `GEOSEARCH`

### Bitmaps

`SETBIT` `GETBIT` `BITCOUNT` `BITOP` `BITPOS`

### Keys and Expiry

`DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` `SCAN` `RENAME` `COPY` `TOUCH` `RANDOMKEY` `SORT` `DUMP` `EXPIRE` `PEXPIRE` `PEXPIREAT` `TTL` `PTTL` `PERSIST` `EXPIRETIME` `PEXPIRETIME` `DBSIZE` `FLUSHDB` `FLUSHALL` `OBJECT ENCODING` `OBJECT REFCOUNT` `OBJECT IDLETIME` `MEMORY USAGE` `LASTSAVE`

### Pub/Sub

`PUBLISH` `SUBSCRIBE` `PSUBSCRIBE` `UNSUBSCRIBE` `PUNSUBSCRIBE`

### Transactions

`MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH`

### Scripting

`EVAL` `EVALSHA` `SCRIPT LOAD` `SCRIPT EXISTS` `SCRIPT FLUSH`

### Server

`PING` `ECHO` `INFO` `SAVE` `BGSAVE` `LASTSAVE` `TIME` `WAIT` `HELLO` `SELECT` `RESET` `QUIT` `AUTH` `CONFIG GET` `CONFIG SET` `COMMAND COUNT` `DEBUG SLEEP`

### Client and Diagnostics

`CLIENT LIST` `CLIENT ID` `CLIENT SETNAME` `CLIENT GETNAME` `CLIENT KILL` `CLIENT INFO` `SLOWLOG GET` `SLOWLOG LEN` `SLOWLOG RESET` `ACL WHOAMI` `ACL LIST` `ACL CAT`

### Replication

`REPLICAOF` `SLAVEOF`

### Cluster

`CLUSTER NODES` `CLUSTER SLOTS` `CLUSTER MEET` `CLUSTER FORGET` `CLUSTER INFO` `CLUSTER MYID` `CLUSTER RESET` `CLUSTER KEYSLOT` `CLUSTER COUNTKEYSINSLOT` `CLUSTER GETKEYSINSLOT` `CLUSTER REPLICATE` `CLUSTER FAILOVER` `CLUSTER SAVECONFIG`

### Vole-specific

`RATELIMIT` `RATELIMIT.PEEK` `RATELIMIT.RESET` `SETDELAYED` `ENQUEUE` `DEQUEUE` `QACK` `QNACK` `QPEEK` `QLEN` `QINFO` `QDEAD` `JSON.SET` `JSON.GET` `JSON.DEL` `JSON.TYPE` `JSON.NUMINCRBY` `JSON.ARRAPPEND` `JSON.ARRLEN` `JSON.KEYS` `TAG` `TAGGET` `TAGDEL` `TAGQUERY` `TS.ADD` `TS.RANGE` `TS.GET` `TS.INFO` `TS.DOWNSAMPLE` `HSEARCH` `CRON.ADD` `CRON.DEL` `CRON.LIST` `CRON.INFO` `AUDIT` `AUDIT.SEARCH` `AUDIT.ENABLE` `AUDIT.DISABLE` `AUDIT.CLEAR` `AUDIT.SIZE` `SCHEMA.SET` `SCHEMA.GET` `SCHEMA.DEL` `SCHEMA.LIST` `WEBHOOK REGISTER` `WEBHOOK LIST` `WEBHOOK UNREGISTER` `NAMESPACE CREATE` `NAMESPACE USE` `NAMESPACE LIST` `NAMESPACE CURRENT` `NAMESPACE DROP`

### Compatibility stubs

These are accepted so clients don't break, but they don't do much:

`LATENCY LATEST` `LATENCY HISTORY` `LATENCY RESET` `ACL GETUSER` `MODULE LIST` `SWAPDB` (error -- use namespaces) `RESTORE` (error -- not implemented)

---

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `127.0.0.1:7379` | RESP listen address |
| `--http-addr` | _(off)_ | HTTP API address |
| `--appendonly` | `true` | AOF persistence |
| `--appendfilename` | `vole.aof` | AOF file path |
| `--appendfsync` | `everysec` | AOF fsync policy |
| `--snapshot` | `vole.rdb.json` | Snapshot file path |
| `--snapshot-interval` | `0` | Snapshot interval (e.g. `5m`) |
| `--maxmemory` | `0` | Memory limit in bytes |
| `--maxmemory-policy` | `noeviction` | Eviction policy |
| `--requirepass` | _(none)_ | Client password |
| `--tls-cert` | _(none)_ | TLS certificate |
| `--tls-key` | _(none)_ | TLS private key |
| `--replicaof` | _(none)_ | Leader address |
| `--node-id` | _(auto)_ | Cluster node ID |
| `--peers` | _(none)_ | Cluster peers |

---

## How it's built

Pure Go. The `go.mod` has no dependencies. `go build` gives you a static binary you can drop anywhere.

Under the hood: read/write lock separation so concurrent reads don't step on each other. Lists use a ring-buffer deque, so push and pop are O(1) from either end. Sorted sets stay sorted on every write, which means range queries never need to re-sort. Stream reads use binary search. Every AOF entry carries a CRC32 checksum.

About 23,000 lines across 34 files. 125 tests, 26 benchmarks.

When you shut it down (Ctrl-C or SIGTERM), Vole drains in-flight connections, writes a final snapshot, flushes the AOF, and stops replication before exiting.

---

## Contributing

Vole is open source. If you want to get involved -- whether that's a bug fix, a new command, better docs, or just telling us about a problem you ran into -- we'd genuinely appreciate it.

[CONTRIBUTING.md](CONTRIBUTING.md) has everything you need: how to build, how the code is organized, and what a good pull request looks like.

We also have a [Code of Conduct](CODE_OF_CONDUCT.md). The gist: be decent to each other.

---

## License

Vole is licensed under the BSD 3-Clause License. See [LICENSE](LICENSE) for the full text.
