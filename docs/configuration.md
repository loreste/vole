# Configuring Vole

Vole can be configured with CLI flags, a config file, or a mix of both. CLI flags always take priority over the config file, so you can set your baseline in the file and override individual settings on the fly.

A handful of settings can also be changed at runtime with `CONFIG SET` without restarting.

## Config file

Copy the example and edit it:

```bash
cp vole.conf.example /etc/vole/vole.conf
```

Then start Vole with:

```bash
vole --config /etc/vole/vole.conf
```

The format is one setting per line: `key value`. Lines starting with `#` are comments. See `vole.conf.example` in the repo for a fully commented template.

You can still pass CLI flags alongside `--config`. If a flag shows up in both places, the CLI wins.

## Flag reference

This page walks through each setting, explains what it does, and tells you when you'd actually want to change it.

## Network

### `--addr`

**Default:** `127.0.0.1:7379`

The address Vole listens on for RESP connections. This is the main protocol -- it's what `vole-cli` and every compatible client library connects to.

If you only need local access, the default is fine. To accept connections from other machines:

```bash
vole --addr 0.0.0.0:7379
```

To run on a non-standard port:

```bash
vole --addr 0.0.0.0:6400
```

### `--http-addr`

**Default:** disabled

Turns on the HTTP/JSON API. If you don't set this flag, the HTTP server doesn't start at all.

```bash
vole --http-addr :8080
```

This gives you REST endpoints for all operations, a Prometheus metrics endpoint at `/metrics`, a health check at `/health`, and Server-Sent Events for real-time key change notifications at `/api/v1/events`.

You can run this on the same machine as the RESP port without issues -- they're independent listeners.

## Persistence

Vole stores everything in memory, but it can write data to disk so it comes back after a restart. You've got two options, and they work well together.

### `--appendonly`

**Default:** `true`

When this is on, every write command gets appended to a log file before the client gets a response. On restart, the log is replayed to rebuild the dataset.

If you're using Vole as a pure cache and don't care about surviving restarts, turn it off:

```bash
vole --appendonly=false
```

### `--appendfilename`

**Default:** `vole.aof`

Where the append-only log goes. Can be a relative or absolute path.

```bash
vole --appendfilename /var/lib/vole/data.aof
```

### `--appendfsync`

**Default:** `everysec`

How often the AOF is flushed to disk. This is the classic durability-vs-speed tradeoff.

| Value | What it means | When to use it |
|-------|--------------|----------------|
| `always` | Flush after every write | You can't afford to lose a single command, even in a crash. This is the slowest option. |
| `everysec` | Flush once a second | Good balance for most workloads. You might lose up to one second of writes in a hard crash. |
| `no` | Let the OS decide | Fastest. The OS will flush eventually, but you could lose more data in a crash. Fine for caches. |

```bash
vole --appendfsync always
```

### `--snapshot`

**Default:** `vole.rdb.json`

Path for point-in-time snapshots. A snapshot is a full dump of everything in memory, written as JSON.

Snapshots are useful because replaying a long AOF can be slow on restart. With both enabled, Vole loads the snapshot first (fast) and then replays only the AOF entries that came after it.

### `--snapshot-interval`

**Default:** `0` (disabled)

How often to take automatic snapshots. Set this to something like `5m` or `1h`. When a snapshot completes, the AOF is truncated since everything in it is now captured in the snapshot.

```bash
vole --snapshot /var/lib/vole/vole.snap --snapshot-interval 5m
```

You can also trigger snapshots manually with the `SAVE` (blocking) or `BGSAVE` (background) commands.

### A note on how the two work together

On startup, Vole does this:

1. If a snapshot file exists, load it.
2. If an AOF file exists, replay it on top of the snapshot.

On each automatic snapshot:

1. Write the snapshot to a temporary file, then rename it into place (atomic on most filesystems).
2. Truncate the AOF.

So even if Vole crashes mid-snapshot, the old snapshot and AOF are still intact. You'll never end up with a half-written snapshot and a truncated AOF.

## Memory management

### `--maxmemory`

**Default:** `0` (unlimited)

A soft memory limit in bytes. When Vole estimates it's using more than this, it starts evicting keys according to the policy.

```bash
vole --maxmemory 536870912  # 512 MB
```

The estimate isn't exact -- it samples the data structures and calculates a rough total. It's good enough to prevent runaway growth, but don't treat it as a hard guarantee down to the byte.

You can change this at runtime:

```
CONFIG SET maxmemory 1073741824
```

### `--maxmemory-policy`

**Default:** `noeviction`

What happens when the memory limit is hit.

| Policy | What it does |
|--------|-------------|
| `noeviction` | Reject writes with an OOM error. Reads still work. |
| `allkeys-random` | Pick a random key and delete it. Any key is fair game. |
| `volatile-random` | Same, but only picks from keys that have a TTL set. If no keys have a TTL, writes fail. |
| `allkeys-lru` | Delete the key that was accessed least recently. Vole samples 5 random keys and evicts the oldest one, so it's an approximation. |

```bash
vole --maxmemory 536870912 --maxmemory-policy allkeys-lru
```

Also changeable at runtime:

```
CONFIG SET maxmemory-policy allkeys-lru
```

## Security

### `--requirepass`

**Default:** none

Sets a password that clients must provide before they can run commands.

```bash
vole --requirepass s3cret
```

On the client side:

```
vole-cli
127.0.0.1:7379> GET foo
(error) NOAUTH Authentication required.
127.0.0.1:7379> AUTH s3cret
OK
127.0.0.1:7379> GET foo
"bar"
```

`PING` works without authenticating, so health checks still function. Everything else is blocked.

This is a single shared password, not per-user accounts. It's meant to keep casual access out, not to implement fine-grained permissions.

### `--tls-cert` and `--tls-key`

**Default:** none

Encrypt all traffic (RESP and HTTP) with TLS.

```bash
vole --tls-cert /etc/vole/cert.pem --tls-key /etc/vole/key.pem
```

Both flags are required -- you can't set one without the other. The minimum TLS version is 1.2.

If you need a quick self-signed cert for testing:

```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj '/CN=localhost'
```

In production, use certs from your CA or Let's Encrypt. Vole doesn't reload certs automatically -- if you rotate them, restart the server.

## Replication

### `--replicaof`

**Default:** none

Starts this instance as a follower of another Vole server. On startup, it connects to the leader, downloads a full snapshot, and then streams every subsequent write in real time.

```bash
# The leader
vole --addr :7379

# A follower
vole --addr :7380 --replicaof localhost:7379
```

The follower is read-only. Writes get rejected with `READONLY`. You can promote a follower to a standalone server at runtime:

```
REPLICAOF NO ONE
```

Or start following from the CLI:

```
REPLICAOF localhost 7379
```

A few things to be aware of:

- Replication is asynchronous. A write that succeeds on the leader might not have reached the follower yet.
- If the follower disconnects and reconnects, it gets a fresh snapshot. There's no partial resync.
- There's no automatic failover. If the leader goes down, you need to promote a follower manually.

## Cluster

### `--node-id`

**Default:** auto-generated from a random value

A stable identifier for this node in a cluster. If you don't set one, Vole generates a random 40-character hex string on startup. The problem is that a new ID gets generated every time, so if you restart a node, the rest of the cluster won't recognize it.

For any cluster setup, set this explicitly:

```bash
vole --node-id node1 --addr :7379
```

### `--peers`

**Default:** none

A comma-separated list of other nodes in the cluster, in the format `nodeID@host:port`.

```bash
vole --node-id node1 --addr :7379 --peers "node2@host2:7380,node3@host3:7381"
```

On startup, Vole divides 16,384 hash slots evenly across all known nodes (including itself). When a command targets a key that belongs to a different node, the client gets a `MOVED` redirect telling it where to go.

You can also add and remove nodes at runtime:

```
CLUSTER MEET host2:7380
CLUSTER FORGET <node-id>
```

### `--multimaster`

**Default:** `false`

Turns on multi-master replication. Every node in the cluster accepts writes, and changes propagate to all peers automatically.

```bash
vole --addr :7379 --node-id node1 --peers "node2@localhost:7380" --multimaster
```

When enabled, Vole connects to every known peer and sets up bidirectional write streaming. If a new node is added via `CLUSTER MEET`, it's automatically connected.

You can also toggle it at runtime:

```
MULTIMASTER ENABLE
MULTIMASTER DISABLE
```

Multi-master uses last-writer-wins for conflict resolution. If two nodes write to the same key at the same time, whichever write arrives last takes precedence. There's no merge or conflict detection -- keep that in mind when designing your key scheme.

You can run multi-master alongside `--peers` but not alongside `--replicaof` (a node can't be both a read-only follower and a multi-master peer).

### How clustering works in practice

Each key maps to one of 16,384 slots via CRC32 hashing. Each node owns a contiguous range of slots. When you send a command for a key that lives on a different node, you get back:

```
MOVED 12182 host2:7380
```

Your client library needs to handle this -- most compatible clients already do. Vole doesn't forward the command for you.

A background heartbeat pings every peer every 5 seconds. If a node stops responding, it gets marked as `failing` and eventually `offline`. When nodes are added or removed, the slot ranges are recalculated automatically.

What clustering does *not* do right now:

- It doesn't move data when slots are reassigned. Only the ownership metadata changes.
- It doesn't automatically replicate data between nodes.
- It doesn't handle failover.

Think of it as "routing with health checks" rather than a fully self-healing distributed system.

## Putting it all together

Here's what a production-ish setup might look like:

```bash
vole \
  --addr 0.0.0.0:7379 \
  --http-addr 0.0.0.0:8080 \
  --appendonly \
  --appendfilename /var/lib/vole/vole.aof \
  --appendfsync everysec \
  --snapshot /var/lib/vole/vole.snap \
  --snapshot-interval 5m \
  --maxmemory 1073741824 \
  --maxmemory-policy allkeys-lru \
  --requirepass changeme \
  --tls-cert /etc/vole/cert.pem \
  --tls-key /etc/vole/key.pem
```

That gives you:

- RESP on port 7379, HTTP on 8080, both encrypted with TLS
- Password required
- AOF persistence with 1-second fsync, plus snapshots every 5 minutes
- 1 GB memory cap with LRU eviction
- Prometheus metrics at `https://host:8080/metrics`

## Quick reference

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `127.0.0.1:7379` | RESP listen address |
| `--http-addr` | _(off)_ | HTTP/JSON API listen address |
| `--appendonly` | `true` | Enable append-only file |
| `--appendfilename` | `vole.aof` | AOF path |
| `--appendfsync` | `everysec` | `always`, `everysec`, or `no` |
| `--snapshot` | `vole.rdb.json` | Snapshot path |
| `--snapshot-interval` | `0` | Auto-snapshot interval (e.g., `5m`) |
| `--maxmemory` | `0` | Memory limit in bytes (0 = unlimited) |
| `--maxmemory-policy` | `noeviction` | Eviction policy |
| `--requirepass` | _(none)_ | Client password |
| `--tls-cert` | _(none)_ | TLS certificate path |
| `--tls-key` | _(none)_ | TLS private key path |
| `--replicaof` | _(none)_ | Leader address (`host:port`) |
| `--multimaster` | `false` | Enable multi-master replication |
| `--node-id` | _(auto)_ | Cluster node ID |
| `--peers` | _(none)_ | Cluster peer list |
