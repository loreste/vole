# Getting Started with Vole

This guide walks you through the basics: starting the server, storing some data, and exploring a few features. It assumes you've already built `vole` and `vole-cli` (see the [installation guide](installation.md) if you haven't).

## Start the server

Open a terminal and run:

```bash
vole
```

That's the minimal setup. Vole is now listening on `127.0.0.1:7379` with AOF persistence enabled. It writes to `vole.aof` in the current directory.

## Connect with the CLI

Open a second terminal:

```bash
vole-cli
```

You'll get an interactive prompt:

```
127.0.0.1:7379>
```

If you ever need a reminder of what's available, type `help`. It lists every command grouped by category -- strings, hashes, lists, queues, JSON, time-series, replication, multi-master, the whole lot.

Try a few things:

```
127.0.0.1:7379> SET greeting "hello from vole"
OK
127.0.0.1:7379> GET greeting
"hello from vole"
127.0.0.1:7379> SET counter 0
OK
127.0.0.1:7379> INCR counter
(integer) 1
127.0.0.1:7379> INCR counter
(integer) 2
```

Standard stuff so far. Let's get more interesting.

## Hashes

Hashes are like little objects. Each key holds a set of field-value pairs.

```
127.0.0.1:7379> HSET user:1 name Alice age 28 city Portland
(integer) 3
127.0.0.1:7379> HGET user:1 name
"Alice"
127.0.0.1:7379> HGETALL user:1
1) "age"
2) "28"
3) "city"
4) "Portland"
5) "name"
6) "Alice"
```

Now here's something you can't do in most key-value stores -- search across hashes by field value:

```
127.0.0.1:7379> HSET user:2 name Bob age 35 city Seattle
(integer) 3
127.0.0.1:7379> HSET user:3 name Carol age 22 city Portland
(integer) 3
127.0.0.1:7379> HSEARCH user:* WHERE city = Portland
1) 1) "user:1"
   2) 1) "age" 2) "28" 3) "city" 4) "Portland" 5) "name" 6) "Alice"
2) 1) "user:3"
   2) 1) "age" 2) "22" 3) "city" 4) "Portland" 5) "name" 6) "Carol"
```

## Lists and queues

Lists are double-ended queues:

```
127.0.0.1:7379> RPUSH shopping milk eggs bread
(integer) 3
127.0.0.1:7379> LRANGE shopping 0 -1
1) "milk"
2) "eggs"
3) "bread"
127.0.0.1:7379> LPOP shopping
"milk"
```

But if you need something more reliable -- where messages don't vanish if a consumer crashes -- use the queue commands:

```
127.0.0.1:7379> ENQUEUE jobs '{"task":"send-email","to":"alice@example.com"}'
"1719700000000-1"
127.0.0.1:7379> DEQUEUE jobs
1) "1719700000000-1"
2) "{\"task\":\"send-email\",\"to\":\"alice@example.com\"}"
3) (integer) 0
127.0.0.1:7379> QACK jobs 1719700000000-1
(integer) 1
```

The message stays in a "processing" state until you `QACK` it. If you don't acknowledge it within 5 minutes, it goes back into the queue for another consumer to pick up.

## JSON documents

Store structured data and reach into it with paths:

```
127.0.0.1:7379> JSON.SET config $ '{"db":{"host":"localhost","port":5432},"cache":{"ttl":300}}'
OK
127.0.0.1:7379> JSON.GET config $.db.host
"\"localhost\""
127.0.0.1:7379> JSON.NUMINCRBY config $.cache.ttl 60
"360"
```

## Rate limiting

If you're building an API and need to throttle requests, this is one command:

```
127.0.0.1:7379> RATELIMIT api:user:42 10 60
1) (integer) 1
2) (integer) 9
3) (integer) 0
4) (integer) 1719700060000
```

That means: user 42 is allowed 10 requests per 60 seconds. The response tells you it was allowed (1), there are 9 requests remaining, no retry-after delay, and the window resets at that timestamp.

Keep calling it and you'll see the remaining count drop. Once it hits zero:

```
127.0.0.1:7379> RATELIMIT api:user:42 10 60
1) (integer) 0
2) (integer) 0
3) (integer) 42000
4) (integer) 1719700060000
```

Denied. Try again in 42 seconds.

## The HTTP API

Stop the server (Ctrl-C) and restart it with the HTTP flag:

```bash
vole --http-addr :8080
```

Now you can talk to Vole with `curl`:

```bash
curl -X PUT http://localhost:8080/api/v1/keys/hello -d '{"value":"world"}'
# {"key":"hello","status":"OK"}

curl http://localhost:8080/api/v1/keys/hello
# {"key":"hello","value":"world","type":"string","ttl":-1}

curl http://localhost:8080/api/v1/dbsize
# {"dbsize":1}
```

The HTTP API covers all the major data types. Check the [README](../README.md) for the full list of endpoints.

## Expiry and notifications

Set a key with a TTL:

```
127.0.0.1:7379> SET session:abc user:1 EX 10
OK
127.0.0.1:7379> TTL session:abc
(integer) 9
```

In about 10 seconds, that key will disappear. If you want to know when it happens, subscribe to expiry events:

```
127.0.0.1:7379> SUBSCRIBE __keyevent__:expired
```

Or register a webhook:

```
127.0.0.1:7379> WEBHOOK REGISTER session:* expired http://localhost:9090/hook
OK
```

When `session:abc` expires, Vole will POST to that URL with the key name and timestamp.

## Namespaces

If you want to keep different datasets separate without running multiple servers:

```
127.0.0.1:7379> NAMESPACE CREATE staging
OK
127.0.0.1:7379> NAMESPACE USE staging
OK
127.0.0.1:7379> SET secret "staging-only data"
OK
127.0.0.1:7379> NAMESPACE USE default
OK
127.0.0.1:7379> GET secret
(nil)
```

The key only exists in the `staging` namespace.

## Persistence in action

Everything you've done so far is being saved to `vole.aof`. To prove it, stop the server and start it again:

```bash
vole
```

Your data is all still there:

```bash
vole-cli GET greeting
# "hello from vole"
```

For additional safety, enable periodic snapshots:

```bash
vole --snapshot vole.snap --snapshot-interval 5m
```

Now Vole takes a full snapshot every 5 minutes and trims the AOF. On startup, it loads the snapshot (fast) and replays only the recent AOF entries.

## Multi-master replication

This is where Vole gets interesting. Most data stores make you pick one node that handles writes and the rest are read-only copies. If the writer goes down, you're stuck doing a manual promotion while your app throws errors.

Vole doesn't work that way. Every node can accept writes, and they all stay in sync.

### Setting it up

You need at least two nodes. They can be on the same machine (different ports) or on different servers. Each node needs a stable ID so the others can recognize it.

**Terminal 1 -- start the first node:**

```bash
vole --addr :7379 --node-id node1 --peers "node2@localhost:7380" --multimaster
```

**Terminal 2 -- start the second node:**

```bash
vole --addr :7380 --node-id node2 --peers "node1@localhost:7379" --multimaster
```

That's it. They find each other, exchange data, and start streaming writes in both directions.

### Verify it works

Write something on node 1:

```bash
vole-cli -p 7379 SET greeting "hello from node 1"
```

Read it on node 2:

```bash
vole-cli -p 7380 GET greeting
# "hello from node 1"
```

Now write on node 2:

```bash
vole-cli -p 7380 SET farewell "goodbye from node 2"
```

And read it on node 1:

```bash
vole-cli -p 7379 GET farewell
# "goodbye from node 2"
```

Both nodes accept writes. Both nodes have all the data.

### Adding a third node

You can add nodes at any time without stopping the cluster. Start the third node:

```bash
vole --addr :7381 --node-id node3 --multimaster
```

Then tell it about an existing node:

```bash
vole-cli -p 7381 CLUSTER MEET localhost:7379
vole-cli -p 7381 MULTIMASTER ENABLE
```

The new node gets a snapshot from the peer and starts streaming. You can also do this from any existing node:

```bash
vole-cli -p 7379 CLUSTER MEET localhost:7381
```

### Check the cluster state

From any node:

```bash
vole-cli -p 7379 MULTIMASTER STATUS
# 1) "enabled"
# 2) "true"
# 3) "peers"
# 4) (integer) 2

vole-cli -p 7379 MULTIMASTER PEERS
# Lists each connected peer with its ID and address

vole-cli -p 7379 CLUSTER NODES
# Shows all nodes, their slot ranges, and health state
```

### On different machines

Same thing, just use real hostnames or IPs instead of localhost:

```bash
# Machine A (10.0.0.1)
vole --addr 0.0.0.0:7379 --node-id node1 \
  --peers "node2@10.0.0.2:7379,node3@10.0.0.3:7379" --multimaster

# Machine B (10.0.0.2)
vole --addr 0.0.0.0:7379 --node-id node2 \
  --peers "node1@10.0.0.1:7379,node3@10.0.0.3:7379" --multimaster

# Machine C (10.0.0.3)
vole --addr 0.0.0.0:7379 --node-id node3 \
  --peers "node1@10.0.0.1:7379,node2@10.0.0.2:7379" --multimaster
```

### Using a config file

For production, you'd probably put this in a config file rather than passing a dozen flags:

```
# /etc/vole/vole.conf
addr 0.0.0.0:7379
node-id node1
peers node2@10.0.0.2:7379,node3@10.0.0.3:7379
multimaster true

appendonly true
appendfilename /var/lib/vole/vole.aof
snapshot /var/lib/vole/vole.snap
snapshot-interval 5m
```

```bash
vole --config /etc/vole/vole.conf
```

### What to expect

- **Writes go everywhere.** A SET on any node shows up on all the others within milliseconds (over a local network).
- **Nodes can come and go.** If a node goes down, the others keep working. When it comes back, it reconnects and catches up.
- **Conflicts are last-writer-wins.** If two nodes write to the same key at the same instant, whichever write arrives last takes precedence. There's no merge. Design your key scheme to avoid this -- use node-specific prefixes or UUIDs if concurrent writes to the same key are possible.
- **Persistence still works.** Each node has its own AOF and snapshots. The data survives restarts on every node independently.

### Disabling multi-master

If you want to go back to standalone or leader-follower mode:

```bash
vole-cli MULTIMASTER DISABLE
```

The node keeps its data but stops propagating to and receiving from peers.

## Using a config file

Once you know which settings you want, you can put them in a file instead of passing flags every time:

```bash
cp vole.conf.example /etc/vole/vole.conf
```

Edit the file, then start Vole with:

```bash
vole --config /etc/vole/vole.conf
```

Anything you pass as a CLI flag overrides the config file, so you can keep your baseline in the file and tweak things on the fly.

## Where to go next

- [Configuration guide](configuration.md) -- every flag and config option explained, with examples
- [Installation guide](installation.md) -- systemd, Docker, upgrading
- [README](../README.md) -- full command reference, HTTP API docs, and architecture overview
- [Contributing](../CONTRIBUTING.md) -- if you want to dig into the code or fix something
