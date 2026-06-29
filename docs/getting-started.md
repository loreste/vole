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

## Where to go next

- [Configuration guide](configuration.md) -- every flag explained, with examples
- [README](../README.md) -- full command reference, HTTP API docs, and architecture overview
- [Contributing](../CONTRIBUTING.md) -- if you want to dig into the code or fix something
