# Contributing to Vole

Hey, welcome. We appreciate you taking the time to look at this.

Vole is still young and there's plenty of room to shape it. If you've found a bug, have an idea, or just want to clean something up -- jump in.

## Getting started

You'll need Go 1.22 or later. That's the only requirement. Seriously.

```bash
# Clone the repo
git clone https://github.com/your-org/vole.git
cd vole

# Build both binaries
go build -o vole ./cmd/vole
go build -o vole-cli ./cmd/vole-cli

# Run the tests
go test ./... -race

# Run the benchmarks
go test -bench=. -benchmem ./internal/store/
```

That's it. No `make`, no Docker, no dependency managers. If `go build` works, you're ready.

## Project layout

```
cmd/
  vole/           Server entry point and CLI flags
  vole-cli/       Interactive client and single-command runner

internal/
  resp/           RESP protocol reader/writer
  store/          In-memory data structures (the core)
    store.go        Main store with all data type operations
    deque.go        Ring-buffer deque for lists
    sortedset.go    Maintained-order sorted set
    hyperloglog.go  Probabilistic cardinality estimator
    jsonstore.go    JSON document type with path-based access
    timeseries.go   Time-series data type
    queue.go        Reliable message queue
    geo.go          Geospatial encoding and search
    bench_test.go   Benchmarks
    store_test.go   Store tests
  server/
    server.go       Command dispatch, connection handling
    aof.go          Append-only file persistence
    snapshot.go     Point-in-time snapshot persistence
    cluster.go      Cluster coordination and health
    pubsub.go       Pub/sub messaging
    http.go         HTTP/JSON API
    replication.go  Leader-follower replication
    namespace.go    Named namespace isolation
    scripting.go    EVAL/EVALSHA script execution
    cron.go         Scheduled task runner
    audit.go        Mutation audit log
    schema.go       Hash field validation
    webhooks.go     Expiry and mutation webhooks
    clients.go      Client connection tracking
    slowlog.go      Slow query log
    metrics.go      Prometheus metrics
```

## How to contribute

### Found a bug?

Open an issue. Tell us what you did, what you expected, and what actually happened. If you can give us the exact sequence of commands to reproduce it, that saves everyone a lot of back-and-forth.

### Have an idea?

Open an issue. Tell us about the problem first, not just the solution. Sometimes there's a simpler way to get there that neither of us has thought of yet.

### Want to write some code?

1. Fork the repo, make a branch off `main`.
2. Write the code. Look at how the files around yours are written and try to match the style.
3. Add tests. New commands should have both a store-level test (in `store_test.go`) and an integration test (in `commands_test.go`).
4. Run `go test ./... -race`. Everything should pass.
5. `go vet ./...` too, while you're at it.
6. Open a PR. Tell us what it does and why you made the choices you made.

We'll take a look, ask questions if something's not clear, and get it merged.

### What does a good PR look like?

Keep it small. One fix or one feature. If you find yourself writing "also while I was in here I..." that's probably two PRs.

Tests matter. If you're adding a command, test it. If you're fixing a bug, add a test that would have caught it. If you're not sure what to test, ask in the issue.

Don't reformat code you didn't touch, and don't add comments that just repeat what the code says. We'd rather read clean code than clean comments.

Try not to break existing commands. If you need to change how something works, flag it in the PR description so we can talk about it.

### Code style

We don't have a style guide. We have `gofmt` and a few conventions:

- Run `gofmt` before you push. Non-negotiable.
- No external dependencies. The `go.mod` is empty on purpose. We'd like to keep it that way.
- Error messages are lowercase. e.g., `"stream ID must be greater than previous ID"`.
- Store methods handle their own locking. Callers shouldn't need to think about it.
- Reads take `RLock`, writes take `Lock`. Getting this wrong causes real bugs.

### Adding a new command

Say you're adding a command called `FROBNICATE`. Here's the path through the codebase:

1. **Store method** in `internal/store/store.go`. This is where the actual logic lives. Pick `RLock` for reads, `Lock` for writes. If it modifies data, call `touchKeyLocked()` at the end.

2. **Handler** in `internal/server/server.go`. Add `case "FROBNICATE":` to the `exec` switch. Parse the arguments, call your store method, write the response.

3. **Persistence** in `internal/server/aof.go`. If it's a write, append it to the AOF and add a replay case so the data comes back after a restart.

4. **Tests**. One in `store_test.go` for the logic, one in `commands_test.go` for the end-to-end RESP flow.

5. **Housekeeping**. Add it to `isWriteCommand()` if it's a write (replication and audit use this). If it takes a key argument, add it to the prefix map in `namespace.go`.

### Running a local server for testing

```bash
# Start Vole with everything turned on
./vole --addr :7379 --http-addr :8080 --appendonly --snapshot vole.snap

# In another terminal
./vole-cli
127.0.0.1:7379> PING
PONG
```

## A note on community

We're building this in the open because we think it's more fun that way. Be decent to each other. There's a real person behind every GitHub handle.

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for the full version of that sentiment.

## License

Contributions are licensed under the same BSD 3-Clause License as the rest of the project.
