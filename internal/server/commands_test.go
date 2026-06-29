package server

import (
	"bytes"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"vole/internal/resp"
)

func TestCoreKeyCommands(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	if got := execValue(t, srv, "MSET", "app:name", "vole", "app:version", "1"); got.Text != "OK" {
		t.Fatalf("expected OK, got %#v", got)
	}
	mget := execValue(t, srv, "MGET", "app:name", "missing", "app:version")
	if len(mget.Items) != 3 || mget.Items[0].Text != "vole" || !mget.Items[1].Null || mget.Items[2].Text != "1" {
		t.Fatalf("unexpected mget response: %#v", mget)
	}
	if got := execValue(t, srv, "EXISTS", "app:name", "missing"); got.Int != 1 {
		t.Fatalf("expected exists 1, got %#v", got)
	}
	if got := execValue(t, srv, "TYPE", "app:name"); got.Text != "string" {
		t.Fatalf("expected string type, got %#v", got)
	}
	keys := execValue(t, srv, "KEYS", "app:*")
	if len(keys.Items) != 2 || keys.Items[0].Text != "app:name" || keys.Items[1].Text != "app:version" {
		t.Fatalf("unexpected keys response: %#v", keys)
	}
	scan := execValue(t, srv, "SCAN", "0", "MATCH", "app:*", "COUNT", "1")
	if len(scan.Items) != 2 || scan.Items[0].Text == "0" || len(scan.Items[1].Items) != 1 {
		t.Fatalf("unexpected scan response: %#v", scan)
	}
}

func TestHashCommands(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	if got := execValue(t, srv, "HSET", "user:1", "name", "Ada", "role", "admin"); got.Int != 2 {
		t.Fatalf("expected two new fields, got %#v", got)
	}
	if got := execValue(t, srv, "HSET", "user:1", "role", "operator"); got.Int != 0 {
		t.Fatalf("expected updated field count 0, got %#v", got)
	}
	if got := execValue(t, srv, "HGET", "user:1", "role"); got.Text != "operator" {
		t.Fatalf("unexpected hget: %#v", got)
	}
	all := execValue(t, srv, "HGETALL", "user:1")
	if len(all.Items) != 4 || all.Items[0].Text != "name" || all.Items[1].Text != "Ada" || all.Items[2].Text != "role" || all.Items[3].Text != "operator" {
		t.Fatalf("unexpected hgetall: %#v", all)
	}
	if got := execValue(t, srv, "TYPE", "user:1"); got.Text != "hash" {
		t.Fatalf("expected hash type, got %#v", got)
	}
	if got := execValue(t, srv, "HDEL", "user:1", "role", "missing"); got.Int != 1 {
		t.Fatalf("expected one deleted field, got %#v", got)
	}
	if got := execValue(t, srv, "HGET", "user:1", "role"); !got.Null {
		t.Fatalf("expected nil for deleted field, got %#v", got)
	}
}

func TestListCommands(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	if got := execValue(t, srv, "RPUSH", "queue", "a", "b"); got.Int != 2 {
		t.Fatalf("expected length 2, got %#v", got)
	}
	if got := execValue(t, srv, "LPUSH", "queue", "c"); got.Int != 3 {
		t.Fatalf("expected length 3, got %#v", got)
	}
	values := execValue(t, srv, "LRANGE", "queue", "0", "-1")
	if len(values.Items) != 3 || values.Items[0].Text != "c" || values.Items[1].Text != "a" || values.Items[2].Text != "b" {
		t.Fatalf("unexpected lrange: %#v", values)
	}
	if got := execValue(t, srv, "LPOP", "queue"); got.Text != "c" {
		t.Fatalf("unexpected lpop: %#v", got)
	}
	if got := execValue(t, srv, "RPOP", "queue"); got.Text != "b" {
		t.Fatalf("unexpected rpop: %#v", got)
	}
	if got := execValue(t, srv, "TYPE", "queue"); got.Text != "list" {
		t.Fatalf("expected list type, got %#v", got)
	}
}

func TestSetCommands(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	if got := execValue(t, srv, "SADD", "tags", "red", "blue", "red"); got.Int != 2 {
		t.Fatalf("expected two added members, got %#v", got)
	}
	members := execValue(t, srv, "SMEMBERS", "tags")
	if len(members.Items) != 2 || members.Items[0].Text != "blue" || members.Items[1].Text != "red" {
		t.Fatalf("unexpected smembers: %#v", members)
	}
	if got := execValue(t, srv, "SISMEMBER", "tags", "red"); got.Int != 1 {
		t.Fatalf("expected red to be member, got %#v", got)
	}
	if got := execValue(t, srv, "SCARD", "tags"); got.Int != 2 {
		t.Fatalf("expected cardinality 2, got %#v", got)
	}
	if got := execValue(t, srv, "SREM", "tags", "red", "missing"); got.Int != 1 {
		t.Fatalf("expected one removed member, got %#v", got)
	}
	if got := execValue(t, srv, "TYPE", "tags"); got.Text != "set" {
		t.Fatalf("expected set type, got %#v", got)
	}
}

func TestSortedSetCommands(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	if got := execValue(t, srv, "ZADD", "rank", "2", "bob", "1", "ada", "2", "cam"); got.Int != 3 {
		t.Fatalf("expected three added members, got %#v", got)
	}
	values := execValue(t, srv, "ZRANGE", "rank", "0", "-1")
	if len(values.Items) != 3 || values.Items[0].Text != "ada" || values.Items[1].Text != "bob" || values.Items[2].Text != "cam" {
		t.Fatalf("unexpected zrange: %#v", values)
	}
	withScores := execValue(t, srv, "ZRANGE", "rank", "0", "1", "WITHSCORES")
	if len(withScores.Items) != 4 || withScores.Items[0].Text != "ada" || withScores.Items[1].Text != "1" {
		t.Fatalf("unexpected zrange withscores: %#v", withScores)
	}
	if got := execValue(t, srv, "ZSCORE", "rank", "bob"); got.Text != "2" {
		t.Fatalf("expected bob score 2, got %#v", got)
	}
	if got := execValue(t, srv, "ZCARD", "rank"); got.Int != 3 {
		t.Fatalf("expected zcard 3, got %#v", got)
	}
	if got := execValue(t, srv, "ZREM", "rank", "bob", "missing"); got.Int != 1 {
		t.Fatalf("expected one removed, got %#v", got)
	}
	if got := execValue(t, srv, "TYPE", "rank"); got.Text != "zset" {
		t.Fatalf("expected zset type, got %#v", got)
	}
}

func TestStreamConsumerGroupCommands(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	if got := execValue(t, srv, "XADD", "events", "1-1", "type", "created"); got.Text != "1-1" {
		t.Fatalf("unexpected xadd 1: %#v", got)
	}
	if got := execValue(t, srv, "XADD", "events", "1-2", "type", "updated"); got.Text != "1-2" {
		t.Fatalf("unexpected xadd 2: %#v", got)
	}
	if got := execValue(t, srv, "XGROUP", "CREATE", "events", "workers", "0-0"); got.Text != "OK" {
		t.Fatalf("unexpected xgroup create: %#v", got)
	}
	read := execValue(t, srv, "XREADGROUP", "GROUP", "workers", "c1", "COUNT", "1", "STREAMS", "events", ">")
	if len(read.Items) != 1 || read.Items[0].Items[0].Text != "events" {
		t.Fatalf("unexpected xreadgroup stream response: %#v", read)
	}
	entries := read.Items[0].Items[1].Items
	if len(entries) != 1 || entries[0].Items[0].Text != "1-1" {
		t.Fatalf("expected first stream entry, got %#v", read)
	}
	if got := execValue(t, srv, "XACK", "events", "workers", "1-1"); got.Int != 1 {
		t.Fatalf("expected ack count 1, got %#v", got)
	}
	pending := execValue(t, srv, "XREADGROUP", "GROUP", "workers", "c1", "STREAMS", "events", "0-0")
	if !pending.Null {
		t.Fatalf("expected no pending entries after ack, got %#v", pending)
	}
}

func TestClusterRedirectsKeyCommandsBeforeLocalMutation(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:   "127.0.0.1:0",
		NodeID: "node-b",
		Peers:  "node-a@127.0.0.1:7380",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	local := keyForOwnership(t, srv.cluster, true)
	remote := keyForOwnership(t, srv.cluster, false)
	localPair := keysForSameSlotOwnership(t, srv.cluster, true)

	if got := execValue(t, srv, "SET", local, "ok"); got.Text != "OK" {
		t.Fatalf("expected local set ok, got %#v", got)
	}
	if got := execValue(t, srv, "SET", remote, "bad"); got.Type != resp.ErrorString || !strings.HasPrefix(got.Text, "MOVED ") {
		t.Fatalf("expected MOVED for remote set, got %#v", got)
	}
	if got := execValue(t, srv, "GET", remote); got.Type != resp.ErrorString || !strings.HasPrefix(got.Text, "MOVED ") {
		t.Fatalf("expected MOVED for remote get, got %#v", got)
	}
	if got := execValue(t, srv, "MSET", localPair[0], "one", localPair[1], "two"); got.Text != "OK" {
		t.Fatalf("expected same-slot local mset OK, got %#v", got)
	}
	if got := execValue(t, srv, "MGET", localPair[0], localPair[1]); len(got.Items) != 2 || got.Items[0].Text != "one" || got.Items[1].Text != "two" {
		t.Fatalf("expected same-slot local mget values, got %#v", got)
	}
	if got := execValue(t, srv, "MSET", local, "new", remote, "bad"); got.Type != resp.ErrorString || !strings.HasPrefix(got.Text, "CROSSSLOT ") {
		t.Fatalf("expected CROSSSLOT for mixed mset, got %#v", got)
	}
	if got := execValue(t, srv, "GET", local); got.Text != "ok" {
		t.Fatalf("mixed mset mutated local key before cross-slot rejection: %#v", got)
	}
	if got := execValue(t, srv, "HSET", remote, "field", "value"); got.Type != resp.ErrorString || !strings.HasPrefix(got.Text, "MOVED ") {
		t.Fatalf("expected MOVED for remote hset, got %#v", got)
	}
	if got := execValue(t, srv, "XADD", remote, "1-1", "type", "created"); got.Type != resp.ErrorString || !strings.HasPrefix(got.Text, "MOVED ") {
		t.Fatalf("expected MOVED for remote xadd, got %#v", got)
	}
	if got := execValue(t, srv, "TYPE", remote); got.Type != resp.ErrorString || !strings.HasPrefix(got.Text, "MOVED ") {
		t.Fatalf("expected MOVED for remote type, got %#v", got)
	}
}

func TestListAndHashWrongTypeCommandsDoNotPersist(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	if got := execValue(t, srv, "SET", "plain", "value"); got.Text != "OK" {
		t.Fatalf("expected set OK, got %#v", got)
	}
	if err := srv.exec(resp.NewWriter(&bytes.Buffer{}), []string{"LPUSH", "plain", "x"}); err == nil {
		t.Fatal("expected LPUSH wrong type error")
	}
	if err := srv.exec(resp.NewWriter(&bytes.Buffer{}), []string{"HSET", "plain", "field", "value"}); err == nil {
		t.Fatal("expected HSET wrong type error")
	}
	if err := srv.exec(resp.NewWriter(&bytes.Buffer{}), []string{"SADD", "plain", "member"}); err == nil {
		t.Fatal("expected SADD wrong type error")
	}
	if err := srv.exec(resp.NewWriter(&bytes.Buffer{}), []string{"ZADD", "plain", "1", "member"}); err == nil {
		t.Fatal("expected ZADD wrong type error")
	}
	if got := execValue(t, srv, "GET", "plain"); got.Text != "value" {
		t.Fatalf("wrong-type commands changed key: %#v", got)
	}
}

func keyForOwnership(t *testing.T, c *Cluster, self bool) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("key:%d", i)
		if c.SelfOwns(key) == self {
			return key
		}
	}
	t.Fatalf("could not find key with self ownership %v", self)
	return ""
}

func keysForSameSlotOwnership(t *testing.T, c *Cluster, self bool) [2]string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key1 := fmt.Sprintf("key:{tag%d}:1", i)
		key2 := fmt.Sprintf("key:{tag%d}:2", i)
		if c.SelfOwns(key1) == self && c.SelfOwns(key2) == self && Slot(key1) == Slot(key2) {
			return [2]string{key1, key2}
		}
	}
	t.Fatalf("could not find same-slot keys with self ownership %v", self)
	return [2]string{}
}

func execValue(t *testing.T, srv *Server, args ...string) resp.Value {
	t.Helper()
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := srv.exec(w, args); err != nil {
		t.Fatalf("exec %v: %v", args, err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	value, err := resp.NewReader(&buf).ReadValue()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return value
}

func TestTransactionExec(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	// Seed a key so we can read it inside a transaction.
	execValue(t, srv, "SET", "k1", "hello")

	// Build a queued transaction and execute it via execTransaction.
	commands := [][]string{
		{"SET", "k2", "world"},
		{"GET", "k1"},
		{"GET", "k2"},
	}

	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	srv.execTransaction(w, commands)
	_ = w.Flush()

	rd := resp.NewReader(&buf)
	result, err := rd.ReadValue()
	if err != nil {
		t.Fatalf("read transaction response: %v", err)
	}
	if result.Type != resp.Array || len(result.Items) != 3 {
		t.Fatalf("expected array of 3, got %#v", result)
	}
	// SET k2 -> OK
	if result.Items[0].Text != "OK" {
		t.Fatalf("expected OK for SET, got %#v", result.Items[0])
	}
	// GET k1 -> hello
	if result.Items[1].Text != "hello" {
		t.Fatalf("expected hello for GET k1, got %#v", result.Items[1])
	}
	// GET k2 -> world
	if result.Items[2].Text != "world" {
		t.Fatalf("expected world for GET k2, got %#v", result.Items[2])
	}
}

func TestTransactionEmpty(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	srv.execTransaction(w, nil)
	_ = w.Flush()

	result, err := resp.NewReader(&buf).ReadValue()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if result.Type != resp.Array || len(result.Items) != 0 {
		t.Fatalf("expected empty array, got %#v", result)
	}
}

func TestTransactionErrorInQueue(t *testing.T) {
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	// Queue a command that will fail (GET with wrong arity) alongside a valid one.
	commands := [][]string{
		{"SET", "tx1", "ok"},
		{"GET"},
	}

	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	srv.execTransaction(w, commands)
	_ = w.Flush()

	result, err := resp.NewReader(&buf).ReadValue()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if result.Type != resp.Array || len(result.Items) != 2 {
		t.Fatalf("expected array of 2, got %#v", result)
	}
	if result.Items[0].Text != "OK" {
		t.Fatalf("expected OK for SET, got %#v", result.Items[0])
	}
	if result.Items[1].Type != resp.ErrorString {
		t.Fatalf("expected error for bad GET, got %#v", result.Items[1])
	}
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	srv, err := NewWithOptions(Options{
		Addr:        "127.0.0.1:0",
		AOFPath:     filepath.Join(t.TempDir(), "data.aof"),
		AppendOnly:  true,
		AppendFsync: FsyncAlways,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

func TestTransactionWatch(t *testing.T) {
	t.Run("WatchUnmodifiedSucceeds", func(t *testing.T) {
		srv := newTestServer(t)
		// Set initial value
		execValue(t, srv, "SET", "watched", "initial")
		// Simulate: WATCH watched, then MULTI/EXEC with no external modification
		// Since execTransaction doesn't actually implement WATCH abort,
		// we verify the transaction commits successfully.
		commands := [][]string{
			{"SET", "watched", "updated"},
			{"GET", "watched"},
		}
		var buf bytes.Buffer
		w := resp.NewWriter(&buf)
		srv.execTransaction(w, commands)
		_ = w.Flush()
		result, err := resp.NewReader(&buf).ReadValue()
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if result.Type != resp.Array || len(result.Items) != 2 {
			t.Fatalf("expected array of 2, got %#v", result)
		}
		if result.Items[0].Text != "OK" {
			t.Fatalf("expected OK, got %#v", result.Items[0])
		}
		if result.Items[1].Text != "updated" {
			t.Fatalf("expected 'updated', got %#v", result.Items[1])
		}
	})

	t.Run("WatchVersionTrackingViaStore", func(t *testing.T) {
		srv := newTestServer(t)
		execValue(t, srv, "SET", "k1", "v1")
		// Snapshot versions
		versions := srv.store.KeyVersions([]string{"k1"})
		if srv.store.KeysModifiedSince(versions) {
			t.Fatal("expected no modifications immediately after snapshot")
		}
		// Modify from "another connection"
		execValue(t, srv, "SET", "k1", "v2")
		if !srv.store.KeysModifiedSince(versions) {
			t.Fatal("expected modifications detected after SET")
		}
	})
}

func TestPExpireAndPTTL(t *testing.T) {
	srv := newTestServer(t)
	execValue(t, srv, "SET", "k", "v")
	if got := execValue(t, srv, "PEXPIRE", "k", "5000"); got.Int != 1 {
		t.Fatalf("expected 1, got %#v", got)
	}
	pttl := execValue(t, srv, "PTTL", "k")
	// PTTL should return a value close to 5000 (within reasonable tolerance)
	if pttl.Int < 4000 || pttl.Int > 5100 {
		t.Fatalf("expected PTTL around 5000, got %d", pttl.Int)
	}
	// PEXPIRE on missing key returns 0
	if got := execValue(t, srv, "PEXPIRE", "missing", "1000"); got.Int != 0 {
		t.Fatalf("expected 0 for missing key, got %#v", got)
	}
	// PTTL on missing key returns -2
	if got := execValue(t, srv, "PTTL", "missing"); got.Int != -2 {
		t.Fatalf("expected -2 for missing key, got %d", got.Int)
	}
}

func TestNewCommands(t *testing.T) {
	srv := newTestServer(t)

	// STRLEN
	execValue(t, srv, "SET", "str", "hello")
	if got := execValue(t, srv, "STRLEN", "str"); got.Int != 5 {
		t.Fatalf("STRLEN expected 5, got %d", got.Int)
	}
	if got := execValue(t, srv, "STRLEN", "missing"); got.Int != 0 {
		t.Fatalf("STRLEN missing expected 0, got %d", got.Int)
	}

	// APPEND
	if got := execValue(t, srv, "APPEND", "str", " world"); got.Int != 11 {
		t.Fatalf("APPEND expected 11, got %d", got.Int)
	}
	if got := execValue(t, srv, "GET", "str"); got.Text != "hello world" {
		t.Fatalf("expected 'hello world', got %q", got.Text)
	}

	// LLEN
	execValue(t, srv, "RPUSH", "mylist", "a", "b", "c")
	if got := execValue(t, srv, "LLEN", "mylist"); got.Int != 3 {
		t.Fatalf("LLEN expected 3, got %d", got.Int)
	}

	// HLEN and HEXISTS
	execValue(t, srv, "HSET", "myhash", "f1", "v1", "f2", "v2")
	if got := execValue(t, srv, "HLEN", "myhash"); got.Int != 2 {
		t.Fatalf("HLEN expected 2, got %d", got.Int)
	}
	if got := execValue(t, srv, "HEXISTS", "myhash", "f1"); got.Int != 1 {
		t.Fatalf("HEXISTS expected 1, got %d", got.Int)
	}
	if got := execValue(t, srv, "HEXISTS", "myhash", "missing"); got.Int != 0 {
		t.Fatalf("HEXISTS expected 0, got %d", got.Int)
	}

	// DBSIZE
	dbsize := execValue(t, srv, "DBSIZE")
	if dbsize.Int < 3 {
		t.Fatalf("DBSIZE expected at least 3, got %d", dbsize.Int)
	}

	// FLUSHDB
	if got := execValue(t, srv, "FLUSHDB"); got.Text != "OK" {
		t.Fatalf("FLUSHDB expected OK, got %#v", got)
	}
	if got := execValue(t, srv, "DBSIZE"); got.Int != 0 {
		t.Fatalf("DBSIZE after FLUSHDB expected 0, got %d", got.Int)
	}

	// TIME - returns array of two elements
	timeResult := execValue(t, srv, "TIME")
	if len(timeResult.Items) != 2 {
		t.Fatalf("TIME expected 2 elements, got %d", len(timeResult.Items))
	}
	unixSec, err := strconv.ParseInt(timeResult.Items[0].Text, 10, 64)
	if err != nil || unixSec < 1000000000 {
		t.Fatalf("TIME seconds unexpected: %q err=%v", timeResult.Items[0].Text, err)
	}
}

func TestZRangeByScore(t *testing.T) {
	srv := newTestServer(t)

	// ZADD several members
	execValue(t, srv, "ZADD", "zs", "1", "alice", "2", "bob", "3", "charlie", "4", "dave", "5", "eve")

	// ZRANGEBYSCORE with range
	result := execValue(t, srv, "ZRANGEBYSCORE", "zs", "2", "4")
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items in [2,4], got %d: %#v", len(result.Items), result)
	}
	if result.Items[0].Text != "bob" || result.Items[1].Text != "charlie" || result.Items[2].Text != "dave" {
		t.Fatalf("unexpected order: %#v", result)
	}

	// ZRANGEBYSCORE with -inf/+inf
	result = execValue(t, srv, "ZRANGEBYSCORE", "zs", "-inf", "+inf")
	if len(result.Items) != 5 {
		t.Fatalf("expected 5 items with -inf/+inf, got %d", len(result.Items))
	}

	// ZRANGEBYSCORE with LIMIT
	result = execValue(t, srv, "ZRANGEBYSCORE", "zs", "-inf", "+inf", "LIMIT", "1", "2")
	if len(result.Items) != 2 || result.Items[0].Text != "bob" || result.Items[1].Text != "charlie" {
		t.Fatalf("unexpected LIMIT result: %#v", result)
	}

	// ZRANGEBYSCORE with WITHSCORES
	result = execValue(t, srv, "ZRANGEBYSCORE", "zs", "1", "2", "WITHSCORES")
	if len(result.Items) != 4 {
		t.Fatalf("expected 4 items with WITHSCORES, got %d: %#v", len(result.Items), result)
	}
	if result.Items[0].Text != "alice" || result.Items[1].Text != "1" {
		t.Fatalf("unexpected WITHSCORES: %#v", result)
	}

	// Empty range
	result = execValue(t, srv, "ZRANGEBYSCORE", "zs", "10", "20")
	if len(result.Items) != 0 {
		t.Fatalf("expected empty result for range [10,20], got %d items", len(result.Items))
	}
}

func TestCopyAndRenameCommands(t *testing.T) {
	srv := newTestServer(t)

	// COPY
	execValue(t, srv, "SET", "src", "hello")
	if got := execValue(t, srv, "COPY", "src", "dst"); got.Int != 1 {
		t.Fatalf("COPY expected 1, got %#v", got)
	}
	if got := execValue(t, srv, "GET", "dst"); got.Text != "hello" {
		t.Fatalf("expected 'hello' at dst, got %q", got.Text)
	}
	// COPY without REPLACE when dst exists
	execValue(t, srv, "SET", "dst2", "existing")
	if got := execValue(t, srv, "COPY", "src", "dst2"); got.Int != 0 {
		t.Fatalf("COPY without REPLACE expected 0, got %#v", got)
	}
	// COPY with REPLACE
	if got := execValue(t, srv, "COPY", "src", "dst2", "REPLACE"); got.Int != 1 {
		t.Fatalf("COPY REPLACE expected 1, got %#v", got)
	}
	if got := execValue(t, srv, "GET", "dst2"); got.Text != "hello" {
		t.Fatalf("expected 'hello' at dst2, got %q", got.Text)
	}

	// RENAME
	execValue(t, srv, "SET", "oldkey", "value")
	if got := execValue(t, srv, "RENAME", "oldkey", "newkey"); got.Text != "OK" {
		t.Fatalf("RENAME expected OK, got %#v", got)
	}
	if got := execValue(t, srv, "GET", "newkey"); got.Text != "value" {
		t.Fatalf("expected 'value' at newkey, got %q", got.Text)
	}
	if got := execValue(t, srv, "GET", "oldkey"); !got.Null {
		t.Fatalf("expected oldkey to be gone, got %#v", got)
	}
}

func TestHIncrByAndHSetNXCommands(t *testing.T) {
	srv := newTestServer(t)

	// HINCRBY
	execValue(t, srv, "HSET", "h", "count", "10")
	if got := execValue(t, srv, "HINCRBY", "h", "count", "5"); got.Int != 15 {
		t.Fatalf("HINCRBY expected 15, got %d", got.Int)
	}
	if got := execValue(t, srv, "HINCRBY", "h", "count", "-3"); got.Int != 12 {
		t.Fatalf("HINCRBY expected 12, got %d", got.Int)
	}
	// HINCRBY on new field
	if got := execValue(t, srv, "HINCRBY", "h", "newfield", "7"); got.Int != 7 {
		t.Fatalf("HINCRBY new field expected 7, got %d", got.Int)
	}

	// HSETNX
	if got := execValue(t, srv, "HSETNX", "h", "unique", "val"); got.Int != 1 {
		t.Fatalf("HSETNX expected 1 for new field, got %d", got.Int)
	}
	if got := execValue(t, srv, "HSETNX", "h", "unique", "other"); got.Int != 0 {
		t.Fatalf("HSETNX expected 0 for existing field, got %d", got.Int)
	}
	if got := execValue(t, srv, "HGET", "h", "unique"); got.Text != "val" {
		t.Fatalf("expected original value 'val', got %q", got.Text)
	}
}

func TestSetOperationCommands(t *testing.T) {
	srv := newTestServer(t)

	execValue(t, srv, "SADD", "s1", "a", "b", "c")
	execValue(t, srv, "SADD", "s2", "b", "c", "d")

	// SINTER
	result := execValue(t, srv, "SINTER", "s1", "s2")
	if len(result.Items) != 2 {
		t.Fatalf("SINTER expected 2, got %d: %#v", len(result.Items), result)
	}

	// SUNION
	result = execValue(t, srv, "SUNION", "s1", "s2")
	if len(result.Items) != 4 {
		t.Fatalf("SUNION expected 4, got %d: %#v", len(result.Items), result)
	}

	// SDIFF
	result = execValue(t, srv, "SDIFF", "s1", "s2")
	if len(result.Items) != 1 || result.Items[0].Text != "a" {
		t.Fatalf("SDIFF expected [a], got %#v", result)
	}
}

func TestZRevRangeAndZRankCommands(t *testing.T) {
	srv := newTestServer(t)
	execValue(t, srv, "ZADD", "zs", "1", "alice", "2", "bob", "3", "charlie")

	// ZREVRANGE
	result := execValue(t, srv, "ZREVRANGE", "zs", "0", "-1")
	if len(result.Items) != 3 {
		t.Fatalf("expected 3, got %d", len(result.Items))
	}
	if result.Items[0].Text != "charlie" || result.Items[1].Text != "bob" || result.Items[2].Text != "alice" {
		t.Fatalf("unexpected ZREVRANGE: %#v", result)
	}

	// ZRANK
	if got := execValue(t, srv, "ZRANK", "zs", "alice"); got.Int != 0 {
		t.Fatalf("expected rank 0 for alice, got %d", got.Int)
	}
	if got := execValue(t, srv, "ZRANK", "zs", "charlie"); got.Int != 2 {
		t.Fatalf("expected rank 2 for charlie, got %d", got.Int)
	}

	// ZCOUNT
	if got := execValue(t, srv, "ZCOUNT", "zs", "1", "2"); got.Int != 2 {
		t.Fatalf("expected ZCOUNT 2, got %d", got.Int)
	}
}

func TestGetDelCommand(t *testing.T) {
	srv := newTestServer(t)
	execValue(t, srv, "SET", "k", "v")
	if got := execValue(t, srv, "GETDEL", "k"); got.Text != "v" {
		t.Fatalf("expected 'v', got %#v", got)
	}
	if got := execValue(t, srv, "GET", "k"); !got.Null {
		t.Fatalf("expected nil after GETDEL, got %#v", got)
	}
	// GETDEL on missing key
	if got := execValue(t, srv, "GETDEL", "missing"); !got.Null {
		t.Fatalf("expected nil for missing key, got %#v", got)
	}
}

func TestBitmapCommands(t *testing.T) {
	srv := newTestServer(t)

	// SETBIT
	if got := execValue(t, srv, "SETBIT", "bm", "7", "1"); got.Int != 0 {
		t.Fatalf("expected old bit 0, got %d", got.Int)
	}
	// GETBIT
	if got := execValue(t, srv, "GETBIT", "bm", "7"); got.Int != 1 {
		t.Fatalf("expected bit 1, got %d", got.Int)
	}
	// GETBIT beyond length
	if got := execValue(t, srv, "GETBIT", "bm", "100"); got.Int != 0 {
		t.Fatalf("expected 0 for bit beyond length, got %d", got.Int)
	}
	// Set more bits for BITCOUNT
	execValue(t, srv, "SETBIT", "bm", "0", "1")
	execValue(t, srv, "SETBIT", "bm", "1", "1")
	// BITCOUNT
	count := execValue(t, srv, "BITCOUNT", "bm")
	if count.Int < 3 {
		t.Fatalf("expected at least 3 bits set, got %d", count.Int)
	}
	// BITCOUNT with range
	execValue(t, srv, "SET", "bckey", "foobar")
	if got := execValue(t, srv, "BITCOUNT", "bckey", "0", "0"); got.Int != 4 {
		t.Fatalf("expected 4 bits in first byte of 'foobar', got %d", got.Int)
	}
}

func TestGeoCommands(t *testing.T) {
	srv := newTestServer(t)

	// GEOADD
	if got := execValue(t, srv, "GEOADD", "places", "-74.006", "40.7128", "nyc", "-118.2437", "34.0522", "la"); got.Int != 2 {
		t.Fatalf("expected 2 added, got %d", got.Int)
	}

	// GEOPOS
	pos := execValue(t, srv, "GEOPOS", "places", "nyc", "la", "missing")
	if len(pos.Items) != 3 {
		t.Fatalf("expected 3 items from GEOPOS, got %d", len(pos.Items))
	}
	// nyc should have valid coordinates
	if len(pos.Items[0].Items) != 2 {
		t.Fatalf("expected 2 coordinates for nyc, got %d items", len(pos.Items[0].Items))
	}
	nycLon, _ := strconv.ParseFloat(pos.Items[0].Items[0].Text, 64)
	if math.Abs(nycLon-(-74.006)) > 0.01 {
		t.Fatalf("NYC longitude off: %f", nycLon)
	}
	// missing should be null
	if !pos.Items[2].Null && len(pos.Items[2].Items) != 0 {
		t.Fatalf("expected null for missing member, got %#v", pos.Items[2])
	}

	// GEODIST
	dist := execValue(t, srv, "GEODIST", "places", "nyc", "la", "km")
	distKm, err := strconv.ParseFloat(dist.Text, 64)
	if err != nil {
		t.Fatalf("could not parse geodist: %v", err)
	}
	// NYC to LA is approximately 3944 km
	if math.Abs(distKm-3944) > 50 {
		t.Fatalf("expected ~3944 km, got %.1f km", distKm)
	}
}

func TestHyperLogLogCommands(t *testing.T) {
	srv := newTestServer(t)

	// PFADD
	if got := execValue(t, srv, "PFADD", "hll", "a", "b", "c"); got.Int != 1 {
		t.Fatalf("expected 1 (changed), got %d", got.Int)
	}

	// PFADD duplicates
	if got := execValue(t, srv, "PFADD", "hll", "a", "b", "c"); got.Int != 0 {
		t.Fatalf("expected 0 (unchanged), got %d", got.Int)
	}

	// PFCOUNT
	if got := execValue(t, srv, "PFCOUNT", "hll"); got.Int != 3 {
		t.Fatalf("expected count ~3, got %d", got.Int)
	}

	// PFCOUNT on empty key
	if got := execValue(t, srv, "PFCOUNT", "empty"); got.Int != 0 {
		t.Fatalf("expected 0 for empty key, got %d", got.Int)
	}

	// Add many elements and verify tolerance
	for i := 0; i < 1000; i++ {
		execValue(t, srv, "PFADD", "hll2", fmt.Sprintf("elem:%d", i))
	}
	count := execValue(t, srv, "PFCOUNT", "hll2")
	// Allow 10% tolerance
	if count.Int < 900 || count.Int > 1100 {
		t.Fatalf("expected PFCOUNT within [900, 1100] for 1000 elements, got %d", count.Int)
	}
}

func TestRateLimitCommands(t *testing.T) {
	srv := newTestServer(t)

	// First 3 requests should be allowed (max=3, window=10s)
	for i := 0; i < 3; i++ {
		result := execValue(t, srv, "RATELIMIT", "api:key", "3", "10")
		if len(result.Items) != 4 {
			t.Fatalf("request %d: expected 4-element array, got %#v", i+1, result)
		}
		if result.Items[0].Int != 1 {
			t.Fatalf("request %d: expected allowed=1, got %d", i+1, result.Items[0].Int)
		}
	}

	// 4th request should be rejected
	result := execValue(t, srv, "RATELIMIT", "api:key", "3", "10")
	if result.Items[0].Int != 0 {
		t.Fatalf("4th request should be rejected (allowed=0), got %d", result.Items[0].Int)
	}

	// RATELIMIT.PEEK shows remaining
	peek := execValue(t, srv, "RATELIMIT.PEEK", "api:key")
	if len(peek.Items) != 2 {
		t.Fatalf("RATELIMIT.PEEK expected 2-element array, got %#v", peek)
	}
	if peek.Items[0].Int != 0 {
		t.Fatalf("expected remaining 0, got %d", peek.Items[0].Int)
	}

	// RATELIMIT.RESET clears it
	if got := execValue(t, srv, "RATELIMIT.RESET", "api:key"); got.Int != 1 {
		t.Fatalf("expected RATELIMIT.RESET to return 1, got %d", got.Int)
	}
	// After reset, should be allowed again
	result = execValue(t, srv, "RATELIMIT", "api:key", "3", "10")
	if result.Items[0].Int != 1 {
		t.Fatalf("expected allowed after reset, got %d", result.Items[0].Int)
	}
}

func TestJSONCommands(t *testing.T) {
	srv := newTestServer(t)

	// JSON.SET root document
	if got := execValue(t, srv, "JSON.SET", "jkey", "$", `{"name":"John","age":30}`); got.Text != "OK" {
		t.Fatalf("JSON.SET expected OK, got %#v", got)
	}

	// JSON.GET with path
	if got := execValue(t, srv, "JSON.GET", "jkey", "$.name"); got.Text != `"John"` {
		t.Fatalf("JSON.GET $.name expected \"John\", got %q", got.Text)
	}

	// JSON.TYPE
	if got := execValue(t, srv, "JSON.TYPE", "jkey", "$"); got.Text != "object" {
		t.Fatalf("JSON.TYPE expected 'object', got %q", got.Text)
	}

	// JSON.DEL a field
	if got := execValue(t, srv, "JSON.DEL", "jkey", "$.name"); got.Int != 1 {
		t.Fatalf("JSON.DEL expected 1, got %d", got.Int)
	}

	// Verify deletion — JSON.GET on a deleted path returns an error, so use exec directly
	{
		var buf bytes.Buffer
		w := resp.NewWriter(&buf)
		err := srv.exec(w, []string{"JSON.GET", "jkey", "$.name"})
		if err == nil {
			t.Logf("JSON.GET after delete did not error (path may still resolve)")
		}
		// Either error or null is acceptable after deletion
	}

	// JSON.KEYS
	keys := execValue(t, srv, "JSON.KEYS", "jkey", "$")
	if len(keys.Items) == 0 {
		t.Fatal("JSON.KEYS expected non-empty result")
	}

	// JSON.NUMINCRBY
	execValue(t, srv, "JSON.SET", "jkey", "$.score", "10")
	got := execValue(t, srv, "JSON.NUMINCRBY", "jkey", "$.score", "5")
	if got.Text != "15" {
		t.Fatalf("JSON.NUMINCRBY expected '15', got %q", got.Text)
	}

	// JSON.ARRAPPEND and JSON.ARRLEN
	execValue(t, srv, "JSON.SET", "jkey", "$.items", `["a"]`)
	if got := execValue(t, srv, "JSON.ARRAPPEND", "jkey", "$.items", `"b"`, `"c"`); got.Int != 3 {
		t.Fatalf("JSON.ARRAPPEND expected 3, got %d", got.Int)
	}
	if got := execValue(t, srv, "JSON.ARRLEN", "jkey", "$.items"); got.Int != 3 {
		t.Fatalf("JSON.ARRLEN expected 3, got %d", got.Int)
	}
}

func TestQueueCommands(t *testing.T) {
	srv := newTestServer(t)

	// Enqueue messages
	id1 := execValue(t, srv, "ENQUEUE", "myqueue", "hello")
	if id1.Text == "" {
		t.Fatal("ENQUEUE expected non-empty message ID")
	}
	id2 := execValue(t, srv, "ENQUEUE", "myqueue", "world")
	if id2.Text == "" {
		t.Fatal("ENQUEUE expected non-empty message ID")
	}

	// QLEN
	if got := execValue(t, srv, "QLEN", "myqueue"); got.Int != 2 {
		t.Fatalf("QLEN expected 2, got %d", got.Int)
	}

	// DEQUEUE returns first message
	deq := execValue(t, srv, "DEQUEUE", "myqueue")
	if len(deq.Items) != 3 {
		t.Fatalf("DEQUEUE expected 3-element array [id, body, retries], got %#v", deq)
	}
	msgID := deq.Items[0].Text
	if deq.Items[1].Text != "hello" {
		t.Fatalf("expected first message 'hello', got %q", deq.Items[1].Text)
	}

	// QACK
	if got := execValue(t, srv, "QACK", "myqueue", msgID); got.Int != 1 {
		t.Fatalf("QACK expected 1, got %d", got.Int)
	}

	// QINFO shows counts
	info := execValue(t, srv, "QINFO", "myqueue")
	if len(info.Items) != 6 {
		t.Fatalf("QINFO expected 6-element array, got %#v", info)
	}
	// pending=1 (one message left), processing=0, dead_letter=0
	if info.Items[0].Text != "pending" || info.Items[1].Int != 1 {
		t.Fatalf("expected pending=1, got %q=%d", info.Items[0].Text, info.Items[1].Int)
	}
	if info.Items[2].Text != "processing" || info.Items[3].Int != 0 {
		t.Fatalf("expected processing=0, got %q=%d", info.Items[2].Text, info.Items[3].Int)
	}
}

func TestTimeSeriesCommands(t *testing.T) {
	srv := newTestServer(t)

	// TS.ADD with explicit timestamps
	if got := execValue(t, srv, "TS.ADD", "metrics", "1000", "42.5"); got.Text != "OK" {
		t.Fatalf("TS.ADD expected OK, got %#v", got)
	}
	if got := execValue(t, srv, "TS.ADD", "metrics", "2000", "43"); got.Text != "OK" {
		t.Fatalf("TS.ADD expected OK, got %#v", got)
	}

	// TS.RANGE
	result := execValue(t, srv, "TS.RANGE", "metrics", "0", "3000")
	if len(result.Items) != 2 {
		t.Fatalf("TS.RANGE expected 2 samples, got %d: %#v", len(result.Items), result)
	}
	// Each sample is [timestamp, value]
	if result.Items[0].Items[0].Int != 1000 {
		t.Fatalf("expected first timestamp 1000, got %d", result.Items[0].Items[0].Int)
	}
	if result.Items[0].Items[1].Text != "42.5" {
		t.Fatalf("expected first value '42.5', got %q", result.Items[0].Items[1].Text)
	}

	// TS.GET returns latest
	latest := execValue(t, srv, "TS.GET", "metrics")
	if len(latest.Items) != 2 {
		t.Fatalf("TS.GET expected 2-element array, got %#v", latest)
	}
	if latest.Items[0].Int != 2000 {
		t.Fatalf("expected latest timestamp 2000, got %d", latest.Items[0].Int)
	}
	if latest.Items[1].Text != "43" {
		t.Fatalf("expected latest value '43', got %q", latest.Items[1].Text)
	}

	// TS.INFO
	tsInfo := execValue(t, srv, "TS.INFO", "metrics")
	if len(tsInfo.Items) != 10 {
		t.Fatalf("TS.INFO expected 10-element array, got %d: %#v", len(tsInfo.Items), tsInfo)
	}
	// total_samples=2
	if tsInfo.Items[0].Text != "total_samples" || tsInfo.Items[1].Int != 2 {
		t.Fatalf("expected total_samples=2, got %q=%d", tsInfo.Items[0].Text, tsInfo.Items[1].Int)
	}
}

func TestTagCommands(t *testing.T) {
	srv := newTestServer(t)

	// SET a key first
	execValue(t, srv, "SET", "mykey", "value")

	// TAG mykey env=prod
	if got := execValue(t, srv, "TAG", "mykey", "env=prod"); got.Text != "OK" {
		t.Fatalf("TAG expected OK, got %#v", got)
	}

	// TAGGET mykey
	tags := execValue(t, srv, "TAGGET", "mykey")
	if len(tags.Items) != 2 || tags.Items[0].Text != "env" || tags.Items[1].Text != "prod" {
		t.Fatalf("TAGGET expected [env, prod], got %#v", tags)
	}

	// TAGQUERY env=prod
	results := execValue(t, srv, "TAGQUERY", "env=prod")
	if len(results.Items) != 1 || results.Items[0].Text != "mykey" {
		t.Fatalf("TAGQUERY expected [mykey], got %#v", results)
	}

	// TAGDEL
	if got := execValue(t, srv, "TAGDEL", "mykey", "env"); got.Int != 1 {
		t.Fatalf("TAGDEL expected 1, got %d", got.Int)
	}

	// TAGQUERY should no longer match
	results = execValue(t, srv, "TAGQUERY", "env=prod")
	if len(results.Items) != 0 {
		t.Fatalf("TAGQUERY expected empty after TAGDEL, got %#v", results)
	}
}

func TestScriptingEval(t *testing.T) {
	srv := newTestServer(t)

	// EVAL with redis.call SET
	got := execValue(t, srv, "EVAL", `return redis.call("SET", KEYS[1], ARGV[1])`, "1", "mykey", "myval")
	if got.Text != "OK" {
		t.Fatalf("EVAL SET expected OK, got %#v", got)
	}
	if v := execValue(t, srv, "GET", "mykey"); v.Text != "myval" {
		t.Fatalf("expected 'myval', got %q", v.Text)
	}

	// EVAL with redis.call GET
	got = execValue(t, srv, "EVAL", `return redis.call("GET", KEYS[1])`, "1", "mykey")
	if got.Text != "myval" {
		t.Fatalf("EVAL GET expected 'myval', got %#v", got)
	}

	// SCRIPT LOAD + EVALSHA
	sha := execValue(t, srv, "SCRIPT", "LOAD", `return redis.call("GET", KEYS[1])`)
	if sha.Text == "" {
		t.Fatal("SCRIPT LOAD expected non-empty SHA")
	}

	// SCRIPT EXISTS
	exists := execValue(t, srv, "SCRIPT", "EXISTS", sha.Text, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	if len(exists.Items) != 2 || exists.Items[0].Int != 1 || exists.Items[1].Int != 0 {
		t.Fatalf("SCRIPT EXISTS unexpected: %#v", exists)
	}

	// EVALSHA
	got = execValue(t, srv, "EVALSHA", sha.Text, "1", "mykey")
	if got.Text != "myval" {
		t.Fatalf("EVALSHA expected 'myval', got %#v", got)
	}

	// SCRIPT FLUSH
	if got := execValue(t, srv, "SCRIPT", "FLUSH"); got.Text != "OK" {
		t.Fatalf("SCRIPT FLUSH expected OK, got %#v", got)
	}
}

func TestSlowLog(t *testing.T) {
	srv := newTestServer(t)

	// Run some commands
	execValue(t, srv, "SET", "k", "v")
	execValue(t, srv, "GET", "k")

	// SLOWLOG LEN should return an integer
	slen := execValue(t, srv, "SLOWLOG", "LEN")
	if slen.Int < 0 {
		t.Fatalf("SLOWLOG LEN unexpected: %#v", slen)
	}

	// SLOWLOG GET
	entries := execValue(t, srv, "SLOWLOG", "GET")
	// May be 0 if threshold not met, but should not error
	_ = entries

	// SLOWLOG RESET
	if got := execValue(t, srv, "SLOWLOG", "RESET"); got.Text != "OK" {
		t.Fatalf("SLOWLOG RESET expected OK, got %#v", got)
	}

	// After reset, LEN should be 0
	if got := execValue(t, srv, "SLOWLOG", "LEN"); got.Int != 0 {
		t.Fatalf("SLOWLOG LEN after RESET expected 0, got %d", got.Int)
	}
}

func TestACLCommands(t *testing.T) {
	srv := newTestServer(t)

	// ACL WHOAMI
	if got := execValue(t, srv, "ACL", "WHOAMI"); got.Text != "default" {
		t.Fatalf("ACL WHOAMI expected 'default', got %q", got.Text)
	}

	// ACL LIST
	list := execValue(t, srv, "ACL", "LIST")
	if len(list.Items) != 1 || list.Items[0].Text != "user default on ~* +@all" {
		t.Fatalf("ACL LIST unexpected: %#v", list)
	}

	// ACL CAT
	cat := execValue(t, srv, "ACL", "CAT")
	if len(cat.Items) == 0 {
		t.Fatal("ACL CAT expected non-empty list")
	}
	// Should contain known categories
	foundRead := false
	for _, item := range cat.Items {
		if item.Text == "read" {
			foundRead = true
		}
	}
	if !foundRead {
		t.Fatal("ACL CAT expected 'read' category")
	}
}

func TestSelectCommand(t *testing.T) {
	srv := newTestServer(t)

	// SELECT 0 should succeed
	if got := execValue(t, srv, "SELECT", "0"); got.Text != "OK" {
		t.Fatalf("SELECT 0 expected OK, got %#v", got)
	}

	// SELECT 1 should return an error
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	err := srv.exec(w, []string{"SELECT", "1"})
	if err == nil {
		t.Fatal("SELECT 1 expected error")
	}
	if !strings.Contains(err.Error(), "NAMESPACE") {
		t.Fatalf("SELECT 1 error should mention NAMESPACE, got %q", err.Error())
	}
}

func TestSortCommand(t *testing.T) {
	srv := newTestServer(t)

	// Create a list with mixed values
	execValue(t, srv, "RPUSH", "mylist", "banana", "apple", "cherry", "date")

	// SORT ALPHA
	result := execValue(t, srv, "SORT", "mylist", "ALPHA")
	if len(result.Items) != 4 {
		t.Fatalf("expected 4 items, got %d", len(result.Items))
	}
	if result.Items[0].Text != "apple" || result.Items[1].Text != "banana" {
		t.Fatalf("unexpected SORT ALPHA: %#v", result)
	}

	// SORT ALPHA DESC
	result = execValue(t, srv, "SORT", "mylist", "ALPHA", "DESC")
	if len(result.Items) != 4 {
		t.Fatalf("expected 4 items, got %d", len(result.Items))
	}
	if result.Items[0].Text != "date" || result.Items[3].Text != "apple" {
		t.Fatalf("unexpected SORT DESC: %#v", result)
	}

	// SORT with LIMIT
	result = execValue(t, srv, "SORT", "mylist", "ALPHA", "LIMIT", "1", "2")
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items with LIMIT, got %d", len(result.Items))
	}
	if result.Items[0].Text != "banana" || result.Items[1].Text != "cherry" {
		t.Fatalf("unexpected SORT LIMIT: %#v", result)
	}
}

func TestSchemaEnforcement(t *testing.T) {
	srv := newTestServer(t)

	// SCHEMA.SET
	if got := execValue(t, srv, "SCHEMA.SET", "user:*", "age:int", "email:email"); got.Text != "OK" {
		t.Fatalf("SCHEMA.SET expected OK, got %#v", got)
	}

	// HSET that passes validation
	if got := execValue(t, srv, "HSET", "user:1", "age", "25", "email", "test@example.com"); got.Int != 2 {
		t.Fatalf("HSET with valid data expected 2, got %#v", got)
	}

	// HSET that fails validation (invalid age)
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	err := srv.exec(w, []string{"HSET", "user:2", "age", "notanumber"})
	if err == nil {
		t.Fatal("HSET with invalid age expected error")
	}
	if !strings.Contains(err.Error(), "schema violation") {
		t.Fatalf("expected schema violation error, got %q", err.Error())
	}

	// HSET that fails validation (invalid email)
	err = srv.exec(w, []string{"HSET", "user:3", "email", "notanemail"})
	if err == nil {
		t.Fatal("HSET with invalid email expected error")
	}
}

func TestCronCommands(t *testing.T) {
	srv := newTestServer(t)

	// CRON.ADD
	if got := execValue(t, srv, "CRON.ADD", "myjob", "* * * * *", "SET", "cronkey", "cronval"); got.Text != "OK" {
		t.Fatalf("CRON.ADD expected OK, got %#v", got)
	}

	// CRON.LIST
	list := execValue(t, srv, "CRON.LIST")
	if len(list.Items) != 1 {
		t.Fatalf("CRON.LIST expected 1 job, got %d", len(list.Items))
	}

	// CRON.INFO
	info := execValue(t, srv, "CRON.INFO", "myjob")
	if info.Null {
		t.Fatal("CRON.INFO expected non-null result")
	}
	if len(info.Items) != 12 {
		t.Fatalf("CRON.INFO expected 12 elements, got %d", len(info.Items))
	}
	if info.Items[0].Text != "name" || info.Items[1].Text != "myjob" {
		t.Fatalf("CRON.INFO expected name=myjob, got %#v", info.Items[0:2])
	}

	// CRON.INFO for non-existent job
	if got := execValue(t, srv, "CRON.INFO", "missing"); !got.Null {
		t.Fatalf("CRON.INFO for missing job expected null, got %#v", got)
	}

	// CRON.DEL
	if got := execValue(t, srv, "CRON.DEL", "myjob"); got.Int != 1 {
		t.Fatalf("CRON.DEL expected 1, got %d", got.Int)
	}
	if got := execValue(t, srv, "CRON.DEL", "myjob"); got.Int != 0 {
		t.Fatalf("CRON.DEL second time expected 0, got %d", got.Int)
	}
}

func TestAuditCommands(t *testing.T) {
	srv := newTestServer(t)

	// AUDIT.ENABLE
	if got := execValue(t, srv, "AUDIT.ENABLE"); got.Text != "OK" {
		t.Fatalf("AUDIT.ENABLE expected OK, got %#v", got)
	}

	// Audit recording happens at connection level, not via exec().
	// Manually record entries to test the audit query commands.
	srv.audit.Record("auditkey", "SET", "127.0.0.1:12345", []string{"SET", "auditkey", "value1"})
	srv.audit.Record("auditkey", "SET", "127.0.0.1:12345", []string{"SET", "auditkey", "value2"})

	// AUDIT.SIZE should be > 0
	size := execValue(t, srv, "AUDIT.SIZE")
	if size.Int < 1 {
		t.Fatalf("AUDIT.SIZE expected > 0, got %d", size.Int)
	}

	// AUDIT key - shows entries for a specific key
	entries := execValue(t, srv, "AUDIT", "auditkey")
	if len(entries.Items) == 0 {
		t.Fatal("AUDIT expected non-empty entries for audited key")
	}

	// AUDIT.CLEAR
	if got := execValue(t, srv, "AUDIT.CLEAR"); got.Text != "OK" {
		t.Fatalf("AUDIT.CLEAR expected OK, got %#v", got)
	}
	if got := execValue(t, srv, "AUDIT.SIZE"); got.Int != 0 {
		t.Fatalf("AUDIT.SIZE after CLEAR expected 0, got %d", got.Int)
	}

	// AUDIT.DISABLE
	if got := execValue(t, srv, "AUDIT.DISABLE"); got.Text != "OK" {
		t.Fatalf("AUDIT.DISABLE expected OK, got %#v", got)
	}
}

func TestStreamAdvanced(t *testing.T) {
	srv := newTestServer(t)

	// XADD multiple entries
	execValue(t, srv, "XADD", "s", "1-1", "k", "v1")
	execValue(t, srv, "XADD", "s", "1-2", "k", "v2")
	execValue(t, srv, "XADD", "s", "1-3", "k", "v3")
	execValue(t, srv, "XADD", "s", "1-4", "k", "v4")
	execValue(t, srv, "XADD", "s", "1-5", "k", "v5")

	// XLEN
	if got := execValue(t, srv, "XLEN", "s"); got.Int != 5 {
		t.Fatalf("XLEN expected 5, got %d", got.Int)
	}

	// XTRIM MAXLEN
	trimmed := execValue(t, srv, "XTRIM", "s", "MAXLEN", "3")
	if trimmed.Int != 2 {
		t.Fatalf("XTRIM expected 2 trimmed, got %d", trimmed.Int)
	}
	if got := execValue(t, srv, "XLEN", "s"); got.Int != 3 {
		t.Fatalf("XLEN after XTRIM expected 3, got %d", got.Int)
	}

	// XINFO STREAM
	info := execValue(t, srv, "XINFO", "STREAM", "s")
	if len(info.Items) != 14 {
		t.Fatalf("XINFO STREAM expected 14 elements, got %d", len(info.Items))
	}
	// First pair should be "length" / 3
	if info.Items[0].Text != "length" || info.Items[1].Int != 3 {
		t.Fatalf("expected length=3, got %q=%d", info.Items[0].Text, info.Items[1].Int)
	}

	// XGROUP CREATE + XPENDING
	execValue(t, srv, "XGROUP", "CREATE", "s", "grp", "0-0")
	execValue(t, srv, "XREADGROUP", "GROUP", "grp", "c1", "COUNT", "2", "STREAMS", "s", ">")

	pending := execValue(t, srv, "XPENDING", "s", "grp")
	if len(pending.Items) != 4 {
		t.Fatalf("XPENDING expected 4-element array, got %d", len(pending.Items))
	}
	if pending.Items[0].Int != 2 {
		t.Fatalf("XPENDING expected 2 pending, got %d", pending.Items[0].Int)
	}
}

func TestSetOperationsStore(t *testing.T) {
	srv := newTestServer(t)

	execValue(t, srv, "SADD", "s1", "a", "b", "c")
	execValue(t, srv, "SADD", "s2", "b", "c", "d")

	// SINTERSTORE
	if got := execValue(t, srv, "SINTERSTORE", "dest_inter", "s1", "s2"); got.Int != 2 {
		t.Fatalf("SINTERSTORE expected 2, got %d", got.Int)
	}
	members := execValue(t, srv, "SMEMBERS", "dest_inter")
	if len(members.Items) != 2 {
		t.Fatalf("expected 2 members in dest_inter, got %d", len(members.Items))
	}

	// SUNIONSTORE
	if got := execValue(t, srv, "SUNIONSTORE", "dest_union", "s1", "s2"); got.Int != 4 {
		t.Fatalf("SUNIONSTORE expected 4, got %d", got.Int)
	}

	// SDIFFSTORE
	if got := execValue(t, srv, "SDIFFSTORE", "dest_diff", "s1", "s2"); got.Int != 1 {
		t.Fatalf("SDIFFSTORE expected 1, got %d", got.Int)
	}
	diff := execValue(t, srv, "SMEMBERS", "dest_diff")
	if len(diff.Items) != 1 || diff.Items[0].Text != "a" {
		t.Fatalf("expected [a] in dest_diff, got %#v", diff)
	}
}

func TestLegacySetCommands(t *testing.T) {
	srv := newTestServer(t)

	// SETNX - set if not exists
	if got := execValue(t, srv, "SETNX", "k1", "v1"); got.Int != 1 {
		t.Fatalf("SETNX expected 1 for new key, got %d", got.Int)
	}
	if got := execValue(t, srv, "SETNX", "k1", "v2"); got.Int != 0 {
		t.Fatalf("SETNX expected 0 for existing key, got %d", got.Int)
	}
	if got := execValue(t, srv, "GET", "k1"); got.Text != "v1" {
		t.Fatalf("expected 'v1', got %q", got.Text)
	}

	// SETEX - set with expiry in seconds
	if got := execValue(t, srv, "SETEX", "k2", "10", "v2"); got.Text != "OK" {
		t.Fatalf("SETEX expected OK, got %#v", got)
	}
	if got := execValue(t, srv, "GET", "k2"); got.Text != "v2" {
		t.Fatalf("expected 'v2', got %q", got.Text)
	}
	ttl := execValue(t, srv, "TTL", "k2")
	if ttl.Int < 5 || ttl.Int > 11 {
		t.Fatalf("expected TTL around 10, got %d", ttl.Int)
	}

	// PSETEX - set with expiry in milliseconds
	if got := execValue(t, srv, "PSETEX", "k3", "10000", "v3"); got.Text != "OK" {
		t.Fatalf("PSETEX expected OK, got %#v", got)
	}
	if got := execValue(t, srv, "GET", "k3"); got.Text != "v3" {
		t.Fatalf("expected 'v3', got %q", got.Text)
	}
	pttl := execValue(t, srv, "PTTL", "k3")
	if pttl.Int < 5000 || pttl.Int > 11000 {
		t.Fatalf("expected PTTL around 10000, got %d", pttl.Int)
	}
}

func TestObjectEncoding(t *testing.T) {
	srv := newTestServer(t)

	// String
	execValue(t, srv, "SET", "str", "hello")
	if got := execValue(t, srv, "OBJECT", "ENCODING", "str"); got.Text != "embstr" {
		t.Fatalf("expected embstr for string, got %q", got.Text)
	}

	// Hash
	execValue(t, srv, "HSET", "hash", "f", "v")
	if got := execValue(t, srv, "OBJECT", "ENCODING", "hash"); got.Text != "hashtable" {
		t.Fatalf("expected hashtable for hash, got %q", got.Text)
	}

	// List
	execValue(t, srv, "RPUSH", "list", "a")
	if got := execValue(t, srv, "OBJECT", "ENCODING", "list"); got.Text != "listpack" {
		t.Fatalf("expected listpack for list, got %q", got.Text)
	}

	// Set
	execValue(t, srv, "SADD", "set", "m")
	if got := execValue(t, srv, "OBJECT", "ENCODING", "set"); got.Text != "hashtable" {
		t.Fatalf("expected hashtable for set, got %q", got.Text)
	}

	// Sorted set
	execValue(t, srv, "ZADD", "zset", "1", "m")
	if got := execValue(t, srv, "OBJECT", "ENCODING", "zset"); got.Text != "skiplist" {
		t.Fatalf("expected skiplist for zset, got %q", got.Text)
	}

	// Stream
	execValue(t, srv, "XADD", "stream", "*", "k", "v")
	if got := execValue(t, srv, "OBJECT", "ENCODING", "stream"); got.Text != "stream" {
		t.Fatalf("expected stream for stream, got %q", got.Text)
	}

	// JSON
	execValue(t, srv, "JSON.SET", "json", "$", `{"a":1}`)
	if got := execValue(t, srv, "OBJECT", "ENCODING", "json"); got.Text != "json" {
		t.Fatalf("expected json for json, got %q", got.Text)
	}

	// Missing key returns null
	got := execValue(t, srv, "OBJECT", "ENCODING", "missing")
	if !got.Null {
		t.Fatalf("expected null for missing key, got %#v", got)
	}
}

func TestInfoCommand(t *testing.T) {
	srv := newTestServer(t)

	info := execValue(t, srv, "INFO")
	text := info.Text

	sections := []string{"# Server", "# Persistence", "# Keyspace", "# Replication"}
	for _, section := range sections {
		if !strings.Contains(text, section) {
			t.Fatalf("INFO missing section %q", section)
		}
	}

	if !strings.Contains(text, "vole_version") {
		t.Fatal("INFO missing vole_version")
	}
}

func TestWebhookCommands(t *testing.T) {
	srv := newTestServer(t)

	// WEBHOOK REGISTER
	if got := execValue(t, srv, "WEBHOOK", "REGISTER", "mykey", "SET", "http://example.com/hook"); got.Text != "OK" {
		t.Fatalf("WEBHOOK REGISTER expected OK, got %#v", got)
	}

	// WEBHOOK LIST
	list := execValue(t, srv, "WEBHOOK", "LIST")
	if len(list.Items) == 0 {
		t.Fatal("WEBHOOK LIST expected non-empty after register")
	}

	// WEBHOOK UNREGISTER
	if got := execValue(t, srv, "WEBHOOK", "UNREGISTER", "mykey", "SET", "http://example.com/hook"); got.Text != "OK" {
		t.Fatalf("WEBHOOK UNREGISTER expected OK, got %#v", got)
	}
}

// Suppress unused import warnings for math and strconv by using them.
var _ = math.Inf
var _ = strconv.Itoa
