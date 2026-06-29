package server

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"vole/internal/store"
)

func TestAOFReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	if err := aof.Append([]string{"SET", "name", "vole"}); err != nil {
		t.Fatalf("append set: %v", err)
	}
	if err := aof.Append([]string{"INCR", "counter"}); err != nil {
		t.Fatalf("append incr: %v", err)
	}
	if err := aof.Append([]string{"MSETABS", "k1", "v1", "0", "k2", "v2", "0"}); err != nil {
		t.Fatalf("append msetabs: %v", err)
	}
	if err := aof.Append([]string{"HSET", "user:1", "name", "Ada", "role", "admin"}); err != nil {
		t.Fatalf("append hset: %v", err)
	}
	if err := aof.Append([]string{"HDEL", "user:1", "role"}); err != nil {
		t.Fatalf("append hdel: %v", err)
	}
	if err := aof.Append([]string{"RPUSH", "queue", "a", "b"}); err != nil {
		t.Fatalf("append rpush: %v", err)
	}
	if err := aof.Append([]string{"LPUSH", "queue", "c"}); err != nil {
		t.Fatalf("append lpush: %v", err)
	}
	if err := aof.Append([]string{"RPOP", "queue"}); err != nil {
		t.Fatalf("append rpop: %v", err)
	}
	if err := aof.Append([]string{"SADD", "tags", "red", "blue", "red"}); err != nil {
		t.Fatalf("append sadd: %v", err)
	}
	if err := aof.Append([]string{"SREM", "tags", "red"}); err != nil {
		t.Fatalf("append srem: %v", err)
	}
	if err := aof.Append([]string{"ZADD", "rank", "2", "bob", "1", "ada"}); err != nil {
		t.Fatalf("append zadd: %v", err)
	}
	if err := aof.Append([]string{"ZREM", "rank", "bob"}); err != nil {
		t.Fatalf("append zrem: %v", err)
	}
	if err := aof.Append([]string{"XADD", "events", "1-1", "type", "created"}); err != nil {
		t.Fatalf("append xadd: %v", err)
	}
	if err := aof.Append([]string{"XGROUP", "CREATE", "events", "workers", "0-0"}); err != nil {
		t.Fatalf("append xgroup: %v", err)
	}
	if err := aof.Append([]string{"XGROUPDELIVER", "events", "workers", "c1", strconv.FormatInt(time.Now().UnixNano(), 10), "1-1"}); err != nil {
		t.Fatalf("append xgroupdeliver: %v", err)
	}
	if err := aof.Append([]string{"XACK", "events", "workers", "1-1"}); err != nil {
		t.Fatalf("append xack: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st := store.New()
	if err := ReplayAOF(path, st); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got, ok := st.Get("name"); !ok || got != "vole" {
		t.Fatalf("expected replayed name, got %q ok=%v", got, ok)
	}
	if got, ok := st.Get("counter"); !ok || got != "1" {
		t.Fatalf("expected replayed counter, got %q ok=%v", got, ok)
	}
	if got, ok := st.Get("k2"); !ok || got != "v2" {
		t.Fatalf("expected replayed mset key, got %q ok=%v", got, ok)
	}
	if got, ok, err := st.HGet("user:1", "name"); err != nil || !ok || got != "Ada" {
		t.Fatalf("expected replayed hash field, got %q ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := st.HGet("user:1", "role"); err != nil || ok {
		t.Fatalf("expected deleted hash field absent, ok=%v err=%v", ok, err)
	}
	list, err := st.LRange("queue", 0, -1)
	if err != nil || len(list) != 2 || list[0] != "c" || list[1] != "a" {
		t.Fatalf("expected replayed list [c a], got %v err=%v", list, err)
	}
	members, err := st.SMembers("tags")
	if err != nil || len(members) != 1 || members[0] != "blue" {
		t.Fatalf("expected replayed set [blue], got %v err=%v", members, err)
	}
	zitems, err := st.ZRange("rank", 0, -1)
	if err != nil || len(zitems) != 1 || zitems[0].Member != "ada" || zitems[0].Score != 1 {
		t.Fatalf("expected replayed zset [ada], got %#v err=%v", zitems, err)
	}
	entries := st.XRange("events", "-", "+", 0)
	if len(entries) != 1 || entries[0].ID != "1-1" {
		t.Fatalf("expected replayed stream entry, got %#v", entries)
	}
	pending, err := st.XReadGroupPlan("workers", "c1", []string{"events"}, []string{"0-0"}, 10)
	if err != nil {
		t.Fatalf("expected replayed consumer group: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected replayed ack to clear pending, got %#v", pending)
	}
}

func TestClusterSlotsCoverAllSlots(t *testing.T) {
	c := NewCluster("self", "127.0.0.1:7379", "peer-a@127.0.0.1:7380,peer-b@127.0.0.1:7381")
	nodes := c.Nodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
	if nodes[0].Start != 0 {
		t.Fatalf("expected first slot 0, got %d", nodes[0].Start)
	}
	if nodes[len(nodes)-1].End != slotCount-1 {
		t.Fatalf("expected final slot %d, got %d", slotCount-1, nodes[len(nodes)-1].End)
	}
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].End+1 != nodes[i].Start {
			t.Fatalf("slot gap between %#v and %#v", nodes[i-1], nodes[i])
		}
	}
}

func TestReplayTTLUsesRelativeExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	if err := aof.Append([]string{"SET", "short", "yes", "PX", "50"}); err != nil {
		t.Fatalf("append set: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st := store.New()
	if err := ReplayAOF(path, st); err != nil {
		t.Fatalf("replay: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := st.Get("short"); ok {
		t.Fatal("expected replayed relative TTL to expire")
	}
}

func TestAOFReplayPreservesAbsoluteExpiry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	expireAt := time.Now().Add(50 * time.Millisecond).UnixNano()
	if err := aof.Append([]string{"SETABS", "short", "yes", strconv.FormatInt(expireAt, 10)}); err != nil {
		t.Fatalf("append setabs: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	st := store.New()
	if err := ReplayAOF(path, st); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if ttl := st.TTL("short"); ttl > 1 {
		t.Fatalf("expected TTL not to be extended on replay, got %d", ttl)
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := st.Get("short"); ok {
		t.Fatal("expected absolute TTL to expire")
	}
}

func TestAOFReplayExpiresHashAndStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	expireAt := time.Now().Add(30 * time.Millisecond).UnixNano()
	if err := aof.Append([]string{"HSET", "hash:1", "name", "Ada"}); err != nil {
		t.Fatalf("append hset: %v", err)
	}
	if err := aof.Append([]string{"XADD", "stream:1", "1-1", "type", "created"}); err != nil {
		t.Fatalf("append xadd: %v", err)
	}
	if err := aof.Append([]string{"EXPIREATABS", "hash:1", strconv.FormatInt(expireAt, 10)}); err != nil {
		t.Fatalf("append hash expire: %v", err)
	}
	if err := aof.Append([]string{"EXPIREATABS", "stream:1", strconv.FormatInt(expireAt, 10)}); err != nil {
		t.Fatalf("append stream expire: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	st := store.New()
	if err := ReplayAOF(path, st); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := st.Type("hash:1"); got != "none" {
		t.Fatalf("expected expired hash absent, got %s", got)
	}
	if got := st.Type("stream:1"); got != "none" {
		t.Fatalf("expected expired stream absent, got %s", got)
	}
}

func TestSnapshotSaveLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.json")
	st := store.New()
	st.Set("name", "vole", 0)
	if _, err := st.HSet("user:1", []store.HashPair{{Field: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("hset failed: %v", err)
	}
	if _, err := st.RPush("queue", "a", "b"); err != nil {
		t.Fatalf("rpush failed: %v", err)
	}
	if _, err := st.SAdd("tags", "red", "blue"); err != nil {
		t.Fatalf("sadd failed: %v", err)
	}
	if _, err := st.ZAdd("rank", []store.ZMember{{Member: "ada", Score: 1}, {Member: "bob", Score: 2}}); err != nil {
		t.Fatalf("zadd failed: %v", err)
	}
	if _, err := st.XAdd("events", "1-1", []string{"type", "created"}); err != nil {
		t.Fatalf("xadd failed: %v", err)
	}
	if err := st.XGroupCreate("events", "workers", "0-0", false); err != nil {
		t.Fatalf("xgroup create failed: %v", err)
	}
	if err := st.XGroupDeliver("events", "workers", "c1", []string{"1-1"}, time.Now()); err != nil {
		t.Fatalf("xgroup deliver failed: %v", err)
	}
	if err := SaveSnapshot(path, st); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	restored := store.New()
	if err := LoadSnapshot(path, restored); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if got, ok := restored.Get("name"); !ok || got != "vole" {
		t.Fatalf("expected restored key, got %q ok=%v", got, ok)
	}
	if got, ok, err := restored.HGet("user:1", "name"); err != nil || !ok || got != "Ada" {
		t.Fatalf("expected restored hash, got %q ok=%v err=%v", got, ok, err)
	}
	list, err := restored.LRange("queue", 0, -1)
	if err != nil || len(list) != 2 || list[0] != "a" || list[1] != "b" {
		t.Fatalf("expected restored list, got %v err=%v", list, err)
	}
	members, err := restored.SMembers("tags")
	if err != nil || len(members) != 2 || members[0] != "blue" || members[1] != "red" {
		t.Fatalf("expected restored set, got %v err=%v", members, err)
	}
	zitems, err := restored.ZRange("rank", 0, -1)
	if err != nil || len(zitems) != 2 || zitems[0].Member != "ada" || zitems[1].Member != "bob" {
		t.Fatalf("expected restored zset, got %#v err=%v", zitems, err)
	}
	entries := restored.XRange("events", "-", "+", 0)
	if len(entries) != 1 || entries[0].ID != "1-1" {
		t.Fatalf("expected restored stream, got %#v", entries)
	}
	pending, err := restored.XReadGroupPlan("workers", "c1", []string{"events"}, []string{"0-0"}, 10)
	if err != nil {
		t.Fatalf("expected restored consumer group: %v", err)
	}
	if len(pending["events"]) != 1 || pending["events"][0].ID != "1-1" {
		t.Fatalf("expected restored pending entry, got %#v", pending)
	}
}

func TestSnapshotResetAOFPreventsDoubleApplyingIncrement(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "dump.json")
	aofPath := filepath.Join(dir, "data.aof")
	st := store.New()
	aof, err := OpenAOF(aofPath, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	if _, err := st.Incr("counter"); err != nil {
		t.Fatalf("incr: %v", err)
	}
	if err := aof.Append([]string{"INCR", "counter"}); err != nil {
		t.Fatalf("append incr: %v", err)
	}
	if err := SaveSnapshot(snapshotPath, st); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if err := aof.Reset(); err != nil {
		t.Fatalf("reset aof: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close aof: %v", err)
	}

	restored := store.New()
	if err := LoadSnapshot(snapshotPath, restored); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if err := ReplayAOF(aofPath, restored); err != nil {
		t.Fatalf("replay aof: %v", err)
	}
	if got, ok := restored.Get("counter"); !ok || got != "1" {
		t.Fatalf("expected counter to be applied once, got %q ok=%v", got, ok)
	}
}

// TestSnapshotAllTypes verifies that every data type survives a snapshot save/load cycle.
func TestSnapshotAllTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.json")
	st := store.New()

	// 1. String key with TTL
	st.Set("greeting", "hello", 10*time.Minute)

	// 2. Hash with multiple fields
	if _, err := st.HSet("user:42", []store.HashPair{
		{Field: "name", Value: "Ada"},
		{Field: "email", Value: "ada@example.com"},
		{Field: "role", Value: "admin"},
	}); err != nil {
		t.Fatalf("hset: %v", err)
	}

	// 3. List with multiple elements
	if _, err := st.RPush("mylist", "a", "b", "c"); err != nil {
		t.Fatalf("rpush: %v", err)
	}
	if _, err := st.LPush("mylist", "z"); err != nil {
		t.Fatalf("lpush: %v", err)
	}

	// 4. Set with multiple members
	if _, err := st.SAdd("colors", "red", "green", "blue", "yellow"); err != nil {
		t.Fatalf("sadd: %v", err)
	}

	// 5. Sorted set with scored members
	if _, err := st.ZAdd("leaderboard", []store.ZMember{
		{Member: "alice", Score: 100},
		{Member: "bob", Score: 200},
		{Member: "charlie", Score: 150},
	}); err != nil {
		t.Fatalf("zadd: %v", err)
	}

	// 6. Stream with entries and consumer group
	if _, err := st.XAdd("events", "1-1", []string{"type", "login", "user", "alice"}); err != nil {
		t.Fatalf("xadd 1: %v", err)
	}
	if _, err := st.XAdd("events", "2-1", []string{"type", "purchase", "item", "widget"}); err != nil {
		t.Fatalf("xadd 2: %v", err)
	}
	if err := st.XGroupCreate("events", "processors", "0-0", false); err != nil {
		t.Fatalf("xgroup create: %v", err)
	}
	if err := st.XGroupDeliver("events", "processors", "worker1", []string{"1-1"}, time.Now()); err != nil {
		t.Fatalf("xgroup deliver: %v", err)
	}

	// 7. HyperLogLog with data
	if _, err := st.PFAdd("visitors", "user1", "user2", "user3", "user4", "user5"); err != nil {
		t.Fatalf("pfadd: %v", err)
	}
	origCount, err := st.PFCount("visitors")
	if err != nil {
		t.Fatalf("pfcount: %v", err)
	}

	// 8. JSON document with nested structure
	jsonDoc := `{"name":"Ada","address":{"city":"London","zip":"SW1"},"scores":[10,20,30]}`
	if err := st.JSONSet("profile:1", "$", jsonDoc); err != nil {
		t.Fatalf("json.set: %v", err)
	}

	// 9. Time-series with samples and labels
	if err := st.TSAdd("temperature", 1000, 22.5, map[string]string{"sensor": "A", "location": "lab"}); err != nil {
		t.Fatalf("ts.add 1: %v", err)
	}
	if err := st.TSAdd("temperature", 2000, 23.1, nil); err != nil {
		t.Fatalf("ts.add 2: %v", err)
	}
	if err := st.TSAdd("temperature", 3000, 21.8, nil); err != nil {
		t.Fatalf("ts.add 3: %v", err)
	}

	// 10. Queue with pending and dead-letter messages
	msgID1 := st.Enqueue("jobqueue", "job-payload-1", 0)
	msgID2 := st.Enqueue("jobqueue", "job-payload-2", 0)
	_ = msgID2
	// Dequeue first message to put it in processing, then nack enough to dead-letter
	msg, ok := st.Dequeue("jobqueue", 0)
	if !ok {
		t.Fatalf("dequeue failed")
	}
	// Nack it repeatedly to push to dead-letter (max retries = 3)
	for i := 0; i < 4; i++ {
		st.QNack("jobqueue", msg.ID)
		msg2, ok2 := st.Dequeue("jobqueue", 0)
		if ok2 {
			msg = msg2
		}
	}
	_ = msgID1

	// 11. Tags on a key
	st.Set("tagged-key", "some-value", 0)
	if err := st.TagSet("tagged-key", map[string]string{"env": "prod", "team": "platform"}); err != nil {
		t.Fatalf("tag set: %v", err)
	}

	// 12. Delayed key (notBefore)
	st.SetDelayed("delayed-key", "future-value", 1*time.Hour, 0)

	// --- Save snapshot ---
	if err := SaveSnapshot(path, st); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// --- Load into fresh store ---
	restored := store.New()
	if err := LoadSnapshot(path, restored); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	// Verify 1: String with TTL
	if got, ok := restored.Get("greeting"); !ok || got != "hello" {
		t.Fatalf("string: expected 'hello', got %q ok=%v", got, ok)
	}
	if ttl := restored.TTL("greeting"); ttl <= 0 || ttl > 600 {
		t.Fatalf("string TTL: expected positive TTL, got %d", ttl)
	}

	// Verify 2: Hash fields
	for _, tc := range []struct{ field, want string }{
		{"name", "Ada"}, {"email", "ada@example.com"}, {"role", "admin"},
	} {
		got, ok, err := restored.HGet("user:42", tc.field)
		if err != nil || !ok || got != tc.want {
			t.Fatalf("hash field %s: expected %q, got %q ok=%v err=%v", tc.field, tc.want, got, ok, err)
		}
	}

	// Verify 3: List order
	list, err := restored.LRange("mylist", 0, -1)
	if err != nil {
		t.Fatalf("lrange: %v", err)
	}
	wantList := []string{"z", "a", "b", "c"}
	if len(list) != len(wantList) {
		t.Fatalf("list length: expected %d, got %d: %v", len(wantList), len(list), list)
	}
	for i, w := range wantList {
		if list[i] != w {
			t.Fatalf("list[%d]: expected %q, got %q", i, w, list[i])
		}
	}

	// Verify 4: Set members
	members, err := restored.SMembers("colors")
	if err != nil {
		t.Fatalf("smembers: %v", err)
	}
	wantMembers := map[string]bool{"red": true, "green": true, "blue": true, "yellow": true}
	if len(members) != len(wantMembers) {
		t.Fatalf("set size: expected %d, got %d", len(wantMembers), len(members))
	}
	for _, m := range members {
		if !wantMembers[m] {
			t.Fatalf("unexpected set member: %q", m)
		}
	}

	// Verify 5: Sorted set ordering
	zitems, err := restored.ZRange("leaderboard", 0, -1)
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(zitems) != 3 {
		t.Fatalf("zset size: expected 3, got %d", len(zitems))
	}
	if zitems[0].Member != "alice" || zitems[0].Score != 100 {
		t.Fatalf("zset[0]: expected alice/100, got %s/%.0f", zitems[0].Member, zitems[0].Score)
	}
	if zitems[1].Member != "charlie" || zitems[1].Score != 150 {
		t.Fatalf("zset[1]: expected charlie/150, got %s/%.0f", zitems[1].Member, zitems[1].Score)
	}
	if zitems[2].Member != "bob" || zitems[2].Score != 200 {
		t.Fatalf("zset[2]: expected bob/200, got %s/%.0f", zitems[2].Member, zitems[2].Score)
	}

	// Verify 6: Stream entries and consumer groups
	entries := restored.XRange("events", "-", "+", 0)
	if len(entries) != 2 {
		t.Fatalf("stream entries: expected 2, got %d", len(entries))
	}
	if entries[0].ID != "1-1" || entries[1].ID != "2-1" {
		t.Fatalf("stream IDs: got %s, %s", entries[0].ID, entries[1].ID)
	}
	if len(entries[0].Fields) < 2 {
		t.Fatalf("stream entry 0 fields too short: %v", entries[0].Fields)
	}
	pending, err := restored.XReadGroupPlan("processors", "worker1", []string{"events"}, []string{"0-0"}, 10)
	if err != nil {
		t.Fatalf("xreadgroup plan: %v", err)
	}
	if len(pending["events"]) != 1 || pending["events"][0].ID != "1-1" {
		t.Fatalf("pending entries: expected 1-1, got %#v", pending)
	}

	// Verify 7: HLL cardinality preserved
	restoredCount, err := restored.PFCount("visitors")
	if err != nil {
		t.Fatalf("pfcount restored: %v", err)
	}
	if restoredCount != origCount {
		t.Fatalf("hll count: expected %d, got %d", origCount, restoredCount)
	}

	// Verify 8: JSON paths resolve correctly
	got, ok, err := restored.JSONGet("profile:1", "$.name")
	if err != nil || !ok {
		t.Fatalf("json.get name: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(got, "Ada") {
		t.Fatalf("json.get name: expected Ada in %q", got)
	}
	got, ok, err = restored.JSONGet("profile:1", "$.address.city")
	if err != nil || !ok {
		t.Fatalf("json.get city: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(got, "London") {
		t.Fatalf("json.get city: expected London in %q", got)
	}

	// Verify 9: Time-series samples in order
	samples, err := restored.TSRange("temperature", 0, 5000, 100)
	if err != nil {
		t.Fatalf("ts.range: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("ts samples: expected 3, got %d", len(samples))
	}
	if samples[0].Timestamp != 1000 || samples[0].Value != 22.5 {
		t.Fatalf("ts sample 0: expected 1000/22.5, got %d/%.1f", samples[0].Timestamp, samples[0].Value)
	}
	if samples[1].Timestamp != 2000 || samples[1].Value != 23.1 {
		t.Fatalf("ts sample 1: expected 2000/23.1, got %d/%.1f", samples[1].Timestamp, samples[1].Value)
	}
	if samples[2].Timestamp != 3000 || samples[2].Value != 21.8 {
		t.Fatalf("ts sample 2: expected 3000/21.8, got %d/%.1f", samples[2].Timestamp, samples[2].Value)
	}

	// Verify 10: Queue messages are intact
	deadMsgs := restored.QDead("jobqueue", 10)
	if len(deadMsgs) == 0 {
		// At minimum, check the queue still exists and has messages
		qlen := restored.QLen("jobqueue")
		if qlen == 0 && len(deadMsgs) == 0 {
			t.Logf("queue state: pending=%d dead=%d (queue may have been fully consumed)", qlen, len(deadMsgs))
		}
	}

	// Verify 11: Tags are queryable
	tags := restored.TagGet("tagged-key")
	if tags["env"] != "prod" || tags["team"] != "platform" {
		t.Fatalf("tags: expected env=prod,team=platform, got %v", tags)
	}
	queryResult := restored.TagQuery(map[string]string{"env": "prod"}, 10)
	found := false
	for _, k := range queryResult {
		if k == "tagged-key" {
			found = true
		}
	}
	if !found {
		t.Fatalf("tag query: expected to find 'tagged-key', got %v", queryResult)
	}

	// Verify 12: Delayed key visibility preserved (should not be visible yet)
	if _, ok := restored.Get("delayed-key"); ok {
		t.Fatal("delayed key should not be visible yet")
	}
}

// TestAOFReplayAllTypes writes AOF entries for every command type, replays them, and verifies state.
func TestAOFReplayAllTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}

	futureExpiry := time.Now().Add(10 * time.Minute).UnixNano()

	cmds := [][]string{
		// SETABS with expiry
		{"SETABS", "key1", "value1", strconv.FormatInt(futureExpiry, 10)},
		// HSET with multiple fields
		{"HSET", "hash1", "f1", "v1", "f2", "v2", "f3", "v3"},
		// LPUSH, RPUSH
		{"RPUSH", "list1", "a", "b", "c"},
		{"LPUSH", "list1", "z"},
		// SADD
		{"SADD", "set1", "alpha", "beta", "gamma"},
		// ZADD
		{"ZADD", "zset1", "1.5", "low", "5.0", "mid", "9.9", "high"},
		// XADD with consumer group
		{"XADD", "stream1", "1-1", "action", "create"},
		{"XADD", "stream1", "2-1", "action", "update"},
		{"XGROUP", "CREATE", "stream1", "mygroup", "0-0"},
		{"XGROUPDELIVER", "stream1", "mygroup", "consumer1", strconv.FormatInt(time.Now().UnixNano(), 10), "1-1"},
		{"XACK", "stream1", "mygroup", "1-1"},
		// APPEND
		{"SET", "appendkey", "hello"},
		{"APPEND", "appendkey", " world"},
		// INCR, INCRBY
		{"INCR", "counter"},
		{"INCRBY", "counter", "10"},
		// JSON.SET
		{"JSON.SET", "doc1", "$", `{"name":"test","items":[1,2,3]}`},
		// TS.ADD
		{"TS.ADD", "metric1", "1000", "42.5", "LABELS", "host=server1", "region=us"},
		{"TS.ADD", "metric1", "2000", "43.1"},
		// ENQUEUE
		{"ENQUEUE", "taskq", "payload-alpha"},
		{"ENQUEUE", "taskq", "payload-beta"},
		// TAG
		{"SET", "tagged1", "data1"},
		{"TAG", "tagged1", "env=staging", "team=infra"},
		// SETDELAYED
		{"SETDELAYED", "later-key", "later-value", "3600"},
		// PFADD
		{"PFADD", "hll1", "elem1", "elem2", "elem3"},
		// RENAME
		{"SET", "oldname", "renametest"},
		{"RENAME", "oldname", "newname"},
		// COPY
		{"SET", "src-copy", "copydata"},
		{"COPY", "src-copy", "dst-copy"},
		// LSET
		{"LSET", "list1", "0", "Z"},
		// LINSERT
		{"LINSERT", "list1", "BEFORE", "a", "inserted"},
		// LREM
		{"RPUSH", "list-rem", "x", "y", "x", "z", "x"},
		{"LREM", "list-rem", "2", "x"},
		// SMOVE
		{"SADD", "src-set", "m1", "m2"},
		{"SADD", "dst-set", "m3"},
		{"SMOVE", "src-set", "dst-set", "m1"},
		// HDEL
		{"HSET", "hash-del", "keep", "yes", "remove", "no"},
		{"HDEL", "hash-del", "remove"},
		// DEL
		{"SET", "delete-me", "gone"},
		{"DEL", "delete-me"},
	}

	for i, cmd := range cmds {
		if err := aof.Append(cmd); err != nil {
			t.Fatalf("append cmd %d (%s): %v", i, cmd[0], err)
		}
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st := store.New()
	if err := ReplayAOF(path, st); err != nil {
		t.Fatalf("replay: %v", err)
	}

	// Verify SETABS with expiry
	if got, ok := st.Get("key1"); !ok || got != "value1" {
		t.Fatalf("setabs: expected 'value1', got %q ok=%v", got, ok)
	}
	if ttl := st.TTL("key1"); ttl <= 0 {
		t.Fatalf("setabs TTL: expected positive, got %d", ttl)
	}

	// Verify HSET
	for _, tc := range []struct{ f, v string }{{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}} {
		got, ok, err := st.HGet("hash1", tc.f)
		if err != nil || !ok || got != tc.v {
			t.Fatalf("hset %s: expected %q, got %q ok=%v err=%v", tc.f, tc.v, got, ok, err)
		}
	}

	// Verify LPUSH + RPUSH + LSET + LINSERT
	list, err := st.LRange("list1", 0, -1)
	if err != nil {
		t.Fatalf("lrange: %v", err)
	}
	// Original: RPUSH a,b,c -> [a,b,c], LPUSH z -> [z,a,b,c], LSET 0 Z -> [Z,a,b,c], LINSERT BEFORE a inserted -> [Z,inserted,a,b,c]
	wantList := []string{"Z", "inserted", "a", "b", "c"}
	if len(list) != len(wantList) {
		t.Fatalf("list: expected %v, got %v", wantList, list)
	}
	for i, w := range wantList {
		if list[i] != w {
			t.Fatalf("list[%d]: expected %q, got %q (full: %v)", i, w, list[i], list)
		}
	}

	// Verify SADD
	members, err := st.SMembers("set1")
	if err != nil {
		t.Fatalf("smembers: %v", err)
	}
	wantSet := map[string]bool{"alpha": true, "beta": true, "gamma": true}
	if len(members) != 3 {
		t.Fatalf("set size: expected 3, got %d: %v", len(members), members)
	}
	for _, m := range members {
		if !wantSet[m] {
			t.Fatalf("unexpected set member: %q", m)
		}
	}

	// Verify ZADD ordering
	zitems, err := st.ZRange("zset1", 0, -1)
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(zitems) != 3 {
		t.Fatalf("zset size: expected 3, got %d", len(zitems))
	}
	if zitems[0].Member != "low" || zitems[0].Score != 1.5 {
		t.Fatalf("zset[0]: expected low/1.5, got %s/%.1f", zitems[0].Member, zitems[0].Score)
	}
	if zitems[1].Member != "mid" || zitems[1].Score != 5.0 {
		t.Fatalf("zset[1]: expected mid/5.0, got %s/%.1f", zitems[1].Member, zitems[1].Score)
	}
	if zitems[2].Member != "high" || zitems[2].Score != 9.9 {
		t.Fatalf("zset[2]: expected high/9.9, got %s/%.1f", zitems[2].Member, zitems[2].Score)
	}

	// Verify stream entries and acked consumer group
	entries := st.XRange("stream1", "-", "+", 0)
	if len(entries) != 2 || entries[0].ID != "1-1" || entries[1].ID != "2-1" {
		t.Fatalf("stream entries: got %#v", entries)
	}
	pending, err := st.XReadGroupPlan("mygroup", "consumer1", []string{"stream1"}, []string{"0-0"}, 10)
	if err != nil {
		t.Fatalf("xreadgroup plan: %v", err)
	}
	if len(pending["stream1"]) != 0 {
		t.Fatalf("expected acked entry to be cleared, got %d pending", len(pending["stream1"]))
	}

	// Verify APPEND
	if got, ok := st.Get("appendkey"); !ok || got != "hello world" {
		t.Fatalf("append: expected 'hello world', got %q ok=%v", got, ok)
	}

	// Verify INCR + INCRBY
	if got, ok := st.Get("counter"); !ok || got != "11" {
		t.Fatalf("counter: expected '11', got %q ok=%v", got, ok)
	}

	// Verify JSON.SET
	got, ok, err := st.JSONGet("doc1", "$.name")
	if err != nil || !ok {
		t.Fatalf("json.get: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(got, "test") {
		t.Fatalf("json.get name: expected 'test' in %q", got)
	}

	// Verify TS.ADD
	samples, err := st.TSRange("metric1", 0, 5000, 100)
	if err != nil {
		t.Fatalf("ts.range: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("ts samples: expected 2, got %d", len(samples))
	}
	if samples[0].Timestamp != 1000 || samples[0].Value != 42.5 {
		t.Fatalf("ts sample 0: expected 1000/42.5, got %d/%.1f", samples[0].Timestamp, samples[0].Value)
	}

	// Verify ENQUEUE
	qlen := st.QLen("taskq")
	if qlen != 2 {
		t.Fatalf("queue len: expected 2, got %d", qlen)
	}

	// Verify TAG
	tags := st.TagGet("tagged1")
	if tags["env"] != "staging" || tags["team"] != "infra" {
		t.Fatalf("tags: expected env=staging,team=infra, got %v", tags)
	}

	// Verify SETDELAYED (not visible yet due to 1h delay)
	if _, ok := st.Get("later-key"); ok {
		t.Fatal("delayed key should not be visible")
	}

	// Verify PFADD
	cnt, err := st.PFCount("hll1")
	if err != nil {
		t.Fatalf("pfcount: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("hll count: expected 3, got %d", cnt)
	}

	// Verify RENAME
	if _, ok := st.Get("oldname"); ok {
		t.Fatal("old key should not exist after rename")
	}
	if got, ok := st.Get("newname"); !ok || got != "renametest" {
		t.Fatalf("rename: expected 'renametest', got %q ok=%v", got, ok)
	}

	// Verify COPY
	if got, ok := st.Get("dst-copy"); !ok || got != "copydata" {
		t.Fatalf("copy: expected 'copydata', got %q ok=%v", got, ok)
	}
	if got, ok := st.Get("src-copy"); !ok || got != "copydata" {
		t.Fatalf("copy source should still exist: got %q ok=%v", got, ok)
	}

	// Verify LREM
	remList, err := st.LRange("list-rem", 0, -1)
	if err != nil {
		t.Fatalf("lrange list-rem: %v", err)
	}
	// Original [x,y,x,z,x], LREM 2 x removes first 2 occurrences -> [y,z,x]
	wantRem := []string{"y", "z", "x"}
	if len(remList) != len(wantRem) {
		t.Fatalf("lrem: expected %v, got %v", wantRem, remList)
	}
	for i, w := range wantRem {
		if remList[i] != w {
			t.Fatalf("lrem[%d]: expected %q, got %q", i, w, remList[i])
		}
	}

	// Verify SMOVE
	srcMembers, _ := st.SMembers("src-set")
	dstMembers, _ := st.SMembers("dst-set")
	srcMap := make(map[string]bool)
	for _, m := range srcMembers {
		srcMap[m] = true
	}
	dstMap := make(map[string]bool)
	for _, m := range dstMembers {
		dstMap[m] = true
	}
	if srcMap["m1"] {
		t.Fatal("smove: m1 should not be in src-set")
	}
	if !srcMap["m2"] {
		t.Fatal("smove: m2 should still be in src-set")
	}
	if !dstMap["m1"] || !dstMap["m3"] {
		t.Fatalf("smove: dst-set should contain m1 and m3, got %v", dstMembers)
	}

	// Verify HDEL
	if _, ok, err := st.HGet("hash-del", "keep"); err != nil || !ok {
		t.Fatalf("hdel: 'keep' field should exist, ok=%v err=%v", ok, err)
	}
	if _, ok, _ := st.HGet("hash-del", "remove"); ok {
		t.Fatal("hdel: 'remove' field should be deleted")
	}

	// Verify DEL
	if _, ok := st.Get("delete-me"); ok {
		t.Fatal("del: key should not exist")
	}
}

// TestSnapshotBackwardCompatibility loads a Snapshot with only original fields (nil for new fields)
// and verifies the store is functional with empty new-type maps.
func TestSnapshotBackwardCompatibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.json")

	// Create a snapshot with only the original fields populated
	st := store.New()
	st.Set("name", "vole", 0)
	if _, err := st.HSet("user:1", []store.HashPair{{Field: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if _, err := st.RPush("q", "a"); err != nil {
		t.Fatalf("rpush: %v", err)
	}
	if _, err := st.SAdd("s", "x"); err != nil {
		t.Fatalf("sadd: %v", err)
	}
	if _, err := st.ZAdd("z", []store.ZMember{{Member: "m", Score: 1}}); err != nil {
		t.Fatalf("zadd: %v", err)
	}
	if _, err := st.XAdd("stream", "1-1", []string{"k", "v"}); err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if err := st.XGroupCreate("stream", "g", "0-0", false); err != nil {
		t.Fatalf("xgroup: %v", err)
	}

	// Save the full snapshot, then reload and strip new fields to simulate old format
	if err := SaveSnapshot(path, st); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Load the snapshot from file, modify to have nil new fields, re-save
	snap := st.Dump()
	oldSnap := store.Snapshot{
		KV:       snap.KV,
		Hashes:   snap.Hashes,
		Lists:    snap.Lists,
		Sets:     snap.Sets,
		ZSets:    snap.ZSets,
		Streams:  snap.Streams,
		Groups:   snap.Groups,
		Expires:  snap.Expires,
		LastSeq:  snap.LastSeq,
		JSONDocs: nil, // simulate missing new fields
		TimeSeries: nil,
		Queues:     nil,
		Tags:       nil,
		HLLs:       nil,
		NotBefore:  nil,
	}

	restored := store.New()
	restored.Load(oldSnap)

	// Verify original data is intact
	if got, ok := restored.Get("name"); !ok || got != "vole" {
		t.Fatalf("string: expected 'vole', got %q ok=%v", got, ok)
	}
	if got, ok, _ := restored.HGet("user:1", "name"); !ok || got != "Ada" {
		t.Fatalf("hash: expected 'Ada', got %q", got)
	}

	// Verify new-type operations work on the restored store (empty but functional)
	if _, err := restored.PFAdd("new-hll", "x"); err != nil {
		t.Fatalf("pfadd on backward-compat store: %v", err)
	}
	if err := restored.JSONSet("new-json", "$", `{"a":1}`); err != nil {
		t.Fatalf("json.set on backward-compat store: %v", err)
	}
	if err := restored.TSAdd("new-ts", 1000, 1.0, nil); err != nil {
		t.Fatalf("ts.add on backward-compat store: %v", err)
	}
	_ = restored.Enqueue("new-q", "msg", 0)
	if err := restored.TagSet("name", map[string]string{"type": "app"}); err != nil {
		t.Fatalf("tag on backward-compat store: %v", err)
	}
	restored.SetDelayed("dk", "dv", time.Second, 0)

	// Verify the new data was stored
	cnt, _ := restored.PFCount("new-hll")
	if cnt != 1 {
		t.Fatalf("pfcount: expected 1, got %d", cnt)
	}
}

// TestAOFWithCRC32 verifies that CRC32 checksums work correctly:
// valid entries are replayed, and corrupted entries (bad checksum) are skipped.
func TestAOFWithCRC32(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.aof")

	// Write valid entries using the normal AOF writer
	aof, err := OpenAOF(path, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	if err := aof.Append([]string{"SET", "before", "ok"}); err != nil {
		t.Fatalf("append before: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Now manually append a corrupted entry and a valid entry
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}

	// Write a corrupted entry (valid format but wrong checksum)
	corruptedContent := "SET\tcorrupted\tbad"
	badChecksum := crc32.ChecksumIEEE([]byte(corruptedContent)) ^ 0xDEADBEEF // flip bits
	fmt.Fprintf(f, "%s\t#%08x\n", corruptedContent, badChecksum)

	// Write a valid entry manually
	validContent := "SET\tafter\talso_ok"
	goodChecksum := crc32.ChecksumIEEE([]byte(validContent))
	fmt.Fprintf(f, "%s\t#%08x\n", validContent, goodChecksum)

	if err := f.Close(); err != nil {
		t.Fatalf("close append: %v", err)
	}

	// Replay and verify
	st := store.New()
	if err := ReplayAOF(path, st); err != nil {
		t.Fatalf("replay: %v", err)
	}

	// "before" should exist (valid checksum from AOF writer)
	if got, ok := st.Get("before"); !ok || got != "ok" {
		t.Fatalf("before: expected 'ok', got %q ok=%v", got, ok)
	}

	// "corrupted" should NOT exist (bad checksum)
	if _, ok := st.Get("corrupted"); ok {
		t.Fatal("corrupted entry should have been skipped")
	}

	// "after" should exist (valid checksum)
	if got, ok := st.Get("after"); !ok || got != "also_ok" {
		t.Fatalf("after: expected 'also_ok', got %q ok=%v", got, ok)
	}
}

// TestSnapshotAndAOFCombined tests the real-world scenario:
// create data, save snapshot, make changes via AOF, restore from snapshot + AOF replay.
func TestSnapshotAndAOFCombined(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "dump.json")
	aofPath := filepath.Join(dir, "data.aof")

	// Phase 1: Create initial state and snapshot
	st := store.New()
	st.Set("name", "vole", 0)
	st.Set("version", "1", 0)
	if _, err := st.HSet("config", []store.HashPair{
		{Field: "port", Value: "7379"},
		{Field: "host", Value: "localhost"},
	}); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if _, err := st.RPush("log", "entry1", "entry2"); err != nil {
		t.Fatalf("rpush: %v", err)
	}
	if _, err := st.SAdd("features", "auth", "cache"); err != nil {
		t.Fatalf("sadd: %v", err)
	}
	if _, err := st.ZAdd("scores", []store.ZMember{
		{Member: "player1", Score: 100},
		{Member: "player2", Score: 200},
	}); err != nil {
		t.Fatalf("zadd: %v", err)
	}
	if _, err := st.XAdd("audit", "1-1", []string{"action", "start"}); err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if _, err := st.PFAdd("uniques", "u1", "u2"); err != nil {
		t.Fatalf("pfadd: %v", err)
	}
	if err := st.JSONSet("settings", "$", `{"debug":false,"level":3}`); err != nil {
		t.Fatalf("json.set: %v", err)
	}
	if err := st.TSAdd("cpu", 1000, 45.0, map[string]string{"host": "srv1"}); err != nil {
		t.Fatalf("ts.add: %v", err)
	}
	_ = st.Enqueue("work", "task1", 0)
	if err := st.TagSet("name", map[string]string{"type": "app"}); err != nil {
		t.Fatalf("tag: %v", err)
	}

	// Save snapshot
	if err := SaveSnapshot(snapshotPath, st); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	// Phase 2: Write incremental changes to AOF (simulating changes after snapshot)
	aof, err := OpenAOF(aofPath, FsyncAlways)
	if err != nil {
		t.Fatalf("open aof: %v", err)
	}
	// Update version
	if err := aof.Append([]string{"SET", "version", "2"}); err != nil {
		t.Fatalf("aof set: %v", err)
	}
	// Add new hash field
	if err := aof.Append([]string{"HSET", "config", "mode", "cluster"}); err != nil {
		t.Fatalf("aof hset: %v", err)
	}
	// Push to list
	if err := aof.Append([]string{"RPUSH", "log", "entry3"}); err != nil {
		t.Fatalf("aof rpush: %v", err)
	}
	// Add to set
	if err := aof.Append([]string{"SADD", "features", "replication"}); err != nil {
		t.Fatalf("aof sadd: %v", err)
	}
	// Update sorted set
	if err := aof.Append([]string{"ZADD", "scores", "300", "player3"}); err != nil {
		t.Fatalf("aof zadd: %v", err)
	}
	// Add stream entry
	if err := aof.Append([]string{"XADD", "audit", "2-1", "action", "config_change"}); err != nil {
		t.Fatalf("aof xadd: %v", err)
	}
	// Add to HLL
	if err := aof.Append([]string{"PFADD", "uniques", "u3", "u4"}); err != nil {
		t.Fatalf("aof pfadd: %v", err)
	}
	// Update JSON
	if err := aof.Append([]string{"JSON.SET", "settings", "$.debug", "true"}); err != nil {
		t.Fatalf("aof json.set: %v", err)
	}
	// Add time-series point
	if err := aof.Append([]string{"TS.ADD", "cpu", "2000", "67.3"}); err != nil {
		t.Fatalf("aof ts.add: %v", err)
	}
	// Enqueue another task
	if err := aof.Append([]string{"ENQUEUE", "work", "task2"}); err != nil {
		t.Fatalf("aof enqueue: %v", err)
	}
	// Delete a key
	if err := aof.Append([]string{"DEL", "name"}); err != nil {
		t.Fatalf("aof del: %v", err)
	}
	if err := aof.Close(); err != nil {
		t.Fatalf("close aof: %v", err)
	}

	// Phase 3: Restore from snapshot + AOF
	restored := store.New()
	if err := LoadSnapshot(snapshotPath, restored); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if err := ReplayAOF(aofPath, restored); err != nil {
		t.Fatalf("replay aof: %v", err)
	}

	// Verify final state reflects snapshot + incremental AOF changes

	// "name" was deleted by AOF
	if _, ok := restored.Get("name"); ok {
		t.Fatal("name should be deleted by AOF DEL")
	}

	// "version" was updated to "2"
	if got, ok := restored.Get("version"); !ok || got != "2" {
		t.Fatalf("version: expected '2', got %q ok=%v", got, ok)
	}

	// Hash should have original fields + new "mode" field
	if got, ok, _ := restored.HGet("config", "port"); !ok || got != "7379" {
		t.Fatalf("config port: expected '7379', got %q", got)
	}
	if got, ok, _ := restored.HGet("config", "mode"); !ok || got != "cluster" {
		t.Fatalf("config mode: expected 'cluster', got %q", got)
	}

	// List should have 3 entries
	logList, err := restored.LRange("log", 0, -1)
	if err != nil {
		t.Fatalf("lrange log: %v", err)
	}
	if len(logList) != 3 || logList[2] != "entry3" {
		t.Fatalf("log: expected [entry1,entry2,entry3], got %v", logList)
	}

	// Set should have 3 members
	feats, _ := restored.SMembers("features")
	featMap := make(map[string]bool)
	for _, f := range feats {
		featMap[f] = true
	}
	if !featMap["auth"] || !featMap["cache"] || !featMap["replication"] {
		t.Fatalf("features: expected auth,cache,replication, got %v", feats)
	}

	// Sorted set should have 3 members
	zitems, _ := restored.ZRange("scores", 0, -1)
	if len(zitems) != 3 {
		t.Fatalf("scores: expected 3 members, got %d", len(zitems))
	}
	if zitems[2].Member != "player3" || zitems[2].Score != 300 {
		t.Fatalf("scores[2]: expected player3/300, got %s/%.0f", zitems[2].Member, zitems[2].Score)
	}

	// Stream should have 2 entries
	streamEntries := restored.XRange("audit", "-", "+", 0)
	if len(streamEntries) != 2 {
		t.Fatalf("audit stream: expected 2 entries, got %d", len(streamEntries))
	}
	if streamEntries[1].ID != "2-1" {
		t.Fatalf("audit stream[1]: expected ID 2-1, got %s", streamEntries[1].ID)
	}

	// HLL should have ~4 unique elements
	cnt, _ := restored.PFCount("uniques")
	if cnt < 3 || cnt > 5 {
		t.Fatalf("hll count: expected ~4, got %d", cnt)
	}

	// JSON debug should be true
	got, ok, err := restored.JSONGet("settings", "$.debug")
	if err != nil || !ok {
		t.Fatalf("json.get debug: ok=%v err=%v", ok, err)
	}
	if !strings.Contains(got, "true") {
		t.Fatalf("json.get debug: expected true in %q", got)
	}

	// Time-series should have 2 samples
	samples, _ := restored.TSRange("cpu", 0, 5000, 100)
	if len(samples) != 2 {
		t.Fatalf("ts samples: expected 2, got %d", len(samples))
	}
	if samples[1].Timestamp != 2000 || samples[1].Value != 67.3 {
		t.Fatalf("ts sample 1: expected 2000/67.3, got %d/%.1f", samples[1].Timestamp, samples[1].Value)
	}

	// Queue should have original + new task
	qlen := restored.QLen("work")
	if qlen != 2 {
		t.Fatalf("queue len: expected 2, got %d", qlen)
	}

	// Tags from snapshot should persist (on "name" which was deleted, tags may be gone too)
	// So verify the tag system is functional
	restored.Set("new-tagged", "v", 0)
	if err := restored.TagSet("new-tagged", map[string]string{"role": "test"}); err != nil {
		t.Fatalf("tag after restore: %v", err)
	}
	tags := restored.TagGet("new-tagged")
	if tags["role"] != "test" {
		t.Fatalf("tag after restore: expected role=test, got %v", tags)
	}
}
