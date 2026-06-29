package store

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestKeyValueTTLAndIncr(t *testing.T) {
	st := New()
	st.Set("count", "1", 0)
	n, err := st.Incr("count")
	if err != nil {
		t.Fatalf("incr failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
	st.Set("short", "yes", time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if _, ok := st.Get("short"); ok {
		t.Fatal("expected expired key to be absent")
	}
}

func TestKeyspaceCommands(t *testing.T) {
	st := New()
	st.Set("app:name", "vole", 0)
	st.Set("app:version", "1", 0)
	if _, err := st.HSet("app:meta", []HashPair{{Field: "kind", Value: "hash"}}); err != nil {
		t.Fatalf("hset failed: %v", err)
	}
	st.Set("tmp", "gone", time.Millisecond)
	if _, err := st.XAdd("events", "1-1", []string{"type", "created"}); err != nil {
		t.Fatalf("xadd failed: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	if got := st.Exists("app:name", "missing", "events", "tmp", "events", "app:meta"); got != 4 {
		t.Fatalf("expected 4 existing keys (events counted twice), got %d", got)
	}
	values := st.MGet("app:name", "missing", "app:version")
	if len(values) != 3 || !values[0].OK || values[0].Value != "vole" || values[1].OK || values[2].Value != "1" {
		t.Fatalf("unexpected mget results: %#v", values)
	}
	if got := st.Type("app:name"); got != "string" {
		t.Fatalf("expected string type, got %s", got)
	}
	if got := st.Type("events"); got != "stream" {
		t.Fatalf("expected stream type, got %s", got)
	}
	if got := st.Type("app:meta"); got != "hash" {
		t.Fatalf("expected hash type, got %s", got)
	}
	if got := st.Type("missing"); got != "none" {
		t.Fatalf("expected none type, got %s", got)
	}
	keys := st.Keys("app:*")
	if len(keys) != 3 || keys[0] != "app:meta" || keys[1] != "app:name" || keys[2] != "app:version" {
		t.Fatalf("unexpected keys: %v", keys)
	}
	next, scan := st.Scan(0, 1, "app:*")
	if next == 0 || len(scan) != 1 {
		t.Fatalf("expected partial scan, next=%d keys=%v", next, scan)
	}
	next, scan = st.Scan(next, 10, "app:*")
	if next != 0 || len(scan) != 2 {
		t.Fatalf("expected final scan, next=%d keys=%v", next, scan)
	}
}

func TestHashCommands(t *testing.T) {
	st := New()
	added, err := st.HSet("user:1", []HashPair{{Field: "name", Value: "Ada"}, {Field: "role", Value: "admin"}})
	if err != nil {
		t.Fatalf("hset failed: %v", err)
	}
	if added != 2 {
		t.Fatalf("expected 2 added fields, got %d", added)
	}
	added, err = st.HSet("user:1", []HashPair{{Field: "role", Value: "operator"}})
	if err != nil {
		t.Fatalf("hset update failed: %v", err)
	}
	if added != 0 {
		t.Fatalf("expected no new fields, got %d", added)
	}
	value, ok, err := st.HGet("user:1", "role")
	if err != nil || !ok || value != "operator" {
		t.Fatalf("unexpected hget value=%q ok=%v err=%v", value, ok, err)
	}
	pairs, err := st.HGetAll("user:1")
	if err != nil {
		t.Fatalf("hgetall failed: %v", err)
	}
	if len(pairs) != 2 || pairs[0].Field != "name" || pairs[1].Field != "role" {
		t.Fatalf("unexpected hgetall: %#v", pairs)
	}
	deleted, err := st.HDel("user:1", "role", "missing")
	if err != nil {
		t.Fatalf("hdel failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected one deleted field, got %d", deleted)
	}
}

func TestExpirationAppliesToHashesAndStreams(t *testing.T) {
	st := New()
	if _, err := st.HSet("hash:1", []HashPair{{Field: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("hset failed: %v", err)
	}
	if _, err := st.XAdd("stream:1", "1-1", []string{"type", "created"}); err != nil {
		t.Fatalf("xadd failed: %v", err)
	}
	if !st.Expire("hash:1", time.Millisecond) {
		t.Fatal("expected hash expire to succeed")
	}
	if !st.Expire("stream:1", time.Millisecond) {
		t.Fatal("expected stream expire to succeed")
	}
	time.Sleep(5 * time.Millisecond)
	if got := st.Type("hash:1"); got != "none" {
		t.Fatalf("expected expired hash type none, got %s", got)
	}
	if got := st.Type("stream:1"); got != "none" {
		t.Fatalf("expected expired stream type none, got %s", got)
	}
	if got := st.Exists("hash:1", "stream:1"); got != 0 {
		t.Fatalf("expected expired keys absent, got %d", got)
	}
	if entries := st.XRange("stream:1", "-", "+", 0); len(entries) != 0 {
		t.Fatalf("expected expired stream empty, got %#v", entries)
	}
}

func TestHashDeleteClearsKeyExpiration(t *testing.T) {
	st := New()
	if _, err := st.HSet("hash:1", []HashPair{{Field: "name", Value: "Ada"}}); err != nil {
		t.Fatalf("hset failed: %v", err)
	}
	if !st.Expire("hash:1", time.Hour) {
		t.Fatal("expected expire to succeed")
	}
	if deleted, err := st.HDel("hash:1", "name"); err != nil || deleted != 1 {
		t.Fatalf("expected one deleted field, deleted=%d err=%v", deleted, err)
	}
	if _, err := st.HSet("hash:1", []HashPair{{Field: "name", Value: "Grace"}}); err != nil {
		t.Fatalf("hset recreate failed: %v", err)
	}
	if ttl := st.TTL("hash:1"); ttl != -1 {
		t.Fatalf("expected recreated hash to have no ttl, got %d", ttl)
	}
}

func TestListCommands(t *testing.T) {
	st := New()
	n, err := st.RPush("queue", "a", "b")
	if err != nil {
		t.Fatalf("rpush failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected list length 2, got %d", n)
	}
	n, err = st.LPush("queue", "c", "d")
	if err != nil {
		t.Fatalf("lpush failed: %v", err)
	}
	if n != 4 {
		t.Fatalf("expected list length 4, got %d", n)
	}
	values, err := st.LRange("queue", 0, -1)
	if err != nil {
		t.Fatalf("lrange failed: %v", err)
	}
	want := []string{"d", "c", "a", "b"}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, values)
		}
	}
	left, ok, err := st.LPop("queue")
	if err != nil || !ok || left != "d" {
		t.Fatalf("unexpected lpop value=%q ok=%v err=%v", left, ok, err)
	}
	right, ok, err := st.RPop("queue")
	if err != nil || !ok || right != "b" {
		t.Fatalf("unexpected rpop value=%q ok=%v err=%v", right, ok, err)
	}
	values, err = st.LRange("queue", -2, -1)
	if err != nil {
		t.Fatalf("lrange tail failed: %v", err)
	}
	if len(values) != 2 || values[0] != "c" || values[1] != "a" {
		t.Fatalf("unexpected tail range: %v", values)
	}
	if got := st.Type("queue"); got != "list" {
		t.Fatalf("expected list type, got %s", got)
	}
}

func TestExpirationAppliesToLists(t *testing.T) {
	st := New()
	if _, err := st.RPush("queue", "a"); err != nil {
		t.Fatalf("rpush failed: %v", err)
	}
	if !st.Expire("queue", time.Millisecond) {
		t.Fatal("expected list expire to succeed")
	}
	time.Sleep(5 * time.Millisecond)
	if got := st.Type("queue"); got != "none" {
		t.Fatalf("expected expired list none, got %s", got)
	}
	if values, err := st.LRange("queue", 0, -1); err != nil || len(values) != 0 {
		t.Fatalf("expected expired list empty, values=%v err=%v", values, err)
	}
}

func TestSetCommands(t *testing.T) {
	st := New()
	added, err := st.SAdd("tags", "red", "blue", "red")
	if err != nil {
		t.Fatalf("sadd failed: %v", err)
	}
	if added != 2 {
		t.Fatalf("expected 2 added members, got %d", added)
	}
	members, err := st.SMembers("tags")
	if err != nil {
		t.Fatalf("smembers failed: %v", err)
	}
	if len(members) != 2 || members[0] != "blue" || members[1] != "red" {
		t.Fatalf("unexpected members: %v", members)
	}
	ok, err := st.SIsMember("tags", "red")
	if err != nil || !ok {
		t.Fatalf("expected red member, ok=%v err=%v", ok, err)
	}
	if n, err := st.SCard("tags"); err != nil || n != 2 {
		t.Fatalf("expected cardinality 2, n=%d err=%v", n, err)
	}
	removed, err := st.SRem("tags", "red", "missing")
	if err != nil {
		t.Fatalf("srem failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected one removed member, got %d", removed)
	}
	if got := st.Type("tags"); got != "set" {
		t.Fatalf("expected set type, got %s", got)
	}
}

func TestExpirationAppliesToSets(t *testing.T) {
	st := New()
	if _, err := st.SAdd("tags", "red"); err != nil {
		t.Fatalf("sadd failed: %v", err)
	}
	if !st.Expire("tags", time.Millisecond) {
		t.Fatal("expected set expire to succeed")
	}
	time.Sleep(5 * time.Millisecond)
	if got := st.Type("tags"); got != "none" {
		t.Fatalf("expected expired set none, got %s", got)
	}
	if members, err := st.SMembers("tags"); err != nil || len(members) != 0 {
		t.Fatalf("expected expired set empty, members=%v err=%v", members, err)
	}
}

func TestSortedSetCommands(t *testing.T) {
	st := New()
	added, err := st.ZAdd("rank", []ZMember{{Member: "bob", Score: 2}, {Member: "ada", Score: 1}, {Member: "cam", Score: 2}})
	if err != nil {
		t.Fatalf("zadd failed: %v", err)
	}
	if added != 3 {
		t.Fatalf("expected 3 added members, got %d", added)
	}
	added, err = st.ZAdd("rank", []ZMember{{Member: "bob", Score: 0.5}})
	if err != nil {
		t.Fatalf("zadd update failed: %v", err)
	}
	if added != 0 {
		t.Fatalf("expected update count 0, got %d", added)
	}
	items, err := st.ZRange("rank", 0, -1)
	if err != nil {
		t.Fatalf("zrange failed: %v", err)
	}
	if len(items) != 3 || items[0].Member != "bob" || items[1].Member != "ada" || items[2].Member != "cam" {
		t.Fatalf("unexpected zrange: %#v", items)
	}
	score, ok, err := st.ZScore("rank", "bob")
	if err != nil || !ok || score != 0.5 {
		t.Fatalf("unexpected zscore score=%v ok=%v err=%v", score, ok, err)
	}
	if card, err := st.ZCard("rank"); err != nil || card != 3 {
		t.Fatalf("unexpected zcard card=%d err=%v", card, err)
	}
	removed, err := st.ZRem("rank", "ada", "missing")
	if err != nil {
		t.Fatalf("zrem failed: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected one removed, got %d", removed)
	}
	if got := st.Type("rank"); got != "zset" {
		t.Fatalf("expected zset type, got %s", got)
	}
}

func TestExpirationAppliesToSortedSets(t *testing.T) {
	st := New()
	if _, err := st.ZAdd("rank", []ZMember{{Member: "ada", Score: 1}}); err != nil {
		t.Fatalf("zadd failed: %v", err)
	}
	if !st.Expire("rank", time.Millisecond) {
		t.Fatal("expected zset expire to succeed")
	}
	time.Sleep(5 * time.Millisecond)
	if got := st.Type("rank"); got != "none" {
		t.Fatalf("expected expired zset none, got %s", got)
	}
	if card, err := st.ZCard("rank"); err != nil || card != 0 {
		t.Fatalf("expected expired zset card 0, card=%d err=%v", card, err)
	}
}

func TestStreamsRangeAndRead(t *testing.T) {
	st := New()
	id1, err := st.XAdd("events", "*", []string{"type", "created"})
	if err != nil {
		t.Fatalf("xadd failed: %v", err)
	}
	id2, err := st.XAdd("events", "*", []string{"type", "updated"})
	if err != nil {
		t.Fatalf("xadd failed: %v", err)
	}
	if compareID(id2, id1) <= 0 {
		t.Fatalf("expected IDs to increase: %s then %s", id1, id2)
	}

	rangeEntries := st.XRange("events", "-", "+", 0)
	if len(rangeEntries) != 2 {
		t.Fatalf("expected 2 range entries, got %d", len(rangeEntries))
	}
	read := st.XRead([]string{"events"}, []string{id1})
	if got := len(read["events"]); got != 1 {
		t.Fatalf("expected one unread entry, got %d", got)
	}
	if read["events"][0].ID != id2 {
		t.Fatalf("expected %s, got %s", id2, read["events"][0].ID)
	}
}

func TestStreamConsumerGroupsPlanDeliverAndAck(t *testing.T) {
	st := New()
	if _, err := st.XAdd("events", "1-1", []string{"type", "created"}); err != nil {
		t.Fatalf("xadd 1 failed: %v", err)
	}
	if _, err := st.XAdd("events", "1-2", []string{"type", "updated"}); err != nil {
		t.Fatalf("xadd 2 failed: %v", err)
	}
	if err := st.XGroupCreate("events", "workers", "0-0", false); err != nil {
		t.Fatalf("xgroup create failed: %v", err)
	}
	planned, err := st.XReadGroupPlan("workers", "c1", []string{"events"}, []string{">"}, 1)
	if err != nil {
		t.Fatalf("xreadgroup plan failed: %v", err)
	}
	if len(planned["events"]) != 1 || planned["events"][0].ID != "1-1" {
		t.Fatalf("expected first entry planned, got %#v", planned)
	}
	if err := st.XGroupDeliver("events", "workers", "c1", []string{"1-1"}, time.Now()); err != nil {
		t.Fatalf("deliver failed: %v", err)
	}
	planned, err = st.XReadGroupPlan("workers", "c1", []string{"events"}, []string{">"}, 10)
	if err != nil {
		t.Fatalf("second xreadgroup plan failed: %v", err)
	}
	if len(planned["events"]) != 1 || planned["events"][0].ID != "1-2" {
		t.Fatalf("expected second entry planned, got %#v", planned)
	}
	pending, err := st.XReadGroupPlan("workers", "c1", []string{"events"}, []string{"0-0"}, 10)
	if err != nil {
		t.Fatalf("pending plan failed: %v", err)
	}
	if len(pending["events"]) != 1 || pending["events"][0].ID != "1-1" {
		t.Fatalf("expected pending first entry, got %#v", pending)
	}
	acked, err := st.XAck("events", "workers", "1-1")
	if err != nil {
		t.Fatalf("xack failed: %v", err)
	}
	if acked != 1 {
		t.Fatalf("expected one acked entry, got %d", acked)
	}
	pending, err = st.XReadGroupPlan("workers", "c1", []string{"events"}, []string{"0-0"}, 10)
	if err != nil {
		t.Fatalf("pending plan after ack failed: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending entries after ack, got %#v", pending)
	}
}

func TestWaitWakesAndCleansMultiStreamWaiter(t *testing.T) {
	st := New()
	done := make(chan struct{})
	go func() {
		st.Wait([]string{"a", "b"}, time.Second)
		close(done)
	}()
	time.Sleep(10 * time.Millisecond)
	if _, err := st.XAdd("a", "*", []string{"k", "v"}); err != nil {
		t.Fatalf("xadd a failed: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waiter did not wake")
	}
	if _, err := st.XAdd("b", "*", []string{"k", "v"}); err != nil {
		t.Fatalf("xadd b failed after waiter cleanup: %v", err)
	}
}

func TestLastIDSupportsFutureOnlyReads(t *testing.T) {
	st := New()
	if _, err := st.XAdd("events", "*", []string{"k", "old"}); err != nil {
		t.Fatalf("xadd old failed: %v", err)
	}
	last := st.LastID("events")
	if got := st.XRead([]string{"events"}, []string{last}); len(got) != 0 {
		t.Fatalf("expected no existing entries after last ID, got %#v", got)
	}
	newID, err := st.XAdd("events", "*", []string{"k", "new"})
	if err != nil {
		t.Fatalf("xadd new failed: %v", err)
	}
	got := st.XRead([]string{"events"}, []string{last})
	if len(got["events"]) != 1 || got["events"][0].ID != newID {
		t.Fatalf("expected only future entry %s, got %#v", newID, got)
	}
}

func TestAutoStreamIDAfterExplicitFutureID(t *testing.T) {
	st := New()
	explicit := "9999999999999-7"
	if _, err := st.XAdd("events", explicit, []string{"k", "v"}); err != nil {
		t.Fatalf("explicit xadd failed: %v", err)
	}
	auto, err := st.XAdd("events", "*", []string{"k", "v2"})
	if err != nil {
		t.Fatalf("auto xadd failed: %v", err)
	}
	if compareID(auto, explicit) <= 0 {
		t.Fatalf("auto ID %s must be greater than explicit ID %s", auto, explicit)
	}
}

func TestDeque(t *testing.T) {
	t.Run("PushFrontAndBack", func(t *testing.T) {
		d := &Deque{}
		d.PushBack("a", "b", "c")
		d.PushFront("x", "y", "z")
		// PushFront: x then y then z — z ends up at front (Redis LPUSH semantics)
		got := d.ToSlice()
		want := []string{"z", "y", "x", "a", "b", "c"}
		if len(got) != len(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("index %d: expected %q, got %q (full: %v)", i, want[i], got[i], got)
			}
		}
	})

	t.Run("PopFrontAndBack", func(t *testing.T) {
		d := &Deque{}
		d.PushBack("a", "b", "c")
		v, ok := d.PopFront()
		if !ok || v != "a" {
			t.Fatalf("popfront expected a, got %q ok=%v", v, ok)
		}
		v, ok = d.PopBack()
		if !ok || v != "c" {
			t.Fatalf("popback expected c, got %q ok=%v", v, ok)
		}
		if d.Len() != 1 {
			t.Fatalf("expected len 1, got %d", d.Len())
		}
		v, ok = d.PopFront()
		if !ok || v != "b" {
			t.Fatalf("expected b, got %q ok=%v", v, ok)
		}
		_, ok = d.PopFront()
		if ok {
			t.Fatal("expected empty pop to return false")
		}
		_, ok = d.PopBack()
		if ok {
			t.Fatal("expected empty pop to return false")
		}
	})

	t.Run("RangeNegativeIndices", func(t *testing.T) {
		d := &Deque{}
		d.PushBack("a", "b", "c", "d", "e")
		got := d.Range(-3, -1)
		want := []string{"c", "d", "e"}
		if len(got) != len(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("index %d: expected %q, got %q", i, want[i], got[i])
			}
		}
		// Out of range
		got = d.Range(10, 20)
		if len(got) != 0 {
			t.Fatalf("expected empty range, got %v", got)
		}
	})

	t.Run("GrowBeyondInitialCapacity", func(t *testing.T) {
		d := &Deque{}
		// Push more than dequeMinCap (8) elements to force multiple grows
		for i := 0; i < 50; i++ {
			d.PushBack(fmt.Sprintf("%d", i))
		}
		if d.Len() != 50 {
			t.Fatalf("expected len 50, got %d", d.Len())
		}
		slice := d.ToSlice()
		for i := 0; i < 50; i++ {
			if slice[i] != fmt.Sprintf("%d", i) {
				t.Fatalf("index %d: expected %q, got %q", i, fmt.Sprintf("%d", i), slice[i])
			}
		}
	})

	t.Run("InsertAndGetSet", func(t *testing.T) {
		d := &Deque{}
		d.PushBack("a", "b", "d")
		d.Insert(2, "c") // insert "c" before "d"
		got := d.ToSlice()
		want := []string{"a", "b", "c", "d"}
		if len(got) != len(want) {
			t.Fatalf("expected %v, got %v", want, got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("index %d: expected %q, got %q", i, want[i], got[i])
			}
		}
		// Test Get
		if d.Get(2) != "c" {
			t.Fatalf("Get(2) expected c, got %q", d.Get(2))
		}
		// Test Set
		d.Set(2, "C")
		if d.Get(2) != "C" {
			t.Fatalf("after Set(2, C), Get(2) expected C, got %q", d.Get(2))
		}
	})
}

func TestSortedSetMaintainedOrder(t *testing.T) {
	t.Run("AddAndRange", func(t *testing.T) {
		zs := NewSortedSet()
		zs.Add("charlie", 3)
		zs.Add("alice", 1)
		zs.Add("bob", 2)
		items := zs.Range(0, -1)
		if len(items) != 3 {
			t.Fatalf("expected 3 items, got %d", len(items))
		}
		if items[0].Member != "alice" || items[1].Member != "bob" || items[2].Member != "charlie" {
			t.Fatalf("unexpected order: %#v", items)
		}
	})

	t.Run("UpdateScore", func(t *testing.T) {
		zs := NewSortedSet()
		zs.Add("alice", 1)
		zs.Add("bob", 2)
		zs.Add("charlie", 3)
		// Move alice to highest score
		zs.Add("alice", 10)
		items := zs.Range(0, -1)
		if items[0].Member != "bob" || items[1].Member != "charlie" || items[2].Member != "alice" {
			t.Fatalf("after score update, unexpected order: %#v", items)
		}
	})

	t.Run("Remove", func(t *testing.T) {
		zs := NewSortedSet()
		zs.Add("alice", 1)
		zs.Add("bob", 2)
		zs.Add("charlie", 3)
		if !zs.Remove("bob") {
			t.Fatal("expected Remove(bob) to return true")
		}
		if zs.Remove("bob") {
			t.Fatal("expected second Remove(bob) to return false")
		}
		if zs.Len() != 2 {
			t.Fatalf("expected len 2, got %d", zs.Len())
		}
		_, ok := zs.Score("bob")
		if ok {
			t.Fatal("expected Score(bob) to not exist after remove")
		}
	})

	t.Run("RangeByScore", func(t *testing.T) {
		zs := NewSortedSet()
		zs.Add("a", 1)
		zs.Add("b", 2)
		zs.Add("c", 3)
		zs.Add("d", 4)
		zs.Add("e", 5)
		items := zs.RangeByScore(2, 4, 0, 0)
		if len(items) != 3 {
			t.Fatalf("expected 3 items in score range [2,4], got %d", len(items))
		}
		if items[0].Member != "b" || items[1].Member != "c" || items[2].Member != "d" {
			t.Fatalf("unexpected range by score: %#v", items)
		}
		// With LIMIT
		items = zs.RangeByScore(1, 5, 1, 2)
		if len(items) != 2 || items[0].Member != "b" || items[1].Member != "c" {
			t.Fatalf("unexpected range by score with limit: %#v", items)
		}
	})

	t.Run("RevRange", func(t *testing.T) {
		zs := NewSortedSet()
		zs.Add("a", 1)
		zs.Add("b", 2)
		zs.Add("c", 3)
		items := zs.RevRange(0, -1)
		if len(items) != 3 {
			t.Fatalf("expected 3 items, got %d", len(items))
		}
		if items[0].Member != "c" || items[1].Member != "b" || items[2].Member != "a" {
			t.Fatalf("unexpected revrange: %#v", items)
		}
		// Partial
		items = zs.RevRange(0, 1)
		if len(items) != 2 || items[0].Member != "c" || items[1].Member != "b" {
			t.Fatalf("unexpected partial revrange: %#v", items)
		}
	})

	t.Run("RankAndCountByScore", func(t *testing.T) {
		zs := NewSortedSet()
		zs.Add("a", 1)
		zs.Add("b", 2)
		zs.Add("c", 3)
		rank, ok := zs.Rank("b")
		if !ok || rank != 1 {
			t.Fatalf("expected rank 1 for b, got %d ok=%v", rank, ok)
		}
		_, ok = zs.Rank("missing")
		if ok {
			t.Fatal("expected Rank(missing) to return false")
		}
		count := zs.CountByScore(1, 2)
		if count != 2 {
			t.Fatalf("expected count 2 for score range [1,2], got %d", count)
		}
		count = zs.CountByScore(5, 10)
		if count != 0 {
			t.Fatalf("expected count 0 for score range [5,10], got %d", count)
		}
	})
}

func TestArithmeticCommands(t *testing.T) {
	t.Run("IncrBy", func(t *testing.T) {
		st := New()
		st.Set("counter", "10", 0)
		n, err := st.IncrBy("counter", 5)
		if err != nil || n != 15 {
			t.Fatalf("expected 15, got %d err=%v", n, err)
		}
		n, err = st.IncrBy("counter", -3)
		if err != nil || n != 12 {
			t.Fatalf("expected 12, got %d err=%v", n, err)
		}
	})

	t.Run("DecrBy", func(t *testing.T) {
		st := New()
		st.Set("counter", "10", 0)
		n, err := st.DecrBy("counter", 3)
		if err != nil || n != 7 {
			t.Fatalf("expected 7, got %d err=%v", n, err)
		}
		n, err = st.DecrBy("counter", 10)
		if err != nil || n != -3 {
			t.Fatalf("expected -3, got %d err=%v", n, err)
		}
	})

	t.Run("IncrByFloat", func(t *testing.T) {
		st := New()
		st.Set("price", "10.5", 0)
		f, err := st.IncrByFloat("price", 1.5)
		if err != nil || f != 12.0 {
			t.Fatalf("expected 12.0, got %f err=%v", f, err)
		}
		f, err = st.IncrByFloat("price", -2.5)
		if err != nil || f != 9.5 {
			t.Fatalf("expected 9.5, got %f err=%v", f, err)
		}
	})

	t.Run("IncrByOnNewKey", func(t *testing.T) {
		st := New()
		n, err := st.IncrBy("newkey", 5)
		if err != nil || n != 5 {
			t.Fatalf("expected 5 on new key, got %d err=%v", n, err)
		}
	})

	t.Run("IncrByFloatOnNewKey", func(t *testing.T) {
		st := New()
		f, err := st.IncrByFloat("newkey", 3.14)
		if err != nil || f != 3.14 {
			t.Fatalf("expected 3.14 on new key, got %f err=%v", f, err)
		}
	})

	t.Run("ErrorOnWrongType", func(t *testing.T) {
		st := New()
		if _, err := st.RPush("mylist", "a"); err != nil {
			t.Fatalf("rpush failed: %v", err)
		}
		_, err := st.IncrBy("mylist", 1)
		if err == nil {
			t.Fatal("expected error on wrong type for IncrBy")
		}
		_, err = st.IncrByFloat("mylist", 1.0)
		if err == nil {
			t.Fatal("expected error on wrong type for IncrByFloat")
		}
	})

	t.Run("ErrorOnNonNumeric", func(t *testing.T) {
		st := New()
		st.Set("str", "notanumber", 0)
		_, err := st.IncrBy("str", 1)
		if err == nil {
			t.Fatal("expected error on non-numeric value for IncrBy")
		}
		st.Set("str2", "notafloat", 0)
		_, err = st.IncrByFloat("str2", 1.0)
		if err == nil {
			t.Fatal("expected error on non-numeric value for IncrByFloat")
		}
	})
}

func TestHashExtended(t *testing.T) {
	t.Run("HIncrByExisting", func(t *testing.T) {
		st := New()
		st.HSet("h", []HashPair{{Field: "count", Value: "10"}})
		n, err := st.HIncrBy("h", "count", 5)
		if err != nil || n != 15 {
			t.Fatalf("expected 15, got %d err=%v", n, err)
		}
	})

	t.Run("HIncrByNewField", func(t *testing.T) {
		st := New()
		n, err := st.HIncrBy("h", "count", 3)
		if err != nil || n != 3 {
			t.Fatalf("expected 3 on new field, got %d err=%v", n, err)
		}
	})

	t.Run("HIncrByWrongType", func(t *testing.T) {
		st := New()
		st.Set("str", "value", 0)
		_, err := st.HIncrBy("str", "field", 1)
		if err == nil {
			t.Fatal("expected wrong type error")
		}
	})

	t.Run("HSetNXFieldDoesNotExist", func(t *testing.T) {
		st := New()
		ok, err := st.HSetNX("h", "name", "Ada")
		if err != nil || !ok {
			t.Fatalf("expected true for new field, ok=%v err=%v", ok, err)
		}
		v, exists, err := st.HGet("h", "name")
		if err != nil || !exists || v != "Ada" {
			t.Fatalf("expected Ada, got %q exists=%v err=%v", v, exists, err)
		}
	})

	t.Run("HSetNXFieldExists", func(t *testing.T) {
		st := New()
		st.HSet("h", []HashPair{{Field: "name", Value: "Ada"}})
		ok, err := st.HSetNX("h", "name", "Grace")
		if err != nil || ok {
			t.Fatalf("expected false for existing field, ok=%v err=%v", ok, err)
		}
		v, exists, _ := st.HGet("h", "name")
		if v != "Ada" || !exists {
			t.Fatalf("expected Ada unchanged, got %q", v)
		}
	})

	t.Run("HLen", func(t *testing.T) {
		st := New()
		st.HSet("h", []HashPair{{Field: "a", Value: "1"}, {Field: "b", Value: "2"}, {Field: "c", Value: "3"}})
		n, err := st.HLen("h")
		if err != nil || n != 3 {
			t.Fatalf("expected 3, got %d err=%v", n, err)
		}
		n, err = st.HLen("missing")
		if err != nil || n != 0 {
			t.Fatalf("expected 0 for missing key, got %d err=%v", n, err)
		}
	})

	t.Run("HExists", func(t *testing.T) {
		st := New()
		st.HSet("h", []HashPair{{Field: "name", Value: "Ada"}})
		ok, err := st.HExists("h", "name")
		if err != nil || !ok {
			t.Fatalf("expected true for existing field, ok=%v err=%v", ok, err)
		}
		ok, err = st.HExists("h", "missing")
		if err != nil || ok {
			t.Fatalf("expected false for missing field, ok=%v err=%v", ok, err)
		}
		ok, err = st.HExists("nokey", "field")
		if err != nil || ok {
			t.Fatalf("expected false for missing key, ok=%v err=%v", ok, err)
		}
	})
}

func TestListExtended(t *testing.T) {
	t.Run("LLen", func(t *testing.T) {
		st := New()
		st.RPush("q", "a", "b", "c")
		n, err := st.LLen("q")
		if err != nil || n != 3 {
			t.Fatalf("expected 3, got %d err=%v", n, err)
		}
		n, err = st.LLen("missing")
		if err != nil || n != 0 {
			t.Fatalf("expected 0, got %d err=%v", n, err)
		}
	})

	t.Run("LLenWrongType", func(t *testing.T) {
		st := New()
		st.Set("str", "value", 0)
		_, err := st.LLen("str")
		if err == nil {
			t.Fatal("expected wrong type error")
		}
	})

	t.Run("LRangeVariousIndices", func(t *testing.T) {
		st := New()
		st.RPush("q", "a", "b", "c", "d", "e")
		// Full range
		vals, _ := st.LRange("q", 0, -1)
		if len(vals) != 5 {
			t.Fatalf("expected 5, got %d", len(vals))
		}
		// Middle
		vals, _ = st.LRange("q", 1, 3)
		if len(vals) != 3 || vals[0] != "b" || vals[1] != "c" || vals[2] != "d" {
			t.Fatalf("unexpected middle range: %v", vals)
		}
		// Negative indices
		vals, _ = st.LRange("q", -2, -1)
		if len(vals) != 2 || vals[0] != "d" || vals[1] != "e" {
			t.Fatalf("unexpected negative range: %v", vals)
		}
		// Out of bounds
		vals, _ = st.LRange("q", 10, 20)
		if len(vals) != 0 {
			t.Fatalf("expected empty for out-of-bounds, got %v", vals)
		}
	})
}

func TestSetOperations(t *testing.T) {
	t.Run("SInter", func(t *testing.T) {
		st := New()
		st.SAdd("s1", "a", "b", "c")
		st.SAdd("s2", "b", "c", "d")
		st.SAdd("s3", "c", "d", "e")
		result, err := st.SInter("s1", "s2", "s3")
		if err != nil {
			t.Fatalf("sinter failed: %v", err)
		}
		if len(result) != 1 || result[0] != "c" {
			t.Fatalf("expected [c], got %v", result)
		}
	})

	t.Run("SInterEmpty", func(t *testing.T) {
		st := New()
		st.SAdd("s1", "a", "b")
		result, err := st.SInter("s1", "missing")
		if err != nil {
			t.Fatalf("sinter failed: %v", err)
		}
		if len(result) != 0 {
			t.Fatalf("expected empty intersection with missing key, got %v", result)
		}
	})

	t.Run("SUnion", func(t *testing.T) {
		st := New()
		st.SAdd("s1", "a", "b")
		st.SAdd("s2", "b", "c")
		result, err := st.SUnion("s1", "s2")
		if err != nil {
			t.Fatalf("sunion failed: %v", err)
		}
		if len(result) != 3 {
			t.Fatalf("expected 3, got %v", result)
		}
		// Should be sorted
		if result[0] != "a" || result[1] != "b" || result[2] != "c" {
			t.Fatalf("unexpected union: %v", result)
		}
	})

	t.Run("SDiff", func(t *testing.T) {
		st := New()
		st.SAdd("s1", "a", "b", "c", "d")
		st.SAdd("s2", "b", "c")
		st.SAdd("s3", "c", "d")
		result, err := st.SDiff("s1", "s2", "s3")
		if err != nil {
			t.Fatalf("sdiff failed: %v", err)
		}
		if len(result) != 1 || result[0] != "a" {
			t.Fatalf("expected [a], got %v", result)
		}
	})

	t.Run("SDiffEmpty", func(t *testing.T) {
		st := New()
		result, err := st.SDiff("missing")
		if err != nil {
			t.Fatalf("sdiff failed: %v", err)
		}
		if len(result) != 0 {
			t.Fatalf("expected empty diff for missing key, got %v", result)
		}
	})

	t.Run("SInterWrongType", func(t *testing.T) {
		st := New()
		st.Set("str", "value", 0)
		_, err := st.SInter("str")
		if err == nil {
			t.Fatal("expected wrong type error")
		}
	})
}

func TestStringExtended(t *testing.T) {
	t.Run("Strlen", func(t *testing.T) {
		st := New()
		st.Set("key", "hello", 0)
		n, err := st.Strlen("key")
		if err != nil || n != 5 {
			t.Fatalf("expected 5, got %d err=%v", n, err)
		}
		n, err = st.Strlen("missing")
		if err != nil || n != 0 {
			t.Fatalf("expected 0 for missing key, got %d err=%v", n, err)
		}
	})

	t.Run("StrlenWrongType", func(t *testing.T) {
		st := New()
		st.RPush("q", "a")
		_, err := st.Strlen("q")
		if err == nil {
			t.Fatal("expected wrong type error")
		}
	})

	t.Run("AppendExisting", func(t *testing.T) {
		st := New()
		st.Set("key", "hello", 0)
		n, err := st.Append("key", " world")
		if err != nil || n != 11 {
			t.Fatalf("expected 11, got %d err=%v", n, err)
		}
		v, ok := st.Get("key")
		if !ok || v != "hello world" {
			t.Fatalf("expected 'hello world', got %q", v)
		}
	})

	t.Run("AppendNew", func(t *testing.T) {
		st := New()
		n, err := st.Append("newkey", "hello")
		if err != nil || n != 5 {
			t.Fatalf("expected 5, got %d err=%v", n, err)
		}
		v, ok := st.Get("newkey")
		if !ok || v != "hello" {
			t.Fatalf("expected 'hello', got %q ok=%v", v, ok)
		}
	})

	t.Run("GetDel", func(t *testing.T) {
		st := New()
		st.Set("key", "value", 0)
		v, ok := st.GetDel("key")
		if !ok || v != "value" {
			t.Fatalf("expected 'value', got %q ok=%v", v, ok)
		}
		_, ok = st.Get("key")
		if ok {
			t.Fatal("expected key to be deleted after GetDel")
		}
	})

	t.Run("GetDelMissing", func(t *testing.T) {
		st := New()
		_, ok := st.GetDel("missing")
		if ok {
			t.Fatal("expected false for missing key")
		}
	})
}

func TestCopyAndRename(t *testing.T) {
	t.Run("CopyString", func(t *testing.T) {
		st := New()
		st.Set("src", "hello", 0)
		ok, err := st.Copy("src", "dst", false)
		if err != nil || !ok {
			t.Fatalf("copy failed: ok=%v err=%v", ok, err)
		}
		v, exists := st.Get("dst")
		if !exists || v != "hello" {
			t.Fatalf("expected 'hello' at dst, got %q exists=%v", v, exists)
		}
		// Original still exists
		v, exists = st.Get("src")
		if !exists || v != "hello" {
			t.Fatalf("expected src to still exist, got %q exists=%v", v, exists)
		}
	})

	t.Run("CopyNoReplace", func(t *testing.T) {
		st := New()
		st.Set("src", "hello", 0)
		st.Set("dst", "existing", 0)
		ok, err := st.Copy("src", "dst", false)
		if err != nil || ok {
			t.Fatalf("expected copy without replace to return false, ok=%v err=%v", ok, err)
		}
		v, _ := st.Get("dst")
		if v != "existing" {
			t.Fatalf("expected dst unchanged, got %q", v)
		}
	})

	t.Run("CopyWithReplace", func(t *testing.T) {
		st := New()
		st.Set("src", "hello", 0)
		st.Set("dst", "existing", 0)
		ok, err := st.Copy("src", "dst", true)
		if err != nil || !ok {
			t.Fatalf("copy with replace failed: ok=%v err=%v", ok, err)
		}
		v, _ := st.Get("dst")
		if v != "hello" {
			t.Fatalf("expected dst to be overwritten, got %q", v)
		}
	})

	t.Run("CopyMissingSource", func(t *testing.T) {
		st := New()
		ok, err := st.Copy("missing", "dst", false)
		if err != nil || ok {
			t.Fatalf("expected false for missing source, ok=%v err=%v", ok, err)
		}
	})

	t.Run("Rename", func(t *testing.T) {
		st := New()
		st.Set("old", "value", 0)
		err := st.Rename("old", "new")
		if err != nil {
			t.Fatalf("rename failed: %v", err)
		}
		v, ok := st.Get("new")
		if !ok || v != "value" {
			t.Fatalf("expected 'value' at new, got %q ok=%v", v, ok)
		}
		_, ok = st.Get("old")
		if ok {
			t.Fatal("expected old key to be gone after rename")
		}
	})

	t.Run("RenameOverwrite", func(t *testing.T) {
		st := New()
		st.Set("a", "val_a", 0)
		st.Set("b", "val_b", 0)
		err := st.Rename("a", "b")
		if err != nil {
			t.Fatalf("rename failed: %v", err)
		}
		v, ok := st.Get("b")
		if !ok || v != "val_a" {
			t.Fatalf("expected val_a at b, got %q", v)
		}
	})

	t.Run("RenameMissing", func(t *testing.T) {
		st := New()
		err := st.Rename("missing", "dst")
		if err == nil {
			t.Fatal("expected error for renaming missing key")
		}
	})
}

func TestBlockingPop(t *testing.T) {
	t.Run("ImmediateReturn", func(t *testing.T) {
		st := New()
		st.RPush("q", "a", "b")
		key, val, ok := st.BPop([]string{"q"}, time.Second, true)
		if !ok || key != "q" || val != "a" {
			t.Fatalf("expected immediate pop of 'a' from 'q', got key=%q val=%q ok=%v", key, val, ok)
		}
	})

	t.Run("TimeoutExpires", func(t *testing.T) {
		st := New()
		start := time.Now()
		_, _, ok := st.BPop([]string{"empty"}, 50*time.Millisecond, true)
		elapsed := time.Since(start)
		if ok {
			t.Fatal("expected timeout, got value")
		}
		if elapsed < 40*time.Millisecond {
			t.Fatalf("returned too fast: %v", elapsed)
		}
	})

	t.Run("WokenByPush", func(t *testing.T) {
		st := New()
		done := make(chan struct{})
		var gotKey, gotVal string
		var gotOK bool
		go func() {
			gotKey, gotVal, gotOK = st.BPop([]string{"q"}, 2*time.Second, true)
			close(done)
		}()
		// Give the goroutine time to register the waiter
		time.Sleep(20 * time.Millisecond)
		st.RPush("q", "hello")
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("BPop did not wake up after push")
		}
		if !gotOK || gotKey != "q" || gotVal != "hello" {
			t.Fatalf("expected q/hello/true, got %q/%q/%v", gotKey, gotVal, gotOK)
		}
	})

	t.Run("BRPop", func(t *testing.T) {
		st := New()
		st.RPush("q", "a", "b")
		key, val, ok := st.BPop([]string{"q"}, time.Second, false)
		if !ok || key != "q" || val != "b" {
			t.Fatalf("expected brpop of 'b' from 'q', got key=%q val=%q ok=%v", key, val, ok)
		}
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		st := New()
		st.RPush("q2", "x")
		key, val, ok := st.BPop([]string{"q1", "q2"}, time.Second, true)
		if !ok || key != "q2" || val != "x" {
			t.Fatalf("expected q2/x, got %q/%q ok=%v", key, val, ok)
		}
	})
}

func TestKeyVersions(t *testing.T) {
	t.Run("VersionsIncrementOnMutation", func(t *testing.T) {
		st := New()
		v1 := st.KeyVersions([]string{"k1"})
		if v1["k1"] != 0 {
			t.Fatalf("expected version 0 for new key, got %d", v1["k1"])
		}
		st.Set("k1", "a", 0)
		v2 := st.KeyVersions([]string{"k1"})
		if v2["k1"] <= v1["k1"] {
			t.Fatalf("expected version to increase after Set, was %d now %d", v1["k1"], v2["k1"])
		}
		st.Set("k1", "b", 0)
		v3 := st.KeyVersions([]string{"k1"})
		if v3["k1"] <= v2["k1"] {
			t.Fatalf("expected version to increase again, was %d now %d", v2["k1"], v3["k1"])
		}
	})

	t.Run("KeysModifiedSince", func(t *testing.T) {
		st := New()
		st.Set("k1", "a", 0)
		st.Set("k2", "b", 0)
		snapshot := st.KeyVersions([]string{"k1", "k2"})
		if st.KeysModifiedSince(snapshot) {
			t.Fatal("expected no modifications right after snapshot")
		}
		st.Set("k1", "c", 0)
		if !st.KeysModifiedSince(snapshot) {
			t.Fatal("expected modifications detected after Set")
		}
	})

	t.Run("DeleteIncrementsVersion", func(t *testing.T) {
		st := New()
		st.Set("k1", "a", 0)
		before := st.KeyVersions([]string{"k1"})
		st.Del("k1")
		after := st.KeyVersions([]string{"k1"})
		if after["k1"] <= before["k1"] {
			t.Fatalf("expected version to increase after Del, was %d now %d", before["k1"], after["k1"])
		}
	})
}

func TestDBSizeAndFlushDB(t *testing.T) {
	st := New()
	st.Set("a", "1", 0)
	st.Set("b", "2", 0)
	st.RPush("list", "x")
	st.SAdd("set", "m")
	st.HSet("hash", []HashPair{{Field: "f", Value: "v"}})
	if got := st.DBSize(); got != 5 {
		t.Fatalf("expected dbsize 5, got %d", got)
	}
	st.FlushDB()
	if got := st.DBSize(); got != 0 {
		t.Fatalf("expected dbsize 0 after flush, got %d", got)
	}
}

func TestBitmapOperations(t *testing.T) {
	t.Run("SetBitAndGetBit", func(t *testing.T) {
		st := New()
		// SetBit at offset 7 (last bit of first byte)
		old, err := st.SetBit("bm", 7, 1)
		if err != nil || old != 0 {
			t.Fatalf("expected old=0, got %d err=%v", old, err)
		}
		got, err := st.GetBit("bm", 7)
		if err != nil || got != 1 {
			t.Fatalf("expected bit=1, got %d err=%v", got, err)
		}
		// SetBit at offset 0 (MSB of first byte)
		old, err = st.SetBit("bm", 0, 1)
		if err != nil || old != 0 {
			t.Fatalf("expected old=0 for offset 0, got %d err=%v", old, err)
		}
		got, err = st.GetBit("bm", 0)
		if err != nil || got != 1 {
			t.Fatalf("expected bit=1 at offset 0, got %d err=%v", got, err)
		}
		// Clear a bit
		old, err = st.SetBit("bm", 7, 0)
		if err != nil || old != 1 {
			t.Fatalf("expected old=1, got %d err=%v", old, err)
		}
		got, err = st.GetBit("bm", 7)
		if err != nil || got != 0 {
			t.Fatalf("expected bit=0 after clear, got %d err=%v", got, err)
		}
	})

	t.Run("BitCountKnownPattern", func(t *testing.T) {
		st := New()
		// Set "foobar" which has a known bit count
		st.Set("mykey", "foobar", 0)
		count, err := st.BitCount("mykey", 0, 0, false)
		if err != nil {
			t.Fatalf("bitcount failed: %v", err)
		}
		// "foobar" = 6 bytes, count all bits
		// f=01100110(4) o=01101111(6) o=01101111(6) b=01100010(3) a=01100001(3) r=01110010(4) = 26
		if count != 26 {
			t.Fatalf("expected bitcount 26 for 'foobar', got %d", count)
		}
		// Count just first byte 'f' = 4 set bits
		count, err = st.BitCount("mykey", 0, 0, true)
		if err != nil {
			t.Fatalf("bitcount range failed: %v", err)
		}
		if count != 4 {
			t.Fatalf("expected bitcount 4 for 'f', got %d", count)
		}
	})

	t.Run("SetBitAutoExtends", func(t *testing.T) {
		st := New()
		// Setting a bit at a large offset should auto-extend the string
		_, err := st.SetBit("ext", 100, 1)
		if err != nil {
			t.Fatalf("setbit auto-extend failed: %v", err)
		}
		got, err := st.GetBit("ext", 100)
		if err != nil || got != 1 {
			t.Fatalf("expected bit=1 at offset 100, got %d err=%v", got, err)
		}
	})

	t.Run("GetBitBeyondLength", func(t *testing.T) {
		st := New()
		st.Set("short", "a", 0) // 1 byte
		got, err := st.GetBit("short", 100)
		if err != nil || got != 0 {
			t.Fatalf("expected 0 for bit beyond length, got %d err=%v", got, err)
		}
	})

	t.Run("BitOpAND", func(t *testing.T) {
		st := New()
		st.Set("a", "\xff\x0f", 0)
		st.Set("b", "\x0f\xff", 0)
		n, err := st.BitOp("AND", "dest", []string{"a", "b"})
		if err != nil || n != 2 {
			t.Fatalf("BitOp AND failed: n=%d err=%v", n, err)
		}
		v, _ := st.Get("dest")
		if v != "\x0f\x0f" {
			t.Fatalf("expected \\x0f\\x0f, got %x", []byte(v))
		}
	})

	t.Run("BitOpOR", func(t *testing.T) {
		st := New()
		st.Set("a", "\xf0\x0f", 0)
		st.Set("b", "\x0f\xf0", 0)
		n, err := st.BitOp("OR", "dest", []string{"a", "b"})
		if err != nil || n != 2 {
			t.Fatalf("BitOp OR failed: n=%d err=%v", n, err)
		}
		v, _ := st.Get("dest")
		if v != "\xff\xff" {
			t.Fatalf("expected \\xff\\xff, got %x", []byte(v))
		}
	})

	t.Run("BitOpXOR", func(t *testing.T) {
		st := New()
		st.Set("a", "\xff\x00", 0)
		st.Set("b", "\xf0\xf0", 0)
		n, err := st.BitOp("XOR", "dest", []string{"a", "b"})
		if err != nil || n != 2 {
			t.Fatalf("BitOp XOR failed: n=%d err=%v", n, err)
		}
		v, _ := st.Get("dest")
		if v != "\x0f\xf0" {
			t.Fatalf("expected \\x0f\\xf0, got %x", []byte(v))
		}
	})

	t.Run("BitOpNOT", func(t *testing.T) {
		st := New()
		st.Set("a", "\xf0\x0f", 0)
		n, err := st.BitOp("NOT", "dest", []string{"a"})
		if err != nil || n != 2 {
			t.Fatalf("BitOp NOT failed: n=%d err=%v", n, err)
		}
		v, _ := st.Get("dest")
		if v != "\x0f\xf0" {
			t.Fatalf("expected \\x0f\\xf0, got %x", []byte(v))
		}
	})
}

func TestBitPos(t *testing.T) {
	st := New()
	// Create a key with byte 0x00 followed by 0xff: first 1 bit at offset 8
	st.Set("bp", "\x00\xff", 0)
	pos, err := st.BitPos("bp", 1, 0, -1, false)
	if err != nil {
		t.Fatalf("bitpos 1 failed: %v", err)
	}
	if pos != 8 {
		t.Fatalf("expected first 1 at position 8, got %d", pos)
	}

	// First 0 bit: byte 0x00 starts at bit 0
	pos, err = st.BitPos("bp", 0, 0, -1, false)
	if err != nil {
		t.Fatalf("bitpos 0 failed: %v", err)
	}
	if pos != 0 {
		t.Fatalf("expected first 0 at position 0, got %d", pos)
	}

	// With 0xff first byte, first 0 in second byte of \xff\x00
	st.Set("bp2", "\xff\x00", 0)
	pos, err = st.BitPos("bp2", 0, 0, -1, false)
	if err != nil {
		t.Fatalf("bitpos 0 for bp2 failed: %v", err)
	}
	if pos != 8 {
		t.Fatalf("expected first 0 at position 8 for bp2, got %d", pos)
	}
}

func TestHyperLogLog(t *testing.T) {
	t.Run("PFAddUnique", func(t *testing.T) {
		st := New()
		changed, err := st.PFAdd("hll", "a", "b", "c")
		if err != nil || !changed {
			t.Fatalf("expected changed=true for new elements, got %v err=%v", changed, err)
		}
		count, err := st.PFCount("hll")
		if err != nil {
			t.Fatalf("pfcount failed: %v", err)
		}
		if count != 3 {
			t.Fatalf("expected count ~3, got %d", count)
		}
	})

	t.Run("PFAddDuplicates", func(t *testing.T) {
		st := New()
		st.PFAdd("hll", "a", "b", "c")
		changed, err := st.PFAdd("hll", "a", "b", "c")
		if err != nil {
			t.Fatalf("pfadd duplicate failed: %v", err)
		}
		if changed {
			t.Fatal("expected changed=false for duplicate elements")
		}
	})

	t.Run("PFCountEmptyKey", func(t *testing.T) {
		st := New()
		count, err := st.PFCount("empty")
		if err != nil || count != 0 {
			t.Fatalf("expected 0 for empty key, got %d err=%v", count, err)
		}
	})

	t.Run("PFMerge", func(t *testing.T) {
		st := New()
		st.PFAdd("hll1", "a", "b", "c")
		st.PFAdd("hll2", "c", "d", "e")
		err := st.PFMerge("merged", "hll1", "hll2")
		if err != nil {
			t.Fatalf("pfmerge failed: %v", err)
		}
		count, err := st.PFCount("merged")
		if err != nil {
			t.Fatalf("pfcount merged failed: %v", err)
		}
		// Union of {a,b,c} and {c,d,e} = {a,b,c,d,e} = 5
		if count < 4 || count > 6 {
			t.Fatalf("expected merged count ~5, got %d", count)
		}
	})

	t.Run("PFCount1000Elements", func(t *testing.T) {
		st := New()
		for i := 0; i < 1000; i++ {
			st.PFAdd("hll", fmt.Sprintf("element:%d", i))
		}
		count, err := st.PFCount("hll")
		if err != nil {
			t.Fatalf("pfcount failed: %v", err)
		}
		// With precision 14 (16384 registers), standard error is ~0.81%
		// We allow 5% tolerance
		lower := int64(950)
		upper := int64(1050)
		if count < lower || count > upper {
			t.Fatalf("expected count within [%d, %d] for 1000 elements, got %d", lower, upper, count)
		}
	})
}

func TestGeoOperations(t *testing.T) {
	t.Run("GeoEncodeDecodeRoundtrip", func(t *testing.T) {
		// New York: lon=-74.006, lat=40.7128
		lon, lat := -74.006, 40.7128
		hash := GeoEncode(lon, lat)
		dlon, dlat := GeoDecode(hash)
		if math.Abs(dlon-lon) > 0.01 {
			t.Fatalf("longitude roundtrip failed: expected ~%f, got %f", lon, dlon)
		}
		if math.Abs(dlat-lat) > 0.01 {
			t.Fatalf("latitude roundtrip failed: expected ~%f, got %f", lat, dlat)
		}
	})

	t.Run("GeoAddAndGeoPos", func(t *testing.T) {
		st := New()
		added, err := st.GeoAdd("places", []GeoMember{
			{Longitude: -74.006, Latitude: 40.7128, Name: "nyc"},
			{Longitude: -118.2437, Latitude: 34.0522, Name: "la"},
		})
		if err != nil || added != 2 {
			t.Fatalf("geoadd failed: added=%d err=%v", added, err)
		}
		points, found := st.GeoPos("places", "nyc", "la", "missing")
		if !found[0] || !found[1] || found[2] {
			t.Fatalf("unexpected found: %v", found)
		}
		if math.Abs(points[0].Longitude-(-74.006)) > 0.01 {
			t.Fatalf("NYC longitude off: %f", points[0].Longitude)
		}
		if math.Abs(points[0].Latitude-40.7128) > 0.01 {
			t.Fatalf("NYC latitude off: %f", points[0].Latitude)
		}
		if math.Abs(points[1].Longitude-(-118.2437)) > 0.01 {
			t.Fatalf("LA longitude off: %f", points[1].Longitude)
		}
		if math.Abs(points[1].Latitude-34.0522) > 0.01 {
			t.Fatalf("LA latitude off: %f", points[1].Latitude)
		}
	})

	t.Run("GeoDistKnownCities", func(t *testing.T) {
		st := New()
		st.GeoAdd("cities", []GeoMember{
			{Longitude: -74.006, Latitude: 40.7128, Name: "nyc"},
			{Longitude: -118.2437, Latitude: 34.0522, Name: "la"},
		})
		dist, ok := st.GeoDist("cities", "nyc", "la")
		if !ok {
			t.Fatal("geodist failed")
		}
		// NYC to LA is approximately 3944 km = 3,944,000 m
		distKm := dist / 1000.0
		if math.Abs(distKm-3944) > 50 {
			t.Fatalf("expected ~3944 km, got %.1f km", distKm)
		}
	})

	t.Run("GeoSearchByRadius", func(t *testing.T) {
		st := New()
		st.GeoAdd("spots", []GeoMember{
			{Longitude: -73.935242, Latitude: 40.730610, Name: "manhattan"},   // NYC area
			{Longitude: -73.944158, Latitude: 40.678178, Name: "brooklyn"},    // NYC area
			{Longitude: -118.2437, Latitude: 34.0522, Name: "la"},            // Far away
			{Longitude: -0.1276, Latitude: 51.5074, Name: "london"},          // Very far
		})
		// Search within 20 km of central Manhattan
		results := st.GeoSearchByRadius("spots", -73.935242, 40.730610, 20000, 0, true)
		// Should find manhattan and brooklyn (close), but not LA or London
		found := make(map[string]bool)
		for _, r := range results {
			found[r.Member] = true
		}
		if !found["manhattan"] || !found["brooklyn"] {
			t.Fatalf("expected manhattan and brooklyn in results, got %v", results)
		}
		if found["la"] || found["london"] {
			t.Fatalf("expected la and london excluded, got %v", results)
		}
	})
}

func TestKeyEviction(t *testing.T) {
	t.Run("AllkeysRandom", func(t *testing.T) {
		st := New()
		st.SetMaxMemory(500) // very small limit
		st.SetEvictPolicy("allkeys-random")

		// Add many keys to exceed the memory limit
		for i := 0; i < 100; i++ {
			st.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("value-%d", i), 0)
		}

		before := st.DBSize()
		err := st.EvictIfNeeded()
		if err != nil {
			t.Fatalf("evict failed: %v", err)
		}
		after := st.DBSize()
		if after >= before {
			t.Fatalf("expected some keys evicted: before=%d after=%d", before, after)
		}
	})

	t.Run("NoevictionReturnsError", func(t *testing.T) {
		st := New()
		st.SetMaxMemory(500)
		st.SetEvictPolicy("noeviction")

		for i := 0; i < 100; i++ {
			st.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("value-%d", i), 0)
		}

		err := st.EvictIfNeeded()
		if err == nil {
			t.Fatal("expected error with noeviction policy")
		}
	})

	t.Run("NoEvictionNeeded", func(t *testing.T) {
		st := New()
		st.SetMaxMemory(0) // unlimited
		st.SetEvictPolicy("allkeys-random")
		st.Set("k", "v", 0)
		err := st.EvictIfNeeded()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if st.DBSize() != 1 {
			t.Fatalf("expected key to survive, dbsize=%d", st.DBSize())
		}
	})
}

func TestRateLimit(t *testing.T) {
	t.Run("SlidingWindow", func(t *testing.T) {
		st := New()
		// Allow 3 requests per 1-second window
		for i := 0; i < 3; i++ {
			allowed, remaining, _, _ := st.RateLimit("api:key", 3, time.Second)
			if !allowed {
				t.Fatalf("request %d should be allowed", i+1)
			}
			expectedRemaining := int64(2 - int64(i))
			if remaining != expectedRemaining {
				t.Fatalf("request %d: expected remaining %d, got %d", i+1, expectedRemaining, remaining)
			}
		}
		// 4th request should be rejected
		allowed, remaining, _, _ := st.RateLimit("api:key", 3, time.Second)
		if allowed {
			t.Fatal("4th request should be rejected")
		}
		if remaining != 0 {
			t.Fatalf("expected remaining 0, got %d", remaining)
		}
	})

	t.Run("WindowExpires", func(t *testing.T) {
		st := New()
		window := 50 * time.Millisecond
		for i := 0; i < 3; i++ {
			st.RateLimit("api:expire", 3, window)
		}
		// Should be rejected
		allowed, _, _, _ := st.RateLimit("api:expire", 3, window)
		if allowed {
			t.Fatal("should be rejected before window expires")
		}
		// Wait for window to pass
		time.Sleep(80 * time.Millisecond)
		allowed, _, _, _ = st.RateLimit("api:expire", 3, window)
		if !allowed {
			t.Fatal("should be allowed after window expires")
		}
	})

	t.Run("PeekDoesNotConsume", func(t *testing.T) {
		st := New()
		st.RateLimit("api:peek", 3, time.Second)
		remaining, _ := st.RateLimitPeek("api:peek")
		if remaining != 2 {
			t.Fatalf("expected remaining 2, got %d", remaining)
		}
		// Peek again — should not change
		remaining2, _ := st.RateLimitPeek("api:peek")
		if remaining2 != remaining {
			t.Fatalf("peek changed remaining: was %d, now %d", remaining, remaining2)
		}
	})

	t.Run("ResetClears", func(t *testing.T) {
		st := New()
		for i := 0; i < 3; i++ {
			st.RateLimit("api:reset", 3, time.Second)
		}
		allowed, _, _, _ := st.RateLimit("api:reset", 3, time.Second)
		if allowed {
			t.Fatal("should be rejected before reset")
		}
		if !st.RateLimitReset("api:reset") {
			t.Fatal("expected reset to return true")
		}
		allowed, _, _, _ = st.RateLimit("api:reset", 3, time.Second)
		if !allowed {
			t.Fatal("should be allowed after reset")
		}
	})
}

func TestDelayedKeys(t *testing.T) {
	t.Run("NotVisibleDuringDelay", func(t *testing.T) {
		st := New()
		st.SetDelayed("delayed:key", "secret", 100*time.Millisecond, 0)
		// Should not be visible immediately
		_, ok := st.Get("delayed:key")
		if ok {
			t.Fatal("expected delayed key to be invisible immediately")
		}
		if st.Exists("delayed:key") != 0 {
			t.Fatal("Exists should return 0 for delayed key before visible")
		}
	})

	t.Run("VisibleAfterDelay", func(t *testing.T) {
		st := New()
		st.SetDelayed("delayed:key2", "hello", 50*time.Millisecond, 0)
		time.Sleep(80 * time.Millisecond)
		v, ok := st.Get("delayed:key2")
		if !ok || v != "hello" {
			t.Fatalf("expected 'hello' after delay, got %q ok=%v", v, ok)
		}
	})

	t.Run("KeysNotShownDuringDelay", func(t *testing.T) {
		st := New()
		st.Set("visible", "yes", 0)
		st.SetDelayed("delayed:hidden", "no", 200*time.Millisecond, 0)
		keys := st.Keys("*")
		for _, k := range keys {
			if k == "delayed:hidden" {
				t.Fatal("delayed key should not appear in Keys before visible")
			}
		}
	})
}

func TestJSONDocument(t *testing.T) {
	st := New()

	// Set root document
	err := st.JSONSet("doc", "$", `{"name":"John","age":30,"address":{"city":"NYC"}}`)
	if err != nil {
		t.Fatalf("JSONSet root failed: %v", err)
	}

	t.Run("GetRoot", func(t *testing.T) {
		val, ok, err := st.JSONGet("doc", "$")
		if err != nil || !ok {
			t.Fatalf("JSONGet $ failed: ok=%v err=%v", ok, err)
		}
		if val == "" {
			t.Fatal("expected non-empty root document")
		}
	})

	t.Run("GetNestedPath", func(t *testing.T) {
		val, ok, err := st.JSONGet("doc", "$.name")
		if err != nil || !ok {
			t.Fatalf("JSONGet $.name failed: ok=%v err=%v", ok, err)
		}
		if val != `"John"` {
			t.Fatalf("expected \"John\", got %q", val)
		}
	})

	t.Run("GetDeepNested", func(t *testing.T) {
		val, ok, err := st.JSONGet("doc", "$.address.city")
		if err != nil || !ok {
			t.Fatalf("JSONGet $.address.city failed: ok=%v err=%v", ok, err)
		}
		if val != `"NYC"` {
			t.Fatalf("expected \"NYC\", got %q", val)
		}
	})

	t.Run("SetNestedPath", func(t *testing.T) {
		err := st.JSONSet("doc", "$.address.city", `"Boston"`)
		if err != nil {
			t.Fatalf("JSONSet nested failed: %v", err)
		}
		val, _, _ := st.JSONGet("doc", "$.address.city")
		if val != `"Boston"` {
			t.Fatalf("expected \"Boston\", got %q", val)
		}
	})

	t.Run("Del", func(t *testing.T) {
		deleted, err := st.JSONDel("doc", "$.age")
		if err != nil {
			t.Fatalf("JSONDel failed: %v", err)
		}
		if !deleted {
			t.Fatal("expected delete to return true")
		}
		_, ok, _ := st.JSONGet("doc", "$.age")
		if ok {
			// Field should not resolve to a value anymore
			val, _, _ := st.JSONGet("doc", "$.age")
			t.Fatalf("expected age to be deleted, but got %q", val)
		}
	})

	t.Run("Type", func(t *testing.T) {
		typ, err := st.JSONType("doc", "$")
		if err != nil {
			t.Fatalf("JSONType failed: %v", err)
		}
		if typ != "object" {
			t.Fatalf("expected 'object', got %q", typ)
		}
	})

	t.Run("NumIncrBy", func(t *testing.T) {
		// Add a numeric field back
		st.JSONSet("doc", "$.score", "10")
		result, err := st.JSONNumIncrBy("doc", "$.score", 5)
		if err != nil {
			t.Fatalf("JSONNumIncrBy failed: %v", err)
		}
		if result != 15 {
			t.Fatalf("expected 15, got %f", result)
		}
	})

	t.Run("ArrAppendAndLen", func(t *testing.T) {
		st.JSONSet("doc", "$.tags", `["go"]`)
		n, err := st.JSONArrAppend("doc", "$.tags", `"rust"`, `"python"`)
		if err != nil {
			t.Fatalf("JSONArrAppend failed: %v", err)
		}
		if n != 3 {
			t.Fatalf("expected array length 3, got %d", n)
		}
		length, err := st.JSONArrLen("doc", "$.tags")
		if err != nil {
			t.Fatalf("JSONArrLen failed: %v", err)
		}
		if length != 3 {
			t.Fatalf("expected 3, got %d", length)
		}
	})

	t.Run("Keys", func(t *testing.T) {
		keys, err := st.JSONKeys("doc", "$")
		if err != nil {
			t.Fatalf("JSONKeys failed: %v", err)
		}
		if len(keys) == 0 {
			t.Fatal("expected non-empty keys")
		}
		found := false
		for _, k := range keys {
			if k == "name" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected 'name' in keys, got %v", keys)
		}
	})
}

func TestReliableQueue(t *testing.T) {
	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		st := New()
		st.Enqueue("q", "msg1", 0)
		st.Enqueue("q", "msg2", 0)
		st.Enqueue("q", "msg3", 0)

		if n := st.QLen("q"); n != 3 {
			t.Fatalf("expected QLen 3, got %d", n)
		}

		msg1, ok := st.Dequeue("q", 0)
		if !ok || msg1.Body != "msg1" {
			t.Fatalf("expected msg1, got %v ok=%v", msg1, ok)
		}
		msg2, ok := st.Dequeue("q", 0)
		if !ok || msg2.Body != "msg2" {
			t.Fatalf("expected msg2, got %v ok=%v", msg2, ok)
		}
	})

	t.Run("QAckRemovesFromProcessing", func(t *testing.T) {
		st := New()
		st.Enqueue("q2", "hello", 0)
		msg, _ := st.Dequeue("q2", 0)
		_, processing, _ := st.QInfo("q2")
		if processing != 1 {
			t.Fatalf("expected 1 processing, got %d", processing)
		}
		if !st.QAck("q2", msg.ID) {
			t.Fatal("expected QAck to succeed")
		}
		_, processing, _ = st.QInfo("q2")
		if processing != 0 {
			t.Fatalf("expected 0 processing after ack, got %d", processing)
		}
	})

	t.Run("QNackReturnsToQueue", func(t *testing.T) {
		st := New()
		st.Enqueue("q3", "retry-me", 0)
		msg, _ := st.Dequeue("q3", 0)
		if !st.QNack("q3", msg.ID) {
			t.Fatal("expected QNack to succeed")
		}
		pending, processing, _ := st.QInfo("q3")
		if pending != 1 || processing != 0 {
			t.Fatalf("expected pending=1 processing=0, got pending=%d processing=%d", pending, processing)
		}
	})

	t.Run("DeadLetterAfterMaxRetries", func(t *testing.T) {
		st := New()
		st.Enqueue("q4", "doomed", 0)
		// Default max retries is 3. Dequeue and nack repeatedly.
		for i := 0; i < 3; i++ {
			msg, ok := st.Dequeue("q4", 0)
			if !ok {
				t.Fatalf("dequeue %d failed", i)
			}
			st.QNack("q4", msg.ID)
		}
		_, _, dead := st.QInfo("q4")
		if dead != 1 {
			t.Fatalf("expected 1 dead-letter, got %d", dead)
		}
		deadMsgs := st.QDead("q4", 10)
		if len(deadMsgs) != 1 || deadMsgs[0].Body != "doomed" {
			t.Fatalf("unexpected dead-letter messages: %v", deadMsgs)
		}
	})

	t.Run("QPeekDoesNotConsume", func(t *testing.T) {
		st := New()
		st.Enqueue("q5", "peek-me", 0)
		msgs := st.QPeek("q5", 1)
		if len(msgs) != 1 || msgs[0].Body != "peek-me" {
			t.Fatalf("unexpected peek result: %v", msgs)
		}
		// Still pending
		if n := st.QLen("q5"); n != 1 {
			t.Fatalf("expected QLen 1 after peek, got %d", n)
		}
	})
}

func TestKeyTagging(t *testing.T) {
	st := New()
	st.Set("server:1", "web", 0)

	// Tag the key
	err := st.TagSet("server:1", map[string]string{"env": "prod", "region": "us-east"})
	if err != nil {
		t.Fatalf("TagSet failed: %v", err)
	}

	t.Run("TagGet", func(t *testing.T) {
		tags := st.TagGet("server:1")
		if tags == nil {
			t.Fatal("expected non-nil tags")
		}
		if tags["env"] != "prod" || tags["region"] != "us-east" {
			t.Fatalf("unexpected tags: %v", tags)
		}
	})

	t.Run("TagQuery", func(t *testing.T) {
		results := st.TagQuery(map[string]string{"env": "prod"}, 100)
		found := false
		for _, r := range results {
			if r == "server:1" {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected server:1 in query results, got %v", results)
		}
	})

	t.Run("TagDel", func(t *testing.T) {
		n := st.TagDel("server:1", []string{"region"})
		if n != 1 {
			t.Fatalf("expected 1 deleted tag, got %d", n)
		}
		tags := st.TagGet("server:1")
		if _, ok := tags["region"]; ok {
			t.Fatal("expected region tag to be deleted")
		}
	})

	t.Run("TagQueryAfterRemoval", func(t *testing.T) {
		results := st.TagQuery(map[string]string{"region": "us-east"}, 100)
		for _, r := range results {
			if r == "server:1" {
				t.Fatal("server:1 should not match region=us-east after tag removal")
			}
		}
	})
}

func TestTimeSeries(t *testing.T) {
	st := New()

	// Add samples with known timestamps
	st.TSAdd("metrics", 1000, 10.0, nil)
	st.TSAdd("metrics", 2000, 20.0, nil)
	st.TSAdd("metrics", 3000, 30.0, nil)
	st.TSAdd("metrics", 4000, 40.0, nil)

	t.Run("TSRange", func(t *testing.T) {
		samples, err := st.TSRange("metrics", 1000, 3000, 0)
		if err != nil {
			t.Fatalf("TSRange failed: %v", err)
		}
		if len(samples) != 3 {
			t.Fatalf("expected 3 samples, got %d", len(samples))
		}
		if samples[0].Value != 10.0 || samples[1].Value != 20.0 || samples[2].Value != 30.0 {
			t.Fatalf("unexpected sample values: %v", samples)
		}
	})

	t.Run("TSGet", func(t *testing.T) {
		sample, ok, err := st.TSGet("metrics")
		if err != nil || !ok {
			t.Fatalf("TSGet failed: ok=%v err=%v", ok, err)
		}
		if sample.Timestamp != 4000 || sample.Value != 40.0 {
			t.Fatalf("expected last sample {4000, 40.0}, got {%d, %f}", sample.Timestamp, sample.Value)
		}
	})

	t.Run("TSInfo", func(t *testing.T) {
		count, _, firstTS, lastTS, err := st.TSInfo("metrics")
		if err != nil {
			t.Fatalf("TSInfo failed: %v", err)
		}
		if count != 4 {
			t.Fatalf("expected 4 samples, got %d", count)
		}
		if firstTS != 1000 || lastTS != 4000 {
			t.Fatalf("expected timestamps 1000-4000, got %d-%d", firstTS, lastTS)
		}
	})

	t.Run("TSDownsample", func(t *testing.T) {
		// Downsample with 2000ms buckets and avg aggregation
		n, err := st.TSDownsample("metrics", "metrics:avg", "avg", 1000, 5000, 2000)
		if err != nil {
			t.Fatalf("TSDownsample failed: %v", err)
		}
		if n != 2 {
			t.Fatalf("expected 2 downsampled buckets, got %d", n)
		}
		samples, _ := st.TSRange("metrics:avg", 0, 9999, 0)
		if len(samples) != 2 {
			t.Fatalf("expected 2 downsampled samples, got %d", len(samples))
		}
		// Bucket [1000, 3000): samples at 1000(10) and 2000(20), avg = 15
		if samples[0].Value != 15.0 {
			t.Fatalf("expected avg 15.0 for first bucket, got %f", samples[0].Value)
		}
		// Bucket [3000, 5000): samples at 3000(30) and 4000(40), avg = 35
		if samples[1].Value != 35.0 {
			t.Fatalf("expected avg 35.0 for second bucket, got %f", samples[1].Value)
		}
	})
}

func TestLRUEviction(t *testing.T) {
	st := New()
	st.SetMaxMemory(500) // small enough to force eviction of most keys but keep some
	st.SetEvictPolicy("allkeys-lru")

	// Add keys
	for i := 0; i < 20; i++ {
		st.Set(fmt.Sprintf("key:%d", i), fmt.Sprintf("value-%d", i), 0)
	}

	// Access some keys to update their LRU timestamp
	accessedKeys := map[string]bool{
		"key:0": true, "key:5": true, "key:10": true, "key:15": true, "key:19": true,
	}
	time.Sleep(time.Millisecond) // ensure time difference
	for k := range accessedKeys {
		st.Get(k)
	}

	// Evict - should prefer unaccessed keys
	err := st.EvictIfNeeded()
	if err != nil {
		t.Fatalf("evict failed: %v", err)
	}

	// Check that accessed keys survived better than unaccessed ones
	accessedSurvived := 0
	unaccessedSurvived := 0
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key:%d", i)
		if _, ok := st.Get(key); ok {
			if accessedKeys[key] {
				accessedSurvived++
			} else {
				unaccessedSurvived++
			}
		}
	}

	// With LRU, accessed keys should have a better survival rate
	// At minimum, we expect the recently accessed keys to survive more often
	if accessedSurvived == 0 {
		t.Fatalf("expected at least some accessed keys to survive, accessed=%d unaccessed=%d",
			accessedSurvived, unaccessedSurvived)
	}
}

func TestLRem(t *testing.T) {
	t.Run("CountPositive", func(t *testing.T) {
		st := New()
		st.RPush("q", "a", "b", "a", "c", "a")
		// Remove first 2 occurrences of "a"
		n, err := st.LRem("q", 2, "a")
		if err != nil {
			t.Fatalf("LRem failed: %v", err)
		}
		if n != 2 {
			t.Fatalf("expected 2 removed, got %d", n)
		}
		vals, _ := st.LRange("q", 0, -1)
		// Should be [b, c, a]
		if len(vals) != 3 || vals[0] != "b" || vals[1] != "c" || vals[2] != "a" {
			t.Fatalf("unexpected list after LRem count>0: %v", vals)
		}
	})

	t.Run("CountNegative", func(t *testing.T) {
		st := New()
		st.RPush("q", "a", "b", "a", "c", "a")
		// Remove last 2 occurrences of "a"
		n, err := st.LRem("q", -2, "a")
		if err != nil {
			t.Fatalf("LRem failed: %v", err)
		}
		if n != 2 {
			t.Fatalf("expected 2 removed, got %d", n)
		}
		vals, _ := st.LRange("q", 0, -1)
		// Should be [a, b, c]
		if len(vals) != 3 || vals[0] != "a" || vals[1] != "b" || vals[2] != "c" {
			t.Fatalf("unexpected list after LRem count<0: %v", vals)
		}
	})

	t.Run("CountZero", func(t *testing.T) {
		st := New()
		st.RPush("q", "a", "b", "a", "c", "a")
		// Remove all occurrences of "a"
		n, err := st.LRem("q", 0, "a")
		if err != nil {
			t.Fatalf("LRem failed: %v", err)
		}
		if n != 3 {
			t.Fatalf("expected 3 removed, got %d", n)
		}
		vals, _ := st.LRange("q", 0, -1)
		if len(vals) != 2 || vals[0] != "b" || vals[1] != "c" {
			t.Fatalf("unexpected list after LRem count=0: %v", vals)
		}
	})

	t.Run("MissingKey", func(t *testing.T) {
		st := New()
		n, err := st.LRem("missing", 0, "a")
		if err != nil {
			t.Fatalf("LRem missing key failed: %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 removed from missing key, got %d", n)
		}
	})

	t.Run("WrongType", func(t *testing.T) {
		st := New()
		st.Set("str", "value", 0)
		_, err := st.LRem("str", 0, "a")
		if err == nil {
			t.Fatal("expected wrong type error")
		}
	})
}

func TestSMove(t *testing.T) {
	t.Run("MoveMember", func(t *testing.T) {
		st := New()
		st.SAdd("s1", "a", "b", "c")
		st.SAdd("s2", "d")
		moved, err := st.SMove("s1", "s2", "b")
		if err != nil {
			t.Fatalf("SMove failed: %v", err)
		}
		if !moved {
			t.Fatal("expected move to succeed")
		}
		// s1 should no longer contain "b"
		ok, _ := st.SIsMember("s1", "b")
		if ok {
			t.Fatal("expected b removed from s1")
		}
		// s2 should contain "b"
		ok, _ = st.SIsMember("s2", "b")
		if !ok {
			t.Fatal("expected b added to s2")
		}
		n, _ := st.SCard("s1")
		if n != 2 {
			t.Fatalf("expected s1 cardinality 2, got %d", n)
		}
		n, _ = st.SCard("s2")
		if n != 2 {
			t.Fatalf("expected s2 cardinality 2, got %d", n)
		}
	})

	t.Run("MoveNonExistentMember", func(t *testing.T) {
		st := New()
		st.SAdd("s1", "a")
		st.SAdd("s2", "d")
		moved, err := st.SMove("s1", "s2", "missing")
		if err != nil {
			t.Fatalf("SMove failed: %v", err)
		}
		if moved {
			t.Fatal("expected move to fail for non-existent member")
		}
	})

	t.Run("MoveFromMissingSrc", func(t *testing.T) {
		st := New()
		st.SAdd("s2", "d")
		moved, err := st.SMove("missing", "s2", "a")
		if err != nil {
			t.Fatalf("SMove failed: %v", err)
		}
		if moved {
			t.Fatal("expected move to fail from missing src")
		}
	})

	t.Run("MoveCreatesDestination", func(t *testing.T) {
		st := New()
		st.SAdd("s1", "a", "b")
		moved, err := st.SMove("s1", "newset", "a")
		if err != nil {
			t.Fatalf("SMove failed: %v", err)
		}
		if !moved {
			t.Fatal("expected move to succeed")
		}
		ok, _ := st.SIsMember("newset", "a")
		if !ok {
			t.Fatal("expected a in newset")
		}
	})
}

func TestSPop(t *testing.T) {
	t.Run("PopOne", func(t *testing.T) {
		st := New()
		st.SAdd("s", "a", "b", "c")
		popped, err := st.SPop("s", 1)
		if err != nil {
			t.Fatalf("SPop failed: %v", err)
		}
		if len(popped) != 1 {
			t.Fatalf("expected 1 popped, got %d", len(popped))
		}
		n, _ := st.SCard("s")
		if n != 2 {
			t.Fatalf("expected cardinality 2 after pop, got %d", n)
		}
		// Popped member should no longer be in set
		ok, _ := st.SIsMember("s", popped[0])
		if ok {
			t.Fatal("popped member should not be in set")
		}
	})

	t.Run("PopAll", func(t *testing.T) {
		st := New()
		st.SAdd("s", "x", "y")
		popped, err := st.SPop("s", 5) // more than cardinality
		if err != nil {
			t.Fatalf("SPop failed: %v", err)
		}
		if len(popped) != 2 {
			t.Fatalf("expected 2 popped, got %d", len(popped))
		}
		n, _ := st.SCard("s")
		if n != 0 {
			t.Fatalf("expected cardinality 0 after pop all, got %d", n)
		}
	})

	t.Run("PopFromEmpty", func(t *testing.T) {
		st := New()
		popped, err := st.SPop("missing", 1)
		if err != nil {
			t.Fatalf("SPop failed: %v", err)
		}
		if len(popped) != 0 {
			t.Fatalf("expected 0 popped from empty, got %d", len(popped))
		}
	})
}

func TestZPopMinMax(t *testing.T) {
	t.Run("ZPopMin", func(t *testing.T) {
		st := New()
		st.ZAdd("zs", []ZMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})
		popped, err := st.ZPopMin("zs", 2)
		if err != nil {
			t.Fatalf("ZPopMin failed: %v", err)
		}
		if len(popped) != 2 {
			t.Fatalf("expected 2 popped, got %d", len(popped))
		}
		if popped[0].Member != "a" || popped[0].Score != 1 {
			t.Fatalf("expected first pop to be a/1, got %v", popped[0])
		}
		if popped[1].Member != "b" || popped[1].Score != 2 {
			t.Fatalf("expected second pop to be b/2, got %v", popped[1])
		}
		card, _ := st.ZCard("zs")
		if card != 1 {
			t.Fatalf("expected 1 remaining, got %d", card)
		}
	})

	t.Run("ZPopMax", func(t *testing.T) {
		st := New()
		st.ZAdd("zs", []ZMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}})
		popped, err := st.ZPopMax("zs", 2)
		if err != nil {
			t.Fatalf("ZPopMax failed: %v", err)
		}
		if len(popped) != 2 {
			t.Fatalf("expected 2 popped, got %d", len(popped))
		}
		if popped[0].Member != "c" || popped[0].Score != 3 {
			t.Fatalf("expected first pop to be c/3, got %v", popped[0])
		}
		if popped[1].Member != "b" || popped[1].Score != 2 {
			t.Fatalf("expected second pop to be b/2, got %v", popped[1])
		}
		card, _ := st.ZCard("zs")
		if card != 1 {
			t.Fatalf("expected 1 remaining, got %d", card)
		}
	})

	t.Run("PopFromEmpty", func(t *testing.T) {
		st := New()
		popped, err := st.ZPopMin("missing", 1)
		if err != nil {
			t.Fatalf("ZPopMin on missing failed: %v", err)
		}
		if len(popped) != 0 {
			t.Fatalf("expected 0 popped from empty, got %d", len(popped))
		}
	})
}

func TestZIncrBy(t *testing.T) {
	t.Run("IncrExisting", func(t *testing.T) {
		st := New()
		st.ZAdd("zs", []ZMember{{Member: "a", Score: 5}})
		newScore, err := st.ZIncrBy("zs", 3, "a")
		if err != nil {
			t.Fatalf("ZIncrBy failed: %v", err)
		}
		if newScore != 8 {
			t.Fatalf("expected 8, got %f", newScore)
		}
		score, ok, _ := st.ZScore("zs", "a")
		if !ok || score != 8 {
			t.Fatalf("expected stored score 8, got %f ok=%v", score, ok)
		}
	})

	t.Run("IncrNew", func(t *testing.T) {
		st := New()
		newScore, err := st.ZIncrBy("zs", 10, "newmember")
		if err != nil {
			t.Fatalf("ZIncrBy failed: %v", err)
		}
		if newScore != 10 {
			t.Fatalf("expected 10, got %f", newScore)
		}
		score, ok, _ := st.ZScore("zs", "newmember")
		if !ok || score != 10 {
			t.Fatalf("expected stored score 10, got %f ok=%v", score, ok)
		}
	})

	t.Run("IncrNegative", func(t *testing.T) {
		st := New()
		st.ZAdd("zs", []ZMember{{Member: "a", Score: 5}})
		newScore, err := st.ZIncrBy("zs", -2, "a")
		if err != nil {
			t.Fatalf("ZIncrBy failed: %v", err)
		}
		if newScore != 3 {
			t.Fatalf("expected 3, got %f", newScore)
		}
	})
}

func TestZRangeByLex(t *testing.T) {
	st := New()
	// All members with same score for lex ordering
	st.ZAdd("zs", []ZMember{
		{Member: "a", Score: 0},
		{Member: "b", Score: 0},
		{Member: "c", Score: 0},
		{Member: "d", Score: 0},
		{Member: "e", Score: 0},
	})

	t.Run("FullRange", func(t *testing.T) {
		items, err := st.ZRangeByLex("zs", "-", "+", 0, 0)
		if err != nil {
			t.Fatalf("ZRangeByLex failed: %v", err)
		}
		if len(items) != 5 {
			t.Fatalf("expected 5 items, got %d", len(items))
		}
	})

	t.Run("InclusiveRange", func(t *testing.T) {
		items, err := st.ZRangeByLex("zs", "[b", "[d", 0, 0)
		if err != nil {
			t.Fatalf("ZRangeByLex failed: %v", err)
		}
		if len(items) != 3 {
			t.Fatalf("expected 3 items [b,c,d], got %d", len(items))
		}
		if items[0].Member != "b" || items[1].Member != "c" || items[2].Member != "d" {
			t.Fatalf("unexpected members: %#v", items)
		}
	})

	t.Run("ExclusiveRange", func(t *testing.T) {
		items, err := st.ZRangeByLex("zs", "(a", "(d", 0, 0)
		if err != nil {
			t.Fatalf("ZRangeByLex failed: %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("expected 2 items (b,c), got %d", len(items))
		}
		if items[0].Member != "b" || items[1].Member != "c" {
			t.Fatalf("unexpected members: %#v", items)
		}
	})

	t.Run("WithLimit", func(t *testing.T) {
		items, err := st.ZRangeByLex("zs", "-", "+", 1, 2)
		if err != nil {
			t.Fatalf("ZRangeByLex failed: %v", err)
		}
		if len(items) != 2 || items[0].Member != "b" || items[1].Member != "c" {
			t.Fatalf("expected [b,c] with offset=1 count=2, got %#v", items)
		}
	})

	t.Run("EmptyKey", func(t *testing.T) {
		items, err := st.ZRangeByLex("missing", "-", "+", 0, 0)
		if err != nil {
			t.Fatalf("ZRangeByLex failed: %v", err)
		}
		if len(items) != 0 {
			t.Fatalf("expected empty result for missing key, got %d", len(items))
		}
	})
}

func TestHRandField(t *testing.T) {
	st := New()
	st.HSet("h", []HashPair{{Field: "a", Value: "1"}, {Field: "b", Value: "2"}, {Field: "c", Value: "3"}})

	t.Run("PositiveCount", func(t *testing.T) {
		fields, err := st.HRandField("h", 2)
		if err != nil {
			t.Fatalf("HRandField failed: %v", err)
		}
		if len(fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(fields))
		}
		// All fields should be from the hash
		valid := map[string]bool{"a": true, "b": true, "c": true}
		for _, f := range fields {
			if !valid[f] {
				t.Fatalf("unexpected field %q", f)
			}
		}
		// Fields should be distinct
		if fields[0] == fields[1] {
			t.Fatalf("expected distinct fields, got %v", fields)
		}
	})

	t.Run("PositiveCountExceedsSize", func(t *testing.T) {
		fields, err := st.HRandField("h", 10)
		if err != nil {
			t.Fatalf("HRandField failed: %v", err)
		}
		if len(fields) != 3 {
			t.Fatalf("expected all 3 fields, got %d", len(fields))
		}
	})

	t.Run("NegativeCount", func(t *testing.T) {
		fields, err := st.HRandField("h", -5)
		if err != nil {
			t.Fatalf("HRandField failed: %v", err)
		}
		if len(fields) != 5 {
			t.Fatalf("expected 5 fields (with duplicates), got %d", len(fields))
		}
		// All returned fields should be from the hash
		valid := map[string]bool{"a": true, "b": true, "c": true}
		for _, f := range fields {
			if !valid[f] {
				t.Fatalf("unexpected field %q", f)
			}
		}
	})

	t.Run("MissingKey", func(t *testing.T) {
		fields, err := st.HRandField("missing", 2)
		if err != nil {
			t.Fatalf("HRandField failed: %v", err)
		}
		if len(fields) != 0 {
			t.Fatalf("expected empty result for missing key, got %v", fields)
		}
	})

	t.Run("ZeroCount", func(t *testing.T) {
		fields, err := st.HRandField("h", 0)
		if err != nil {
			t.Fatalf("HRandField failed: %v", err)
		}
		if len(fields) != 0 {
			t.Fatalf("expected empty result for count=0, got %v", fields)
		}
	})
}

func TestXClaim(t *testing.T) {
	st := New()
	st.XAdd("stream", "1-1", []string{"k", "v1"})
	st.XAdd("stream", "1-2", []string{"k", "v2"})
	st.XGroupCreate("stream", "grp", "0-0", false)
	// Deliver both entries to consumer c1
	planned, _ := st.XReadGroupPlan("grp", "c1", []string{"stream"}, []string{">"}, 10)
	if len(planned["stream"]) != 2 {
		t.Fatalf("expected 2 planned entries, got %d", len(planned["stream"]))
	}
	st.XGroupDeliver("stream", "grp", "c1", []string{"1-1", "1-2"}, time.Now().Add(-time.Second))

	// Claim entry 1-1 to consumer c2 with minIdle=0 (always claim)
	claimed, err := st.XClaim("stream", "grp", "c2", 0, []string{"1-1"})
	if err != nil {
		t.Fatalf("XClaim failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "1-1" {
		t.Fatalf("expected claimed entry 1-1, got %#v", claimed)
	}

	// Verify c2 now owns the pending entry by checking pending for c2
	pending, _ := st.XReadGroupPlan("grp", "c2", []string{"stream"}, []string{"0-0"}, 10)
	found := false
	for _, e := range pending["stream"] {
		if e.ID == "1-1" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected 1-1 in c2's pending list after claim")
	}
}

func TestXTrim(t *testing.T) {
	st := New()
	st.XAdd("stream", "1-1", []string{"k", "v1"})
	st.XAdd("stream", "1-2", []string{"k", "v2"})
	st.XAdd("stream", "1-3", []string{"k", "v3"})
	st.XAdd("stream", "1-4", []string{"k", "v4"})
	st.XAdd("stream", "1-5", []string{"k", "v5"})

	trimmed, err := st.XTrim("stream", 3)
	if err != nil {
		t.Fatalf("XTrim failed: %v", err)
	}
	if trimmed != 2 {
		t.Fatalf("expected 2 trimmed, got %d", trimmed)
	}

	entries := st.XRange("stream", "-", "+", 0)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries after trim, got %d", len(entries))
	}
	if entries[0].ID != "1-3" {
		t.Fatalf("expected oldest remaining entry to be 1-3, got %s", entries[0].ID)
	}

	// Trim to same size should remove nothing
	trimmed, err = st.XTrim("stream", 3)
	if err != nil {
		t.Fatalf("XTrim failed: %v", err)
	}
	if trimmed != 0 {
		t.Fatalf("expected 0 trimmed when already at maxlen, got %d", trimmed)
	}
}

func TestXPendingSummary(t *testing.T) {
	st := New()
	st.XAdd("stream", "1-1", []string{"k", "v1"})
	st.XAdd("stream", "1-2", []string{"k", "v2"})
	st.XAdd("stream", "1-3", []string{"k", "v3"})
	st.XGroupCreate("stream", "grp", "0-0", false)

	// No pending entries initially
	total, minID, maxID, consumers := st.XPendingSummary("stream", "grp")
	if total != 0 || minID != "" || maxID != "" || consumers != nil {
		t.Fatalf("expected empty pending summary, got total=%d min=%s max=%s consumers=%v", total, minID, maxID, consumers)
	}

	// Deliver entries to two consumers
	st.XReadGroupPlan("grp", "c1", []string{"stream"}, []string{">"}, 2)
	st.XGroupDeliver("stream", "grp", "c1", []string{"1-1", "1-2"}, time.Now())
	st.XReadGroupPlan("grp", "c2", []string{"stream"}, []string{">"}, 1)
	st.XGroupDeliver("stream", "grp", "c2", []string{"1-3"}, time.Now())

	total, minID, maxID, consumers = st.XPendingSummary("stream", "grp")
	if total != 3 {
		t.Fatalf("expected 3 pending, got %d", total)
	}
	if minID != "1-1" {
		t.Fatalf("expected minID 1-1, got %s", minID)
	}
	if maxID != "1-3" {
		t.Fatalf("expected maxID 1-3, got %s", maxID)
	}
	if consumers["c1"] != 2 || consumers["c2"] != 1 {
		t.Fatalf("expected c1=2, c2=1, got %v", consumers)
	}

	// Ack one entry and verify summary updates
	st.XAck("stream", "grp", "1-1")
	total, _, _, _ = st.XPendingSummary("stream", "grp")
	if total != 2 {
		t.Fatalf("expected 2 pending after ack, got %d", total)
	}
}

func TestGetSetAndGetEx(t *testing.T) {
	t.Run("GetSet", func(t *testing.T) {
		st := New()
		// GetSet on non-existent key
		old, ok, err := st.GetSet("k", "first")
		if err != nil {
			t.Fatalf("GetSet failed: %v", err)
		}
		if ok {
			t.Fatal("expected no old value for new key")
		}
		// GetSet on existing key
		old, ok, err = st.GetSet("k", "second")
		if err != nil {
			t.Fatalf("GetSet failed: %v", err)
		}
		if !ok || old != "first" {
			t.Fatalf("expected old value 'first', got %q ok=%v", old, ok)
		}
		v, _ := st.Get("k")
		if v != "second" {
			t.Fatalf("expected current value 'second', got %q", v)
		}
	})

	t.Run("GetEx", func(t *testing.T) {
		st := New()
		st.Set("k", "value", 0)
		// GetEx with TTL
		v, ok := st.GetEx("k", 50*time.Millisecond, false)
		if !ok || v != "value" {
			t.Fatalf("GetEx expected 'value', got %q ok=%v", v, ok)
		}
		// Verify TTL was set
		ttl := st.PTTL("k")
		if ttl < 0 || ttl > 60 {
			t.Fatalf("expected PTTL around 50, got %d", ttl)
		}
		// Wait for expiry
		time.Sleep(60 * time.Millisecond)
		_, ok = st.Get("k")
		if ok {
			t.Fatal("expected key to expire after GetEx with TTL")
		}
	})

	t.Run("GetExPersist", func(t *testing.T) {
		st := New()
		st.Set("k", "value", time.Hour)
		// GetEx with persist should remove TTL
		v, ok := st.GetEx("k", 0, true)
		if !ok || v != "value" {
			t.Fatalf("GetEx expected 'value', got %q ok=%v", v, ok)
		}
		ttl := st.TTL("k")
		if ttl != -1 {
			t.Fatalf("expected TTL -1 after persist, got %d", ttl)
		}
	})

	t.Run("GetExMissing", func(t *testing.T) {
		st := New()
		_, ok := st.GetEx("missing", 0, false)
		if ok {
			t.Fatal("expected false for missing key")
		}
	})
}

func TestSetRangeGetRange(t *testing.T) {
	t.Run("SetRange", func(t *testing.T) {
		st := New()
		st.Set("k", "Hello World", 0)
		n, err := st.SetRange("k", 6, "Redis")
		if err != nil {
			t.Fatalf("SetRange failed: %v", err)
		}
		if n != 11 {
			t.Fatalf("expected 11, got %d", n)
		}
		v, _ := st.Get("k")
		if v != "Hello Redis" {
			t.Fatalf("expected 'Hello Redis', got %q", v)
		}
	})

	t.Run("SetRangeExtend", func(t *testing.T) {
		st := New()
		n, err := st.SetRange("k", 5, "abc")
		if err != nil {
			t.Fatalf("SetRange failed: %v", err)
		}
		if n != 8 {
			t.Fatalf("expected 8, got %d", n)
		}
		v, _ := st.Get("k")
		if len(v) != 8 {
			t.Fatalf("expected len 8, got %d", len(v))
		}
	})

	t.Run("GetRange", func(t *testing.T) {
		st := New()
		st.Set("k", "Hello World", 0)
		v, err := st.GetRange("k", 0, 4)
		if err != nil {
			t.Fatalf("GetRange failed: %v", err)
		}
		if v != "Hello" {
			t.Fatalf("expected 'Hello', got %q", v)
		}
	})

	t.Run("GetRangeNegative", func(t *testing.T) {
		st := New()
		st.Set("k", "Hello World", 0)
		v, err := st.GetRange("k", -5, -1)
		if err != nil {
			t.Fatalf("GetRange failed: %v", err)
		}
		if v != "World" {
			t.Fatalf("expected 'World', got %q", v)
		}
	})

	t.Run("GetRangeMissing", func(t *testing.T) {
		st := New()
		v, err := st.GetRange("missing", 0, 10)
		if err != nil {
			t.Fatalf("GetRange failed: %v", err)
		}
		if v != "" {
			t.Fatalf("expected empty string for missing key, got %q", v)
		}
	})
}

func TestMSetNX(t *testing.T) {
	t.Run("AllNew", func(t *testing.T) {
		st := New()
		ok := st.MSetNX([]string{"a", "b", "c"}, []string{"1", "2", "3"})
		if !ok {
			t.Fatal("expected MSetNX to succeed for all new keys")
		}
		v, exists := st.Get("a")
		if !exists || v != "1" {
			t.Fatalf("expected a=1, got %q exists=%v", v, exists)
		}
		v, exists = st.Get("b")
		if !exists || v != "2" {
			t.Fatalf("expected b=2, got %q exists=%v", v, exists)
		}
	})

	t.Run("SomeExist", func(t *testing.T) {
		st := New()
		st.Set("a", "existing", 0)
		ok := st.MSetNX([]string{"a", "b"}, []string{"new", "new"})
		if ok {
			t.Fatal("expected MSetNX to fail when some keys exist")
		}
		// None should be set
		_, exists := st.Get("b")
		if exists {
			t.Fatal("expected b not to be set when MSetNX fails")
		}
		v, _ := st.Get("a")
		if v != "existing" {
			t.Fatalf("expected a unchanged, got %q", v)
		}
	})
}

func TestCopyAllTypes(t *testing.T) {
	t.Run("CopyHash", func(t *testing.T) {
		st := New()
		st.HSet("src", []HashPair{{Field: "f1", Value: "v1"}, {Field: "f2", Value: "v2"}})
		ok, err := st.Copy("src", "dst", false)
		if err != nil || !ok {
			t.Fatalf("copy hash failed: ok=%v err=%v", ok, err)
		}
		v, exists, _ := st.HGet("dst", "f1")
		if !exists || v != "v1" {
			t.Fatalf("expected f1=v1, got %q exists=%v", v, exists)
		}
		// Verify independence
		st.HSet("src", []HashPair{{Field: "f1", Value: "changed"}})
		v, _, _ = st.HGet("dst", "f1")
		if v != "v1" {
			t.Fatalf("expected dst to be independent, got %q", v)
		}
	})

	t.Run("CopyList", func(t *testing.T) {
		st := New()
		st.RPush("src", "a", "b", "c")
		ok, err := st.Copy("src", "dst", false)
		if err != nil || !ok {
			t.Fatalf("copy list failed: ok=%v err=%v", ok, err)
		}
		vals, _ := st.LRange("dst", 0, -1)
		if len(vals) != 3 || vals[0] != "a" || vals[1] != "b" || vals[2] != "c" {
			t.Fatalf("expected [a,b,c], got %v", vals)
		}
	})

	t.Run("CopySet", func(t *testing.T) {
		st := New()
		st.SAdd("src", "x", "y", "z")
		ok, err := st.Copy("src", "dst", false)
		if err != nil || !ok {
			t.Fatalf("copy set failed: ok=%v err=%v", ok, err)
		}
		n, _ := st.SCard("dst")
		if n != 3 {
			t.Fatalf("expected cardinality 3, got %d", n)
		}
		member, _ := st.SIsMember("dst", "x")
		if !member {
			t.Fatal("expected x in copied set")
		}
	})

	t.Run("CopyZSet", func(t *testing.T) {
		st := New()
		st.ZAdd("src", []ZMember{{Member: "a", Score: 1}, {Member: "b", Score: 2}})
		ok, err := st.Copy("src", "dst", false)
		if err != nil || !ok {
			t.Fatalf("copy zset failed: ok=%v err=%v", ok, err)
		}
		items, _ := st.ZRange("dst", 0, -1)
		if len(items) != 2 || items[0].Member != "a" || items[1].Member != "b" {
			t.Fatalf("expected [a,b], got %#v", items)
		}
		score, ok2, _ := st.ZScore("dst", "a")
		if !ok2 || score != 1 {
			t.Fatalf("expected score 1, got %f ok=%v", score, ok2)
		}
	})
}

func TestSchemaValidation(t *testing.T) {
	// Test evalCondition directly for all comparison types
	t.Run("EqualityOps", func(t *testing.T) {
		if !evalCondition("hello", "=", "hello") {
			t.Fatal("expected = to match")
		}
		if !evalCondition("hello", "==", "hello") {
			t.Fatal("expected == to match")
		}
		if evalCondition("hello", "!=", "hello") {
			t.Fatal("expected != to not match for equal values")
		}
		if !evalCondition("hello", "!=", "world") {
			t.Fatal("expected != to match for different values")
		}
	})

	t.Run("StringOps", func(t *testing.T) {
		if !evalCondition("Hello World", "CONTAINS", "world") {
			t.Fatal("expected CONTAINS to be case-insensitive")
		}
		if !evalCondition("Hello World", "STARTSWITH", "hello") {
			t.Fatal("expected STARTSWITH to be case-insensitive")
		}
		if evalCondition("Hello World", "STARTSWITH", "world") {
			t.Fatal("expected STARTSWITH to fail for non-prefix")
		}
	})

	t.Run("NumericComparisons", func(t *testing.T) {
		if !evalCondition("10", ">", "5") {
			t.Fatal("expected 10 > 5")
		}
		if evalCondition("3", ">", "5") {
			t.Fatal("expected 3 > 5 to be false")
		}
		if !evalCondition("5", ">=", "5") {
			t.Fatal("expected 5 >= 5")
		}
		if !evalCondition("3", "<", "5") {
			t.Fatal("expected 3 < 5")
		}
		if !evalCondition("5", "<=", "5") {
			t.Fatal("expected 5 <= 5")
		}
	})

	t.Run("FloatComparisons", func(t *testing.T) {
		if !evalCondition("3.14", ">", "2.71") {
			t.Fatal("expected 3.14 > 2.71")
		}
		if !evalCondition("1.5", "<", "2.5") {
			t.Fatal("expected 1.5 < 2.5")
		}
	})

	t.Run("StringFallbackComparisons", func(t *testing.T) {
		if !evalCondition("b", ">", "a") {
			t.Fatal("expected 'b' > 'a' string comparison")
		}
		if !evalCondition("a", "<", "b") {
			t.Fatal("expected 'a' < 'b' string comparison")
		}
	})
}
