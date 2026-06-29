package store

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func BenchmarkSet(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("key" + strconv.Itoa(i%10000))
	}
}

func BenchmarkGetParallel(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get("key" + strconv.Itoa(i%10000))
			i++
		}
	})
}

func BenchmarkSetParallel(b *testing.B) {
	s := New()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set("key"+strconv.Itoa(i), "value", 0)
			i++
		}
	})
}

func BenchmarkIncr(b *testing.B) {
	s := New()
	s.Set("counter", "0", 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Incr("counter")
	}
}

func BenchmarkHSet(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.HSet("hash", []HashPair{{Field: "field" + strconv.Itoa(i), Value: "val"}})
	}
}

func BenchmarkHGet(b *testing.B) {
	s := New()
	for i := 0; i < 1000; i++ {
		s.HSet("hash", []HashPair{{Field: "field" + strconv.Itoa(i), Value: "val"}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.HGet("hash", "field"+strconv.Itoa(i%1000))
	}
}

func BenchmarkLPush(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.LPush("list", "value")
	}
}

func BenchmarkRPush(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.RPush("list", "value")
	}
}

func BenchmarkLPop(b *testing.B) {
	s := New()
	for i := 0; i < b.N+1000; i++ {
		s.LPush("list", "value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.LPop("list")
	}
}

func BenchmarkSAdd(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SAdd("set", "member"+strconv.Itoa(i))
	}
}

func BenchmarkSIsMember(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.SAdd("set", "member"+strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SIsMember("set", "member"+strconv.Itoa(i%10000))
	}
}

func BenchmarkZAdd(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ZAdd("zset", []ZMember{{Member: "m" + strconv.Itoa(i), Score: float64(i)}})
	}
}

func BenchmarkZRange(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.ZAdd("zset", []ZMember{{Member: "m" + strconv.Itoa(i), Score: float64(i)}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ZRange("zset", 0, 99) // Top 100
	}
}

func BenchmarkZRangeByScore(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.ZAdd("zset", []ZMember{{Member: "m" + strconv.Itoa(i), Score: float64(i)}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ZRangeByScore("zset", 100, 200, 0, 0)
	}
}

func BenchmarkXAdd(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.XAdd("stream", "*", []string{"key", "value"})
	}
}

func BenchmarkXRange(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.XAdd("stream", "*", []string{"key", "value"})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.XRange("stream", "-", "+", 100)
	}
}

func BenchmarkKeys(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Keys("key*")
	}
}

func BenchmarkExists(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Exists("key" + strconv.Itoa(i%10000))
	}
}

func BenchmarkExpireAndTTL(b *testing.B) {
	s := New()
	for i := 0; i < 1000; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i%1000)
		s.Expire(key, 60*time.Second)
		s.TTL(key)
	}
}

func BenchmarkScan(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Scan(0, 100, "key*")
	}
}

// Benchmark with mixed read/write parallel access to stress the RLock/Lock separation
func BenchmarkMixedParallel(b *testing.B) {
	s := New()
	for i := 0; i < 10000; i++ {
		s.Set("key"+strconv.Itoa(i), "value", 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				// 10% writes
				s.Set("key"+strconv.Itoa(i%10000), "newvalue", 0)
			} else {
				// 90% reads
				s.Get("key" + strconv.Itoa(i%10000))
			}
			i++
		}
	})
}

// Deque benchmark to show O(1) push improvement
func BenchmarkDequePushFront(b *testing.B) {
	d := &Deque{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.PushFront("value")
	}
}

func BenchmarkDequePopFront(b *testing.B) {
	d := &Deque{}
	for i := 0; i < b.N+1000; i++ {
		d.PushBack("value")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.PopFront()
	}
}

// SortedSet benchmark
func BenchmarkSortedSetAdd(b *testing.B) {
	z := NewSortedSet()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.Add("member"+strconv.Itoa(i), float64(i))
	}
}

func BenchmarkSortedSetRange(b *testing.B) {
	z := NewSortedSet()
	for i := 0; i < 10000; i++ {
		z.Add("member"+strconv.Itoa(i), float64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.Range(0, 99)
	}
}

// Suppress unused import warning
var _ = fmt.Sprintf
