package server

import (
	"sync"
	"time"
)

type SlowLogEntry struct {
	ID        int64
	Timestamp time.Time
	Duration  time.Duration
	Command   []string
	Client    string
}

type SlowLog struct {
	mu        sync.RWMutex
	entries   []SlowLogEntry
	threshold time.Duration
	maxLen    int
	nextID    int64
}

func NewSlowLog(threshold time.Duration, maxLen int) *SlowLog {
	return &SlowLog{
		threshold: threshold,
		maxLen:    maxLen,
	}
}

func (sl *SlowLog) Record(duration time.Duration, command []string, client string) {
	if duration < sl.threshold {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.nextID++
	entry := SlowLogEntry{
		ID:        sl.nextID,
		Timestamp: time.Now(),
		Duration:  duration,
		Command:   append([]string(nil), command...),
		Client:    client,
	}
	sl.entries = append(sl.entries, entry)
	if len(sl.entries) > sl.maxLen {
		sl.entries = sl.entries[len(sl.entries)-sl.maxLen:]
	}
}

func (sl *SlowLog) Get(count int) []SlowLogEntry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	if count <= 0 || count > len(sl.entries) {
		count = len(sl.entries)
	}
	// Return most recent first
	result := make([]SlowLogEntry, count)
	for i := 0; i < count; i++ {
		result[i] = sl.entries[len(sl.entries)-1-i]
	}
	return result
}

func (sl *SlowLog) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return len(sl.entries)
}

func (sl *SlowLog) Reset() {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.entries = nil
}
