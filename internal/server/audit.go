package server

import (
	"sync"
	"time"

	"vole/internal/store"
)

// AuditEntry represents a single recorded mutation event.
type AuditEntry struct {
	Timestamp time.Time
	Key       string
	Command   string
	Args      []string
	Client    string // client address
}

// AuditLog tracks mutations for queryable auditing.
type AuditLog struct {
	mu      sync.RWMutex
	entries []AuditEntry
	enabled bool
	maxSize int // max entries to keep
}

// NewAuditLog creates a new AuditLog with the given maximum entry count.
func NewAuditLog(maxSize int) *AuditLog {
	return &AuditLog{
		enabled: false,
		maxSize: maxSize,
	}
}

// Enabled returns whether audit logging is active.
func (al *AuditLog) Enabled() bool {
	al.mu.RLock()
	defer al.mu.RUnlock()
	return al.enabled
}

// Enable turns on audit logging.
func (al *AuditLog) Enable() {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.enabled = true
}

// Disable turns off audit logging.
func (al *AuditLog) Disable() {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.enabled = false
}

// Record adds an audit entry if logging is enabled.
func (al *AuditLog) Record(key, command, client string, args []string) {
	if !al.Enabled() {
		return
	}
	al.mu.Lock()
	defer al.mu.Unlock()
	entry := AuditEntry{
		Timestamp: time.Now(),
		Key:       key,
		Command:   command,
		Args:      append([]string(nil), args...),
		Client:    client,
	}
	al.entries = append(al.entries, entry)
	// Trim if over max size
	if al.maxSize > 0 && len(al.entries) > al.maxSize {
		al.entries = al.entries[len(al.entries)-al.maxSize:]
	}
}

// ForKey returns recent audit entries for a specific key.
func (al *AuditLog) ForKey(key string, count int) []AuditEntry {
	al.mu.RLock()
	defer al.mu.RUnlock()
	if count <= 0 {
		count = 10
	}
	var results []AuditEntry
	// Search backwards for most recent
	for i := len(al.entries) - 1; i >= 0 && len(results) < count; i-- {
		if al.entries[i].Key == key {
			results = append(results, al.entries[i])
		}
	}
	return results
}

// Search returns entries matching a key pattern (glob-style).
func (al *AuditLog) Search(pattern string, count int) []AuditEntry {
	al.mu.RLock()
	defer al.mu.RUnlock()
	if count <= 0 {
		count = 50
	}
	var results []AuditEntry
	for i := len(al.entries) - 1; i >= 0 && len(results) < count; i-- {
		if store.MatchGlob(pattern, al.entries[i].Key) {
			results = append(results, al.entries[i])
		}
	}
	return results
}

// Clear removes all audit entries.
func (al *AuditLog) Clear() {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.entries = nil
}

// Size returns the number of entries.
func (al *AuditLog) Size() int {
	al.mu.RLock()
	defer al.mu.RUnlock()
	return len(al.entries)
}
