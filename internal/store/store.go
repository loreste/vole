package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Value struct {
	Data string
}

type StreamEntry struct {
	ID     string
	Fields []string
}

type ConsumerGroup struct {
	LastID  string
	Pending map[string]PendingEntry
}

type PendingEntry struct {
	Consumer  string
	Delivered time.Time
}

type HashPair struct {
	Field string
	Value string
}

type ZMember struct {
	Member string
	Score  float64
}

// RateLimiter tracks request timestamps for sliding-window rate limiting.
type RateLimiter struct {
	Window   time.Duration
	Max      int64
	Requests []time.Time // timestamps of requests in current window
}

type Store struct {
	mu         sync.RWMutex
	kv         map[string]Value
	hashes     map[string]map[string]string
	lists      map[string]*Deque
	sets       map[string]map[string]struct{}
	zsets       map[string]*SortedSet
	streams    map[string][]StreamEntry
	hlls       map[string]*HyperLogLog
	jsons      map[string]*JSONDoc
	groups     map[string]map[string]*ConsumerGroup
	expires    map[string]time.Time
	lastSeq    map[string]int64
	waits      map[string][]*waiter
	listWaits  map[string][]*listWaiter
	stopExpiry chan struct{}
	keyVersion map[string]uint64
	version    uint64

	// Rate limiting (independent metadata, not a data type)
	rateLimiters map[string]*RateLimiter

	// Reliable queue support
	queues     map[string]*Queue
	queueWaits map[string][]*waiter

	// Key tagging / metadata
	tags map[string]map[string]string

	// Time-series data
	timeseries map[string]*TimeSeries

	// Delayed/scheduled key visibility
	notBefore map[string]time.Time

	// Key change notification callback
	onChange func(event, key, namespace string)

	// Eviction support
	maxMemory        int64  // max memory in bytes, 0 = unlimited
	evictPolicy      string // noeviction, allkeys-random, volatile-random, allkeys-lru
	accessMu         sync.Mutex
	accessTime       map[string]int64 // unix nano timestamp of last access (for LRU)
	memEstimate      int64
	opsSinceEstimate int64
}

type waiter struct {
	ch   chan struct{}
	once sync.Once
}

type listWaiter struct {
	ch   chan listWaitResult
	once sync.Once
	left bool
}

type listWaitResult struct {
	key   string
	value string
}

func New() *Store {
	return &Store{
		kv:           make(map[string]Value),
		hashes:       make(map[string]map[string]string),
		lists:        make(map[string]*Deque),
		sets:         make(map[string]map[string]struct{}),
		zsets:        make(map[string]*SortedSet),
		streams:      make(map[string][]StreamEntry),
		hlls:         make(map[string]*HyperLogLog),
		jsons:        make(map[string]*JSONDoc),
		groups:       make(map[string]map[string]*ConsumerGroup),
		expires:      make(map[string]time.Time),
		lastSeq:      make(map[string]int64),
		waits:        make(map[string][]*waiter),
		listWaits:    make(map[string][]*listWaiter),
		stopExpiry:   make(chan struct{}),
		keyVersion:   make(map[string]uint64),
		accessTime:   make(map[string]int64),
		rateLimiters: make(map[string]*RateLimiter),
		notBefore:    make(map[string]time.Time),
		tags:         make(map[string]map[string]string),
		timeseries:   make(map[string]*TimeSeries),
		queues:       make(map[string]*Queue),
		queueWaits:   make(map[string][]*waiter),
	}
}

// OnChange registers a callback that fires whenever a key is modified or
// deleted. The callback receives an event name ("set", "del", "expired"),
// the key, and the namespace (currently always ""). The callback should be
// non-blocking; the store calls it while holding its write lock, so heavy
// work should be dispatched asynchronously by the caller.
func (s *Store) OnChange(fn func(event, key, namespace string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onChange = fn
}

// touchKeyLocked increments the global version counter and records the new
// version for each given key. Also updates access time for LRU eviction.
// Must be called under write Lock.
func (s *Store) touchKeyLocked(keys ...string) {
	s.version++
	now := time.Now().UnixNano()
	for _, key := range keys {
		s.keyVersion[key] = s.version
	}
	if s.onChange != nil {
		for _, key := range keys {
			s.onChange("set", key, "")
		}
	}
	if s.maxMemory > 0 {
		s.accessMu.Lock()
		for _, key := range keys {
			s.accessTime[key] = now
		}
		s.accessMu.Unlock()
	}
}

// KeyVersions returns the current version of each key. Safe for concurrent use.
func (s *Store) KeyVersions(keys []string) map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]uint64, len(keys))
	for _, key := range keys {
		out[key] = s.keyVersion[key]
	}
	return out
}

// KeysModifiedSince returns true if any key's version differs from the given map.
func (s *Store) KeysModifiedSince(versions map[string]uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for key, ver := range versions {
		if s.keyVersion[key] != ver {
			return true
		}
	}
	return false
}

// StartExpiry spawns a background goroutine that periodically samples expired
// keys and deletes them. At most 100 keys are sampled per cycle.
func (s *Store) StartExpiry(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopExpiry:
				return
			case <-ticker.C:
				s.sampleExpired()
			}
		}
	}()
}

func (s *Store) sampleExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	n := 0
	for key, expireAt := range s.expires {
		if n >= 100 {
			break
		}
		n++
		if !expireAt.IsZero() && now.After(expireAt) {
			// Fire "expired" before "del" so subscribers can distinguish
			// expiration from explicit deletion.
			if s.onChange != nil {
				s.onChange("expired", key, "")
			}
			s.deleteKeyLocked(key)
		}
	}
}

// StopExpiry signals the background expiry goroutine to stop.
func (s *Store) StopExpiry() {
	close(s.stopExpiry)
}

// isExpiredRLocked checks if a key is expired WITHOUT deleting it.
// Safe to call under RLock.
func (s *Store) isExpiredRLocked(key string, now time.Time) bool {
	expireAt, ok := s.expires[key]
	if !ok {
		return false
	}
	if expireAt.IsZero() || now.Before(expireAt) {
		return false
	}
	return true
}

// existsStringRLocked checks if a key exists as a string value, read-safe.
func (s *Store) existsStringRLocked(key string, now time.Time) bool {
	if s.isExpiredRLocked(key, now) {
		return false
	}
	if nb, ok := s.notBefore[key]; ok && now.Before(nb) {
		return false
	}
	_, ok := s.kv[key]
	return ok
}

// existsRLocked checks if a key exists in any data type, read-safe.
func (s *Store) existsRLocked(key string, now time.Time) bool {
	if s.isExpiredRLocked(key, now) {
		return false
	}
	if nb, ok := s.notBefore[key]; ok && now.Before(nb) {
		return false
	}
	_, inKV := s.kv[key]
	return inKV || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil
}

func (s *Store) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := Value{Data: value}
	if ttl > 0 {
		s.expires[key] = time.Now().Add(ttl)
	} else {
		delete(s.expires, key)
	}
	s.kv[key] = v
	delete(s.hashes, key)
	delete(s.lists, key)
	delete(s.sets, key)
	delete(s.zsets, key)
	delete(s.streams, key)
	delete(s.hlls, key)
	delete(s.lastSeq, key)
	s.touchKeyLocked(key)
	if s.maxMemory > 0 {
		s.accessMu.Lock()
		s.accessTime[key] = time.Now().UnixNano()
		s.accessMu.Unlock()
	}
}

func (s *Store) SetAbsolute(key, value string, expireAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[key] = Value{Data: value}
	if expireAt.IsZero() {
		delete(s.expires, key)
	} else {
		s.expires[key] = expireAt
	}
	delete(s.hashes, key)
	delete(s.lists, key)
	delete(s.sets, key)
	delete(s.zsets, key)
	delete(s.streams, key)
	delete(s.hlls, key)
	delete(s.lastSeq, key)
	s.touchKeyLocked(key)
}

// SetNX sets the key only if it does NOT already exist. Returns true if the key was set.
func (s *Store) SetNX(key, value string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		// key was expired and deleted, treat as non-existent
	}
	if s.existsLocked(key) {
		return false
	}
	v := Value{Data: value}
	if ttl > 0 {
		s.expires[key] = now.Add(ttl)
	}
	s.kv[key] = v
	s.touchKeyLocked(key)
	return true
}

// SetXX sets the key only if it DOES already exist. Returns true if the key was set.
func (s *Store) SetXX(key, value string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return false
	}
	if !s.existsLocked(key) {
		return false
	}
	v := Value{Data: value}
	if ttl > 0 {
		s.expires[key] = now.Add(ttl)
	} else {
		delete(s.expires, key)
	}
	s.kv[key] = v
	delete(s.hashes, key)
	delete(s.lists, key)
	delete(s.sets, key)
	delete(s.zsets, key)
	delete(s.streams, key)
	delete(s.hlls, key)
	delete(s.lastSeq, key)
	s.touchKeyLocked(key)
	return true
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		s.mu.RUnlock()
		return "", false
	}
	// Check if key is not yet visible (delayed/scheduled)
	if nb, ok := s.notBefore[key]; ok && now.Before(nb) {
		s.mu.RUnlock()
		return "", false
	}
	v, ok := s.kv[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	if s.maxMemory > 0 {
		s.accessMu.Lock()
		s.accessTime[key] = now.UnixNano()
		s.accessMu.Unlock()
	}
	return v.Data, true
}

func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	n := 0
	for _, key := range keys {
		if s.existsRLocked(key, now) {
			n++
		}
	}
	return n
}

func (s *Store) MGet(keys ...string) []ValueResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	out := make([]ValueResult, len(keys))
	for i, key := range keys {
		if s.isExpiredRLocked(key, now) {
			continue
		}
		if nb, ok := s.notBefore[key]; ok && now.Before(nb) {
			continue
		}
		v, ok := s.kv[key]
		if ok {
			out[i] = ValueResult{Value: v.Data, OK: true}
		}
	}
	return out
}

func (s *Store) Type(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return "none"
	}
	if nb, ok := s.notBefore[key]; ok && now.Before(nb) {
		return "none"
	}
	if s.existsStringRLocked(key, now) {
		return "string"
	}
	if _, ok := s.hashes[key]; ok {
		return "hash"
	}
	if _, ok := s.lists[key]; ok {
		return "list"
	}
	if _, ok := s.sets[key]; ok {
		return "set"
	}
	if _, ok := s.zsets[key]; ok {
		return "zset"
	}
	if _, ok := s.streams[key]; ok {
		return "stream"
	}
	if _, ok := s.hlls[key]; ok {
		return "string" // Redis reports HLL as string type
	}
	if _, ok := s.jsons[key]; ok {
		return "json"
	}
	if _, ok := s.timeseries[key]; ok {
		return "timeseries"
	}
	return "none"
}

func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := s.keysLocked(pattern)
	sort.Strings(keys)
	return keys
}

func (s *Store) Scan(cursor, count int, pattern string) (int, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	all := s.allKeysRLocked(now)
	sort.Strings(all)
	if count <= 0 {
		count = 10
	}
	// Filter by pattern
	var keys []string
	if pattern == "" || pattern == "*" {
		keys = all
	} else {
		keys = make([]string, 0)
		for _, key := range all {
			if matchPattern(pattern, key) {
				keys = append(keys, key)
			}
		}
	}
	if cursor < 0 || cursor >= len(keys) {
		return 0, nil
	}
	end := cursor + count
	if end >= len(keys) {
		return 0, keys[cursor:]
	}
	return end, keys[cursor:end]
}

func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	n := 0
	for _, key := range keys {
		if s.expiredKeyLocked(key, now) {
			continue
		}
		if s.existsLocked(key) {
			s.deleteKeyLocked(key)
			n++
		}
	}
	return n
}

type ValueResult struct {
	Value string
	OK    bool
}

func (s *Store) Incr(key string) (int64, error) {
	return s.IncrBy(key, 1)
}

func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		delete(s.kv, key)
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	v, ok := s.kv[key]
	var n int64
	if ok {
		parsed, err := strconv.ParseInt(v.Data, 10, 64)
		if err != nil {
			return 0, errors.New("value is not an integer")
		}
		n = parsed
	}
	n += delta
	s.kv[key] = Value{Data: strconv.FormatInt(n, 10)}
	s.touchKeyLocked(key)
	return n, nil
}

func (s *Store) DecrBy(key string, delta int64) (int64, error) {
	return s.IncrBy(key, -delta)
}

func (s *Store) IncrByFloat(key string, delta float64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		delete(s.kv, key)
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	v, ok := s.kv[key]
	var n float64
	if ok {
		parsed, err := strconv.ParseFloat(v.Data, 64)
		if err != nil {
			return 0, errors.New("value is not a valid float")
		}
		n = parsed
	}
	n += delta
	s.kv[key] = Value{Data: strconv.FormatFloat(n, 'f', -1, 64)}
	s.touchKeyLocked(key)
	return n, nil
}

func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) || !s.existsLocked(key) {
		return false
	}
	s.expires[key] = time.Now().Add(ttl)
	s.touchKeyLocked(key)
	return true
}

func (s *Store) ExpireAt(key string, expireAt time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) || !s.existsLocked(key) {
		return false
	}
	if expireAt.IsZero() {
		delete(s.expires, key)
	} else {
		s.expires[key] = expireAt
	}
	s.touchKeyLocked(key)
	return true
}

func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) || !s.existsRLocked(key, now) {
		return -2
	}
	expireAt := s.expires[key]
	if expireAt.IsZero() {
		return -1
	}
	return int64(time.Until(expireAt).Seconds())
}

func (s *Store) PTTL(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) || !s.existsRLocked(key, now) {
		return -2
	}
	expireAt := s.expires[key]
	if expireAt.IsZero() {
		return -1
	}
	return time.Until(expireAt).Milliseconds()
}

func (s *Store) ExpireTime(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) || !s.existsRLocked(key, now) {
		return -2
	}
	exp := s.expires[key]
	if exp.IsZero() {
		return -1
	}
	return exp.Unix()
}

func (s *Store) PExpireTime(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) || !s.existsRLocked(key, now) {
		return -2
	}
	exp := s.expires[key]
	if exp.IsZero() {
		return -1
	}
	return exp.UnixMilli()
}

func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) || !s.existsLocked(key) {
		return false
	}
	_, had := s.expires[key]
	delete(s.expires, key)
	if had {
		s.touchKeyLocked(key)
	}
	return had
}

func (s *Store) DBSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.allKeysRLocked(time.Now()))
}

func (s *Store) Rename(key, newkey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return errors.New("no such key")
	}
	if !s.existsLocked(key) {
		return errors.New("no such key")
	}
	// Delete newkey if it exists
	s.deleteKeyLocked(newkey)
	// Move data from key to newkey across all types
	if v, ok := s.kv[key]; ok {
		s.kv[newkey] = v
		delete(s.kv, key)
	}
	if v, ok := s.hashes[key]; ok {
		s.hashes[newkey] = v
		delete(s.hashes, key)
	}
	if v, ok := s.lists[key]; ok {
		s.lists[newkey] = v
		delete(s.lists, key)
	}
	if v, ok := s.sets[key]; ok {
		s.sets[newkey] = v
		delete(s.sets, key)
	}
	if v, ok := s.zsets[key]; ok {
		s.zsets[newkey] = v
		delete(s.zsets, key)
	}
	if v, ok := s.streams[key]; ok {
		s.streams[newkey] = v
		delete(s.streams, key)
	}
	if v, ok := s.hlls[key]; ok {
		s.hlls[newkey] = v
		delete(s.hlls, key)
	}
	if v, ok := s.groups[key]; ok {
		s.groups[newkey] = v
		delete(s.groups, key)
	}
	if v, ok := s.lastSeq[key]; ok {
		s.lastSeq[newkey] = v
		delete(s.lastSeq, key)
	}
	if v, ok := s.expires[key]; ok {
		s.expires[newkey] = v
		delete(s.expires, key)
	}
	s.touchKeyLocked(key, newkey)
	return nil
}

func (s *Store) HSet(key string, pairs []HashPair) (int, error) {
	if len(pairs) == 0 {
		return 0, errors.New("wrong number of hash field arguments")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		// expired keys can be reused with a new type
	}
	if s.existsStringLocked(key) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	if s.hashes[key] == nil {
		s.hashes[key] = make(map[string]string)
	}
	added := 0
	for _, pair := range pairs {
		if _, ok := s.hashes[key][pair.Field]; !ok {
			added++
		}
		s.hashes[key][pair.Field] = pair.Value
	}
	s.touchKeyLocked(key)
	return added, nil
}

func (s *Store) HGet(key, field string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return "", false, nil
	}
	if s.existsStringRLocked(key, now) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return "", false, errors.New("wrong type")
	}
	hash, ok := s.hashes[key]
	if !ok {
		return "", false, nil
	}
	value, ok := hash[field]
	return value, ok, nil
}

func (s *Store) HGetAll(key string) ([]HashPair, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	hash := s.hashes[key]
	pairs := make([]HashPair, 0, len(hash))
	for field, value := range hash {
		pairs = append(pairs, HashPair{Field: field, Value: value})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Field < pairs[j].Field })
	return pairs, nil
}

func (s *Store) HVals(key string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	h := s.hashes[key]
	vals := make([]string, 0, len(h))
	for _, v := range h {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	return vals, nil
}

func (s *Store) HKeys(key string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	h := s.hashes[key]
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *Store) HDel(key string, fields ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return 0, nil
	}
	if s.existsStringLocked(key) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	hash, ok := s.hashes[key]
	if !ok {
		return 0, nil
	}
	deleted := 0
	for _, field := range fields {
		if _, ok := hash[field]; ok {
			delete(hash, field)
			deleted++
		}
	}
	if len(hash) == 0 {
		delete(s.hashes, key)
		delete(s.expires, key)
	}
	if deleted > 0 {
		s.touchKeyLocked(key)
	}
	return deleted, nil
}

func (s *Store) HIncrBy(key, field string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		// expired, treat as new
	}
	if s.existsStringLocked(key) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	if s.hashes[key] == nil {
		s.hashes[key] = make(map[string]string)
	}
	var n int64
	if v, ok := s.hashes[key][field]; ok {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, errors.New("hash value is not an integer")
		}
		n = parsed
	}
	n += delta
	s.hashes[key][field] = strconv.FormatInt(n, 10)
	s.touchKeyLocked(key)
	return n, nil
}

func (s *Store) HSetNX(key, field, value string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		// expired, treat as new
	}
	if s.existsStringLocked(key) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return false, errors.New("wrong type")
	}
	if s.hashes[key] == nil {
		s.hashes[key] = make(map[string]string)
	}
	if _, ok := s.hashes[key][field]; ok {
		return false, nil
	}
	s.hashes[key][field] = value
	s.touchKeyLocked(key)
	return true, nil
}

func (s *Store) LPush(key string, values ...string) (int, error) {
	return s.pushList(key, true, values...)
}

func (s *Store) RPush(key string, values ...string) (int, error) {
	return s.pushList(key, false, values...)
}

func (s *Store) LPop(key string) (string, bool, error) {
	return s.popList(key, true)
}

func (s *Store) RPop(key string) (string, bool, error) {
	return s.popList(key, false)
}

// LMove pops an element from one list and pushes it to another.
// srcLeft/dstLeft control which end to pop from / push to.
func (s *Store) LMove(src, dst string, srcLeft, dstLeft bool) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(src, now)

	// type check src
	if s.existsStringLocked(src) || s.hashes[src] != nil || s.sets[src] != nil || s.zsets[src] != nil || s.streams[src] != nil || s.hlls[src] != nil {
		return "", false, errors.New("wrong type")
	}
	srcD := s.lists[src]
	if srcD == nil || srcD.Len() == 0 {
		return "", false, nil
	}

	// Check dst type if it exists and is not src
	if src != dst {
		s.expiredKeyLocked(dst, now)
		if s.existsStringLocked(dst) || s.hashes[dst] != nil || s.sets[dst] != nil || s.zsets[dst] != nil || s.streams[dst] != nil || s.hlls[dst] != nil {
			return "", false, errors.New("wrong type")
		}
	}

	// Pop from source
	var val string
	if srcLeft {
		val, _ = srcD.PopFront()
	} else {
		val, _ = srcD.PopBack()
	}
	if srcD.Len() == 0 {
		delete(s.lists, src)
		delete(s.expires, src)
	}

	// Push to destination
	dstD := s.lists[dst]
	if dstD == nil {
		dstD = &Deque{}
		s.lists[dst] = dstD
	}
	if dstLeft {
		dstD.PushFront(val)
	} else {
		dstD.PushBack(val)
	}

	s.touchKeyLocked(src, dst)
	s.wakeListWaitersLocked(dst)
	return val, true, nil
}

func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil || d.Len() == 0 {
		return nil, nil
	}
	return d.Range(start, stop), nil
}

func (s *Store) pushList(key string, left bool, values ...string) (int, error) {
	if len(values) == 0 {
		return 0, errors.New("wrong number of list values")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		// expired keys can be reused with a new type
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil {
		d = &Deque{}
		s.lists[key] = d
	}
	if left {
		d.PushFront(values...)
	} else {
		d.PushBack(values...)
	}

	s.touchKeyLocked(key)

	// Wake blocked list waiters
	s.wakeListWaitersLocked(key)

	n := 0
	if s.lists[key] != nil {
		n = s.lists[key].Len()
	}
	return n, nil
}

func (s *Store) popList(key string, left bool) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return "", false, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return "", false, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil || d.Len() == 0 {
		return "", false, nil
	}
	var value string
	var ok bool
	if left {
		value, ok = d.PopFront()
	} else {
		value, ok = d.PopBack()
	}
	if !ok {
		return "", false, nil
	}
	if d.Len() == 0 {
		delete(s.lists, key)
		delete(s.expires, key)
	}
	s.touchKeyLocked(key)
	return value, true, nil
}

func (s *Store) SAdd(key string, members ...string) (int, error) {
	if len(members) == 0 {
		return 0, errors.New("wrong number of set members")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		// expired keys can be reused with a new type
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	if s.sets[key] == nil {
		s.sets[key] = make(map[string]struct{})
	}
	added := 0
	for _, member := range members {
		if _, ok := s.sets[key][member]; !ok {
			s.sets[key][member] = struct{}{}
			added++
		}
	}
	s.touchKeyLocked(key)
	return added, nil
}

func (s *Store) SRem(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return 0, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	set := s.sets[key]
	if len(set) == 0 {
		return 0, nil
	}
	removed := 0
	for _, member := range members {
		if _, ok := set[member]; ok {
			delete(set, member)
			removed++
		}
	}
	if len(set) == 0 {
		delete(s.sets, key)
		delete(s.expires, key)
	}
	return removed, nil
}

func (s *Store) SMembers(key string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	members := make([]string, 0, len(s.sets[key]))
	for member := range s.sets[key] {
		members = append(members, member)
	}
	sort.Strings(members)
	return members, nil
}

func (s *Store) SIsMember(key, member string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return false, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return false, errors.New("wrong type")
	}
	_, ok := s.sets[key][member]
	return ok, nil
}

func (s *Store) SCard(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	return len(s.sets[key]), nil
}

// Strlen returns the length of the string value stored at key.
// Returns 0 if the key does not exist. Error if wrong type.
func (s *Store) Strlen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	v, ok := s.kv[key]
	if !ok {
		return 0, nil
	}
	return len(v.Data), nil
}

// Append appends the value to the string stored at key.
// If the key does not exist it is created. Returns the new length. Error if wrong type.
func (s *Store) Append(key, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		// expired keys can be reused
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	v := s.kv[key]
	v.Data += value
	s.kv[key] = v
	s.touchKeyLocked(key)
	return len(v.Data), nil
}

// GetSet atomically sets key to value and returns the old value.
// Returns an error if the key holds a non-string type.
func (s *Store) GetSet(key, value string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(key, now)
	// type check - only works on strings
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return "", false, errors.New("wrong type")
	}
	old, had := s.kv[key]
	s.kv[key] = Value{Data: value}
	delete(s.expires, key)
	// clean other types just in case
	delete(s.hashes, key)
	delete(s.lists, key)
	delete(s.sets, key)
	delete(s.zsets, key)
	delete(s.streams, key)
	delete(s.lastSeq, key)
	s.touchKeyLocked(key)
	if had {
		return old.Data, true, nil
	}
	return "", false, nil
}

// GetEx gets a key and optionally updates its expiry.
// If persist is true, the expiry is removed. If ttl > 0, the expiry is set.
func (s *Store) GetEx(key string, ttl time.Duration, persist bool) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return "", false
	}
	v, ok := s.kv[key]
	if !ok {
		return "", false
	}
	if persist {
		delete(s.expires, key)
	} else if ttl > 0 {
		s.expires[key] = now.Add(ttl)
	}
	if persist || ttl > 0 {
		s.touchKeyLocked(key)
	}
	return v.Data, true
}

// SetRange overwrites part of the string at key starting at offset.
// Pads with zero bytes if the current string is shorter than offset.
func (s *Store) SetRange(key string, offset int, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(key, now)
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	if offset < 0 {
		return 0, errors.New("offset is out of range")
	}
	current := ""
	if v, ok := s.kv[key]; ok {
		current = v.Data
	}
	// Extend with zero bytes if needed
	needed := offset + len(value)
	if needed > len(current) {
		buf := make([]byte, needed)
		copy(buf, current)
		copy(buf[offset:], value)
		current = string(buf)
	} else {
		buf := []byte(current)
		copy(buf[offset:], value)
		current = string(buf)
	}
	s.kv[key] = Value{Data: current}
	s.touchKeyLocked(key)
	return len(current), nil
}

// GetRange returns a substring of the string stored at key.
// Supports negative indices (counting from end).
func (s *Store) GetRange(key string, start, end int) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return "", nil
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return "", errors.New("wrong type")
	}
	v, ok := s.kv[key]
	if !ok {
		return "", nil
	}
	str := v.Data
	n := len(str)
	if start < 0 {
		start = n + start
	}
	if end < 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end || start >= n {
		return "", nil
	}
	return str[start : end+1], nil
}

// MSetNX sets multiple keys only if none of them exist.
// Returns true if all keys were set, false if none were set.
func (s *Store) MSetNX(keys []string, values []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	// Check all keys first
	for _, key := range keys {
		s.expiredKeyLocked(key, now)
		if s.existsLocked(key) {
			return false
		}
	}
	// Set all
	for i, key := range keys {
		s.kv[key] = Value{Data: values[i]}
		delete(s.expires, key)
		s.touchKeyLocked(key)
	}
	return true
}

// GetDel gets the value of a string key and deletes it.
// Returns the value and whether the key existed.
func (s *Store) GetDel(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return "", false
	}
	v, ok := s.kv[key]
	if !ok {
		return "", false
	}
	s.deleteKeyLocked(key)
	return v.Data, true
}

// LLen returns the length of the list stored at key.
// Returns 0 if the key does not exist. Error if wrong type.
func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil {
		return 0, nil
	}
	return d.Len(), nil
}

// HLen returns the number of fields in the hash stored at key.
// Returns 0 if the key does not exist. Error if wrong type.
func (s *Store) HLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.existsStringRLocked(key, now) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	return len(s.hashes[key]), nil
}

// HExists checks if a field exists in the hash stored at key.
// Error if wrong type.
func (s *Store) HExists(key, field string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return false, nil
	}
	if s.existsStringRLocked(key, now) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return false, errors.New("wrong type")
	}
	hash := s.hashes[key]
	if hash == nil {
		return false, nil
	}
	_, ok := hash[field]
	return ok, nil
}

// SInter returns the intersection of all given sets.
// Non-existent keys are treated as empty sets. Error if wrong type.
func (s *Store) SInter(keys ...string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()

	// Collect all sets; type-check each key
	sets := make([]map[string]struct{}, 0, len(keys))
	for _, key := range keys {
		if s.isExpiredRLocked(key, now) {
			// treated as empty set — intersection with empty is empty
			return []string{}, nil
		}
		if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
			return nil, errors.New("wrong type")
		}
		set := s.sets[key]
		if set == nil || len(set) == 0 {
			return []string{}, nil
		}
		sets = append(sets, set)
	}
	if len(sets) == 0 {
		return []string{}, nil
	}

	// Find smallest set
	smallest := 0
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) < len(sets[smallest]) {
			smallest = i
		}
	}

	var result []string
	for member := range sets[smallest] {
		inAll := true
		for i, set := range sets {
			if i == smallest {
				continue
			}
			if _, ok := set[member]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			result = append(result, member)
		}
	}
	sort.Strings(result)
	return result, nil
}

// SUnion returns the union of all given sets.
// Non-existent keys are treated as empty sets. Error if wrong type.
func (s *Store) SUnion(keys ...string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()

	union := make(map[string]struct{})
	for _, key := range keys {
		if s.isExpiredRLocked(key, now) {
			continue
		}
		if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
			return nil, errors.New("wrong type")
		}
		for member := range s.sets[key] {
			union[member] = struct{}{}
		}
	}

	result := make([]string, 0, len(union))
	for member := range union {
		result = append(result, member)
	}
	sort.Strings(result)
	return result, nil
}

// SDiff returns elements in the first set that are not in any other set.
// Non-existent keys are treated as empty sets. Error if wrong type.
func (s *Store) SDiff(keys ...string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()

	// Type-check all keys and collect sets
	sets := make([]map[string]struct{}, len(keys))
	for i, key := range keys {
		if s.isExpiredRLocked(key, now) {
			continue
		}
		if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
			return nil, errors.New("wrong type")
		}
		sets[i] = s.sets[key]
	}

	if sets[0] == nil || len(sets[0]) == 0 {
		return []string{}, nil
	}

	var result []string
	for member := range sets[0] {
		inOther := false
		for _, set := range sets[1:] {
			if set != nil {
				if _, ok := set[member]; ok {
					inOther = true
					break
				}
			}
		}
		if !inOther {
			result = append(result, member)
		}
	}
	sort.Strings(result)
	return result, nil
}

func (s *Store) ZAdd(key string, members []ZMember) (int, error) {
	if len(members) == 0 {
		return 0, errors.New("wrong number of sorted set members")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		// expired keys can be reused with a new type
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	if s.zsets[key] == nil {
		s.zsets[key] = NewSortedSet()
	}
	added := 0
	for _, item := range members {
		if s.zsets[key].Add(item.Member, item.Score) {
			added++
		}
	}
	s.touchKeyLocked(key)
	return added, nil
}

func (s *Store) ZRange(key string, start, stop int) ([]ZMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil || zs.Len() == 0 {
		return nil, nil
	}
	return zs.Range(start, stop), nil
}

func (s *Store) ZRem(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return 0, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil || zs.Len() == 0 {
		return 0, nil
	}
	removed := 0
	for _, member := range members {
		if zs.Remove(member) {
			removed++
		}
	}
	if zs.Len() == 0 {
		delete(s.zsets, key)
		delete(s.expires, key)
	}
	if removed > 0 {
		s.touchKeyLocked(key)
	}
	return removed, nil
}

func (s *Store) ZScore(key, member string) (float64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, false, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, false, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil {
		return 0, false, nil
	}
	score, ok := zs.Score(member)
	return score, ok, nil
}

func (s *Store) ZRangeByScore(key string, min, max float64, offset, count int) ([]ZMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil || zs.Len() == 0 {
		return nil, nil
	}
	return zs.RangeByScore(min, max, offset, count), nil
}

func (s *Store) ZRevRange(key string, start, stop int) ([]ZMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil || zs.Len() == 0 {
		return nil, nil
	}
	return zs.RevRange(start, stop), nil
}

func (s *Store) ZRank(key, member string) (int64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, false, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, false, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil {
		return 0, false, nil
	}
	rank, ok := zs.Rank(member)
	if !ok {
		return 0, false, nil
	}
	return int64(rank), true, nil
}

func (s *Store) ZRevRank(key, member string) (int64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, false, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, false, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil {
		return 0, false, nil
	}
	rank, ok := zs.Rank(member)
	if !ok {
		return 0, false, nil
	}
	return int64(zs.Len() - 1 - rank), true, nil
}

func (s *Store) ZCount(key string, min, max float64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil {
		return 0, nil
	}
	return zs.CountByScore(min, max), nil
}

func (s *Store) XLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	return len(s.streams[key]), nil
}

func (s *Store) Copy(src, dst string, replace bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(src, now)
	if !s.existsLocked(src) {
		return false, nil
	}
	if s.existsLocked(dst) && !replace {
		return false, nil
	}
	if s.existsLocked(dst) {
		s.deleteKeyLocked(dst)
	}
	if v, ok := s.kv[src]; ok {
		s.kv[dst] = v
	}
	if h, ok := s.hashes[src]; ok {
		newH := make(map[string]string, len(h))
		for k, v := range h {
			newH[k] = v
		}
		s.hashes[dst] = newH
	}
	if d := s.lists[src]; d != nil {
		newD := &Deque{}
		newD.PushBack(d.ToSlice()...)
		s.lists[dst] = newD
	}
	if set, ok := s.sets[src]; ok {
		newSet := make(map[string]struct{}, len(set))
		for k := range set {
			newSet[k] = struct{}{}
		}
		s.sets[dst] = newSet
	}
	if zs := s.zsets[src]; zs != nil {
		newZS := NewSortedSet()
		for _, m := range zs.Members() {
			newZS.Add(m.Member, m.Score)
		}
		s.zsets[dst] = newZS
	}
	if entries, ok := s.streams[src]; ok {
		newEntries := make([]StreamEntry, len(entries))
		for i, e := range entries {
			newEntries[i] = cloneEntry(e)
		}
		s.streams[dst] = newEntries
	}
	if hll, ok := s.hlls[src]; ok {
		newHLL := NewHyperLogLog()
		newHLL.registers = hll.registers
		s.hlls[dst] = newHLL
	}
	if exp, ok := s.expires[src]; ok {
		s.expires[dst] = exp
	}
	if seq, ok := s.lastSeq[src]; ok {
		s.lastSeq[dst] = seq
	}
	s.touchKeyLocked(dst)
	return true, nil
}

func (s *Store) ZCard(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil {
		return 0, nil
	}
	return zs.Len(), nil
}

// ── GEO commands ─────────────────────────────────────────────────────

func (s *Store) GeoAdd(key string, members []GeoMember) (int, error) {
	zm := make([]ZMember, len(members))
	for i, m := range members {
		zm[i] = ZMember{Member: m.Name, Score: GeoEncode(m.Longitude, m.Latitude)}
	}
	return s.ZAdd(key, zm)
}

func (s *Store) GeoPos(key string, members ...string) ([]GeoPoint, []bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return make([]GeoPoint, len(members)), make([]bool, len(members))
	}
	zs := s.zsets[key]
	points := make([]GeoPoint, len(members))
	found := make([]bool, len(members))
	if zs == nil {
		return points, found
	}
	for i, m := range members {
		score, ok := zs.Score(m)
		if ok {
			lon, lat := GeoDecode(score)
			points[i] = GeoPoint{Longitude: lon, Latitude: lat}
			found[i] = true
		}
	}
	return points, found
}

func (s *Store) GeoDist(key, member1, member2 string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, false
	}
	zs := s.zsets[key]
	if zs == nil {
		return 0, false
	}
	s1, ok1 := zs.Score(member1)
	s2, ok2 := zs.Score(member2)
	if !ok1 || !ok2 {
		return 0, false
	}
	lon1, lat1 := GeoDecode(s1)
	lon2, lat2 := GeoDecode(s2)
	return GeoDistBetween(lon1, lat1, lon2, lat2), true
}

func (s *Store) GeoSearchByRadius(key string, lon, lat, radius float64, count int, asc bool) []GeoResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil
	}
	zs := s.zsets[key]
	if zs == nil {
		return nil
	}
	results := GeoSearch(zs.Members(), lon, lat, radius)
	if !asc {
		for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
			results[i], results[j] = results[j], results[i]
		}
	}
	if count > 0 && len(results) > count {
		results = results[:count]
	}
	return results
}

func (s *Store) GeoSearchByMember(key, member string, radius float64, count int, asc bool) ([]GeoResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	zs := s.zsets[key]
	if zs == nil {
		return nil, nil
	}
	score, ok := zs.Score(member)
	if !ok {
		return nil, errors.New("member not found")
	}
	lon, lat := GeoDecode(score)
	results := GeoSearch(zs.Members(), lon, lat, radius)
	if !asc {
		for i, j := 0, len(results)-1; i < j; i, j = i+1, j-1 {
			results[i], results[j] = results[j], results[i]
		}
	}
	if count > 0 && len(results) > count {
		results = results[:count]
	}
	return results, nil
}

func (s *Store) XAdd(stream, id string, fields []string) (string, error) {
	if len(fields) == 0 || len(fields)%2 != 0 {
		return "", errors.New("wrong number of stream field arguments")
	}
	s.mu.Lock()
	if s.expiredKeyLocked(stream, time.Now()) {
		// expired keys can be reused with a new type
	}
	if s.existsStringLocked(stream) || s.hashes[stream] != nil || s.lists[stream] != nil || s.sets[stream] != nil || s.zsets[stream] != nil || s.hlls[stream] != nil {
		s.mu.Unlock()
		return "", errors.New("wrong type")
	}
	id, err := s.prepareStreamIDLocked(stream, id)
	if err != nil {
		s.mu.Unlock()
		return "", err
	}
	waiters := s.addStreamEntryLocked(stream, id, fields)
	s.mu.Unlock()

	for _, w := range waiters {
		w.close()
	}
	return id, nil
}

func (s *Store) NextStreamID(stream, id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(stream, time.Now()) {
		// expired keys can be reused with a new type
	}
	if s.existsStringLocked(stream) || s.hashes[stream] != nil || s.lists[stream] != nil || s.sets[stream] != nil || s.zsets[stream] != nil || s.hlls[stream] != nil {
		return "", errors.New("wrong type")
	}
	return s.prepareStreamIDLocked(stream, id)
}

func (s *Store) XAddPrepared(stream, id string, fields []string) error {
	if len(fields) == 0 || len(fields)%2 != 0 {
		return errors.New("wrong number of stream field arguments")
	}
	s.mu.Lock()
	if s.expiredKeyLocked(stream, time.Now()) {
		s.mu.Unlock()
		return errors.New("stream ID must be greater than previous ID")
	}
	if compareID(id, s.lastIDLocked(stream)) <= 0 {
		s.mu.Unlock()
		return errors.New("stream ID must be greater than previous ID")
	}
	parsed := parseID(id)
	s.lastSeq[stream] = parsed[1]
	waiters := s.addStreamEntryLocked(stream, id, fields)
	s.mu.Unlock()

	for _, w := range waiters {
		w.close()
	}
	return nil
}

func (s *Store) prepareStreamIDLocked(stream, id string) (string, error) {
	if id == "*" {
		last := parseID(s.lastIDLocked(stream))
		ms := time.Now().UnixMilli()
		if ms <= last[0] {
			ms = last[0]
			s.lastSeq[stream] = last[1]
		}
		s.lastSeq[stream]++
		id = fmt.Sprintf("%d-%d", ms, s.lastSeq[stream])
	} else if compareID(id, s.lastIDLocked(stream)) <= 0 {
		return "", errors.New("stream ID must be greater than previous ID")
	} else {
		parsed := parseID(id)
		s.lastSeq[stream] = parsed[1]
	}
	return id, nil
}

func (s *Store) addStreamEntryLocked(stream, id string, fields []string) []*waiter {
	s.streams[stream] = append(s.streams[stream], StreamEntry{ID: id, Fields: append([]string(nil), fields...)})
	s.touchKeyLocked(stream)
	waiters := s.waits[stream]
	delete(s.waits, stream)
	return waiters
}

// searchStreamStart returns the index of the first entry whose ID is > afterID.
// Uses binary search via sort.Search.
func searchStreamStart(entries []StreamEntry, afterID string) int {
	return sort.Search(len(entries), func(i int) bool {
		return compareID(entries[i].ID, afterID) > 0
	})
}

func (s *Store) XRange(stream, start, end string, count int) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(stream, now) {
		return nil
	}
	entries := s.streams[stream]
	if len(entries) == 0 {
		return nil
	}
	// Use binary search for the start position when start is not "-".
	begin := 0
	if start != "-" {
		begin = sort.Search(len(entries), func(i int) bool {
			return compareID(entries[i].ID, start) >= 0
		})
	}
	out := make([]StreamEntry, 0, len(entries)-begin)
	for i := begin; i < len(entries); i++ {
		entry := entries[i]
		if end != "+" && compareID(entry.ID, end) > 0 {
			break
		}
		out = append(out, cloneEntry(entry))
		if count > 0 && len(out) >= count {
			break
		}
	}
	return out
}

func (s *Store) XRead(streams []string, ids []string) map[string][]StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	out := make(map[string][]StreamEntry)
	for i, stream := range streams {
		if s.isExpiredRLocked(stream, now) {
			continue
		}
		entries := s.streams[stream]
		if len(entries) == 0 {
			continue
		}
		// Binary search for the first entry after ids[i].
		start := searchStreamStart(entries, ids[i])
		for j := start; j < len(entries); j++ {
			out[stream] = append(out[stream], cloneEntry(entries[j]))
		}
	}
	return out
}

func (s *Store) XGroupCreate(stream, group, id string, mkstream bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(stream, time.Now()) {
		// expired keys can be reused with a stream group
	}
	if s.existsStringLocked(stream) || s.hashes[stream] != nil || s.lists[stream] != nil || s.sets[stream] != nil || s.zsets[stream] != nil || s.hlls[stream] != nil {
		return errors.New("wrong type")
	}
	if _, ok := s.streams[stream]; !ok && !mkstream {
		return errors.New("stream does not exist")
	}
	if id == "$" {
		id = s.lastIDLocked(stream)
	}
	if s.groups[stream] == nil {
		s.groups[stream] = make(map[string]*ConsumerGroup)
	}
	if _, ok := s.groups[stream][group]; ok {
		return errors.New("consumer group already exists")
	}
	s.groups[stream][group] = &ConsumerGroup{LastID: id, Pending: make(map[string]PendingEntry)}
	s.touchKeyLocked(stream)
	return nil
}

func (s *Store) XGroupExists(stream, group string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(stream, now) {
		return false
	}
	return s.groups[stream][group] != nil
}

func (s *Store) XReadGroupPlan(group, consumer string, streams []string, ids []string, count int) (map[string][]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if count <= 0 {
		count = 10
	}
	out := make(map[string][]StreamEntry)
	for i, stream := range streams {
		if s.isExpiredRLocked(stream, now) {
			continue
		}
		g := s.groups[stream][group]
		if g == nil {
			return nil, errors.New("consumer group does not exist")
		}
		entries := s.streams[stream]
		if ids[i] == ">" {
			// Binary search for entries after g.LastID.
			start := searchStreamStart(entries, g.LastID)
			for j := start; j < len(entries); j++ {
				out[stream] = append(out[stream], cloneEntry(entries[j]))
				if len(out[stream]) >= count {
					break
				}
			}
			continue
		}
		for _, entry := range entries {
			pending, ok := g.Pending[entry.ID]
			if ok && pending.Consumer == consumer && compareID(entry.ID, ids[i]) > 0 {
				out[stream] = append(out[stream], cloneEntry(entry))
				if len(out[stream]) >= count {
					break
				}
			}
		}
	}
	return out, nil
}

func (s *Store) XGroupDeliver(stream, group, consumer string, ids []string, delivered time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.groups[stream][group]
	if g == nil {
		return errors.New("consumer group does not exist")
	}
	if delivered.IsZero() {
		delivered = time.Now()
	}
	if g.Pending == nil {
		g.Pending = make(map[string]PendingEntry)
	}
	for _, id := range ids {
		g.Pending[id] = PendingEntry{Consumer: consumer, Delivered: delivered}
		if compareID(id, g.LastID) > 0 {
			g.LastID = id
		}
	}
	s.touchKeyLocked(stream)
	return nil
}

func (s *Store) XAck(stream, group string, ids ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(stream, time.Now()) {
		return 0, nil
	}
	g := s.groups[stream][group]
	if g == nil {
		return 0, errors.New("consumer group does not exist")
	}
	n := 0
	for _, id := range ids {
		if _, ok := g.Pending[id]; ok {
			delete(g.Pending, id)
			n++
		}
	}
	if n > 0 {
		s.touchKeyLocked(stream)
	}
	return n, nil
}

func (s *Store) LastID(stream string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(stream, now) {
		return "0-0"
	}
	return s.lastIDLocked(stream)
}

// XClaim transfers ownership of pending stream entries from one consumer to another.
func (s *Store) XClaim(stream, group, consumer string, minIdleMs int64, ids []string) ([]StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groups[stream] == nil || s.groups[stream][group] == nil {
		return nil, errors.New("consumer group does not exist")
	}
	g := s.groups[stream][group]

	now := time.Now()
	var claimed []StreamEntry
	for _, id := range ids {
		if pending, ok := g.Pending[id]; ok {
			idle := now.Sub(pending.Delivered)
			if idle.Milliseconds() >= minIdleMs || minIdleMs == 0 {
				g.Pending[id] = PendingEntry{Consumer: consumer, Delivered: now}
				for _, entry := range s.streams[stream] {
					if entry.ID == id {
						claimed = append(claimed, cloneEntry(entry))
						break
					}
				}
			}
		}
	}
	if len(claimed) > 0 {
		s.touchKeyLocked(stream)
	}
	return claimed, nil
}

// XAutoClaim claims pending entries that have been idle for at least minIdleMs.
func (s *Store) XAutoClaim(stream, group, consumer string, minIdleMs int64, startID string, count int) (string, []StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.groups[stream] == nil || s.groups[stream][group] == nil {
		return "0-0", nil, errors.New("consumer group does not exist")
	}
	g := s.groups[stream][group]
	if count <= 0 {
		count = 100
	}

	now := time.Now()
	// Collect pending IDs that qualify, in sorted order.
	var candidateIDs []string
	for id := range g.Pending {
		if compareID(id, startID) >= 0 {
			candidateIDs = append(candidateIDs, id)
		}
	}
	sort.Slice(candidateIDs, func(i, j int) bool {
		return compareID(candidateIDs[i], candidateIDs[j]) < 0
	})

	var claimed []StreamEntry
	nextID := "0-0"
	for _, id := range candidateIDs {
		if len(claimed) >= count {
			nextID = id
			break
		}
		pending := g.Pending[id]
		idle := now.Sub(pending.Delivered)
		if idle.Milliseconds() >= minIdleMs || minIdleMs == 0 {
			g.Pending[id] = PendingEntry{Consumer: consumer, Delivered: now}
			for _, entry := range s.streams[stream] {
				if entry.ID == id {
					claimed = append(claimed, cloneEntry(entry))
					break
				}
			}
		}
	}
	if len(claimed) > 0 {
		s.touchKeyLocked(stream)
	}
	return nextID, claimed, nil
}

// XTrim trims the stream to at most maxLen entries, removing the oldest.
func (s *Store) XTrim(stream string, maxLen int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := s.streams[stream]
	if len(entries) <= maxLen {
		return 0, nil
	}
	trimmed := len(entries) - maxLen
	s.streams[stream] = entries[trimmed:]
	s.touchKeyLocked(stream)
	return trimmed, nil
}

// XPendingSummary returns a summary of pending entries for a consumer group.
func (s *Store) XPendingSummary(stream, group string) (int, string, string, map[string]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.groups[stream] == nil || s.groups[stream][group] == nil {
		return 0, "", "", nil
	}
	g := s.groups[stream][group]
	if len(g.Pending) == 0 {
		return 0, "", "", nil
	}
	var minID, maxID string
	consumers := make(map[string]int)
	for id, pe := range g.Pending {
		if minID == "" || compareID(id, minID) < 0 {
			minID = id
		}
		if maxID == "" || compareID(id, maxID) > 0 {
			maxID = id
		}
		consumers[pe.Consumer]++
	}
	return len(g.Pending), minID, maxID, consumers
}

// XInfoStream returns basic stream information.
func (s *Store) XInfoStream(stream string) (int, string, string, int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := s.streams[stream]
	length := len(entries)
	groups := len(s.groups[stream])
	if length == 0 {
		return 0, "0-0", "0-0", groups, s.streams[stream] != nil
	}
	return length, entries[0].ID, entries[length-1].ID, groups, true
}

// XInfoGroups returns consumer group info for a stream.
func (s *Store) XInfoGroups(stream string) []map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []map[string]interface{}
	for name, g := range s.groups[stream] {
		consumers := make(map[string]bool)
		for _, pe := range g.Pending {
			consumers[pe.Consumer] = true
		}
		result = append(result, map[string]interface{}{
			"name":            name,
			"consumers":       len(consumers),
			"pending":         len(g.Pending),
			"last-delivered-id": g.LastID,
		})
	}
	return result
}

func (s *Store) Wait(streams []string, timeout time.Duration) {
	w := &waiter{ch: make(chan struct{})}
	s.mu.Lock()
	for _, stream := range streams {
		s.waits[stream] = append(s.waits[stream], w)
	}
	s.mu.Unlock()
	defer s.removeWaiter(streams, w)

	if timeout <= 0 {
		<-w.ch
		return
	}
	select {
	case <-w.ch:
	case <-time.After(timeout):
	}
}

func (s *Store) removeWaiter(streams []string, target *waiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stream := range streams {
		waiters := s.waits[stream]
		if len(waiters) == 0 {
			continue
		}
		kept := waiters[:0]
		for _, w := range waiters {
			if w != target {
				kept = append(kept, w)
			}
		}
		if len(kept) == 0 {
			delete(s.waits, stream)
		} else {
			s.waits[stream] = kept
		}
	}
}

func (w *waiter) close() {
	w.once.Do(func() { close(w.ch) })
}

// BPop tries to pop from the first non-empty list key. If all are empty, it
// blocks until an element is pushed or timeout expires. When left is true
// (BLPOP) elements are popped from the front; when false (BRPOP) from the back.
func (s *Store) BPop(keys []string, timeout time.Duration, left bool) (string, string, bool) {
	s.mu.Lock()
	for _, key := range keys {
		if s.expiredKeyLocked(key, time.Now()) {
			continue
		}
		if d := s.lists[key]; d != nil && d.Len() > 0 {
			var val string
			if left {
				val, _ = d.PopFront()
			} else {
				val, _ = d.PopBack()
			}
			if d.Len() == 0 {
				delete(s.lists, key)
				delete(s.expires, key)
			}
			s.touchKeyLocked(key)
			s.mu.Unlock()
			return key, val, true
		}
	}

	// Register waiter
	w := &listWaiter{ch: make(chan listWaitResult, 1), left: left}
	for _, key := range keys {
		s.listWaits[key] = append(s.listWaits[key], w)
	}
	s.mu.Unlock()

	defer s.removeListWaiter(keys, w)

	if timeout <= 0 {
		result := <-w.ch
		return result.key, result.value, true
	}
	select {
	case result := <-w.ch:
		return result.key, result.value, true
	case <-time.After(timeout):
		return "", "", false
	}
}

func (s *Store) wakeListWaitersLocked(key string) {
	for len(s.listWaits[key]) > 0 {
		d := s.lists[key]
		if d == nil || d.Len() == 0 {
			break
		}
		w := s.listWaits[key][0]
		s.listWaits[key] = s.listWaits[key][1:]
		if len(s.listWaits[key]) == 0 {
			delete(s.listWaits, key)
		}
		var val string
		if w.left {
			val, _ = d.PopFront()
		} else {
			val, _ = d.PopBack()
		}
		if d.Len() == 0 {
			delete(s.lists, key)
			delete(s.expires, key)
		}
		w.once.Do(func() {
			w.ch <- listWaitResult{key: key, value: val}
			close(w.ch)
		})
	}
}

func (s *Store) removeListWaiter(keys []string, target *listWaiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		waiters := s.listWaits[key]
		kept := waiters[:0]
		for _, w := range waiters {
			if w != target {
				kept = append(kept, w)
			}
		}
		if len(kept) == 0 {
			delete(s.listWaits, key)
		} else {
			s.listWaits[key] = kept
		}
	}
}

// FlushDB deletes all keys from the store.
func (s *Store) FlushDB() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv = make(map[string]Value)
	s.hashes = make(map[string]map[string]string)
	s.lists = make(map[string]*Deque)
	s.sets = make(map[string]map[string]struct{})
	s.zsets = make(map[string]*SortedSet)
	s.streams = make(map[string][]StreamEntry)
	s.hlls = make(map[string]*HyperLogLog)
	s.jsons = make(map[string]*JSONDoc)
	s.groups = make(map[string]map[string]*ConsumerGroup)
	s.expires = make(map[string]time.Time)
	s.lastSeq = make(map[string]int64)
	s.waits = make(map[string][]*waiter)
	s.listWaits = make(map[string][]*listWaiter)
	s.version++
	s.keyVersion = make(map[string]uint64)
	s.accessMu.Lock()
	s.accessTime = make(map[string]int64)
	s.accessMu.Unlock()
	s.memEstimate = 0
	s.opsSinceEstimate = 0
	s.rateLimiters = make(map[string]*RateLimiter)
	s.notBefore = make(map[string]time.Time)
	s.tags = make(map[string]map[string]string)
	s.timeseries = make(map[string]*TimeSeries)
	s.queues = make(map[string]*Queue)
	s.queueWaits = make(map[string][]*waiter)
}

// RandomKey returns a random non-expired key from any data type.
// Go map iteration is randomized, so iterating and returning the first
// non-expired key effectively gives a random key.
func (s *Store) RandomKey() (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	for key := range s.kv {
		if !s.isExpiredRLocked(key, now) {
			return key, true
		}
	}
	for key := range s.hashes {
		if !s.isExpiredRLocked(key, now) {
			return key, true
		}
	}
	for key := range s.lists {
		if !s.isExpiredRLocked(key, now) {
			return key, true
		}
	}
	for key := range s.sets {
		if !s.isExpiredRLocked(key, now) {
			return key, true
		}
	}
	for key := range s.zsets {
		if !s.isExpiredRLocked(key, now) {
			return key, true
		}
	}
	for key := range s.streams {
		if !s.isExpiredRLocked(key, now) {
			return key, true
		}
	}
	for key := range s.hlls {
		if !s.isExpiredRLocked(key, now) {
			return key, true
		}
	}
	return "", false
}

func (s *Store) Stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(time.Now())
	return len(s.kv), len(s.streams)
}

type Snapshot struct {
	KV         map[string]Value
	Hashes     map[string]map[string]string
	Lists      map[string][]string
	Sets       map[string][]string
	ZSets      map[string][]ZMember
	Streams    map[string][]StreamEntry
	Groups     map[string]map[string]*ConsumerGroup
	Expires    map[string]time.Time
	LastSeq    map[string]int64
	JSONDocs   map[string]string
	TimeSeries map[string]*TimeSeries
	Queues     map[string]*Queue
	Tags       map[string]map[string]string
	HLLs       map[string][]uint8
	NotBefore  map[string]time.Time
}

func (s *Store) Dump() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.purgeExpiredLocked(now)
	kv := make(map[string]Value, len(s.kv))
	for k, v := range s.kv {
		kv[k] = v
	}
	hashes := make(map[string]map[string]string, len(s.hashes))
	for key, hash := range s.hashes {
		hashes[key] = make(map[string]string, len(hash))
		for field, value := range hash {
			hashes[key][field] = value
		}
	}
	lists := make(map[string][]string, len(s.lists))
	for key, d := range s.lists {
		lists[key] = d.ToSlice()
	}
	sets := make(map[string][]string, len(s.sets))
	for key, set := range s.sets {
		members := make([]string, 0, len(set))
		for member := range set {
			members = append(members, member)
		}
		sort.Strings(members)
		sets[key] = members
	}
	zsets := make(map[string][]ZMember, len(s.zsets))
	for key, zs := range s.zsets {
		zsets[key] = zs.Members()
	}
	streams := make(map[string][]StreamEntry, len(s.streams))
	for name, entries := range s.streams {
		streams[name] = make([]StreamEntry, len(entries))
		for i, entry := range entries {
			streams[name][i] = cloneEntry(entry)
		}
	}
	groups := make(map[string]map[string]*ConsumerGroup, len(s.groups))
	for stream, streamGroups := range s.groups {
		groups[stream] = make(map[string]*ConsumerGroup, len(streamGroups))
		for name, group := range streamGroups {
			pending := make(map[string]PendingEntry, len(group.Pending))
			for id, entry := range group.Pending {
				pending[id] = entry
			}
			groups[stream][name] = &ConsumerGroup{LastID: group.LastID, Pending: pending}
		}
	}
	lastSeq := make(map[string]int64, len(s.lastSeq))
	for name, seq := range s.lastSeq {
		lastSeq[name] = seq
	}
	expires := make(map[string]time.Time, len(s.expires))
	for key, expireAt := range s.expires {
		expires[key] = expireAt
	}
	jsonDocs := make(map[string]string, len(s.jsons))
	for key, doc := range s.jsons {
		jsonDocs[key] = doc.String()
	}
	tsCopy := make(map[string]*TimeSeries, len(s.timeseries))
	for key, ts := range s.timeseries {
		newTS := NewTimeSeries()
		newTS.Samples = append([]TSSample(nil), ts.Samples...)
		for k, v := range ts.Labels {
			newTS.Labels[k] = v
		}
		tsCopy[key] = newTS
	}
	queuesCopy := make(map[string]*Queue, len(s.queues))
	for key, q := range s.queues {
		newQ := &Queue{
			Processing: make(map[string]*QueueMessage),
			AckTimeout: q.AckTimeout,
			MaxRetries: q.MaxRetries,
		}
		for _, msg := range q.Pending {
			m := *msg
			newQ.Pending = append(newQ.Pending, &m)
		}
		for id, msg := range q.Processing {
			m := *msg
			newQ.Processing[id] = &m
		}
		for _, msg := range q.DeadLetter {
			m := *msg
			newQ.DeadLetter = append(newQ.DeadLetter, &m)
		}
		queuesCopy[key] = newQ
	}
	tagsCopy := make(map[string]map[string]string, len(s.tags))
	for key, tagMap := range s.tags {
		t := make(map[string]string, len(tagMap))
		for k, v := range tagMap {
			t[k] = v
		}
		tagsCopy[key] = t
	}
	hllsCopy := make(map[string][]uint8, len(s.hlls))
	for key, hll := range s.hlls {
		hllsCopy[key] = hll.Registers()
	}
	notBeforeCopy := make(map[string]time.Time, len(s.notBefore))
	for k, t := range s.notBefore {
		notBeforeCopy[k] = t
	}
	return Snapshot{
		KV: kv, Hashes: hashes, Lists: lists, Sets: sets, ZSets: zsets,
		Streams: streams, Groups: groups, Expires: expires, LastSeq: lastSeq,
		JSONDocs: jsonDocs, TimeSeries: tsCopy, Queues: queuesCopy,
		Tags: tagsCopy, HLLs: hllsCopy, NotBefore: notBeforeCopy,
	}
}

func (s *Store) Load(snapshot Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.kv = make(map[string]Value, len(snapshot.KV))
	for k, v := range snapshot.KV {
		s.kv[k] = v
	}
	s.hashes = make(map[string]map[string]string, len(snapshot.Hashes))
	for key, hash := range snapshot.Hashes {
		s.hashes[key] = make(map[string]string, len(hash))
		for field, value := range hash {
			s.hashes[key][field] = value
		}
	}
	s.lists = make(map[string]*Deque, len(snapshot.Lists))
	for key, list := range snapshot.Lists {
		d := &Deque{}
		d.PushBack(list...)
		s.lists[key] = d
	}
	s.sets = make(map[string]map[string]struct{}, len(snapshot.Sets))
	for key, members := range snapshot.Sets {
		s.sets[key] = make(map[string]struct{}, len(members))
		for _, member := range members {
			s.sets[key][member] = struct{}{}
		}
	}
	s.zsets = make(map[string]*SortedSet, len(snapshot.ZSets))
	for key, members := range snapshot.ZSets {
		zs := NewSortedSet()
		for _, m := range members {
			zs.Add(m.Member, m.Score)
		}
		s.zsets[key] = zs
	}
	s.streams = make(map[string][]StreamEntry, len(snapshot.Streams))
	for name, entries := range snapshot.Streams {
		s.streams[name] = make([]StreamEntry, len(entries))
		for i, entry := range entries {
			s.streams[name][i] = cloneEntry(entry)
		}
	}
	s.groups = make(map[string]map[string]*ConsumerGroup, len(snapshot.Groups))
	for stream, streamGroups := range snapshot.Groups {
		s.groups[stream] = make(map[string]*ConsumerGroup, len(streamGroups))
		for name, group := range streamGroups {
			pending := make(map[string]PendingEntry, len(group.Pending))
			for id, entry := range group.Pending {
				pending[id] = entry
			}
			s.groups[stream][name] = &ConsumerGroup{LastID: group.LastID, Pending: pending}
		}
	}
	s.lastSeq = make(map[string]int64, len(snapshot.LastSeq))
	for name, seq := range snapshot.LastSeq {
		s.lastSeq[name] = seq
	}
	s.expires = make(map[string]time.Time, len(snapshot.Expires))
	for key, expireAt := range snapshot.Expires {
		s.expires[key] = expireAt
	}
	if snapshot.JSONDocs != nil {
		s.jsons = make(map[string]*JSONDoc, len(snapshot.JSONDocs))
		for key, raw := range snapshot.JSONDocs {
			doc, err := NewJSONDoc(raw)
			if err == nil {
				s.jsons[key] = doc
			}
		}
	} else {
		s.jsons = make(map[string]*JSONDoc)
	}
	if snapshot.TimeSeries != nil {
		s.timeseries = make(map[string]*TimeSeries, len(snapshot.TimeSeries))
		for key, ts := range snapshot.TimeSeries {
			newTS := NewTimeSeries()
			newTS.Samples = append([]TSSample(nil), ts.Samples...)
			for k, v := range ts.Labels {
				newTS.Labels[k] = v
			}
			s.timeseries[key] = newTS
		}
	} else {
		s.timeseries = make(map[string]*TimeSeries)
	}
	if snapshot.Queues != nil {
		s.queues = make(map[string]*Queue, len(snapshot.Queues))
		for key, q := range snapshot.Queues {
			newQ := &Queue{
				Processing: make(map[string]*QueueMessage),
				AckTimeout: q.AckTimeout,
				MaxRetries: q.MaxRetries,
			}
			for _, msg := range q.Pending {
				m := *msg
				newQ.Pending = append(newQ.Pending, &m)
			}
			for id, msg := range q.Processing {
				m := *msg
				newQ.Processing[id] = &m
			}
			for _, msg := range q.DeadLetter {
				m := *msg
				newQ.DeadLetter = append(newQ.DeadLetter, &m)
			}
			s.queues[key] = newQ
		}
	} else {
		s.queues = make(map[string]*Queue)
	}
	if snapshot.Tags != nil {
		s.tags = make(map[string]map[string]string, len(snapshot.Tags))
		for key, tagMap := range snapshot.Tags {
			t := make(map[string]string, len(tagMap))
			for k, v := range tagMap {
				t[k] = v
			}
			s.tags[key] = t
		}
	} else {
		s.tags = make(map[string]map[string]string)
	}
	if snapshot.HLLs != nil {
		s.hlls = make(map[string]*HyperLogLog, len(snapshot.HLLs))
		for key, regs := range snapshot.HLLs {
			hll := NewHyperLogLog()
			hll.LoadRegisters(regs)
			s.hlls[key] = hll
		}
	} else {
		s.hlls = make(map[string]*HyperLogLog)
	}
	if snapshot.NotBefore != nil {
		s.notBefore = make(map[string]time.Time, len(snapshot.NotBefore))
		for k, t := range snapshot.NotBefore {
			s.notBefore[k] = t
		}
	} else {
		s.notBefore = make(map[string]time.Time)
	}
	s.version++
	s.keyVersion = make(map[string]uint64)
	s.purgeExpiredLocked(now)
}

func (s *Store) existsLocked(key string) bool {
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return false
	}
	if nb, ok := s.notBefore[key]; ok && now.Before(nb) {
		return false
	}
	return s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil
}

func (s *Store) existsStringLocked(key string) bool {
	if s.expiredKeyLocked(key, time.Now()) {
		return false
	}
	_, ok := s.kv[key]
	if !ok {
		return false
	}
	return true
}

// isVisibleRLocked checks that a key is neither expired nor still delayed.
func (s *Store) isVisibleRLocked(key string, now time.Time) bool {
	if s.isExpiredRLocked(key, now) {
		return false
	}
	if nb, ok := s.notBefore[key]; ok && now.Before(nb) {
		return false
	}
	return true
}

// allKeysRLocked collects all non-expired, visible keys. Caller must hold at least RLock.
func (s *Store) allKeysRLocked(now time.Time) []string {
	keys := make([]string, 0, len(s.kv)+len(s.hashes)+len(s.lists)+len(s.sets)+len(s.zsets)+len(s.streams)+len(s.hlls)+len(s.jsons))
	for key := range s.kv {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.hashes {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.lists {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.sets {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.zsets {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.streams {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.hlls {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.jsons {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	for key := range s.timeseries {
		if s.isVisibleRLocked(key, now) {
			keys = append(keys, key)
		}
	}
	return keys
}

// keysLocked collects all non-expired keys matching the pattern.
// Safe to call under either Lock or RLock (uses isExpiredRLocked, no mutations).
func (s *Store) keysLocked(pattern string) []string {
	if pattern == "" {
		pattern = "*"
	}
	now := time.Now()
	all := s.allKeysRLocked(now)
	if pattern == "*" {
		return all
	}
	keys := make([]string, 0, len(all))
	for _, key := range all {
		if matchPattern(pattern, key) {
			keys = append(keys, key)
		}
	}
	return keys
}

func matchPattern(pattern, key string) bool {
	return MatchGlob(pattern, key)
}

// SearchCondition represents a single WHERE clause in HSEARCH.
type SearchCondition struct {
	Field string
	Op    string // =, !=, >, <, >=, <=, CONTAINS, STARTSWITH
	Value string
}

// SearchResult holds a matching key and its hash fields.
type SearchResult struct {
	Key    string
	Fields map[string]string
}

// HSearch scans all hash keys matching pattern and returns those whose fields
// satisfy every condition. Results are sorted by key and paginated via offset/count.
func (s *Store) HSearch(pattern string, conditions []SearchCondition, offset, count int) []SearchResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()

	var results []SearchResult

	for key, hash := range s.hashes {
		if s.isExpiredRLocked(key, now) {
			continue
		}
		if !MatchGlob(pattern, key) {
			continue
		}
		if matchesConditions(hash, conditions) {
			result := SearchResult{Key: key, Fields: make(map[string]string, len(hash))}
			for f, v := range hash {
				result.Fields[f] = v
			}
			results = append(results, result)
		}
	}

	// Sort by key for deterministic results
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	// Apply LIMIT
	if offset > 0 {
		if offset >= len(results) {
			return nil
		}
		results = results[offset:]
	}
	if count > 0 && count < len(results) {
		results = results[:count]
	}

	return results
}

func matchesConditions(hash map[string]string, conditions []SearchCondition) bool {
	for _, cond := range conditions {
		value, exists := hash[cond.Field]
		if !exists {
			return false // field doesn't exist, condition fails
		}
		if !evalCondition(value, cond.Op, cond.Value) {
			return false
		}
	}
	return true
}

func evalCondition(fieldValue, op, condValue string) bool {
	switch strings.ToUpper(op) {
	case "=", "==":
		return fieldValue == condValue
	case "!=":
		return fieldValue != condValue
	case "CONTAINS":
		return strings.Contains(strings.ToLower(fieldValue), strings.ToLower(condValue))
	case "STARTSWITH":
		return strings.HasPrefix(strings.ToLower(fieldValue), strings.ToLower(condValue))
	case ">", "<", ">=", "<=":
		// Try numeric comparison first
		fv, errF := strconv.ParseFloat(fieldValue, 64)
		cv, errC := strconv.ParseFloat(condValue, 64)
		if errF == nil && errC == nil {
			switch op {
			case ">":
				return fv > cv
			case "<":
				return fv < cv
			case ">=":
				return fv >= cv
			case "<=":
				return fv <= cv
			}
		}
		// Fall back to string comparison
		switch op {
		case ">":
			return fieldValue > condValue
		case "<":
			return fieldValue < condValue
		case ">=":
			return fieldValue >= condValue
		case "<=":
			return fieldValue <= condValue
		}
	}
	return false
}

// MatchGlob performs glob-style pattern matching against a string.
// It supports *, ?, [chars], [^chars], [a-z] ranges, and \\ escaping.
func MatchGlob(pattern, str string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Skip consecutive stars
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			// Try matching rest of pattern at every position
			for i := 0; i <= len(str); i++ {
				if MatchGlob(pattern, str[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(str) == 0 {
				return false
			}
			pattern = pattern[1:]
			str = str[1:]
		case '[':
			if len(str) == 0 {
				return false
			}
			var ok bool
			pattern, ok = matchBracket(pattern[1:], str[0])
			if !ok {
				return false
			}
			str = str[1:]
		case '\\':
			pattern = pattern[1:]
			if len(pattern) == 0 {
				return false
			}
			if len(str) == 0 || pattern[0] != str[0] {
				return false
			}
			pattern = pattern[1:]
			str = str[1:]
		default:
			if len(str) == 0 || pattern[0] != str[0] {
				return false
			}
			pattern = pattern[1:]
			str = str[1:]
		}
	}
	return len(str) == 0
}

func matchBracket(pattern string, ch byte) (string, bool) {
	negate := false
	if len(pattern) > 0 && (pattern[0] == '^' || pattern[0] == '!') {
		negate = true
		pattern = pattern[1:]
	}
	matched := false
	i := 0
	for i < len(pattern) {
		if pattern[i] == ']' && i > 0 {
			break
		}
		if pattern[i] == '\\' && i+1 < len(pattern) {
			i++
			if pattern[i] == ch {
				matched = true
			}
			i++
			continue
		}
		if i+2 < len(pattern) && pattern[i+1] == '-' {
			lo := pattern[i]
			hi := pattern[i+2]
			if ch >= lo && ch <= hi {
				matched = true
			}
			i += 3
			continue
		}
		if pattern[i] == ch {
			matched = true
		}
		i++
	}
	if i >= len(pattern) {
		// No closing bracket found
		return "", false
	}
	if negate {
		matched = !matched
	}
	if !matched {
		return "", false
	}
	return pattern[i+1:], true
}

func (s *Store) expiredKeyLocked(key string, now time.Time) bool {
	expireAt, ok := s.expires[key]
	if !ok {
		return false
	}
	if expireAt.IsZero() || now.Before(expireAt) {
		return false
	}
	s.deleteKeyLocked(key)
	return true
}

func (s *Store) purgeExpiredLocked(now time.Time) {
	for key := range s.expires {
		s.expiredKeyLocked(key, now)
	}
}

// Sort reads elements from a list, set, or sorted set, sorts them, and
// optionally stores the result in a destination list.
func (s *Store) Sort(key string, alpha, desc bool, offset, count int, storeKey string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		if storeKey != "" {
			delete(s.lists, storeKey)
			s.touchKeyLocked(storeKey)
		}
		return nil, nil
	}

	var elements []string
	if d := s.lists[key]; d != nil {
		elements = d.ToSlice()
	} else if set := s.sets[key]; set != nil {
		elements = make([]string, 0, len(set))
		for m := range set {
			elements = append(elements, m)
		}
	} else if zs := s.zsets[key]; zs != nil {
		members := zs.Members()
		elements = make([]string, 0, len(members))
		for _, m := range members {
			elements = append(elements, m.Member)
		}
	} else if _, ok := s.kv[key]; ok {
		return nil, errors.New("wrong type")
	} else {
		if storeKey != "" {
			s.deleteKeyLocked(storeKey)
			s.touchKeyLocked(storeKey)
		}
		return nil, nil
	}

	if alpha {
		sort.Strings(elements)
	} else {
		sort.SliceStable(elements, func(i, j int) bool {
			a, errA := strconv.ParseFloat(elements[i], 64)
			b, errB := strconv.ParseFloat(elements[j], 64)
			if errA != nil || errB != nil {
				return elements[i] < elements[j]
			}
			return a < b
		})
	}

	if desc {
		for i, j := 0, len(elements)-1; i < j; i, j = i+1, j-1 {
			elements[i], elements[j] = elements[j], elements[i]
		}
	}

	if offset > 0 || count > 0 {
		if offset >= len(elements) {
			elements = nil
		} else {
			elements = elements[offset:]
			if count > 0 && count < len(elements) {
				elements = elements[:count]
			}
		}
	}

	if storeKey != "" {
		s.deleteKeyLocked(storeKey)
		if len(elements) > 0 {
			d := &Deque{}
			d.PushBack(elements...)
			s.lists[storeKey] = d
		}
		s.touchKeyLocked(storeKey)
	}

	return elements, nil
}

// PFAdd adds elements to the HyperLogLog at key. Returns true if the internal state changed.
func (s *Store) PFAdd(key string, elements ...string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(key, now)
	// Check for conflicting types
	if _, ok := s.kv[key]; ok {
		return false, errors.New("wrong type")
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil {
		return false, errors.New("wrong type")
	}

	hll := s.hlls[key]
	if hll == nil {
		hll = NewHyperLogLog()
		s.hlls[key] = hll
	}
	changed := false
	for _, elem := range elements {
		if hll.Add(elem) {
			changed = true
		}
	}
	s.touchKeyLocked(key)
	return changed, nil
}

// PFCount returns the estimated cardinality of the HyperLogLog(s).
func (s *Store) PFCount(keys ...string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if len(keys) == 1 {
		key := keys[0]
		if s.isExpiredRLocked(key, now) {
			return 0, nil
		}
		hll := s.hlls[key]
		if hll == nil {
			return 0, nil
		}
		return hll.Count(), nil
	}
	// Multiple keys: merge into temp HLL and count
	merged := NewHyperLogLog()
	for _, key := range keys {
		if s.isExpiredRLocked(key, now) {
			continue
		}
		hll := s.hlls[key]
		if hll != nil {
			merged.Merge(hll)
		}
	}
	return merged.Count(), nil
}

// PFMerge merges one or more HyperLogLogs into a destination key.
func (s *Store) PFMerge(destKey string, srcKeys ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(destKey, now)

	dest := s.hlls[destKey]
	if dest == nil {
		dest = NewHyperLogLog()
		s.hlls[destKey] = dest
	}

	for _, key := range srcKeys {
		s.expiredKeyLocked(key, now)
		if hll := s.hlls[key]; hll != nil {
			dest.Merge(hll)
		}
	}
	s.touchKeyLocked(destKey)
	return nil
}

func (s *Store) deleteKeyLocked(key string) {
	delete(s.kv, key)
	delete(s.hashes, key)
	delete(s.lists, key)
	delete(s.sets, key)
	delete(s.zsets, key)
	delete(s.streams, key)
	delete(s.hlls, key)
	delete(s.jsons, key)
	delete(s.groups, key)
	delete(s.lastSeq, key)
	delete(s.expires, key)
	delete(s.rateLimiters, key)
	delete(s.notBefore, key)
	delete(s.tags, key)
	delete(s.timeseries, key)
	delete(s.queues, key)
	s.accessMu.Lock()
	delete(s.accessTime, key)
	s.accessMu.Unlock()
	// Fire "del" before touchKeyLocked which would fire "set".
	if s.onChange != nil {
		s.onChange("del", key, "")
	}
	s.touchKeyLockedNoEvent(key)
}

// touchKeyLockedNoEvent is like touchKeyLocked but does not fire the
// onChange callback. Used by deleteKeyLocked which fires its own event.
func (s *Store) touchKeyLockedNoEvent(keys ...string) {
	s.version++
	now := time.Now().UnixNano()
	for _, key := range keys {
		s.keyVersion[key] = s.version
	}
	if s.maxMemory > 0 {
		s.accessMu.Lock()
		for _, key := range keys {
			s.accessTime[key] = now
		}
		s.accessMu.Unlock()
	}
}

func normalizeRange(start, stop, length int) (int, int, bool) {
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length || stop < 0 {
		return 0, 0, false
	}
	return start, stop, true
}

func inRange(id, start, end string) bool {
	if start != "-" && compareID(id, start) < 0 {
		return false
	}
	if end != "+" && compareID(id, end) > 0 {
		return false
	}
	return true
}

func compareID(a, b string) int {
	if b == "$" {
		return 1
	}
	ap := parseID(a)
	bp := parseID(b)
	if ap[0] != bp[0] {
		if ap[0] < bp[0] {
			return -1
		}
		return 1
	}
	if ap[1] < bp[1] {
		return -1
	}
	if ap[1] > bp[1] {
		return 1
	}
	return 0
}

func parseID(id string) [2]int64 {
	if id == "-" {
		return [2]int64{0, 0}
	}
	if id == "+" || id == "$" {
		return [2]int64{1<<62 - 1, 1<<62 - 1}
	}
	parts := strings.SplitN(id, "-", 2)
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	var seq int64
	if len(parts) == 2 {
		seq, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	return [2]int64{ms, seq}
}

func (s *Store) lastIDLocked(stream string) string {
	entries := s.streams[stream]
	if len(entries) == 0 {
		return "0-0"
	}
	return entries[len(entries)-1].ID
}

func cloneEntry(entry StreamEntry) StreamEntry {
	return StreamEntry{ID: entry.ID, Fields: append([]string(nil), entry.Fields...)}
}

func SortStreamNames(m map[string][]StreamEntry) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// LIndex returns the element at index in the list stored at key.
func (s *Store) LIndex(key string, index int) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return "", false, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return "", false, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil || d.Len() == 0 {
		return "", false, nil
	}
	if index < 0 {
		index = d.Len() + index
	}
	if index < 0 || index >= d.Len() {
		return "", false, nil
	}
	return d.Get(index), true, nil
}

// LSet sets the element at index in the list stored at key.
func (s *Store) LSet(key string, index int, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return errors.New("no such key")
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil || d.Len() == 0 {
		return errors.New("no such key")
	}
	if index < 0 {
		index = d.Len() + index
	}
	if index < 0 || index >= d.Len() {
		return errors.New("index out of range")
	}
	d.Set(index, value)
	s.touchKeyLocked(key)
	return nil
}

// LInsert inserts value before or after pivot in the list stored at key.
// Returns the new length of the list, or -1 if pivot was not found.
func (s *Store) LInsert(key string, before bool, pivot, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return 0, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil || d.Len() == 0 {
		return 0, nil
	}
	// Find pivot
	idx := -1
	for i := 0; i < d.Len(); i++ {
		if d.Get(i) == pivot {
			idx = i
			break
		}
	}
	if idx == -1 {
		return -1, nil
	}
	if !before {
		idx++
	}
	d.Insert(idx, value)
	s.touchKeyLocked(key)
	return d.Len(), nil
}

// LPos finds the first occurrence of element in the list stored at key.
func (s *Store) LPos(key, element string) (int64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, false, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, false, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil || d.Len() == 0 {
		return 0, false, nil
	}
	for i := 0; i < d.Len(); i++ {
		if d.Get(i) == element {
			return int64(i), true, nil
		}
	}
	return 0, false, nil
}

// SRandMember returns random members from a set.
// If count > 0, returns up to count distinct members.
// If count < 0, returns abs(count) members allowing duplicates.
func (s *Store) SRandMember(key string, count int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	set := s.sets[key]
	if set == nil || len(set) == 0 {
		return nil, nil
	}
	if count == 0 {
		return nil, nil
	}

	allowDupes := count < 0
	if count < 0 {
		count = -count
	}

	members := make([]string, 0, len(set))
	for m := range set {
		members = append(members, m)
	}

	if !allowDupes {
		if count >= len(members) {
			sort.Strings(members)
			return members, nil
		}
		// Fisher-Yates shuffle and take first count
		for i := len(members) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			members[i], members[j] = members[j], members[i]
		}
		return members[:count], nil
	}
	// Allow duplicates
	result := make([]string, count)
	for i := range result {
		result[i] = members[rand.Intn(len(members))]
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Bitmap operations
// ---------------------------------------------------------------------------

func popcount(b byte) int {
	count := 0
	for b != 0 {
		count += int(b & 1)
		b >>= 1
	}
	return count
}

func (s *Store) SetBit(key string, offset int64, value int) (int, error) {
	if offset < 0 {
		return 0, errors.New("bit offset is not an integer or out of range")
	}
	if value != 0 && value != 1 {
		return 0, errors.New("bit is not an integer or out of range")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(key, now)
	// type check - must be string or non-existent
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	byteIndex := int(offset / 8)
	bitIndex := uint(7 - offset%8) // Redis uses big-endian bit order

	v := s.kv[key]
	buf := []byte(v.Data)
	// Extend if needed
	if byteIndex >= len(buf) {
		extended := make([]byte, byteIndex+1)
		copy(extended, buf)
		buf = extended
	}

	// Get old bit
	oldBit := int((buf[byteIndex] >> bitIndex) & 1)

	// Set new bit
	if value == 1 {
		buf[byteIndex] |= 1 << bitIndex
	} else {
		buf[byteIndex] &^= 1 << bitIndex
	}

	s.kv[key] = Value{Data: string(buf)}
	s.touchKeyLocked(key)
	return oldBit, nil
}

func (s *Store) GetBit(key string, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("bit offset is not an integer or out of range")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	v, ok := s.kv[key]
	if !ok {
		return 0, nil
	}
	byteIndex := int(offset / 8)
	bitIndex := uint(7 - offset%8)
	if byteIndex >= len(v.Data) {
		return 0, nil
	}
	return int((v.Data[byteIndex] >> bitIndex) & 1), nil
}

func (s *Store) BitCount(key string, start, end int, hasBounds bool) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	v, ok := s.kv[key]
	if !ok {
		return 0, nil
	}
	data := []byte(v.Data)
	n := len(data)
	if !hasBounds {
		start = 0
		end = n - 1
	}
	// Normalize negative indices
	if start < 0 {
		start = n + start
	}
	if end < 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end {
		return 0, nil
	}

	var count int64
	for i := start; i <= end; i++ {
		count += int64(popcount(data[i]))
	}
	return count, nil
}

func (s *Store) BitOp(op, destKey string, keys []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()

	op = strings.ToUpper(op)
	if op == "NOT" && len(keys) != 1 {
		return 0, errors.New("BITOP NOT requires one and only one key")
	}

	// Read all source values
	bufs := make([][]byte, len(keys))
	maxLen := 0
	for i, key := range keys {
		s.expiredKeyLocked(key, now)
		if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
			return 0, errors.New("wrong type")
		}
		if v, ok := s.kv[key]; ok {
			bufs[i] = []byte(v.Data)
		}
		if len(bufs[i]) > maxLen {
			maxLen = len(bufs[i])
		}
	}

	result := make([]byte, maxLen)
	switch op {
	case "AND":
		for i := range result {
			result[i] = 0xFF
		}
		for _, buf := range bufs {
			for i := 0; i < maxLen; i++ {
				var b byte
				if i < len(buf) {
					b = buf[i]
				}
				result[i] &= b
			}
		}
	case "OR":
		for _, buf := range bufs {
			for i := 0; i < len(buf); i++ {
				result[i] |= buf[i]
			}
		}
	case "XOR":
		for _, buf := range bufs {
			for i := 0; i < len(buf); i++ {
				result[i] ^= buf[i]
			}
		}
	case "NOT":
		for i := 0; i < maxLen; i++ {
			result[i] = ^bufs[0][i]
		}
	default:
		return 0, fmt.Errorf("unsupported BITOP operation %q", op)
	}

	s.deleteKeyLocked(destKey)
	s.kv[destKey] = Value{Data: string(result)}
	s.touchKeyLocked(destKey)
	return maxLen, nil
}

func (s *Store) BitPos(key string, bit int, start, end int, hasBounds bool) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		if bit == 0 {
			return 0, nil
		}
		return -1, nil
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	v, ok := s.kv[key]
	if !ok {
		if bit == 0 {
			return 0, nil
		}
		return -1, nil
	}
	data := []byte(v.Data)
	n := len(data)
	if !hasBounds {
		start = 0
		end = n - 1
	}
	if start < 0 {
		start = n + start
	}
	if end < 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if start > end {
		return -1, nil
	}

	for i := start; i <= end; i++ {
		for j := 7; j >= 0; j-- {
			b := int((data[i] >> uint(j)) & 1)
			if b == bit {
				return int64(i*8 + (7 - j)), nil
			}
		}
	}
	// If looking for 0 and no bounds given, the first 0 bit would be after the string
	if bit == 0 && !hasBounds {
		return int64(n * 8), nil
	}
	return -1, nil
}

// ---------------------------------------------------------------------------
// Eviction support
// ---------------------------------------------------------------------------

// SetMaxMemory configures the maximum memory limit in bytes. 0 = unlimited.
func (s *Store) SetMaxMemory(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxMemory = bytes
	s.memEstimate = 0
	s.opsSinceEstimate = 0
}

// SetEvictPolicy configures the eviction policy.
// Supported: noeviction, allkeys-random, volatile-random, allkeys-lru.
func (s *Store) SetEvictPolicy(policy string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictPolicy = policy
}

// GetMaxMemory returns the configured max memory limit.
func (s *Store) GetMaxMemory() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxMemory
}

// GetEvictPolicy returns the configured eviction policy.
func (s *Store) GetEvictPolicy() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.evictPolicy == "" {
		return "noeviction"
	}
	return s.evictPolicy
}

// ApproxMemory returns the approximate memory usage in bytes.
func (s *Store) ApproxMemory() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.estimateMemoryLocked()
}

// NeedsEviction returns true if a maxMemory limit is configured.
func (s *Store) NeedsEviction() bool {
	return s.maxMemory > 0
}

// EvictIfNeeded checks if eviction is required and evicts keys.
// Returns error if policy is noeviction and limit is exceeded.
// Called before write operations.
func (s *Store) EvictIfNeeded() error {
	if s.maxMemory <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.estimateMemoryLocked() > s.maxMemory {
		switch s.evictPolicy {
		case "noeviction", "":
			return errors.New("OOM command not allowed when used memory > 'maxmemory'")
		case "allkeys-random":
			if !s.evictRandomKeyLocked(false) {
				return nil // no keys to evict
			}
		case "volatile-random":
			if !s.evictRandomKeyLocked(true) {
				// No volatile keys to evict; behave like noeviction.
				return errors.New("OOM command not allowed when used memory > 'maxmemory'")
			}
		case "allkeys-lru":
			if !s.evictLRUKeyLocked() {
				return nil
			}
		default:
			return nil
		}
	}
	return nil
}

// estimateMemoryLocked returns a rough estimate of memory usage.
// Results are cached and recomputed every 1000 operations.
// Must be called under write lock.
func (s *Store) estimateMemoryLocked() int64 {
	s.opsSinceEstimate++
	if s.opsSinceEstimate < 1000 && s.memEstimate > 0 {
		return s.memEstimate
	}
	s.opsSinceEstimate = 0

	var mem int64
	for k, v := range s.kv {
		mem += int64(len(k) + len(v.Data) + 64)
	}
	for k, h := range s.hashes {
		mem += int64(len(k) + 64)
		for f, v := range h {
			mem += int64(len(f) + len(v) + 32)
		}
	}
	for k, d := range s.lists {
		mem += int64(len(k) + 64 + d.Len()*48)
	}
	for k, set := range s.sets {
		mem += int64(len(k) + 64 + len(set)*48)
	}
	for k, zs := range s.zsets {
		mem += int64(len(k) + 64 + zs.Len()*64)
	}
	for k, entries := range s.streams {
		mem += int64(len(k) + 64)
		for _, e := range entries {
			mem += int64(len(e.ID) + 48)
			for _, f := range e.Fields {
				mem += int64(len(f) + 16)
			}
		}
	}
	for k := range s.hlls {
		mem += int64(len(k) + hllRegisters + 64)
	}
	s.memEstimate = mem
	return mem
}

// evictRandomKeyLocked evicts a random key.
// If onlyVolatile is true, only keys with an expiry are considered.
// Returns true if a key was evicted.
func (s *Store) evictRandomKeyLocked(onlyVolatile bool) bool {
	if onlyVolatile {
		for key := range s.expires {
			s.deleteKeyLocked(key)
			s.memEstimate = 0
			return true
		}
		return false
	}
	// Pick from any data-type map; Go map iteration order is random.
	for key := range s.kv {
		s.deleteKeyLocked(key)
		s.memEstimate = 0
		return true
	}
	for key := range s.hashes {
		s.deleteKeyLocked(key)
		s.memEstimate = 0
		return true
	}
	for key := range s.lists {
		s.deleteKeyLocked(key)
		s.memEstimate = 0
		return true
	}
	for key := range s.sets {
		s.deleteKeyLocked(key)
		s.memEstimate = 0
		return true
	}
	for key := range s.zsets {
		s.deleteKeyLocked(key)
		s.memEstimate = 0
		return true
	}
	for key := range s.streams {
		s.deleteKeyLocked(key)
		s.memEstimate = 0
		return true
	}
	for key := range s.hlls {
		s.deleteKeyLocked(key)
		s.memEstimate = 0
		return true
	}
	return false
}

// evictLRUKeyLocked evicts the least-recently-used key using approximate LRU.
// It samples up to 5 keys and evicts the one with the oldest access time.
// Returns true if a key was evicted.
func (s *Store) evictLRUKeyLocked() bool {
	s.accessMu.Lock()
	var oldest string
	var oldestTime int64 = math.MaxInt64
	sampled := 0
	for key, t := range s.accessTime {
		if t < oldestTime {
			oldest = key
			oldestTime = t
		}
		sampled++
		if sampled >= 5 {
			break
		}
	}
	s.accessMu.Unlock()

	if oldest == "" {
		// No access time tracked; fall back to random eviction.
		return s.evictRandomKeyLocked(false)
	}
	s.deleteKeyLocked(oldest)
	s.accessMu.Lock()
	delete(s.accessTime, oldest)
	s.accessMu.Unlock()
	s.memEstimate = 0
	return true
}

// ---------------------------------------------------------------------------
// Rate Limiting — sliding window algorithm
// ---------------------------------------------------------------------------

// RateLimit checks whether a request is allowed under the sliding window for
// the given key. It records the request timestamp if allowed.
func (s *Store) RateLimit(key string, max int64, window time.Duration) (allowed bool, remaining int64, retryAfterMs int64, resetAtMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()

	rl := s.rateLimiters[key]
	if rl == nil {
		rl = &RateLimiter{Window: window, Max: max}
		s.rateLimiters[key] = rl
	}

	// Update window and max in case they changed
	rl.Window = window
	rl.Max = max

	// Remove expired timestamps (outside the window)
	cutoff := now.Add(-window)
	valid := rl.Requests[:0]
	for _, t := range rl.Requests {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}
	rl.Requests = valid

	remaining = max - int64(len(rl.Requests))

	// Calculate reset time
	if len(rl.Requests) > 0 {
		resetAt := rl.Requests[0].Add(window)
		resetAtMs = resetAt.UnixMilli()
	} else {
		resetAtMs = now.Add(window).UnixMilli()
	}

	if int64(len(rl.Requests)) >= max {
		// Rate limited
		retryAfter := rl.Requests[0].Add(window).Sub(now)
		if retryAfter < 0 {
			retryAfter = 0
		}
		return false, 0, retryAfter.Milliseconds(), resetAtMs
	}

	// Allow request
	rl.Requests = append(rl.Requests, now)
	remaining = max - int64(len(rl.Requests))
	s.touchKeyLocked(key)
	return true, remaining, 0, resetAtMs
}

// RateLimitPeek checks the current rate limit state without consuming a request.
func (s *Store) RateLimitPeek(key string) (remaining int64, resetAtMs int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	rl := s.rateLimiters[key]
	if rl == nil {
		return -1, 0
	}
	cutoff := now.Add(-rl.Window)
	count := int64(0)
	for _, t := range rl.Requests {
		if t.After(cutoff) {
			count++
		}
	}
	remaining = rl.Max - count
	if len(rl.Requests) > 0 {
		resetAtMs = rl.Requests[0].Add(rl.Window).UnixMilli()
	}
	return remaining, resetAtMs
}

// RateLimitReset clears a rate limiter for the given key.
func (s *Store) RateLimitReset(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rateLimiters[key]; !ok {
		return false
	}
	delete(s.rateLimiters, key)
	return true
}

// ---------------------------------------------------------------------------
// Delayed / Scheduled Keys
// ---------------------------------------------------------------------------

// SetDelayed stores a key that becomes visible only after the given delay.
// If ttl > 0 the key will expire ttl after it becomes visible.
func (s *Store) SetDelayed(key, value string, delay time.Duration, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.kv[key] = Value{Data: value}
	s.notBefore[key] = now.Add(delay)
	if ttl > 0 {
		s.expires[key] = now.Add(delay).Add(ttl) // TTL starts after visibility
	} else {
		delete(s.expires, key)
	}
	// Clean other types
	delete(s.hashes, key)
	delete(s.lists, key)
	delete(s.sets, key)
	delete(s.zsets, key)
	delete(s.streams, key)
	delete(s.lastSeq, key)
	delete(s.hlls, key)
	delete(s.jsons, key)
	s.touchKeyLocked(key)
}

// ---------------------------------------------------------------------------
// JSON document methods
// ---------------------------------------------------------------------------

func (s *Store) JSONSet(key, path, raw string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(key, now)

	// Type check — only json or non-existent
	if _, ok := s.kv[key]; ok {
		return errors.New("wrong type")
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil {
		return errors.New("wrong type")
	}

	var value interface{}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return err
	}

	doc := s.jsons[key]
	if doc == nil {
		if path != "$" && path != "." && path != "" {
			return errors.New("new doc must use root path $")
		}
		doc = &JSONDoc{data: value}
		s.jsons[key] = doc
	} else {
		if err := doc.Set(path, value); err != nil {
			return err
		}
	}
	s.touchKeyLocked(key)
	return nil
}

func (s *Store) JSONGet(key, path string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return "", false, nil
	}
	doc := s.jsons[key]
	if doc == nil {
		return "", false, nil
	}
	val, err := doc.Get(path)
	if err != nil {
		return "", false, err
	}
	b, err := json.Marshal(val)
	if err != nil {
		return "", false, err
	}
	return string(b), true, nil
}

func (s *Store) JSONDel(key, path string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return false, nil
	}
	doc := s.jsons[key]
	if doc == nil {
		return false, nil
	}
	if path == "$" || path == "" {
		delete(s.jsons, key)
		s.touchKeyLocked(key)
		return true, nil
	}
	if err := doc.Del(path); err != nil {
		return false, err
	}
	s.touchKeyLocked(key)
	return true, nil
}

func (s *Store) JSONType(key, path string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return "null", nil
	}
	doc := s.jsons[key]
	if doc == nil {
		return "null", nil
	}
	return doc.Type(path), nil
}

func (s *Store) JSONNumIncrBy(key, path string, delta float64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return 0, errors.New("key not found")
	}
	doc := s.jsons[key]
	if doc == nil {
		return 0, errors.New("key not found")
	}
	result, err := doc.NumIncrBy(path, delta)
	if err != nil {
		return 0, err
	}
	s.touchKeyLocked(key)
	return result, nil
}

func (s *Store) JSONArrAppend(key, path string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return 0, errors.New("key not found")
	}
	doc := s.jsons[key]
	if doc == nil {
		return 0, errors.New("key not found")
	}
	parsed := make([]interface{}, len(values))
	for i, v := range values {
		var val interface{}
		if err := json.Unmarshal([]byte(v), &val); err != nil {
			return 0, err
		}
		parsed[i] = val
	}
	n, err := doc.ArrAppend(path, parsed...)
	if err != nil {
		return 0, err
	}
	s.touchKeyLocked(key)
	return n, nil
}

func (s *Store) JSONArrLen(key, path string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, errors.New("key not found")
	}
	doc := s.jsons[key]
	if doc == nil {
		return 0, errors.New("key not found")
	}
	return doc.ArrLen(path)
}

func (s *Store) JSONKeys(key, path string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, errors.New("key not found")
	}
	doc := s.jsons[key]
	if doc == nil {
		return nil, errors.New("key not found")
	}
	return doc.Keys(path)
}

// ---------------------------------------------------------------------------
// Key Tagging / Metadata
// ---------------------------------------------------------------------------

func (s *Store) TagSet(key string, tags map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(key, now) {
		return errors.New("key not found")
	}
	if !s.existsLocked(key) {
		return errors.New("key not found")
	}
	if s.tags[key] == nil {
		s.tags[key] = make(map[string]string)
	}
	for k, v := range tags {
		s.tags[key][k] = v
	}
	return nil
}

func (s *Store) TagGet(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil
	}
	t := s.tags[key]
	if t == nil {
		return nil
	}
	out := make(map[string]string, len(t))
	for k, v := range t {
		out[k] = v
	}
	return out
}

func (s *Store) TagDel(key string, fields []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tags[key]
	if t == nil {
		return 0
	}
	n := 0
	for _, f := range fields {
		if _, ok := t[f]; ok {
			delete(t, f)
			n++
		}
	}
	if len(t) == 0 {
		delete(s.tags, key)
	}
	return n
}

// TagQuery finds keys matching tag criteria. Each criterion is "field=value".
func (s *Store) TagQuery(criteria map[string]string, limit int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if limit <= 0 {
		limit = 100
	}
	var results []string
	for key, tags := range s.tags {
		if s.isExpiredRLocked(key, now) {
			continue
		}
		match := true
		for field, value := range criteria {
			if tags[field] != value {
				match = false
				break
			}
		}
		if match {
			results = append(results, key)
			if len(results) >= limit {
				break
			}
		}
	}
	sort.Strings(results)
	return results
}

// ---------------------------------------------------------------------------
// Time-Series Data
// ---------------------------------------------------------------------------

func (s *Store) TSAdd(key string, timestamp int64, value float64, labels map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(key, now)
	// Type check
	if _, ok := s.kv[key]; ok {
		return errors.New("wrong type")
	}
	if s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil {
		return errors.New("wrong type")
	}
	ts := s.timeseries[key]
	if ts == nil {
		ts = NewTimeSeries()
		s.timeseries[key] = ts
	}
	if timestamp == 0 {
		timestamp = now.UnixMilli()
	}
	ts.Add(timestamp, value)
	for k, v := range labels {
		ts.Labels[k] = v
	}
	s.touchKeyLocked(key)
	return nil
}

func (s *Store) TSRange(key string, from, to int64, count int) ([]TSSample, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	ts := s.timeseries[key]
	if ts == nil {
		return nil, nil
	}
	return ts.Range(from, to, count), nil
}

func (s *Store) TSGet(key string) (TSSample, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return TSSample{}, false, nil
	}
	ts := s.timeseries[key]
	if ts == nil {
		return TSSample{}, false, nil
	}
	sample, ok := ts.Last()
	return sample, ok, nil
}

func (s *Store) TSInfo(key string) (int, map[string]string, int64, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return 0, nil, 0, 0, errors.New("key not found")
	}
	ts := s.timeseries[key]
	if ts == nil {
		return 0, nil, 0, 0, errors.New("key not found")
	}
	labels := make(map[string]string, len(ts.Labels))
	for k, v := range ts.Labels {
		labels[k] = v
	}
	var firstTS, lastTS int64
	if len(ts.Samples) > 0 {
		firstTS = ts.Samples[0].Timestamp
		lastTS = ts.Samples[len(ts.Samples)-1].Timestamp
	}
	return ts.Len(), labels, firstTS, lastTS, nil
}

func (s *Store) TSDownsample(srcKey, dstKey, aggType string, from, to, bucketSize int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expiredKeyLocked(srcKey, now) {
		return 0, errors.New("source key not found")
	}
	src := s.timeseries[srcKey]
	if src == nil {
		return 0, errors.New("source key not found")
	}

	results := src.Downsample(from, to, bucketSize, aggType)

	dst := NewTimeSeries()
	for _, r := range results {
		dst.Add(r.Timestamp, r.Value)
	}
	s.deleteKeyLocked(dstKey)
	s.timeseries[dstKey] = dst
	s.touchKeyLocked(dstKey)
	return len(results), nil
}

// ---------------------------------------------------------------------------
// LRem removes count occurrences of element from the list at key.
// count > 0: remove first count occurrences (head to tail)
// count < 0: remove last |count| occurrences (tail to head)
// count == 0: remove all occurrences
// ---------------------------------------------------------------------------
func (s *Store) LRem(key string, count int, element string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return 0, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	d := s.lists[key]
	if d == nil || d.Len() == 0 {
		return 0, nil
	}

	items := d.ToSlice()
	var kept []string
	removed := 0

	if count > 0 {
		for _, item := range items {
			if item == element && removed < count {
				removed++
			} else {
				kept = append(kept, item)
			}
		}
	} else if count < 0 {
		absCount := -count
		removeIdx := make(map[int]bool)
		for i := len(items) - 1; i >= 0 && removed < absCount; i-- {
			if items[i] == element {
				removeIdx[i] = true
				removed++
			}
		}
		for i, item := range items {
			if !removeIdx[i] {
				kept = append(kept, item)
			}
		}
	} else {
		for _, item := range items {
			if item == element {
				removed++
			} else {
				kept = append(kept, item)
			}
		}
	}

	if len(kept) == 0 {
		delete(s.lists, key)
		delete(s.expires, key)
	} else {
		newD := &Deque{}
		newD.PushBack(kept...)
		s.lists[key] = newD
	}
	if removed > 0 {
		s.touchKeyLocked(key)
	}
	return removed, nil
}

// SMove moves member from src set to dst set. Returns true if moved.
func (s *Store) SMove(src, dst, member string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expiredKeyLocked(src, now)
	s.expiredKeyLocked(dst, now)

	// Type check src
	if s.existsStringLocked(src) || s.hashes[src] != nil || s.lists[src] != nil || s.zsets[src] != nil || s.streams[src] != nil || s.hlls[src] != nil || s.jsons[src] != nil || s.timeseries[src] != nil {
		return false, errors.New("wrong type")
	}
	// Type check dst
	if s.existsStringLocked(dst) || s.hashes[dst] != nil || s.lists[dst] != nil || s.zsets[dst] != nil || s.streams[dst] != nil || s.hlls[dst] != nil || s.jsons[dst] != nil || s.timeseries[dst] != nil {
		return false, errors.New("wrong type")
	}

	srcSet := s.sets[src]
	if srcSet == nil {
		return false, nil
	}
	if _, ok := srcSet[member]; !ok {
		return false, nil
	}
	delete(srcSet, member)
	if len(srcSet) == 0 {
		delete(s.sets, src)
		delete(s.expires, src)
	}
	if s.sets[dst] == nil {
		s.sets[dst] = make(map[string]struct{})
	}
	s.sets[dst][member] = struct{}{}
	s.touchKeyLocked(src)
	s.touchKeyLocked(dst)
	return true, nil
}

// SPop removes and returns up to count random members from the set at key.
func (s *Store) SPop(key string, count int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return nil, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	set := s.sets[key]
	if len(set) == 0 {
		return nil, nil
	}
	if count <= 0 {
		count = 1
	}
	var result []string
	for member := range set {
		if len(result) >= count {
			break
		}
		result = append(result, member)
		delete(set, member)
	}
	if len(set) == 0 {
		delete(s.sets, key)
		delete(s.expires, key)
	}
	s.touchKeyLocked(key)
	return result, nil
}

// ZIncrBy increments the score of member in the sorted set at key by incr.
// If the member does not exist, it is added with incr as the score.
func (s *Store) ZIncrBy(key string, incr float64, member string) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		// expired, treat as new
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return 0, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil {
		zs = NewSortedSet()
		s.zsets[key] = zs
	}
	score, exists := zs.Score(member)
	if exists {
		zs.Remove(member)
	}
	newScore := score + incr
	zs.Add(member, newScore)
	s.touchKeyLocked(key)
	return newScore, nil
}

// ZPopMin removes and returns the count lowest-scored members from sorted set at key.
func (s *Store) ZPopMin(key string, count int) ([]ZMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return nil, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil || zs.Len() == 0 {
		return nil, nil
	}
	result := zs.PopMin(count)
	if zs.Len() == 0 {
		delete(s.zsets, key)
		delete(s.expires, key)
	}
	s.touchKeyLocked(key)
	return result, nil
}

// ZPopMax removes and returns the count highest-scored members from sorted set at key.
func (s *Store) ZPopMax(key string, count int) ([]ZMember, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expiredKeyLocked(key, time.Now()) {
		return nil, nil
	}
	if s.existsStringLocked(key) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil || zs.Len() == 0 {
		return nil, nil
	}
	result := zs.PopMax(count)
	if zs.Len() == 0 {
		delete(s.zsets, key)
		delete(s.expires, key)
	}
	s.touchKeyLocked(key)
	return result, nil
}

// ZRangeByLex returns members in a sorted set by lexicographic range.
func (s *Store) ZRangeByLex(key, min, max string, offset, count int) ([]ZMember, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.hashes[key] != nil || s.lists[key] != nil || s.sets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	zs := s.zsets[key]
	if zs == nil || zs.Len() == 0 {
		return nil, nil
	}
	return zs.RangeByLex(min, max, offset, count), nil
}

// HRandField returns random fields from the hash at key.
// If count > 0, return up to count distinct fields.
// If count < 0, return |count| fields (possibly with duplicates).
func (s *Store) HRandField(key string, count int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return nil, nil
	}
	if s.existsStringRLocked(key, now) || s.lists[key] != nil || s.sets[key] != nil || s.zsets[key] != nil || s.streams[key] != nil || s.hlls[key] != nil || s.jsons[key] != nil || s.timeseries[key] != nil {
		return nil, errors.New("wrong type")
	}
	h := s.hashes[key]
	if h == nil || len(h) == 0 {
		return nil, nil
	}
	if count == 0 {
		return nil, nil
	}
	allowDupes := count < 0
	if count < 0 {
		count = -count
	}
	fields := make([]string, 0, len(h))
	for f := range h {
		fields = append(fields, f)
	}
	if !allowDupes {
		if count >= len(fields) {
			sort.Strings(fields)
			return fields, nil
		}
		for i := len(fields) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			fields[i], fields[j] = fields[j], fields[i]
		}
		return fields[:count], nil
	}
	result := make([]string, count)
	for i := range result {
		result[i] = fields[rand.Intn(len(fields))]
	}
	return result, nil
}

// DumpKey serializes a single key's value into a simple string representation.
func (s *Store) DumpKey(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	if s.isExpiredRLocked(key, now) {
		return "", false
	}
	if v, ok := s.kv[key]; ok {
		return "STRING:" + v.Data, true
	}
	if h := s.hashes[key]; h != nil {
		var sb strings.Builder
		sb.WriteString("HASH:")
		first := true
		for k, v := range h {
			if !first {
				sb.WriteByte(',')
			}
			sb.WriteString(k)
			sb.WriteByte('=')
			sb.WriteString(v)
			first = false
		}
		return sb.String(), true
	}
	if d := s.lists[key]; d != nil {
		items := d.ToSlice()
		return "LIST:" + strings.Join(items, ","), true
	}
	if set := s.sets[key]; set != nil {
		members := make([]string, 0, len(set))
		for m := range set {
			members = append(members, m)
		}
		sort.Strings(members)
		return "SET:" + strings.Join(members, ","), true
	}
	if zs := s.zsets[key]; zs != nil {
		members := zs.Members()
		var sb strings.Builder
		sb.WriteString("ZSET:")
		for i, m := range members {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(m.Member)
			sb.WriteByte('=')
			sb.WriteString(strconv.FormatFloat(m.Score, 'f', -1, 64))
		}
		return sb.String(), true
	}
	return "", false
}
