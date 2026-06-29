package server

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"vole/internal/store"
)

// NamespaceManager manages multiple named namespaces, each with its own Store.
type NamespaceManager struct {
	mu         sync.RWMutex
	namespaces map[string]*Namespace
}

// Namespace wraps a Store with a name, providing key isolation between namespaces.
type Namespace struct {
	Name    string
	Store   *store.Store
	Created time.Time
}

// NewNamespaceManager creates a manager with a "default" namespace pre-created.
func NewNamespaceManager(defaultStore *store.Store) *NamespaceManager {
	nm := &NamespaceManager{
		namespaces: make(map[string]*Namespace),
	}
	nm.namespaces["default"] = &Namespace{
		Name:    "default",
		Store:   defaultStore,
		Created: time.Now(),
	}
	return nm
}

// Get returns the namespace with the given name.
func (nm *NamespaceManager) Get(name string) (*Namespace, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	ns, ok := nm.namespaces[name]
	return ns, ok
}

// Create creates a new namespace with the given name.
func (nm *NamespaceManager) Create(name string) error {
	if name == "" {
		return errors.New("namespace name cannot be empty")
	}
	if strings.Contains(name, ":") {
		return errors.New("namespace name cannot contain ':'")
	}
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if _, ok := nm.namespaces[name]; ok {
		return errors.New("namespace already exists")
	}
	ns := &Namespace{
		Name:    name,
		Store:   store.New(),
		Created: time.Now(),
	}
	ns.Store.StartExpiry(500 * time.Millisecond)
	nm.namespaces[name] = ns
	return nil
}

// Drop removes the namespace with the given name. The "default" namespace cannot be dropped.
func (nm *NamespaceManager) Drop(name string) error {
	if name == "default" {
		return errors.New("cannot drop default namespace")
	}
	nm.mu.Lock()
	defer nm.mu.Unlock()
	ns, ok := nm.namespaces[name]
	if !ok {
		return errors.New("namespace does not exist")
	}
	ns.Store.StopExpiry()
	delete(nm.namespaces, name)
	return nil
}

// List returns all namespace names sorted alphabetically.
func (nm *NamespaceManager) List() []string {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	names := make([]string, 0, len(nm.namespaces))
	for name := range nm.namespaces {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Default returns the default namespace.
func (nm *NamespaceManager) Default() *Namespace {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return nm.namespaces["default"]
}

// StopAll stops background expiry on all non-default namespaces.
// The default namespace's expiry is managed by the server directly.
func (nm *NamespaceManager) StopAll() {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	for name, ns := range nm.namespaces {
		if name != "default" {
			ns.Store.StopExpiry()
		}
	}
}

// prefixKeyArgs returns a copy of args with key arguments prefixed for namespace isolation.
// When prefix is empty (default namespace), args are returned unchanged.
func prefixKeyArgs(args []string, prefix string) []string {
	if prefix == "" {
		return args
	}
	cmd := strings.ToUpper(args[0])
	out := make([]string, len(args))
	copy(out, args)

	switch cmd {
	// Commands where only the first argument (index 1) is a key
	case "GET", "SET", "SETNX", "SETEX", "PSETEX", "TYPE", "TTL", "PTTL",
		"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT",
		"PERSIST", "INCR", "INCRBY", "DECR", "DECRBY", "INCRBYFLOAT",
		"STRLEN", "APPEND", "GETDEL", "GETSET", "GETEX", "GETRANGE", "SETRANGE",
		"RATELIMIT", "RATELIMIT.PEEK", "RATELIMIT.RESET", "SETDELAYED",
		"HSET", "HGET", "HGETALL", "HDEL", "HLEN", "HEXISTS", "HKEYS", "HVALS",
		"HINCRBY", "HINCRBYFLOAT", "HSETNX", "HMSET", "HMGET",
		"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP",
		"LRANGE", "LLEN", "LINDEX", "LSET", "LINSERT", "LPOS",
		"SADD", "SREM", "SMEMBERS", "SISMEMBER", "SCARD", "SRANDMEMBER", "SPOP",
		"ZADD", "ZRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZREVRANGE",
		"ZREM", "ZSCORE", "ZCARD", "ZRANK", "ZREVRANK", "ZCOUNT",
		"ZINCRBY", "ZLEXCOUNT", "ZRANGEBYLEX",
		"XADD", "XRANGE", "XREVRANGE", "XLEN", "XREAD",
		"SETBIT", "GETBIT", "BITCOUNT", "BITPOS",
		"PFADD", "PFCOUNT",
		"GEOADD", "GEOPOS", "GEODIST", "GEOSEARCH",
		"OBJECT", "EXPIRETIME", "PEXPIRETIME", "TOUCH", "UNLINK",
		"SORT", "DUMP", "RESTORE", "DEBUG":
		if len(out) > 1 {
			out[1] = prefix + out[1]
		}

	// KEYS: prefix the pattern
	case "KEYS":
		if len(out) > 1 {
			out[1] = prefix + out[1]
		}

	// DEL, EXISTS, MGET: all args after command are keys
	case "DEL", "EXISTS", "MGET":
		for i := 1; i < len(out); i++ {
			out[i] = prefix + out[i]
		}

	// MSET, MSETNX: alternating key-value pairs
	case "MSET", "MSETNX":
		for i := 1; i < len(out); i += 2 {
			out[i] = prefix + out[i]
		}

	// Set operations with destination: all args are keys
	case "SINTER", "SUNION", "SDIFF", "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE":
		for i := 1; i < len(out); i++ {
			out[i] = prefix + out[i]
		}

	// RENAME, RENAMENX: both args are keys
	case "RENAME", "RENAMENX":
		if len(out) > 1 {
			out[1] = prefix + out[1]
		}
		if len(out) > 2 {
			out[2] = prefix + out[2]
		}

	// COPY: src and dest are keys
	case "COPY":
		if len(out) > 1 {
			out[1] = prefix + out[1]
		}
		if len(out) > 2 {
			out[2] = prefix + out[2]
		}

	// RPOPLPUSH, LMOVE, SMOVE: two key args
	case "RPOPLPUSH", "LMOVE", "SMOVE":
		if len(out) > 1 {
			out[1] = prefix + out[1]
		}
		if len(out) > 2 {
			out[2] = prefix + out[2]
		}

	// BLPOP, BRPOP: all args except the last (timeout) are keys
	case "BLPOP", "BRPOP":
		for i := 1; i < len(out)-1; i++ {
			out[i] = prefix + out[i]
		}

	// PFMERGE: all args are keys (dest + sources)
	case "PFMERGE":
		for i := 1; i < len(out); i++ {
			out[i] = prefix + out[i]
		}

	// BITOP: dest key + source keys (skip operation name at index 1)
	case "BITOP":
		for i := 2; i < len(out); i++ {
			out[i] = prefix + out[i]
		}

	// ZUNIONSTORE, ZINTERSTORE: dest key at 1, numkeys at 2, then keys
	// Already handled above in the set operations block
	}

	return out
}

// stripKeyPrefix removes the namespace prefix from a key string.
func stripKeyPrefix(key, prefix string) string {
	if prefix == "" {
		return key
	}
	return strings.TrimPrefix(key, prefix)
}
