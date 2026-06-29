package server

import (
	"reflect"
	"testing"

	"vole/internal/store"
)

func TestNamespaceManagerDefault(t *testing.T) {
	st := store.New()
	nm := NewNamespaceManager(st)

	ns := nm.Default()
	if ns == nil {
		t.Fatal("default namespace should exist")
	}
	if ns.Name != "default" {
		t.Fatalf("expected name 'default', got %q", ns.Name)
	}
	if ns.Store != st {
		t.Fatal("default namespace store should be the provided store")
	}
}

func TestNamespaceManagerCreateAndGet(t *testing.T) {
	nm := NewNamespaceManager(store.New())

	if err := nm.Create("analytics"); err != nil {
		t.Fatalf("create: %v", err)
	}

	ns, ok := nm.Get("analytics")
	if !ok {
		t.Fatal("expected namespace 'analytics' to exist")
	}
	if ns.Name != "analytics" {
		t.Fatalf("expected name 'analytics', got %q", ns.Name)
	}

	// Duplicate create should fail
	if err := nm.Create("analytics"); err == nil {
		t.Fatal("expected error for duplicate namespace")
	}

	// Empty name should fail
	if err := nm.Create(""); err == nil {
		t.Fatal("expected error for empty name")
	}

	// Name with colon should fail
	if err := nm.Create("bad:name"); err == nil {
		t.Fatal("expected error for name containing ':'")
	}
}

func TestNamespaceManagerDrop(t *testing.T) {
	nm := NewNamespaceManager(store.New())

	_ = nm.Create("temp")
	if err := nm.Drop("temp"); err != nil {
		t.Fatalf("drop: %v", err)
	}

	if _, ok := nm.Get("temp"); ok {
		t.Fatal("expected namespace 'temp' to be gone after drop")
	}

	// Drop non-existent
	if err := nm.Drop("nope"); err == nil {
		t.Fatal("expected error dropping non-existent namespace")
	}

	// Cannot drop default
	if err := nm.Drop("default"); err == nil {
		t.Fatal("expected error dropping default namespace")
	}
}

func TestNamespaceManagerList(t *testing.T) {
	nm := NewNamespaceManager(store.New())
	_ = nm.Create("beta")
	_ = nm.Create("alpha")

	names := nm.List()
	expected := []string{"alpha", "beta", "default"}
	if !reflect.DeepEqual(names, expected) {
		t.Fatalf("expected %v, got %v", expected, names)
	}
}

func TestPrefixKeyArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		prefix   string
		expected []string
	}{
		{
			name:     "empty prefix passthrough",
			args:     []string{"GET", "mykey"},
			prefix:   "",
			expected: []string{"GET", "mykey"},
		},
		{
			name:     "GET single key",
			args:     []string{"GET", "mykey"},
			prefix:   "ns1:",
			expected: []string{"GET", "ns1:mykey"},
		},
		{
			name:     "SET key value",
			args:     []string{"SET", "mykey", "myvalue"},
			prefix:   "ns1:",
			expected: []string{"SET", "ns1:mykey", "myvalue"},
		},
		{
			name:     "MGET multiple keys",
			args:     []string{"MGET", "k1", "k2", "k3"},
			prefix:   "ns1:",
			expected: []string{"MGET", "ns1:k1", "ns1:k2", "ns1:k3"},
		},
		{
			name:     "MSET alternating key-value",
			args:     []string{"MSET", "k1", "v1", "k2", "v2"},
			prefix:   "ns1:",
			expected: []string{"MSET", "ns1:k1", "v1", "ns1:k2", "v2"},
		},
		{
			name:     "DEL multiple keys",
			args:     []string{"DEL", "k1", "k2"},
			prefix:   "ns1:",
			expected: []string{"DEL", "ns1:k1", "ns1:k2"},
		},
		{
			name:     "RENAME both keys",
			args:     []string{"RENAME", "old", "new"},
			prefix:   "ns1:",
			expected: []string{"RENAME", "ns1:old", "ns1:new"},
		},
		{
			name:     "KEYS pattern",
			args:     []string{"KEYS", "*"},
			prefix:   "ns1:",
			expected: []string{"KEYS", "ns1:*"},
		},
		{
			name:     "HSET hash key",
			args:     []string{"HSET", "myhash", "field1", "value1"},
			prefix:   "ns1:",
			expected: []string{"HSET", "ns1:myhash", "field1", "value1"},
		},
		{
			name:     "LPUSH list key",
			args:     []string{"LPUSH", "mylist", "a", "b"},
			prefix:   "ns1:",
			expected: []string{"LPUSH", "ns1:mylist", "a", "b"},
		},
		{
			name:     "SADD set key",
			args:     []string{"SADD", "myset", "a"},
			prefix:   "ns1:",
			expected: []string{"SADD", "ns1:myset", "a"},
		},
		{
			name:     "SINTER multiple set keys",
			args:     []string{"SINTER", "s1", "s2"},
			prefix:   "ns1:",
			expected: []string{"SINTER", "ns1:s1", "ns1:s2"},
		},
		{
			name:     "ZADD sorted set key",
			args:     []string{"ZADD", "myzset", "1", "member"},
			prefix:   "ns1:",
			expected: []string{"ZADD", "ns1:myzset", "1", "member"},
		},
		{
			name:     "BLPOP keys with timeout",
			args:     []string{"BLPOP", "k1", "k2", "5"},
			prefix:   "ns1:",
			expected: []string{"BLPOP", "ns1:k1", "ns1:k2", "5"},
		},
		{
			name:     "PING no keys",
			args:     []string{"PING"},
			prefix:   "ns1:",
			expected: []string{"PING"},
		},
		{
			name:     "EXISTS multiple keys",
			args:     []string{"EXISTS", "k1", "k2"},
			prefix:   "ns1:",
			expected: []string{"EXISTS", "ns1:k1", "ns1:k2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prefixKeyArgs(tt.args, tt.prefix)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("prefixKeyArgs(%v, %q) = %v, want %v", tt.args, tt.prefix, result, tt.expected)
			}
		})
	}
}

func TestPrefixKeyArgsDoesNotMutateOriginal(t *testing.T) {
	original := []string{"SET", "mykey", "myvalue"}
	origCopy := make([]string, len(original))
	copy(origCopy, original)

	_ = prefixKeyArgs(original, "ns1:")

	if !reflect.DeepEqual(original, origCopy) {
		t.Errorf("original args were mutated: got %v, want %v", original, origCopy)
	}
}

func TestStripKeyPrefix(t *testing.T) {
	if got := stripKeyPrefix("ns1:mykey", "ns1:"); got != "mykey" {
		t.Errorf("expected 'mykey', got %q", got)
	}
	if got := stripKeyPrefix("mykey", ""); got != "mykey" {
		t.Errorf("expected 'mykey', got %q", got)
	}
	if got := stripKeyPrefix("other:mykey", "ns1:"); got != "other:mykey" {
		t.Errorf("expected 'other:mykey', got %q", got)
	}
}
