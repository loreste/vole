package server

import (
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// Metrics tracks server-level counters.
type Metrics struct {
	CommandsProcessed  int64
	ConnectionsTotal   int64
	ConnectionsCurrent int64
	KeyspaceHits       int64
	KeyspaceMisses     int64
	BytesReceived      int64
	BytesSent          int64
	StartTime          time.Time
}

// NewMetrics creates a new Metrics instance with the start time set to now.
func NewMetrics() *Metrics {
	return &Metrics{StartTime: time.Now()}
}

func (m *Metrics) IncrCommands()    { atomic.AddInt64(&m.CommandsProcessed, 1) }
func (m *Metrics) IncrConnections() { atomic.AddInt64(&m.ConnectionsTotal, 1); atomic.AddInt64(&m.ConnectionsCurrent, 1) }
func (m *Metrics) DecrConnections() { atomic.AddInt64(&m.ConnectionsCurrent, -1) }
func (m *Metrics) IncrHits()        { atomic.AddInt64(&m.KeyspaceHits, 1) }
func (m *Metrics) IncrMisses()      { atomic.AddInt64(&m.KeyspaceMisses, 1) }

// PrometheusFormat returns metrics in Prometheus text exposition format.
func (m *Metrics) PrometheusFormat(s *Server) string {
	var b strings.Builder

	uptime := time.Since(m.StartTime).Seconds()
	cmds := atomic.LoadInt64(&m.CommandsProcessed)
	connsTotal := atomic.LoadInt64(&m.ConnectionsTotal)
	connsCurr := atomic.LoadInt64(&m.ConnectionsCurrent)
	hits := atomic.LoadInt64(&m.KeyspaceHits)
	misses := atomic.LoadInt64(&m.KeyspaceMisses)

	// Server info
	fmt.Fprintf(&b, "# HELP vole_uptime_seconds Server uptime in seconds\n")
	fmt.Fprintf(&b, "# TYPE vole_uptime_seconds gauge\n")
	fmt.Fprintf(&b, "vole_uptime_seconds %.2f\n", uptime)

	fmt.Fprintf(&b, "# HELP vole_commands_processed_total Total commands processed\n")
	fmt.Fprintf(&b, "# TYPE vole_commands_processed_total counter\n")
	fmt.Fprintf(&b, "vole_commands_processed_total %d\n", cmds)

	fmt.Fprintf(&b, "# HELP vole_connections_total Total connections received\n")
	fmt.Fprintf(&b, "# TYPE vole_connections_total counter\n")
	fmt.Fprintf(&b, "vole_connections_total %d\n", connsTotal)

	fmt.Fprintf(&b, "# HELP vole_connections_current Current open connections\n")
	fmt.Fprintf(&b, "# TYPE vole_connections_current gauge\n")
	fmt.Fprintf(&b, "vole_connections_current %d\n", connsCurr)

	fmt.Fprintf(&b, "# HELP vole_keyspace_hits_total Keyspace hits\n")
	fmt.Fprintf(&b, "# TYPE vole_keyspace_hits_total counter\n")
	fmt.Fprintf(&b, "vole_keyspace_hits_total %d\n", hits)

	fmt.Fprintf(&b, "# HELP vole_keyspace_misses_total Keyspace misses\n")
	fmt.Fprintf(&b, "# TYPE vole_keyspace_misses_total counter\n")
	fmt.Fprintf(&b, "vole_keyspace_misses_total %d\n", misses)

	// Keyspace
	dbSize := s.store.DBSize()
	fmt.Fprintf(&b, "# HELP vole_db_keys Total keys in database\n")
	fmt.Fprintf(&b, "# TYPE vole_db_keys gauge\n")
	fmt.Fprintf(&b, "vole_db_keys %d\n", dbSize)

	// Memory
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Fprintf(&b, "# HELP vole_memory_used_bytes Memory used by Go runtime\n")
	fmt.Fprintf(&b, "# TYPE vole_memory_used_bytes gauge\n")
	fmt.Fprintf(&b, "vole_memory_used_bytes %d\n", mem.Alloc)

	fmt.Fprintf(&b, "# HELP vole_memory_sys_bytes Total memory obtained from OS\n")
	fmt.Fprintf(&b, "# TYPE vole_memory_sys_bytes gauge\n")
	fmt.Fprintf(&b, "vole_memory_sys_bytes %d\n", mem.Sys)

	fmt.Fprintf(&b, "# HELP vole_goroutines Current goroutine count\n")
	fmt.Fprintf(&b, "# TYPE vole_goroutines gauge\n")
	fmt.Fprintf(&b, "vole_goroutines %d\n", runtime.NumGoroutine())

	// Hit rate
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}
	fmt.Fprintf(&b, "# HELP vole_keyspace_hit_rate Keyspace hit rate\n")
	fmt.Fprintf(&b, "# TYPE vole_keyspace_hit_rate gauge\n")
	fmt.Fprintf(&b, "vole_keyspace_hit_rate %.4f\n", hitRate)

	return b.String()
}
