package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"vole/internal/resp"
	"vole/internal/store"
)

// ReplicationRole indicates whether this node acts as a leader or follower.
type ReplicationRole string

const (
	RoleMaster  ReplicationRole = "master"
	RoleReplica ReplicationRole = "replica"
)

// ReplicationState tracks the replication role, connected replicas (when
// acting as a leader), and the follow-loop cancel function (when acting as
// a follower).
type ReplicationState struct {
	mu         sync.RWMutex
	role       ReplicationRole
	leaderAddr string
	replicas   map[string]*Replica // addr -> replica
	cancel     context.CancelFunc  // cancel follower sync
}

// Replica represents a connected follower.
type Replica struct {
	Addr        string
	Conn        net.Conn
	Writer      *resp.Writer
	ConnectedAt time.Time
}

// NewReplicationState returns a new ReplicationState defaulting to leader.
func NewReplicationState() *ReplicationState {
	return &ReplicationState{
		role:     RoleMaster,
		replicas: make(map[string]*Replica),
	}
}

// Role returns the current replication role.
func (rs *ReplicationState) Role() ReplicationRole {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.role
}

// IsReplica returns true when acting as a follower.
func (rs *ReplicationState) IsReplica() bool {
	return rs.Role() == RoleReplica
}

// LeaderAddr returns the address of the leader being followed.
func (rs *ReplicationState) LeaderAddr() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.leaderAddr
}

// ReplicaCount returns the number of connected followers.
func (rs *ReplicationState) ReplicaCount() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.replicas)
}

// PropagateToReplicas sends a write command to all connected replicas.
func (rs *ReplicationState) PropagateToReplicas(args []string) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	for addr, r := range rs.replicas {
		if err := r.Writer.Command(args); err != nil {
			log.Printf("replication: failed to propagate to %s: %v", addr, err)
			continue
		}
		if err := r.Writer.Flush(); err != nil {
			log.Printf("replication: flush failed for %s: %v", addr, err)
		}
	}
}

// AddReplica registers a new follower connection.
func (rs *ReplicationState) AddReplica(conn net.Conn) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	addr := conn.RemoteAddr().String()
	rs.replicas[addr] = &Replica{
		Addr:        addr,
		Conn:        conn,
		Writer:      resp.NewWriter(conn),
		ConnectedAt: time.Now(),
	}
	log.Printf("replication: replica connected from %s (total: %d)", addr, len(rs.replicas))
}

// RemoveReplica removes a follower.
func (rs *ReplicationState) RemoveReplica(addr string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if r, ok := rs.replicas[addr]; ok {
		r.Conn.Close()
		delete(rs.replicas, addr)
	}
}

// StartFollowing connects to a leader and begins replication.
func (rs *ReplicationState) StartFollowing(ctx context.Context, leaderAddr string, st *store.Store) error {
	rs.mu.Lock()
	// Cancel any previous following
	if rs.cancel != nil {
		rs.cancel()
	}
	followCtx, cancel := context.WithCancel(ctx)
	rs.cancel = cancel
	rs.role = RoleReplica
	rs.leaderAddr = leaderAddr
	rs.mu.Unlock()

	go rs.followLoop(followCtx, leaderAddr, st)
	return nil
}

// StopFollowing stops replication and becomes a leader.
func (rs *ReplicationState) StopFollowing() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.cancel != nil {
		rs.cancel()
		rs.cancel = nil
	}
	rs.role = RoleMaster
	rs.leaderAddr = ""
}

func (rs *ReplicationState) followLoop(ctx context.Context, leaderAddr string, st *store.Store) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := rs.syncFromLeader(ctx, leaderAddr, st); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("replication: sync error: %v, retrying in 5s", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (rs *ReplicationState) syncFromLeader(ctx context.Context, leaderAddr string, st *store.Store) error {
	conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect to leader: %w", err)
	}
	defer conn.Close()

	w := resp.NewWriter(conn)
	rd := resp.NewReader(conn)

	// Step 1: Request full sync via REPLSYNC command
	if err := w.Command([]string{"REPLSYNC"}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// Step 2: Read the snapshot (JSON blob)
	v, err := rd.ReadValue()
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	if v.Type == resp.ErrorString {
		return fmt.Errorf("leader error: %s", v.Text)
	}
	if v.Type != resp.BulkString {
		return fmt.Errorf("unexpected response type for snapshot")
	}

	// Load snapshot into store
	var snap store.Snapshot
	if err := json.Unmarshal([]byte(v.Text), &snap); err != nil {
		return fmt.Errorf("parse snapshot: %w", err)
	}
	st.Load(snap)
	log.Printf("replication: loaded snapshot from %s", leaderAddr)

	// Step 3: Stream incremental commands
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		args, err := rd.ReadCommand()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Send PING to keep alive
				_ = w.Command([]string{"PING"})
				_ = w.Flush()
				continue
			}
			return fmt.Errorf("read command: %w", err)
		}
		if len(args) == 0 {
			continue
		}

		// Execute the replicated command
		cmd := strings.ToUpper(args[0])
		if cmd == "PING" || cmd == "PONG" {
			continue
		}

		applyReplicatedCommand(st, args)
	}
}

// applyReplicatedCommand applies a replicated write command to the local
// store. This mirrors the AOF replay logic so that followers stay in sync.
func applyReplicatedCommand(st *store.Store, args []string) {
	if len(args) == 0 {
		return
	}
	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "SET":
		if len(args) >= 3 {
			var ttl time.Duration
			if len(args) >= 5 {
				switch strings.ToUpper(args[3]) {
				case "EX":
					sec, _ := strconv.ParseInt(args[4], 10, 64)
					ttl = time.Duration(sec) * time.Second
				case "PX":
					ms, _ := strconv.ParseInt(args[4], 10, 64)
					ttl = time.Duration(ms) * time.Millisecond
				}
			}
			st.Set(args[1], args[2], ttl)
		}
	case "SETABS":
		if len(args) == 4 {
			var ns int64
			fmt.Sscan(args[3], &ns)
			var expireAt time.Time
			if ns > 0 {
				expireAt = time.Unix(0, ns)
			}
			st.SetAbsolute(args[1], args[2], expireAt)
		}
	case "DEL":
		if len(args) > 1 {
			st.Del(args[1:]...)
		}
	case "HSET":
		if len(args) >= 4 && len(args)%2 == 0 {
			pairs := make([]store.HashPair, 0, (len(args)-2)/2)
			for i := 2; i < len(args); i += 2 {
				pairs = append(pairs, store.HashPair{Field: args[i], Value: args[i+1]})
			}
			st.HSet(args[1], pairs)
		}
	case "HDEL":
		if len(args) > 2 {
			st.HDel(args[1], args[2:]...)
		}
	case "LPUSH":
		if len(args) > 2 {
			st.LPush(args[1], args[2:]...)
		}
	case "RPUSH":
		if len(args) > 2 {
			st.RPush(args[1], args[2:]...)
		}
	case "LPOP":
		if len(args) == 2 {
			st.LPop(args[1])
		}
	case "RPOP":
		if len(args) == 2 {
			st.RPop(args[1])
		}
	case "LSET":
		if len(args) == 4 {
			index, _ := strconv.Atoi(args[2])
			st.LSet(args[1], index, args[3])
		}
	case "LINSERT":
		if len(args) == 5 {
			before := strings.EqualFold(args[2], "BEFORE")
			st.LInsert(args[1], before, args[3], args[4])
		}
	case "SADD":
		if len(args) > 2 {
			st.SAdd(args[1], args[2:]...)
		}
	case "SREM":
		if len(args) > 2 {
			st.SRem(args[1], args[2:]...)
		}
	case "ZADD":
		if len(args) >= 4 && len(args)%2 == 0 {
			members := make([]store.ZMember, 0, (len(args)-2)/2)
			for i := 2; i < len(args); i += 2 {
				var score float64
				fmt.Sscan(args[i], &score)
				members = append(members, store.ZMember{Score: score, Member: args[i+1]})
			}
			st.ZAdd(args[1], members)
		}
	case "ZREM":
		if len(args) > 2 {
			st.ZRem(args[1], args[2:]...)
		}
	case "INCR":
		if len(args) == 2 {
			st.Incr(args[1])
		}
	case "INCRBY":
		if len(args) == 3 {
			var delta int64
			fmt.Sscan(args[2], &delta)
			st.IncrBy(args[1], delta)
		}
	case "DECRBY":
		if len(args) == 3 {
			var delta int64
			fmt.Sscan(args[2], &delta)
			st.DecrBy(args[1], delta)
		}
	case "DECR":
		if len(args) == 2 {
			st.IncrBy(args[1], -1)
		}
	case "INCRBYFLOAT":
		if len(args) == 3 {
			var delta float64
			fmt.Sscan(args[2], &delta)
			st.IncrByFloat(args[1], delta)
		}
	case "HINCRBY":
		if len(args) == 4 {
			var delta int64
			fmt.Sscan(args[3], &delta)
			st.HIncrBy(args[1], args[2], delta)
		}
	case "APPEND":
		if len(args) == 3 {
			st.Append(args[1], args[2])
		}
	case "EXPIRE":
		if len(args) == 3 {
			var sec int64
			fmt.Sscan(args[2], &sec)
			st.Expire(args[1], time.Duration(sec)*time.Second)
		}
	case "EXPIREATABS":
		if len(args) == 3 {
			var ns int64
			fmt.Sscan(args[2], &ns)
			if ns == 0 {
				st.ExpireAt(args[1], time.Time{})
			} else {
				st.ExpireAt(args[1], time.Unix(0, ns))
			}
		}
	case "RENAME":
		if len(args) == 3 {
			st.Rename(args[1], args[2])
		}
	case "COPY":
		if len(args) >= 3 {
			replace := false
			for _, a := range args[3:] {
				if strings.EqualFold(a, "REPLACE") {
					replace = true
				}
			}
			st.Copy(args[1], args[2], replace)
		}
	case "MSETABS":
		if len(args) >= 4 && (len(args)-1)%3 == 0 {
			for i := 1; i < len(args); i += 3 {
				var ns int64
				fmt.Sscan(args[i+2], &ns)
				var expireAt time.Time
				if ns > 0 {
					expireAt = time.Unix(0, ns)
				}
				st.SetAbsolute(args[i], args[i+1], expireAt)
			}
		}
	case "XADD":
		if len(args) >= 5 {
			st.XAdd(args[1], args[2], args[3:])
		}
	case "XGROUP":
		if len(args) >= 5 && strings.EqualFold(args[1], "CREATE") {
			mkstream := len(args) == 6 && strings.EqualFold(args[5], "MKSTREAM")
			st.XGroupCreate(args[2], args[3], args[4], mkstream)
		}
	case "XGROUPDELIVER":
		if len(args) >= 6 {
			var ns int64
			fmt.Sscan(args[4], &ns)
			st.XGroupDeliver(args[1], args[2], args[3], args[5:], time.Unix(0, ns))
		}
	case "XACK":
		if len(args) >= 4 {
			st.XAck(args[1], args[2], args[3:]...)
		}
	case "PFADD":
		if len(args) >= 2 {
			st.PFAdd(args[1], args[2:]...)
		}
	case "PFMERGE":
		if len(args) >= 2 {
			st.PFMerge(args[1], args[2:]...)
		}
	case "SETDELAYED":
		if len(args) >= 4 {
			var delaySec int64
			fmt.Sscan(args[3], &delaySec)
			var ttl time.Duration
			if len(args) >= 6 && strings.EqualFold(args[4], "EX") {
				var sec int64
				fmt.Sscan(args[5], &sec)
				ttl = time.Duration(sec) * time.Second
			}
			st.SetDelayed(args[1], args[2], time.Duration(delaySec)*time.Second, ttl)
		}
	case "ENQUEUE":
		if len(args) >= 3 {
			var delay time.Duration
			if len(args) >= 5 && strings.EqualFold(args[3], "DELAY") {
				var sec int64
				fmt.Sscan(args[4], &sec)
				delay = time.Duration(sec) * time.Second
			}
			st.Enqueue(args[1], args[2], delay)
		}
	case "QACK":
		if len(args) == 3 {
			st.QAck(args[1], args[2])
		}
	case "QNACK":
		if len(args) == 3 {
			st.QNack(args[1], args[2])
		}
	case "JSON.SET":
		if len(args) == 4 {
			st.JSONSet(args[1], args[2], args[3])
		}
	case "JSON.DEL":
		if len(args) >= 2 {
			path := "$"
			if len(args) == 3 {
				path = args[2]
			}
			st.JSONDel(args[1], path)
		}
	case "JSON.NUMINCRBY":
		if len(args) == 4 {
			delta, _ := strconv.ParseFloat(args[3], 64)
			st.JSONNumIncrBy(args[1], args[2], delta)
		}
	case "JSON.ARRAPPEND":
		if len(args) >= 4 {
			st.JSONArrAppend(args[1], args[2], args[3:]...)
		}
	case "TAG":
		if len(args) >= 3 {
			tags := make(map[string]string)
			for _, arg := range args[2:] {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) == 2 {
					tags[parts[0]] = parts[1]
				}
			}
			st.TagSet(args[1], tags)
		}
	case "TAGDEL":
		if len(args) >= 3 {
			st.TagDel(args[1], args[2:])
		}
	case "TS.ADD":
		if len(args) >= 4 {
			var ts int64
			if args[2] != "*" {
				fmt.Sscan(args[2], &ts)
			}
			var val float64
			fmt.Sscan(args[3], &val)
			labels := make(map[string]string)
			labelStart := 4
			if len(args) > 4 && strings.EqualFold(args[4], "LABELS") {
				labelStart = 5
			}
			for i := labelStart; i < len(args); i++ {
				parts := strings.SplitN(args[i], "=", 2)
				if len(parts) == 2 {
					labels[parts[0]] = parts[1]
				}
			}
			st.TSAdd(args[1], ts, val, labels)
		}
	}
}

// handleReplSync handles the internal REPLSYNC command sent by followers.
// It sends a snapshot then keeps the connection open for streaming writes.
func (s *Server) handleReplSync(conn net.Conn, w *resp.Writer) {
	// Send snapshot
	snap := s.store.Dump()
	data, err := json.Marshal(snap)
	if err != nil {
		_ = w.Error("ERR " + err.Error())
		_ = w.Flush()
		return
	}
	_ = w.Bulk(string(data))
	_ = w.Flush()

	// Register as replica — the connection stays open for streaming
	s.repl.AddReplica(conn)
	defer s.repl.RemoveReplica(conn.RemoteAddr().String())

	// Keep connection alive — read PINGs from replica
	rd := resp.NewReader(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		args, err := rd.ReadCommand()
		if err != nil {
			return
		}
		if len(args) > 0 && strings.EqualFold(args[0], "PING") {
			_ = w.Simple("PONG")
			_ = w.Flush()
		}
	}
}

// ReplicaOf starts following the given leader address, or stops replication
// if addr is empty. This is the exported API for use from main.
func (s *Server) ReplicaOf(ctx context.Context, addr string) error {
	return s.repl.StartFollowing(ctx, addr, s.store)
}
