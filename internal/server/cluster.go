package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

const slotCount = 16384

// NodeState represents the health state of a cluster node.
type NodeState string

const (
	NodeOnline  NodeState = "online"
	NodeFailing NodeState = "failing"
	NodeOffline NodeState = "offline"
)

// Node describes a single node in the cluster.
type Node struct {
	ID       string
	Addr     string
	Start    int
	End      int
	Self     bool
	State    NodeState
	LastPing time.Time
	LastPong time.Time
	JoinedAt time.Time
}

// Cluster manages cluster membership, slot assignment, and node health.
type Cluster struct {
	mu    sync.RWMutex
	nodes []Node
	self  Node
}

// NewCluster creates a cluster with the local node and optional static peers.
func NewCluster(selfID, addr, peers string) *Cluster {
	self := Node{
		ID:       selfID,
		Addr:     addr,
		Start:    0,
		End:      slotCount - 1,
		Self:     true,
		State:    NodeOnline,
		JoinedAt: time.Now(),
		LastPong: time.Now(),
	}
	nodes := []Node{self}
	for _, peer := range strings.Split(peers, ",") {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		parts := strings.Split(peer, "@")
		id := parts[0]
		paddr := peer
		if len(parts) == 2 {
			paddr = parts[1]
		}
		nodes = append(nodes, Node{
			ID:       id,
			Addr:     paddr,
			State:    NodeOnline,
			JoinedAt: time.Now(),
			LastPong: time.Now(),
		})
	}
	assignSlots(nodes)
	for i, node := range nodes {
		if node.ID == selfID {
			nodes[i].Self = true
			self = nodes[i]
		}
	}
	return &Cluster{nodes: nodes, self: self}
}

// Nodes returns a sorted copy of all known nodes.
func (c *Cluster) Nodes() []Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Node, len(c.nodes))
	copy(out, c.nodes)
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

// NodeCount returns the number of known nodes (thread-safe).
func (c *Cluster) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes)
}

// Owner returns the node responsible for the given key's slot.
func (c *Cluster) Owner(key string) Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	slot := Slot(key)
	for _, node := range c.nodes {
		if slot >= node.Start && slot <= node.End {
			return node
		}
	}
	return c.self
}

// SelfOwns returns true if this node owns the slot for the given key.
func (c *Cluster) SelfOwns(key string) bool {
	return c.Owner(key).ID == c.Self().ID
}

// Self returns the local node descriptor.
func (c *Cluster) Self() Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.self
}

// ---------------------------------------------------------------------------
// Heartbeat / gossip
// ---------------------------------------------------------------------------

// StartHeartbeat launches a background goroutine that periodically pings
// all peer nodes and updates their health state. It stops when ctx is
// cancelled.
func (c *Cluster) StartHeartbeat(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.pingPeers()
			}
		}
	}()
}

func (c *Cluster) pingPeers() {
	c.mu.RLock()
	peers := make([]Node, 0)
	for _, n := range c.nodes {
		if !n.Self {
			peers = append(peers, n)
		}
	}
	c.mu.RUnlock()

	for _, peer := range peers {
		go func(p Node) {
			ok := c.pingNode(p.Addr)
			c.mu.Lock()
			defer c.mu.Unlock()
			for i := range c.nodes {
				if c.nodes[i].ID == p.ID {
					c.nodes[i].LastPing = time.Now()
					if ok {
						c.nodes[i].LastPong = time.Now()
						c.nodes[i].State = NodeOnline
					} else if time.Since(c.nodes[i].LastPong) > 30*time.Second {
						c.nodes[i].State = NodeOffline
					} else {
						c.nodes[i].State = NodeFailing
					}
					break
				}
			}
		}(peer)
	}
}

func (c *Cluster) pingNode(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	// Send PING in RESP protocol, expect +PONG
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		return false
	}
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		return false
	}
	return strings.Contains(string(buf[:n]), "PONG")
}

// ---------------------------------------------------------------------------
// Dynamic join / leave
// ---------------------------------------------------------------------------

// Meet adds a remote node to the cluster. It pings the node first to verify
// connectivity and fetch its node ID.
func (c *Cluster) Meet(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Check if already known
	for _, n := range c.nodes {
		if n.Addr == addr {
			return nil
		}
	}
	// Ping the node to verify it is alive and get its ID
	id := c.fetchNodeID(addr)
	if id == "" {
		return errors.New("cannot reach node at " + addr)
	}
	c.nodes = append(c.nodes, Node{
		ID:       id,
		Addr:     addr,
		State:    NodeOnline,
		JoinedAt: time.Now(),
		LastPong: time.Now(),
	})
	c.rebalanceSlots()
	return nil
}

func (c *Cluster) fetchNodeID(addr string) string {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	// Send CLUSTER MYID
	_, err = conn.Write([]byte("*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n"))
	if err != nil {
		return ""
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		return ""
	}
	resp := string(buf[:n])
	// Parse bulk string response: $<len>\r\n<data>\r\n
	if strings.HasPrefix(resp, "$") {
		lines := strings.SplitN(resp, "\r\n", 3)
		if len(lines) >= 2 {
			return lines[1]
		}
	}
	return ""
}

// Forget removes a node from the cluster by its ID.
func (c *Cluster) Forget(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if nodeID == c.self.ID {
		return errors.New("cannot forget self")
	}
	found := false
	kept := make([]Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		if n.ID == nodeID {
			found = true
			continue
		}
		kept = append(kept, n)
	}
	if !found {
		return errors.New("unknown node " + nodeID)
	}
	c.nodes = kept
	c.rebalanceSlots()
	return nil
}

// rebalanceSlots redistributes the 16384 hash slots evenly across all
// online (or self) nodes. Must be called with c.mu held.
func (c *Cluster) rebalanceSlots() {
	var active []int
	for i, n := range c.nodes {
		if n.Self || n.State == NodeOnline || n.State == "" {
			active = append(active, i)
		}
	}
	if len(active) == 0 {
		return
	}

	// Clear all slots first
	for i := range c.nodes {
		c.nodes[i].Start = -1
		c.nodes[i].End = -1
	}

	// Sort active indices by node ID for deterministic assignment
	sort.Slice(active, func(a, b int) bool {
		return c.nodes[active[a]].ID < c.nodes[active[b]].ID
	})

	width := slotCount / len(active)
	for j, idx := range active {
		c.nodes[idx].Start = j * width
		c.nodes[idx].End = (j+1)*width - 1
		if j == len(active)-1 {
			c.nodes[idx].End = slotCount - 1
		}
	}

	// Refresh self snapshot
	for _, n := range c.nodes {
		if n.ID == c.self.ID {
			c.self = n
		}
	}
}

// Reset removes all peers and assigns all slots to this node.
func (c *Cluster) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.self.Start = 0
	c.self.End = slotCount - 1
	c.self.State = NodeOnline
	c.nodes = []Node{c.self}
}

// ---------------------------------------------------------------------------
// Info
// ---------------------------------------------------------------------------

// Info returns a bulk string describing cluster state (similar to Redis
// CLUSTER INFO output).
func (c *Cluster) Info() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	online := 0
	for _, n := range c.nodes {
		if n.State == NodeOnline || n.Self {
			online++
		}
	}
	return fmt.Sprintf(
		"cluster_enabled:1\r\ncluster_state:ok\r\ncluster_known_nodes:%d\r\ncluster_online_nodes:%d\r\ncluster_size:%d\r\ncluster_slots_assigned:%d\r\ncluster_self_id:%s\r\n",
		len(c.nodes), online, len(c.nodes), slotCount, c.self.ID)
}

// ---------------------------------------------------------------------------
// Slot hashing & helpers
// ---------------------------------------------------------------------------

// Slot computes the hash slot for a given key, honoring {hashtag} syntax.
func Slot(key string) int {
	if start := strings.IndexByte(key, '{'); start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	return int(crc32.ChecksumIEEE([]byte(key)) % slotCount)
}

func assignSlots(nodes []Node) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	width := slotCount / len(nodes)
	for i := range nodes {
		nodes[i].Start = i * width
		nodes[i].End = (i+1)*width - 1
		if i == len(nodes)-1 {
			nodes[i].End = slotCount - 1
		}
	}
}

// RandomNodeID generates a random 40-hex-char node identifier.
func RandomNodeID(addr string) string {
	buf := make([]byte, 20) // 20 bytes = 40 hex chars
	if _, err := rand.Read(buf); err != nil {
		// Fallback to deterministic ID if crypto/rand fails
		sum := crc32.ChecksumIEEE([]byte(addr))
		return fmt.Sprintf("%040x", sum)
	}
	return hex.EncodeToString(buf)
}
