package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vole/internal/resp"
	"vole/internal/store"
)

// PeerState tracks a connected peer for multi-master replication.
type PeerState struct {
	mu     sync.Mutex
	ID     string
	Addr   string
	Conn   net.Conn
	Writer *resp.Writer
	Reader *resp.Reader
	Active bool
}

// MultiMaster manages peer-to-peer replication across cluster nodes.
type MultiMaster struct {
	mu       sync.RWMutex
	peers    map[string]*PeerState // nodeID -> peer
	store    *store.Store
	selfID   string
	selfAddr string
	clock    int64 // Lamport clock (atomic)
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup // tracks active peerLoop goroutines
	enabled  bool
}

// NewMultiMaster creates a new MultiMaster instance.
func NewMultiMaster(selfID, selfAddr string, st *store.Store) *MultiMaster {
	ctx, cancel := context.WithCancel(context.Background())
	return &MultiMaster{
		peers:    make(map[string]*PeerState),
		store:    st,
		selfID:   selfID,
		selfAddr: selfAddr,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Enable turns on multi-master replication.
func (mm *MultiMaster) Enable() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.enabled = true
	log.Printf("multi-master: enabled for node %s", mm.selfID)
}

// Disable turns off multi-master replication.
func (mm *MultiMaster) Disable() {
	mm.mu.Lock()
	mm.enabled = false
	mm.mu.Unlock()
	mm.Stop()
}

// IsEnabled returns whether multi-master is active.
func (mm *MultiMaster) IsEnabled() bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.enabled
}

// Tick increments and returns the Lamport clock.
func (mm *MultiMaster) Tick() int64 {
	return atomic.AddInt64(&mm.clock, 1)
}

// Merge updates the clock if the received timestamp is higher.
func (mm *MultiMaster) Merge(remote int64) {
	for {
		local := atomic.LoadInt64(&mm.clock)
		if remote <= local {
			return
		}
		if atomic.CompareAndSwapInt64(&mm.clock, local, remote) {
			return
		}
	}
}

// Clock returns the current Lamport clock value.
func (mm *MultiMaster) Clock() int64 {
	return atomic.LoadInt64(&mm.clock)
}

// ConnectToPeer establishes a replication connection to another node.
// The connection uses REPLPEER to set up bidirectional replication:
// we receive the peer's writes over this connection, and the peer
// initiates a reverse REPLPEER back to us for our writes.
func (mm *MultiMaster) ConnectToPeer(nodeID, addr string) error {
	mm.mu.Lock()
	if _, ok := mm.peers[nodeID]; ok {
		mm.mu.Unlock()
		return nil // already connected
	}
	// Mark as connecting with a placeholder so no other goroutine starts
	// a duplicate connection for the same nodeID.
	mm.peers[nodeID] = &PeerState{ID: nodeID, Addr: addr, Active: false}
	mm.mu.Unlock()

	mm.wg.Add(1)
	go mm.peerLoop(nodeID, addr)
	return nil
}

func (mm *MultiMaster) peerLoop(nodeID, addr string) {
	defer mm.wg.Done()
	for {
		select {
		case <-mm.ctx.Done():
			return
		default:
		}

		if err := mm.syncWithPeer(nodeID, addr); err != nil {
			if mm.ctx.Err() != nil {
				return
			}
			log.Printf("multi-master: peer %s (%s) sync error: %v, retrying in 5s", nodeID, addr, err)
			select {
			case <-mm.ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

func (mm *MultiMaster) syncWithPeer(nodeID, addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer func() {
		conn.Close()
		mm.mu.Lock()
		delete(mm.peers, nodeID)
		mm.mu.Unlock()
	}()

	w := resp.NewWriter(conn)
	rd := resp.NewReader(conn)

	// Send REPLPEER with our ID and address so the remote node can
	// establish a reverse connection back to us.
	if err := w.Command([]string{"REPLPEER", mm.selfID, mm.selfAddr}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	// Read the snapshot response.
	v, err := rd.ReadValue()
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	if v.Type == resp.ErrorString {
		return fmt.Errorf("peer error: %s", v.Text)
	}
	if v.Type != resp.BulkString {
		return fmt.Errorf("unexpected snapshot response type")
	}

	// Apply snapshot only if our store is empty (initial sync).
	if mm.store.DBSize() == 0 {
		var snap store.Snapshot
		if err := json.Unmarshal([]byte(v.Text), &snap); err != nil {
			return fmt.Errorf("parse snapshot: %w", err)
		}
		mm.store.Load(snap)
		log.Printf("multi-master: loaded snapshot from peer %s", nodeID)
	} else {
		log.Printf("multi-master: connected to peer %s (skipped snapshot, already have data)", nodeID)
	}

	// Register peer.
	peer := &PeerState{
		ID:     nodeID,
		Addr:   addr,
		Conn:   conn,
		Writer: w,
		Reader: rd,
		Active: true,
	}
	mm.mu.Lock()
	mm.peers[nodeID] = peer
	mm.mu.Unlock()

	// Stream commands from the peer. The peer sends us its writes via the
	// existing PropagateToReplicas mechanism (we are registered as a replica
	// on the peer side via handleReplPeer). Commands received here are
	// applied locally but NOT re-propagated, avoiding infinite loops.
	for {
		select {
		case <-mm.ctx.Done():
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		args, err := rd.ReadCommand()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Send PING to keep alive. Lock the peer mutex because
				// PropagateToAllPeers may be writing concurrently.
				peer.mu.Lock()
				_ = w.Command([]string{"PING"})
				_ = w.Flush()
				peer.mu.Unlock()
				continue
			}
			return fmt.Errorf("read command: %w", err)
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		if cmd == "PING" || cmd == "PONG" {
			continue
		}

		// Apply the replicated command locally (no re-propagation).
		applyReplicatedCommand(mm.store, args)
	}
}

// PropagateToAllPeers sends a write command to every connected peer.
// Called by the server after executing a client-originated write.
func (mm *MultiMaster) PropagateToAllPeers(args []string) {
	if !mm.IsEnabled() {
		return
	}

	mm.mu.RLock()
	peers := make([]*PeerState, 0, len(mm.peers))
	for _, p := range mm.peers {
		if p.Active {
			peers = append(peers, p)
		}
	}
	mm.mu.RUnlock()

	for _, p := range peers {
		p.mu.Lock()
		err := p.Writer.Command(args)
		if err == nil {
			err = p.Writer.Flush()
		}
		p.mu.Unlock()
		if err != nil {
			log.Printf("multi-master: propagate to %s failed: %v", p.Addr, err)
		}
	}
}

// PeerCount returns the number of active peers.
func (mm *MultiMaster) PeerCount() int {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	n := 0
	for _, p := range mm.peers {
		if p.Active {
			n++
		}
	}
	return n
}

// PeerSummary is a read-only snapshot of a peer for reporting.
type PeerSummary struct {
	ID     string
	Addr   string
	Active bool
}

// PeerInfo returns info about connected peers.
func (mm *MultiMaster) PeerInfo() []PeerSummary {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	out := make([]PeerSummary, 0, len(mm.peers))
	for _, p := range mm.peers {
		out = append(out, PeerSummary{ID: p.ID, Addr: p.Addr, Active: p.Active})
	}
	return out
}

// Stop shuts down all peer connections and waits for goroutines to exit.
func (mm *MultiMaster) Stop() {
	mm.cancel()

	// Close all peer connections to unblock reads.
	mm.mu.Lock()
	for _, p := range mm.peers {
		if p.Conn != nil {
			p.Conn.Close()
		}
	}
	mm.mu.Unlock()

	// Wait for all peerLoop goroutines to finish.
	mm.wg.Wait()

	mm.mu.Lock()
	mm.peers = make(map[string]*PeerState)
	// Reset context so Enable can be called again.
	mm.ctx, mm.cancel = context.WithCancel(context.Background())
	mm.mu.Unlock()
}

// ConnectToClusterPeers connects to all known cluster nodes for replication.
func (mm *MultiMaster) ConnectToClusterPeers(cluster *Cluster) {
	nodes := cluster.Nodes()
	self := cluster.Self()
	for _, n := range nodes {
		if n.ID == self.ID || n.Self {
			continue
		}
		mm.ConnectToPeer(n.ID, n.Addr)
	}
}
