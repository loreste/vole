package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var clientIDSeq uint64

type ClientInfo struct {
	ID          uint64
	Addr        string
	Name        string
	ConnectedAt time.Time
	LastCmd     string
	LastCmdAt   time.Time
	DB          int
	Flags       string
}

type ClientManager struct {
	mu      sync.RWMutex
	clients map[uint64]*ClientInfo
}

func NewClientManager() *ClientManager {
	return &ClientManager{clients: make(map[uint64]*ClientInfo)}
}

func (cm *ClientManager) Register(addr string) uint64 {
	id := atomic.AddUint64(&clientIDSeq, 1)
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.clients[id] = &ClientInfo{
		ID:          id,
		Addr:        addr,
		ConnectedAt: time.Now(),
		Flags:       "N",
	}
	return id
}

func (cm *ClientManager) Unregister(id uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.clients, id)
}

func (cm *ClientManager) SetName(id uint64, name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if c, ok := cm.clients[id]; ok {
		c.Name = name
	}
}

func (cm *ClientManager) GetName(id uint64) string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if c, ok := cm.clients[id]; ok {
		return c.Name
	}
	return ""
}

func (cm *ClientManager) RecordCommand(id uint64, cmd string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if c, ok := cm.clients[id]; ok {
		c.LastCmd = cmd
		c.LastCmdAt = time.Now()
	}
}

func (cm *ClientManager) List() []*ClientInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	list := make([]*ClientInfo, 0, len(cm.clients))
	for _, c := range cm.clients {
		info := *c
		list = append(list, &info)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return list
}

func (cm *ClientManager) Count() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.clients)
}

func (cm *ClientManager) Kill(id uint64) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, ok := cm.clients[id]; ok {
		delete(cm.clients, id)
		return true
	}
	return false
}

// FormatList returns CLIENT LIST format string.
func (cm *ClientManager) FormatList() string {
	clients := cm.List()
	var b strings.Builder
	for _, c := range clients {
		age := int(time.Since(c.ConnectedAt).Seconds())
		idle := 0
		if !c.LastCmdAt.IsZero() {
			idle = int(time.Since(c.LastCmdAt).Seconds())
		}
		fmt.Fprintf(&b, "id=%d addr=%s name=%s age=%d idle=%d flags=%s cmd=%s\n",
			c.ID, c.Addr, c.Name, age, idle, c.Flags, c.LastCmd)
	}
	return b.String()
}
