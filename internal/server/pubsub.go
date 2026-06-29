package server

import (
	"sync"

	"vole/internal/store"
)

// PSubMessage is delivered to pattern subscribers and includes the
// pattern that matched, the actual channel name, and the message data.
type PSubMessage struct {
	Pattern string
	Channel string
	Data    string
}

// PubSub manages channel subscriptions (SUBSCRIBE) and pattern-based
// subscriptions (PSUBSCRIBE) for the Pub/Sub subsystem.
type PubSub struct {
	mu    sync.RWMutex
	subs  map[string]map[chan string]struct{}
	psubs map[string]map[chan PSubMessage]struct{}
}

// NewPubSub creates an initialised PubSub instance.
func NewPubSub() *PubSub {
	return &PubSub{
		subs:  make(map[string]map[chan string]struct{}),
		psubs: make(map[string]map[chan PSubMessage]struct{}),
	}
}

// Subscribe registers a direct channel subscription and returns a
// channel on which messages published to that channel will be sent.
func (p *PubSub) Subscribe(channel string) chan string {
	ch := make(chan string, 128)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.subs[channel] == nil {
		p.subs[channel] = make(map[chan string]struct{})
	}
	p.subs[channel][ch] = struct{}{}
	return ch
}

// Unsubscribe removes a direct channel subscription and closes the
// subscriber channel.
func (p *PubSub) Unsubscribe(channel string, ch chan string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.subs[channel], ch)
	if len(p.subs[channel]) == 0 {
		delete(p.subs, channel)
	}
	close(ch)
}

// PSubscribe registers a pattern-based subscription and returns a
// channel on which matching messages will be delivered.
func (p *PubSub) PSubscribe(pattern string) chan PSubMessage {
	ch := make(chan PSubMessage, 128)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.psubs[pattern] == nil {
		p.psubs[pattern] = make(map[chan PSubMessage]struct{})
	}
	p.psubs[pattern][ch] = struct{}{}
	return ch
}

// PUnsubscribe removes a pattern subscription and closes the
// subscriber channel.
func (p *PubSub) PUnsubscribe(pattern string, ch chan PSubMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.psubs[pattern], ch)
	if len(p.psubs[pattern]) == 0 {
		delete(p.psubs, pattern)
	}
	close(ch)
}

// Publish sends a message to all direct subscribers of the given channel
// and to all pattern subscribers whose pattern matches the channel.
// It returns the total number of clients that received the message.
func (p *PubSub) Publish(channel, message string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	// Direct subscribers
	for ch := range p.subs[channel] {
		select {
		case ch <- message:
			n++
		default:
		}
	}
	// Pattern subscribers
	for pattern, subscribers := range p.psubs {
		if store.MatchGlob(pattern, channel) {
			for ch := range subscribers {
				select {
				case ch <- PSubMessage{Pattern: pattern, Channel: channel, Data: message}:
					n++
				default:
				}
			}
		}
	}
	return n
}
