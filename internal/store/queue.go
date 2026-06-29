package store

import (
	"fmt"
	"sync/atomic"
	"time"
)

var queueSeq uint64

// QueueMessage represents a message in a queue.
type QueueMessage struct {
	ID          string
	Body        string
	CreatedAt   time.Time
	Retries     int
	MaxRetry    int
	AckDeadline time.Time // when this message must be ACK'd by
}

// Queue is a reliable message queue with ack/nack support.
type Queue struct {
	Pending    []*QueueMessage          // messages waiting to be dequeued
	Processing map[string]*QueueMessage // messageID -> message being processed
	DeadLetter []*QueueMessage          // messages that exceeded max retries
	AckTimeout time.Duration            // how long before unacked messages are retried
	MaxRetries int                      // max retries before dead-lettering
}

// NewQueue creates a new reliable queue with default settings.
func NewQueue() *Queue {
	return &Queue{
		Processing: make(map[string]*QueueMessage),
		AckTimeout: 5 * time.Minute,
		MaxRetries: 3,
	}
}

func nextQueueID() string {
	seq := atomic.AddUint64(&queueSeq, 1)
	return fmt.Sprintf("%d-%d", time.Now().UnixMilli(), seq)
}

// Enqueue adds a message to the named queue. If delay > 0, the message won't
// be visible until delay has elapsed. Returns the message ID.
func (s *Store) Enqueue(queueName, body string, delay time.Duration) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[queueName]
	if q == nil {
		q = NewQueue()
		s.queues[queueName] = q
	}
	msg := &QueueMessage{
		ID:        nextQueueID(),
		Body:      body,
		CreatedAt: time.Now(),
		MaxRetry:  q.MaxRetries,
	}
	if delay > 0 {
		msg.CreatedAt = time.Now().Add(delay)
	}
	q.Pending = append(q.Pending, msg)
	s.touchKeyLocked(queueName)

	// Wake any waiting dequeue operations
	waiters := s.queueWaits[queueName]
	if len(waiters) > 0 {
		w := waiters[0]
		s.queueWaits[queueName] = waiters[1:]
		if len(s.queueWaits[queueName]) == 0 {
			delete(s.queueWaits, queueName)
		}
		w.once.Do(func() { close(w.ch) })
	}

	return msg.ID
}

// Dequeue removes and returns the next available message from the queue,
// moving it to the processing state. If no message is available and timeout > 0,
// it blocks until a message arrives or the timeout expires.
func (s *Store) Dequeue(queueName string, timeout time.Duration) (*QueueMessage, bool) {
	s.mu.Lock()

	// First, requeue any expired processing messages
	s.requeueExpiredLocked(queueName)

	q := s.queues[queueName]
	if q != nil {
		now := time.Now()
		for i, msg := range q.Pending {
			if msg.CreatedAt.After(now) {
				continue // delayed message, not ready yet
			}
			// Dequeue this message
			q.Pending = append(q.Pending[:i], q.Pending[i+1:]...)
			msg.AckDeadline = now.Add(q.AckTimeout)
			q.Processing[msg.ID] = msg
			s.touchKeyLocked(queueName)
			s.mu.Unlock()
			return msg, true
		}
	}

	if timeout <= 0 {
		s.mu.Unlock()
		return nil, false
	}

	// Block and wait
	w := &waiter{ch: make(chan struct{})}
	s.queueWaits[queueName] = append(s.queueWaits[queueName], w)
	s.mu.Unlock()

	// Wait for signal or timeout
	select {
	case <-w.ch:
	case <-time.After(timeout):
	}

	// Try again
	s.mu.Lock()
	s.requeueExpiredLocked(queueName)
	q = s.queues[queueName]
	if q != nil {
		now := time.Now()
		for i, msg := range q.Pending {
			if msg.CreatedAt.After(now) {
				continue
			}
			q.Pending = append(q.Pending[:i], q.Pending[i+1:]...)
			msg.AckDeadline = now.Add(q.AckTimeout)
			q.Processing[msg.ID] = msg
			s.touchKeyLocked(queueName)
			s.mu.Unlock()
			return msg, true
		}
	}
	s.mu.Unlock()
	return nil, false
}

// QAck acknowledges successful processing of a message, removing it from
// the processing set.
func (s *Store) QAck(queueName, messageID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[queueName]
	if q == nil {
		return false
	}
	if _, ok := q.Processing[messageID]; !ok {
		return false
	}
	delete(q.Processing, messageID)
	s.touchKeyLocked(queueName)
	return true
}

// QNack negatively acknowledges a message. It is returned to the pending
// queue for retry, or moved to the dead-letter queue if max retries exceeded.
func (s *Store) QNack(queueName, messageID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[queueName]
	if q == nil {
		return false
	}
	msg, ok := q.Processing[messageID]
	if !ok {
		return false
	}
	delete(q.Processing, messageID)
	msg.Retries++
	if msg.Retries >= msg.MaxRetry {
		q.DeadLetter = append(q.DeadLetter, msg)
	} else {
		msg.CreatedAt = time.Now() // reset for immediate availability
		q.Pending = append(q.Pending, msg)
	}
	s.touchKeyLocked(queueName)
	return true
}

// QPeek returns up to count messages from the front of the pending queue
// without removing them.
func (s *Store) QPeek(queueName string, count int) []*QueueMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := s.queues[queueName]
	if q == nil {
		return nil
	}
	if count <= 0 {
		count = 1
	}
	n := count
	if n > len(q.Pending) {
		n = len(q.Pending)
	}
	out := make([]*QueueMessage, n)
	copy(out, q.Pending[:n])
	return out
}

// QLen returns the number of pending messages in the named queue.
func (s *Store) QLen(queueName string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := s.queues[queueName]
	if q == nil {
		return 0
	}
	return len(q.Pending)
}

// QInfo returns counts of pending, processing, and dead-letter messages.
func (s *Store) QInfo(queueName string) (pending, processing, deadLetter int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := s.queues[queueName]
	if q == nil {
		return 0, 0, 0
	}
	return len(q.Pending), len(q.Processing), len(q.DeadLetter)
}

// QDead returns up to count messages from the dead-letter queue.
func (s *Store) QDead(queueName string, count int) []*QueueMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := s.queues[queueName]
	if q == nil {
		return nil
	}
	if count <= 0 {
		count = 10
	}
	n := count
	if n > len(q.DeadLetter) {
		n = len(q.DeadLetter)
	}
	out := make([]*QueueMessage, n)
	copy(out, q.DeadLetter[:n])
	return out
}

// requeueExpiredLocked checks for messages whose ack deadline has passed
// and requeues or dead-letters them. Must be called under write lock.
func (s *Store) requeueExpiredLocked(queueName string) {
	q := s.queues[queueName]
	if q == nil {
		return
	}
	now := time.Now()
	for id, msg := range q.Processing {
		if now.After(msg.AckDeadline) {
			delete(q.Processing, id)
			msg.Retries++
			if msg.Retries >= msg.MaxRetry {
				q.DeadLetter = append(q.DeadLetter, msg)
			} else {
				msg.CreatedAt = now
				q.Pending = append(q.Pending, msg)
			}
		}
	}
}
