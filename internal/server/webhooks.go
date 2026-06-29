package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"vole/internal/resp"
	"vole/internal/store"
)

// WebhookManager manages event-driven HTTP webhooks that fire on key changes
// and TTL expiry. Each webhook is registered with an event type and a key
// pattern (glob).
type WebhookManager struct {
	mu     sync.RWMutex
	hooks  map[string][]string // "event:pattern" -> webhook URLs
	client *http.Client
}

// NewWebhookManager creates a ready-to-use WebhookManager.
func NewWebhookManager() *WebhookManager {
	return &WebhookManager{
		hooks:  make(map[string][]string),
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

// Register adds a webhook URL for the given key pattern and event type.
// Pattern uses glob syntax (e.g. "user:*"). Event can be "set", "del",
// "expired", or "*" to match all events.
func (wm *WebhookManager) Register(pattern, event, url string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	key := event + ":" + pattern
	wm.hooks[key] = append(wm.hooks[key], url)
}

// Unregister removes a specific webhook URL for the given pattern and event.
func (wm *WebhookManager) Unregister(pattern, event, url string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	key := event + ":" + pattern
	urls := wm.hooks[key]
	for i, u := range urls {
		if u == url {
			wm.hooks[key] = append(urls[:i], urls[i+1:]...)
			break
		}
	}
	if len(wm.hooks[key]) == 0 {
		delete(wm.hooks, key)
	}
}

// List returns a copy of all registered webhooks.
func (wm *WebhookManager) List() map[string][]string {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	out := make(map[string][]string, len(wm.hooks))
	for k, v := range wm.hooks {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// Fire sends webhook notifications for all hooks whose event and pattern
// match. Delivery is asynchronous (fire-and-forget).
func (wm *WebhookManager) Fire(event, key string) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	for hookKey, urls := range wm.hooks {
		parts := splitWebhookKey(hookKey)
		if parts[0] != event && parts[0] != "*" {
			continue
		}
		if parts[1] != "*" && !store.MatchGlob(parts[1], key) {
			continue
		}
		for _, u := range urls {
			go wm.send(u, event, key)
		}
	}
}

func splitWebhookKey(key string) [2]string {
	idx := strings.IndexByte(key, ':')
	if idx < 0 {
		return [2]string{key, "*"}
	}
	return [2]string{key[:idx], key[idx+1:]}
}

func (wm *WebhookManager) send(hookURL, event, key string) {
	payload := map[string]string{
		"event":     event,
		"key":       key,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}
	body, _ := json.Marshal(payload)
	r, err := wm.client.Post(hookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("webhook delivery failed for %s: %v", hookURL, err)
		return
	}
	r.Body.Close()
}

// webhookCmd handles the WEBHOOK RESP command.
//
//	WEBHOOK REGISTER <pattern> <event> <url>
//	WEBHOOK UNREGISTER <pattern> <event> <url>
//	WEBHOOK LIST
func (s *Server) webhookCmd(w *resp.Writer, args []string) error {
	if len(args) < 2 {
		return wrongArgs("WEBHOOK")
	}
	sub := strings.ToUpper(args[1])
	switch sub {
	case "REGISTER":
		if len(args) != 5 {
			return wrongArgs("WEBHOOK REGISTER")
		}
		s.webhooks.Register(args[2], args[3], args[4])
		return w.Simple("OK")
	case "UNREGISTER":
		if len(args) != 5 {
			return wrongArgs("WEBHOOK UNREGISTER")
		}
		s.webhooks.Unregister(args[2], args[3], args[4])
		return w.Simple("OK")
	case "LIST":
		hooks := s.webhooks.List()
		if err := w.ArrayLen(len(hooks)); err != nil {
			return err
		}
		for key, urls := range hooks {
			if err := w.ArrayLen(2); err != nil {
				return err
			}
			if err := w.Bulk(key); err != nil {
				return err
			}
			if err := writeBulkStrings(w, urls); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported WEBHOOK subcommand %q", sub)
	}
}
