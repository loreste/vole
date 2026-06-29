package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"vole/internal/store"
)

// HTTPServer exposes a JSON REST API alongside the RESP server.
type HTTPServer struct {
	server  *Server
	httpSrv *http.Server
}

// NewHTTPServer creates a new HTTP API server backed by the given Server.
func NewHTTPServer(srv *Server, addr string) *HTTPServer {
	h := &HTTPServer{server: srv}
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/keys/", h.handleKeys)
	mux.HandleFunc("/api/v1/keys", h.handleKeysScan)
	mux.HandleFunc("/api/v1/hash/", h.handleHash)
	mux.HandleFunc("/api/v1/list/", h.handleList)
	mux.HandleFunc("/api/v1/set/", h.handleSet)
	mux.HandleFunc("/api/v1/zset/", h.handleZSet)
	mux.HandleFunc("/api/v1/publish/", h.handlePublish)
	mux.HandleFunc("/api/v1/info", h.handleInfo)
	mux.HandleFunc("/api/v1/flush", h.handleFlush)
	mux.HandleFunc("/api/v1/dbsize", h.handleDBSize)
	mux.HandleFunc("/api/v1/events", h.handleEvents)
	mux.HandleFunc("/api/v1/webhooks", h.handleWebhooks)
	mux.HandleFunc("/api/v1/search/hash", h.handleHashSearch)
	mux.HandleFunc("/api/v1/cluster/info", h.handleClusterInfo)
	mux.HandleFunc("/api/v1/cluster/meet", h.handleClusterMeet)
	mux.HandleFunc("/api/v1/cluster/nodes/", h.handleClusterForget)
	mux.HandleFunc("/api/v1/ratelimit/", h.handleRateLimit)
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	h.httpSrv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
	return h
}

// ListenAndServe starts the HTTP server and blocks until ctx is cancelled.
func (h *HTTPServer) ListenAndServe(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		h.httpSrv.Shutdown(context.Background())
	}()
	if h.server.tlsCert != "" && h.server.tlsKey != "" {
		err := h.httpSrv.ListenAndServeTLS(h.server.tlsCert, h.server.tlsKey)
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
	err := h.httpSrv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func readJSON(r *http.Request, dst interface{}) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1 MiB limit
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil // allow empty body
	}
	return json.Unmarshal(body, dst)
}

// persistWrite appends a command to the AOF under the persist lock.
func (h *HTTPServer) persistWrite(args []string) error {
	h.server.persistMu.Lock()
	defer h.server.persistMu.Unlock()
	return h.server.aof.Append(args)
}

// ---------------------------------------------------------------------------
// Keys (GET / PUT / DELETE / INCR)
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleKeys(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/keys/")
	if path == "" {
		jsonError(w, "key required", http.StatusBadRequest)
		return
	}

	// Handle /api/v1/keys/:key/incr
	if strings.HasSuffix(path, "/incr") {
		h.handleIncr(w, r, strings.TrimSuffix(path, "/incr"))
		return
	}

	key := path

	switch r.Method {
	case http.MethodGet:
		v, ok := h.server.store.Get(key)
		if !ok {
			jsonError(w, "key not found", http.StatusNotFound)
			return
		}
		ttl := h.server.store.TTL(key)
		jsonResponse(w, map[string]interface{}{
			"key":   key,
			"value": v,
			"type":  "string",
			"ttl":   ttl,
		})

	case http.MethodPut:
		var body struct {
			Value string `json:"value"`
			EX    int64  `json:"ex,omitempty"`
			PX    int64  `json:"px,omitempty"`
			NX    bool   `json:"nx,omitempty"`
			XX    bool   `json:"xx,omitempty"`
		}
		if err := readJSON(r, &body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}

		var ttl time.Duration
		if body.EX > 0 {
			ttl = time.Duration(body.EX) * time.Second
		}
		if body.PX > 0 {
			ttl = time.Duration(body.PX) * time.Millisecond
		}

		// Build AOF args
		aofArgs := []string{"SET", key, body.Value}
		if body.EX > 0 {
			aofArgs = append(aofArgs, "EX", strconv.FormatInt(body.EX, 10))
		} else if body.PX > 0 {
			aofArgs = append(aofArgs, "PX", strconv.FormatInt(body.PX, 10))
		}
		if body.NX {
			aofArgs = append(aofArgs, "NX")
		}
		if body.XX {
			aofArgs = append(aofArgs, "XX")
		}

		if body.NX {
			if err := h.persistWrite(aofArgs); err != nil {
				jsonError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !h.server.store.SetNX(key, body.Value, ttl) {
				jsonError(w, "key already exists", http.StatusConflict)
				return
			}
		} else if body.XX {
			if err := h.persistWrite(aofArgs); err != nil {
				jsonError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !h.server.store.SetXX(key, body.Value, ttl) {
				jsonError(w, "key does not exist", http.StatusNotFound)
				return
			}
		} else {
			if err := h.persistWrite(aofArgs); err != nil {
				jsonError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			h.server.store.Set(key, body.Value, ttl)
		}
		jsonResponse(w, map[string]interface{}{"status": "OK"})

	case http.MethodDelete:
		if err := h.persistWrite([]string{"DEL", key}); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n := h.server.store.Del(key)
		jsonResponse(w, map[string]interface{}{"deleted": n})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *HTTPServer) handleIncr(w http.ResponseWriter, r *http.Request, key string) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if key == "" {
		jsonError(w, "key required", http.StatusBadRequest)
		return
	}

	var body struct {
		Delta *int64 `json:"delta,omitempty"`
	}
	if err := readJSON(r, &body); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	delta := int64(1)
	if body.Delta != nil {
		delta = *body.Delta
	}

	if delta == 1 {
		if err := h.persistWrite([]string{"INCR", key}); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n, err := h.server.store.Incr(key)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"value": n})
	} else {
		if err := h.persistWrite([]string{"INCRBY", key, strconv.FormatInt(delta, 10)}); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n, err := h.server.store.IncrBy(key, delta)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"value": n})
	}
}

// ---------------------------------------------------------------------------
// SCAN
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleKeysScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	pattern := q.Get("pattern")
	if pattern == "" {
		pattern = "*"
	}
	cursor, _ := strconv.Atoi(q.Get("cursor"))
	count, _ := strconv.Atoi(q.Get("count"))
	if count <= 0 {
		count = 10
	}

	nextCursor, keys := h.server.store.Scan(cursor, count, pattern)
	jsonResponse(w, map[string]interface{}{
		"cursor": nextCursor,
		"keys":   keys,
	})
}

// ---------------------------------------------------------------------------
// Hash
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleHash(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/hash/")
	if path == "" {
		jsonError(w, "key required", http.StatusBadRequest)
		return
	}

	// Split into key and optional field: "mykey/myfield"
	parts := strings.SplitN(path, "/", 2)
	key := parts[0]
	field := ""
	if len(parts) == 2 {
		field = parts[1]
	}

	switch r.Method {
	case http.MethodGet:
		if field != "" {
			// HGET
			v, ok, err := h.server.store.HGet(key, field)
			if err != nil {
				jsonError(w, err.Error(), http.StatusBadRequest)
				return
			}
			if !ok {
				jsonError(w, "field not found", http.StatusNotFound)
				return
			}
			jsonResponse(w, map[string]interface{}{
				"key":   key,
				"field": field,
				"value": v,
			})
		} else {
			// HGETALL
			pairs, err := h.server.store.HGetAll(key)
			if err != nil {
				jsonError(w, err.Error(), http.StatusBadRequest)
				return
			}
			obj := make(map[string]string, len(pairs))
			for _, p := range pairs {
				obj[p.Field] = p.Value
			}
			jsonResponse(w, map[string]interface{}{
				"key":  key,
				"data": obj,
			})
		}

	case http.MethodPut:
		// HSET - body is {"field1":"val1","field2":"val2"}
		var body map[string]string
		if err := readJSON(r, &body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body) == 0 {
			jsonError(w, "no fields provided", http.StatusBadRequest)
			return
		}

		pairs := make([]store.HashPair, 0, len(body))
		aofArgs := []string{"HSET", key}
		for f, v := range body {
			pairs = append(pairs, store.HashPair{Field: f, Value: v})
			aofArgs = append(aofArgs, f, v)
		}

		if err := h.persistWrite(aofArgs); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n, err := h.server.store.HSet(key, pairs)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"created": n})

	case http.MethodDelete:
		if field == "" {
			jsonError(w, "field required for DELETE", http.StatusBadRequest)
			return
		}
		if err := h.persistWrite([]string{"HDEL", key, field}); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n, err := h.server.store.HDel(key, field)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"deleted": n})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleList(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/list/")
	if path == "" {
		jsonError(w, "key required", http.StatusBadRequest)
		return
	}

	// Check for sub-commands: /push, /pop
	parts := strings.SplitN(path, "/", 2)
	key := parts[0]
	sub := ""
	if len(parts) == 2 {
		sub = parts[1]
	}

	switch sub {
	case "push":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Values []string `json:"values"`
			Side   string   `json:"side"`
		}
		if err := readJSON(r, &body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body.Values) == 0 {
			jsonError(w, "values required", http.StatusBadRequest)
			return
		}

		left := strings.EqualFold(body.Side, "left")
		cmd := "RPUSH"
		if left {
			cmd = "LPUSH"
		}
		aofArgs := append([]string{cmd, key}, body.Values...)
		if err := h.persistWrite(aofArgs); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var n int
		var err error
		if left {
			n, err = h.server.store.LPush(key, body.Values...)
		} else {
			n, err = h.server.store.RPush(key, body.Values...)
		}
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"length": n})

	case "pop":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Side string `json:"side"`
		}
		if err := readJSON(r, &body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}

		left := strings.EqualFold(body.Side, "left")
		cmd := "RPOP"
		if left {
			cmd = "LPOP"
		}
		if err := h.persistWrite([]string{cmd, key}); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var v string
		var ok bool
		var err error
		if left {
			v, ok, err = h.server.store.LPop(key)
		} else {
			v, ok, err = h.server.store.RPop(key)
		}
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			jsonError(w, "list empty or not found", http.StatusNotFound)
			return
		}
		jsonResponse(w, map[string]interface{}{"value": v})

	default:
		// LRANGE
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := r.URL.Query()
		start, _ := strconv.Atoi(q.Get("start"))
		stop := -1
		if s := q.Get("stop"); s != "" {
			stop, _ = strconv.Atoi(s)
		}
		vals, err := h.server.store.LRange(key, start, stop)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{
			"key":    key,
			"values": vals,
		})
	}
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleSet(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/set/")
	if path == "" {
		jsonError(w, "key required", http.StatusBadRequest)
		return
	}

	parts := strings.SplitN(path, "/", 2)
	key := parts[0]
	member := ""
	if len(parts) == 2 {
		member = parts[1]
	}

	switch r.Method {
	case http.MethodGet:
		// SMEMBERS
		members, err := h.server.store.SMembers(key)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{
			"key":     key,
			"members": members,
		})

	case http.MethodPost:
		// SADD
		var body struct {
			Members []string `json:"members"`
		}
		if err := readJSON(r, &body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body.Members) == 0 {
			jsonError(w, "members required", http.StatusBadRequest)
			return
		}

		aofArgs := append([]string{"SADD", key}, body.Members...)
		if err := h.persistWrite(aofArgs); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n, err := h.server.store.SAdd(key, body.Members...)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"added": n})

	case http.MethodDelete:
		if member == "" {
			jsonError(w, "member required for DELETE", http.StatusBadRequest)
			return
		}
		if err := h.persistWrite([]string{"SREM", key, member}); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n, err := h.server.store.SRem(key, member)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"removed": n})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// Sorted Set
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleZSet(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/zset/")
	if path == "" {
		jsonError(w, "key required", http.StatusBadRequest)
		return
	}
	key := path

	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		start, _ := strconv.Atoi(q.Get("start"))
		stop := -1
		if s := q.Get("stop"); s != "" {
			stop, _ = strconv.Atoi(s)
		}
		withScores := q.Get("withscores") == "true"

		members, err := h.server.store.ZRange(key, start, stop)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}

		if withScores {
			type scoredMember struct {
				Member string  `json:"member"`
				Score  float64 `json:"score"`
			}
			result := make([]scoredMember, len(members))
			for i, m := range members {
				result[i] = scoredMember{Member: m.Member, Score: m.Score}
			}
			jsonResponse(w, map[string]interface{}{
				"key":     key,
				"members": result,
			})
		} else {
			names := make([]string, len(members))
			for i, m := range members {
				names[i] = m.Member
			}
			jsonResponse(w, map[string]interface{}{
				"key":     key,
				"members": names,
			})
		}

	case http.MethodPost:
		// ZADD
		var body struct {
			Members []struct {
				Member string  `json:"member"`
				Score  float64 `json:"score"`
			} `json:"members"`
		}
		if err := readJSON(r, &body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body.Members) == 0 {
			jsonError(w, "members required", http.StatusBadRequest)
			return
		}

		zm := make([]store.ZMember, len(body.Members))
		aofArgs := []string{"ZADD", key}
		for i, m := range body.Members {
			zm[i] = store.ZMember{Member: m.Member, Score: m.Score}
			aofArgs = append(aofArgs, strconv.FormatFloat(m.Score, 'f', -1, 64), m.Member)
		}

		if err := h.persistWrite(aofArgs); err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		n, err := h.server.store.ZAdd(key, zm)
		if err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		jsonResponse(w, map[string]interface{}{"added": n})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// Pub/Sub (publish only via REST)
// ---------------------------------------------------------------------------

func (h *HTTPServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	channel := strings.TrimPrefix(r.URL.Path, "/api/v1/publish/")
	if channel == "" {
		jsonError(w, "channel required", http.StatusBadRequest)
		return
	}

	var body struct {
		Message string `json:"message"`
	}
	if err := readJSON(r, &body); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	n := h.server.pubsub.Publish(channel, body.Message)
	jsonResponse(w, map[string]interface{}{"receivers": n})
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	keys, streams := h.server.store.Stats()
	self := h.server.cluster.Self()
	appendOnly := h.server.aof != nil

	jsonResponse(w, map[string]interface{}{
		"vole_version":  "0.1.0",
		"node_id":       self.ID,
		"addr":          self.Addr,
		"append_only":   appendOnly,
		"snapshot_path": h.server.snapshotPath,
		"keys":          keys,
		"streams":       streams,
	})
}

func (h *HTTPServer) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := h.persistWrite([]string{"FLUSHDB"}); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.server.store.FlushDB()
	jsonResponse(w, map[string]interface{}{"status": "OK"})
}

func (h *HTTPServer) handleDBSize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	n := h.server.store.DBSize()
	jsonResponse(w, map[string]interface{}{"dbsize": n})
}

// GET /api/v1/search/hash?pattern=user:*&where=age>18,status=active&limit=50
func (h *HTTPServer) handleHashSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	pattern := r.URL.Query().Get("pattern")
	if pattern == "" {
		pattern = "*"
	}

	whereStr := r.URL.Query().Get("where")
	var conditions []store.SearchCondition
	if whereStr != "" {
		for _, cond := range strings.Split(whereStr, ",") {
			cond = strings.TrimSpace(cond)
			// Parse "field op value"
			// Try operators in order of length (>=, <=, !=, CONTAINS, STARTSWITH, >, <, =)
			parsed := false
			for _, op := range []string{">=", "<=", "!=", "CONTAINS ", "STARTSWITH ", ">", "<", "="} {
				opTrim := strings.TrimSpace(op)
				idx := strings.Index(cond, op)
				if idx > 0 {
					field := strings.TrimSpace(cond[:idx])
					value := strings.TrimSpace(cond[idx+len(op):])
					conditions = append(conditions, store.SearchCondition{Field: field, Op: opTrim, Value: value})
					parsed = true
					break
				}
			}
			if !parsed {
				jsonError(w, "invalid condition: "+cond, http.StatusBadRequest)
				return
			}
		}
	}

	offset := 0
	count := 100 // default limit
	if v := r.URL.Query().Get("offset"); v != "" {
		offset, _ = strconv.Atoi(v)
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		count, _ = strconv.Atoi(v)
	}

	results := h.server.store.HSearch(pattern, conditions, offset, count)

	type resultJSON struct {
		Key    string            `json:"key"`
		Fields map[string]string `json:"fields"`
	}
	out := make([]resultJSON, len(results))
	for i, res := range results {
		out[i] = resultJSON{Key: res.Key, Fields: res.Fields}
	}

	jsonResponse(w, map[string]interface{}{
		"results": out,
		"count":   len(out),
	})
}

// ---------------------------------------------------------------------------
// SSE event stream — GET /api/v1/events?channels=...&patterns=...
// ---------------------------------------------------------------------------

// handleEvents opens a Server-Sent Events stream that relays pub/sub
// messages to the HTTP client. Query parameters:
//
//	channels — comma-separated list of literal channel names
//	patterns — comma-separated list of glob patterns (PSUBSCRIBE style)
//
// Example: GET /api/v1/events?patterns=__keyspace__:user:*,__keyevent__:expired
func (h *HTTPServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	channels := strings.Split(r.URL.Query().Get("channels"), ",")
	patterns := strings.Split(r.URL.Query().Get("patterns"), ",")

	// Subscribe to direct channels.
	type sub struct {
		name string
		ch   chan string
	}
	var subs []sub
	for _, ch := range channels {
		if ch == "" {
			continue
		}
		subCh := h.server.pubsub.Subscribe(ch)
		subs = append(subs, sub{name: ch, ch: subCh})
	}

	// Subscribe to patterns.
	type psub struct {
		pattern string
		ch      chan PSubMessage
	}
	var psubs []psub
	for _, p := range patterns {
		if p == "" {
			continue
		}
		pch := h.server.pubsub.PSubscribe(p)
		psubs = append(psubs, psub{pattern: p, ch: pch})
	}

	defer func() {
		for _, s := range subs {
			h.server.pubsub.Unsubscribe(s.name, s.ch)
		}
		for _, p := range psubs {
			h.server.pubsub.PUnsubscribe(p.pattern, p.ch)
		}
	}()

	// Merge all subscription channels into one.
	type sseEvent struct {
		Channel string `json:"channel"`
		Pattern string `json:"pattern,omitempty"`
		Data    string `json:"data"`
	}
	merged := make(chan sseEvent, 128)
	done := make(chan struct{})
	defer close(done)

	for _, s := range subs {
		go func(name string, ch chan string) {
			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					select {
					case merged <- sseEvent{Channel: name, Data: msg}:
					case <-done:
						return
					}
				case <-done:
					return
				}
			}
		}(s.name, s.ch)
	}

	for _, p := range psubs {
		go func(pat string, ch chan PSubMessage) {
			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					select {
					case merged <- sseEvent{Channel: msg.Channel, Pattern: msg.Pattern, Data: msg.Data}:
					case <-done:
						return
					}
				case <-done:
					return
				}
			}
		}(p.pattern, p.ch)
	}

	// Stream events as SSE until client disconnects.
	ctx := r.Context()
	for {
		select {
		case ev := <-merged:
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Webhooks — /api/v1/webhooks
// ---------------------------------------------------------------------------

// handleWebhooks manages webhook registration via HTTP.
//
//	POST   — register a webhook:   {"pattern":"user:*","event":"expired","url":"https://..."}
//	DELETE — unregister a webhook: {"pattern":"user:*","event":"expired","url":"https://..."}
//	GET    — list all webhooks
func (h *HTTPServer) handleWebhooks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		jsonResponse(w, h.server.webhooks.List())
	case http.MethodPost:
		var req struct {
			Pattern string `json:"pattern"`
			Event   string `json:"event"`
			URL     string `json:"url"`
		}
		if err := readJSON(r, &req); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Pattern == "" || req.Event == "" || req.URL == "" {
			jsonError(w, "pattern, event, and url are required", http.StatusBadRequest)
			return
		}
		h.server.webhooks.Register(req.Pattern, req.Event, req.URL)
		jsonResponse(w, map[string]string{"status": "OK"})
	case http.MethodDelete:
		var req struct {
			Pattern string `json:"pattern"`
			Event   string `json:"event"`
			URL     string `json:"url"`
		}
		if err := readJSON(r, &req); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Pattern == "" || req.Event == "" || req.URL == "" {
			jsonError(w, "pattern, event, and url are required", http.StatusBadRequest)
			return
		}
		h.server.webhooks.Unregister(req.Pattern, req.Event, req.URL)
		jsonResponse(w, map[string]string{"status": "OK"})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// Cluster management
// ---------------------------------------------------------------------------

// GET /api/v1/cluster/info — cluster status and node list
func (h *HTTPServer) handleClusterInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	nodes := h.server.cluster.Nodes()
	type nodeJSON struct {
		ID       string    `json:"id"`
		Addr     string    `json:"addr"`
		Slots    [2]int    `json:"slots"`
		Self     bool      `json:"self"`
		State    string    `json:"state"`
		LastPong time.Time `json:"last_pong"`
	}
	out := make([]nodeJSON, len(nodes))
	for i, n := range nodes {
		state := string(n.State)
		if n.Self {
			state = "self"
		}
		if state == "" {
			state = "online"
		}
		out[i] = nodeJSON{
			ID:       n.ID,
			Addr:     n.Addr,
			Slots:    [2]int{n.Start, n.End},
			Self:     n.Self,
			State:    state,
			LastPong: n.LastPong,
		}
	}
	jsonResponse(w, map[string]interface{}{
		"nodes":      out,
		"node_count": len(nodes),
		"self_id":    h.server.cluster.Self().ID,
	})
}

// POST /api/v1/cluster/meet — add a node to the cluster
func (h *HTTPServer) handleClusterMeet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if body.Addr == "" {
		jsonError(w, "addr is required", http.StatusBadRequest)
		return
	}
	if err := h.server.cluster.Meet(body.Addr); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, map[string]string{"status": "OK"})
}

// DELETE /api/v1/cluster/nodes/:id — remove a node from the cluster
func (h *HTTPServer) handleClusterForget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	nodeID := strings.TrimPrefix(r.URL.Path, "/api/v1/cluster/nodes/")
	if nodeID == "" {
		jsonError(w, "node ID required", http.StatusBadRequest)
		return
	}
	if err := h.server.cluster.Forget(nodeID); err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	jsonResponse(w, map[string]string{"status": "OK"})
}

// ---------------------------------------------------------------------------
// Rate Limiting — POST/GET/DELETE /api/v1/ratelimit/:key
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleRateLimit(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/ratelimit/")
	if key == "" {
		jsonError(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		var body struct {
			Max    int64 `json:"max"`
			Window int64 `json:"window"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			jsonError(w, err.Error(), http.StatusBadRequest)
			return
		}
		allowed, remaining, retryAfterMs, resetAtMs := h.server.store.RateLimit(
			key, body.Max, time.Duration(body.Window)*time.Second,
		)
		status := http.StatusOK
		if !allowed {
			status = http.StatusTooManyRequests
		}
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAtMs, 10))
		if retryAfterMs > 0 {
			w.Header().Set("Retry-After", strconv.FormatInt(retryAfterMs/1000+1, 10))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"allowed":        allowed,
			"remaining":      remaining,
			"retry_after_ms": retryAfterMs,
			"reset_at_ms":    resetAtMs,
		})
	case http.MethodGet:
		remaining, resetAtMs := h.server.store.RateLimitPeek(key)
		jsonResponse(w, map[string]interface{}{"remaining": remaining, "reset_at_ms": resetAtMs})
	case http.MethodDelete:
		h.server.store.RateLimitReset(key)
		jsonResponse(w, map[string]string{"status": "OK"})
	default:
		jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ---------------------------------------------------------------------------
// Metrics — GET /metrics (Prometheus text format)
// ---------------------------------------------------------------------------

func (h *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprint(w, h.server.metrics.PrometheusFormat(h.server))
}

