package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vole/internal/resp"
	"vole/internal/store"
)

type connTx struct {
	active  bool
	queue   [][]string
	watched map[string]uint64 // key -> version at watch time
}

type Server struct {
	addr             string
	store            *store.Store
	namespaces       *NamespaceManager
	aof              *AOF
	cluster          *Cluster
	pubsub           *PubSub
	webhooks         *WebhookManager
	snapshotPath     string
	snapshotInterval time.Duration
	persistMu        sync.Mutex
	connCount        int64
	httpAddr         string
	audit            *AuditLog
	cron             *CronManager
	schemas          *SchemaManager
	password         string
	tlsCert          string
	tlsKey           string
	metrics          *Metrics
	repl             *ReplicationState
	lastSave         time.Time
	scripts          *ScriptManager
	clients          *ClientManager
	slowlog          *SlowLog
}

type Options struct {
	Addr             string
	AOFPath          string
	AppendOnly       bool
	AppendFsync      string
	SnapshotPath     string
	SnapshotInterval time.Duration
	NodeID           string
	Peers            string
	MaxMemory        int64
	MaxMemoryPolicy  string
	HTTPAddr         string
	Password         string
	TLSCert          string
	TLSKey           string
}

func New(addr, dataPath, nodeID, peers string) (*Server, error) {
	return NewWithOptions(Options{
		Addr:        addr,
		AOFPath:     dataPath,
		AppendOnly:  dataPath != "",
		AppendFsync: FsyncAlways,
		NodeID:      nodeID,
		Peers:       peers,
	})
}

func NewWithOptions(opts Options) (*Server, error) {
	st := store.New()
	if opts.MaxMemory > 0 {
		st.SetMaxMemory(opts.MaxMemory)
	}
	if opts.MaxMemoryPolicy != "" {
		st.SetEvictPolicy(opts.MaxMemoryPolicy)
	}
	st.StartExpiry(500 * time.Millisecond)
	if err := LoadSnapshot(opts.SnapshotPath, st); err != nil {
		return nil, err
	}
	if opts.AppendOnly {
		if err := ReplayAOF(opts.AOFPath, st); err != nil {
			return nil, err
		}
	}
	var aof *AOF
	var err error
	if opts.AppendOnly {
		aof, err = OpenAOF(opts.AOFPath, opts.AppendFsync)
		if err != nil {
			return nil, err
		}
	}
	if opts.NodeID == "" {
		opts.NodeID = RandomNodeID(opts.Addr)
	}
	ps := NewPubSub()
	wh := NewWebhookManager()

	// Wire key-change notifications: publish to __keyevent__ and __keyspace__
	// channels and fire webhooks. The callback is invoked under the store's
	// write lock, so we keep it cheap — PubSub.Publish is non-blocking
	// (drops if subscriber buffer full) and WebhookManager.Fire dispatches
	// HTTP calls in goroutines.
	st.OnChange(func(event, key, ns string) {
		ps.Publish("__keyevent__:"+event, key)
		ps.Publish("__keyspace__:"+key, event)
		wh.Fire(event, key)
	})

	return &Server{
		addr:             opts.Addr,
		store:            st,
		namespaces:       NewNamespaceManager(st),
		aof:              aof,
		cluster:          NewCluster(opts.NodeID, opts.Addr, opts.Peers),
		pubsub:           ps,
		webhooks:         wh,
		snapshotPath:     opts.SnapshotPath,
		snapshotInterval: opts.SnapshotInterval,
		httpAddr:         opts.HTTPAddr,
		audit:            NewAuditLog(10000),
		cron:             NewCronManager(),
		schemas:          NewSchemaManager(),
		password:         opts.Password,
		tlsCert:          opts.TLSCert,
		tlsKey:           opts.TLSKey,
		metrics:          NewMetrics(),
		repl:             NewReplicationState(),
		scripts:          NewScriptManager(),
		clients:          NewClientManager(),
		slowlog:          NewSlowLog(10*time.Millisecond, 128),
	}, nil
}

func (s *Server) Close() error {
	log.Println("shutting down: stopping replication...")
	if s.repl != nil {
		s.repl.StopFollowing()
	}

	// Brief drain period for in-flight connections to complete.
	log.Println("shutting down: draining connections...")
	time.Sleep(1 * time.Second)

	log.Println("shutting down: stopping namespaces...")
	s.namespaces.StopAll()

	log.Println("shutting down: saving final snapshot...")
	if s.snapshotPath != "" {
		if err := s.saveSnapshot(true); err != nil {
			log.Printf("final snapshot failed: %v", err)
		}
	}

	log.Println("shutting down: stopping background expiry...")
	s.store.StopExpiry()

	log.Println("shutting down: closing AOF...")
	if err := s.aof.Close(); err != nil {
		return err
	}

	log.Println("shutdown complete")
	return nil
}

func (s *Server) Cluster() *Cluster {
	return s.cluster
}

// HTTPAddr returns the configured HTTP API listen address (empty if disabled).
func (s *Server) HTTPAddr() string { return s.httpAddr }

func (s *Server) ListenAndServe(ctx context.Context) error {
	var ln net.Listener
	var err error

	if s.tlsCert != "" && s.tlsKey != "" {
		cert, certErr := tls.LoadX509KeyPair(s.tlsCert, s.tlsKey)
		if certErr != nil {
			return fmt.Errorf("failed to load TLS certificate: %w", certErr)
		}
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
		ln, err = tls.Listen("tcp", s.addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", s.addr)
	}
	if err != nil {
		return err
	}
	defer ln.Close()
	if s.snapshotPath != "" && s.snapshotInterval > 0 {
		go s.snapshotLoop(ctx)
	}
	s.cluster.StartHeartbeat(ctx, 5*time.Second)
	s.cron.StartLoop(ctx, func(args []string) error {
		var buf bytes.Buffer
		bw := resp.NewWriter(&buf)
		if err := s.exec(bw, args); err != nil {
			return err
		}
		return bw.Flush()
	})
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if atomic.LoadInt64(&s.connCount) >= 10000 {
			conn.Close()
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	atomic.AddInt64(&s.connCount, 1)
	defer atomic.AddInt64(&s.connCount, -1)
	s.metrics.IncrConnections()
	defer s.metrics.DecrConnections()
	defer conn.Close()
	clientID := s.clients.Register(conn.RemoteAddr().String())
	defer s.clients.Unregister(clientID)
	rd := resp.NewReader(conn)
	wr := resp.NewWriter(conn)
	var tx connTx
	authenticated := s.password == "" // auto-authenticated if no password set
	currentNS := "default"
	nsPrefix := "" // empty for default namespace, "name:" for others
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		args, err := rd.ReadCommand()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			_ = wr.Error("ERR " + err.Error())
			_ = wr.Flush()
			return
		}
		if len(args) == 0 {
			continue
		}
		cmd := strings.ToUpper(args[0])

		// Authentication gate: reject all commands except AUTH, PING, and
		// HELLO when a password is required and the client hasn't authenticated.
		if !authenticated && cmd != "AUTH" && cmd != "PING" && cmd != "HELLO" {
			_ = wr.Error("NOAUTH Authentication required.")
			if rd.Buffered() == 0 {
				if err := wr.Flush(); err != nil {
					return
				}
			}
			continue
		}

		// Handle AUTH command
		if cmd == "AUTH" {
			if s.password == "" {
				_ = wr.Error("ERR Client sent AUTH, but no password is set")
			} else {
				pw := ""
				if len(args) == 2 {
					pw = args[1]
				} else if len(args) == 3 {
					pw = args[2] // AUTH username password — ignore username
				} else {
					_ = wr.Error("ERR wrong number of arguments for 'auth' command")
					if rd.Buffered() == 0 {
						if err := wr.Flush(); err != nil {
							return
						}
					}
					continue
				}
				if pw == s.password {
					authenticated = true
					_ = wr.Simple("OK")
				} else {
					_ = wr.Error("WRONGPASS invalid password")
				}
			}
			if rd.Buffered() == 0 {
				if err := wr.Flush(); err != nil {
					return
				}
			}
			continue
		}

		if cmd == "SUBSCRIBE" {
			s.subscribe(conn, wr, args[1:])
			return
		}
		if cmd == "PSUBSCRIBE" {
			s.psubscribe(conn, wr, args[1:])
			return
		}
		if cmd == "REPLSYNC" {
			s.handleReplSync(conn, wr)
			return
		}
		if cmd == "UNSUBSCRIBE" {
			// UNSUBSCRIBE outside of subscription mode is a no-op;
			// confirm with zero subscriptions remaining.
			if err := wr.ArrayLen(3); err == nil {
				ch := ""
				if len(args) > 1 {
					ch = args[1]
				}
				_ = wr.Bulk("unsubscribe")
				_ = wr.Bulk(ch)
				_ = wr.Int(0)
			}
			_ = wr.Flush()
			continue
		}
		if cmd == "PUNSUBSCRIBE" {
			// PUNSUBSCRIBE outside of subscription mode is a no-op.
			if err := wr.ArrayLen(3); err == nil {
				pat := ""
				if len(args) > 1 {
					pat = args[1]
				}
				_ = wr.Bulk("punsubscribe")
				_ = wr.Bulk(pat)
				_ = wr.Int(0)
			}
			_ = wr.Flush()
			continue
		}

		if cmd == "QUIT" {
			_ = wr.Simple("OK")
			_ = wr.Flush()
			return
		}

		if cmd == "NAMESPACE" {
			if len(args) < 2 {
				_ = wr.Error("ERR wrong number of arguments for NAMESPACE")
			} else {
				subcmd := strings.ToUpper(args[1])
				switch subcmd {
				case "CREATE":
					if len(args) != 3 {
						_ = wr.Error("ERR wrong number of arguments for NAMESPACE CREATE")
					} else if err := s.namespaces.Create(args[2]); err != nil {
						_ = wr.Error("ERR " + err.Error())
					} else {
						_ = wr.Simple("OK")
					}
				case "USE":
					if len(args) != 3 {
						_ = wr.Error("ERR wrong number of arguments for NAMESPACE USE")
					} else {
						name := args[2]
						if name == "default" {
							nsPrefix = ""
							currentNS = "default"
							_ = wr.Simple("OK")
						} else if _, ok := s.namespaces.Get(name); ok {
							nsPrefix = name + ":"
							currentNS = name
							_ = wr.Simple("OK")
						} else {
							_ = wr.Error("ERR namespace does not exist")
						}
					}
				case "LIST":
					names := s.namespaces.List()
					_ = writeBulkStrings(wr, names)
				case "DROP":
					if len(args) != 3 {
						_ = wr.Error("ERR wrong number of arguments for NAMESPACE DROP")
					} else {
						if args[2] == currentNS {
							_ = wr.Error("ERR cannot drop the currently active namespace")
						} else if err := s.namespaces.Drop(args[2]); err != nil {
							_ = wr.Error("ERR " + err.Error())
						} else {
							_ = wr.Simple("OK")
						}
					}
				case "CURRENT":
					_ = wr.Bulk(currentNS)
				default:
					_ = wr.Error("ERR unknown NAMESPACE subcommand '" + args[1] + "'")
				}
			}
			// flush and continue — NAMESPACE is handled entirely here
			if rd.Buffered() == 0 {
				if err := wr.Flush(); err != nil {
					return
				}
			}
			continue
		}

		if cmd == "CLIENT" {
			if len(args) < 2 {
				_ = wr.Error("ERR wrong number of arguments for CLIENT")
			} else {
				switch strings.ToUpper(args[1]) {
				case "LIST":
					_ = wr.Bulk(s.clients.FormatList())
				case "SETNAME":
					if len(args) != 3 {
						_ = wr.Error("ERR wrong number of arguments for CLIENT SETNAME")
					} else {
						s.clients.SetName(clientID, args[2])
						_ = wr.Simple("OK")
					}
				case "GETNAME":
					name := s.clients.GetName(clientID)
					if name == "" {
						_ = wr.Null()
					} else {
						_ = wr.Bulk(name)
					}
				case "ID":
					_ = wr.Int(int64(clientID))
				case "KILL":
					if len(args) >= 4 && strings.EqualFold(args[2], "ID") {
						id, err := strconv.ParseUint(args[3], 10, 64)
						if err != nil {
							_ = wr.Error("ERR " + err.Error())
						} else if s.clients.Kill(id) {
							_ = wr.Int(1)
						} else {
							_ = wr.Int(0)
						}
					} else {
						_ = wr.Int(0)
					}
				case "INFO":
					_ = wr.Bulk(fmt.Sprintf("id=%d", clientID))
				default:
					_ = wr.Error(fmt.Sprintf("ERR unsupported CLIENT subcommand %q", args[1]))
				}
			}
			s.clients.RecordCommand(clientID, cmd)
			if rd.Buffered() == 0 {
				if err := wr.Flush(); err != nil {
					return
				}
			}
			continue
		}

		// Apply namespace key prefix for non-default namespaces
		if nsPrefix != "" {
			args = prefixKeyArgs(args, nsPrefix)
		}

		// For commands that return key names, strip the namespace prefix from output
		if nsPrefix != "" && (cmd == "KEYS" || cmd == "RANDOMKEY" || cmd == "SCAN") {
			handled := true
			switch cmd {
			case "KEYS":
				if len(args) != 2 {
					_ = wr.Error("ERR wrong number of arguments for 'keys' command")
				} else {
					keys := s.store.Keys(args[1])
					stripped := make([]string, len(keys))
					for i, k := range keys {
						stripped[i] = stripKeyPrefix(k, nsPrefix)
					}
					_ = writeBulkStrings(wr, stripped)
				}
			case "RANDOMKEY":
				if len(args) != 1 {
					_ = wr.Error("ERR wrong number of arguments for 'randomkey' command")
				} else {
					keys := s.store.Keys(nsPrefix + "*")
					if len(keys) == 0 {
						_ = wr.Null()
					} else {
						_ = wr.Bulk(stripKeyPrefix(keys[0], nsPrefix))
					}
				}
			case "SCAN":
				if len(args) < 2 {
					_ = wr.Error("ERR wrong number of arguments for 'scan' command")
				} else {
					cursor, cerr := strconv.Atoi(args[1])
					if cerr != nil {
						_ = wr.Error("ERR invalid cursor")
					} else {
						pattern := "*"
						count := 10
						scanErr := false
						for i := 2; i < len(args); i += 2 {
							if i+1 >= len(args) {
								_ = wr.Error("ERR syntax error")
								scanErr = true
								break
							}
							switch strings.ToUpper(args[i]) {
							case "MATCH":
								pattern = args[i+1]
							case "COUNT":
								n, ne := strconv.Atoi(args[i+1])
								if ne != nil {
									_ = wr.Error("ERR value is not an integer or out of range")
									scanErr = true
								} else {
									count = n
								}
							}
							if scanErr {
								break
							}
						}
						if !scanErr {
							next, keys := s.store.Scan(cursor, count, pattern)
							stripped := make([]string, len(keys))
							for i, k := range keys {
								stripped[i] = stripKeyPrefix(k, nsPrefix)
							}
							_ = wr.ArrayLen(2)
							_ = wr.Bulk(strconv.Itoa(next))
							_ = writeBulkStrings(wr, stripped)
						}
					}
				}
			default:
				handled = false
			}
			if handled {
				if rd.Buffered() == 0 {
					if flushErr := wr.Flush(); flushErr != nil {
						return
					}
				}
				continue
			}
		}

		switch cmd {
		case "MULTI":
			if tx.active {
				_ = wr.Error("ERR MULTI calls can not be nested")
			} else {
				tx.active = true
				tx.queue = nil
				_ = wr.Simple("OK")
			}
		case "EXEC":
			if !tx.active {
				_ = wr.Error("ERR EXEC without MULTI")
			} else {
				if tx.watched != nil && s.store.KeysModifiedSince(tx.watched) {
					_ = wr.NullArray()
				} else {
					s.execTransaction(wr, tx.queue)
				}
				tx.active = false
				tx.queue = nil
				tx.watched = nil
			}
		case "DISCARD":
			if !tx.active {
				_ = wr.Error("ERR DISCARD without MULTI")
			} else {
				tx.active = false
				tx.queue = nil
				tx.watched = nil
				_ = wr.Simple("OK")
			}
		case "WATCH":
			if tx.active {
				_ = wr.Error("ERR WATCH inside MULTI is not allowed")
			} else if len(args) < 2 {
				_ = wr.Error("ERR wrong number of arguments for WATCH")
			} else {
				if tx.watched == nil {
					tx.watched = make(map[string]uint64)
				}
				versions := s.store.KeyVersions(args[1:])
				for k, v := range versions {
					tx.watched[k] = v
				}
				_ = wr.Simple("OK")
			}
		case "UNWATCH":
			tx.watched = nil
			_ = wr.Simple("OK")
		default:
			if tx.active {
				tx.queue = append(tx.queue, args)
				_ = wr.Simple("QUEUED")
			} else {
				if isWriteCommand(cmd) {
					if s.repl.IsReplica() {
						_ = wr.Error("READONLY You can't write against a read only replica")
						break
					}
					if err := s.store.EvictIfNeeded(); err != nil {
						_ = wr.Error("OOM " + err.Error())
						break
					}
				}
				cmdStart := time.Now()
				if err := s.exec(wr, args); err != nil {
					_ = wr.Error("ERR " + err.Error())
				} else {
					if isWriteCommand(cmd) && s.repl.Role() == RoleMaster {
						s.repl.PropagateToReplicas(args)
					}
					if s.audit.Enabled() && isWriteCommand(cmd) {
						auditKey := ""
						if len(args) > 1 {
							auditKey = args[1]
						}
						s.audit.Record(auditKey, cmd, conn.RemoteAddr().String(), args)
					}
				}
				s.slowlog.Record(time.Since(cmdStart), args, conn.RemoteAddr().String())
				s.clients.RecordCommand(clientID, cmd)
				s.metrics.IncrCommands()
			}
		}

		// Only flush if no more pipelined commands are buffered
		if rd.Buffered() == 0 {
			if err := wr.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Server) execTransaction(w *resp.Writer, commands [][]string) {
	if len(commands) == 0 {
		_ = w.ArrayLen(0)
		return
	}

	// Execute each queued command, capturing its RESP response in a buffer.
	responses := make([][]byte, len(commands))
	for i, args := range commands {
		var buf bytes.Buffer
		bw := resp.NewWriter(&buf)
		if err := s.exec(bw, args); err != nil {
			_ = bw.Error("ERR " + err.Error())
		}
		_ = bw.Flush()
		responses[i] = buf.Bytes()
	}

	// Write an array header followed by the raw RESP responses.
	_ = w.ArrayLen(len(commands))
	for _, r := range responses {
		_ = w.WriteRaw(r)
	}
}

func (s *Server) exec(w *resp.Writer, args []string) error {
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		if len(args) > 1 {
			return w.Bulk(args[1])
		}
		return w.Simple("PONG")
	case "ECHO":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		return w.Bulk(args[1])
	case "GET":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		v, ok := s.store.Get(args[1])
		if ok {
			s.metrics.IncrHits()
		} else {
			s.metrics.IncrMisses()
		}
		if !ok {
			return w.Null()
		}
		return w.Bulk(v)
	case "MGET":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		results := s.store.MGet(args[1:]...)
		if err := w.ArrayLen(len(results)); err != nil {
			return err
		}
		for _, result := range results {
			if !result.OK {
				if err := w.Null(); err != nil {
					return err
				}
				continue
			}
			if err := w.Bulk(result.Value); err != nil {
				return err
			}
		}
		return nil
	case "SET":
		return s.set(w, args)
	case "SETNX":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if s.store.SetNX(args[1], args[2], 0) {
			persist := []string{"SETABS", args[1], args[2], "0"}
			_ = s.aof.Append(persist)
			return w.Int(1)
		}
		return w.Int(0)
	case "SETEX":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		sec, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		if sec <= 0 {
			return errors.New("invalid expire time in SETEX")
		}
		ttl := time.Duration(sec) * time.Second
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		expireAt := time.Now().Add(ttl)
		persist := []string{"SETABS", args[1], args[3], strconv.FormatInt(expireAt.UnixNano(), 10)}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		s.store.SetAbsolute(args[1], args[3], expireAt)
		return w.Simple("OK")
	case "PSETEX":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		ms, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		if ms <= 0 {
			return errors.New("invalid expire time in PSETEX")
		}
		ttl := time.Duration(ms) * time.Millisecond
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		expireAt := time.Now().Add(ttl)
		persist := []string{"SETABS", args[1], args[3], strconv.FormatInt(expireAt.UnixNano(), 10)}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		s.store.SetAbsolute(args[1], args[3], expireAt)
		return w.Simple("OK")
	case "MSET":
		return s.mset(w, args)
	case "EXISTS":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		return w.Int(int64(s.store.Exists(args[1:]...)))
	case "TYPE":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		return w.Simple(s.store.Type(args[1]))
	case "KEYS":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		return writeBulkStrings(w, s.store.Keys(args[1]))
	case "SCAN":
		return s.scan(w, args)
	case "DEL":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		return w.Int(int64(s.store.Del(args[1:]...)))
	case "INCR":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if existing, ok := s.store.Get(args[1]); ok {
			if _, err := strconv.ParseInt(existing, 10, 64); err != nil {
				return errors.New("value is not an integer")
			}
		}
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n, err := s.store.Incr(args[1])
		if err != nil {
			return err
		}
		return w.Int(n)
	case "INCRBY":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		delta, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n, err := s.store.IncrBy(args[1], delta)
		if err != nil {
			return err
		}
		return w.Int(n)
	case "DECRBY":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		delta, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n, err := s.store.DecrBy(args[1], delta)
		if err != nil {
			return err
		}
		return w.Int(n)
	case "DECR":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n, err := s.store.IncrBy(args[1], -1)
		if err != nil {
			return err
		}
		return w.Int(n)
	case "INCRBYFLOAT":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		delta, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		result, err := s.store.IncrByFloat(args[1], delta)
		if err != nil {
			return err
		}
		return w.Bulk(strconv.FormatFloat(result, 'f', -1, 64))
	case "EXPIRE":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		sec, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if s.store.TTL(args[1]) == -2 {
			return w.Int(0)
		}
		expireAt := time.Now().Add(time.Duration(sec) * time.Second)
		persist := []string{"EXPIREATABS", args[1], strconv.FormatInt(expireAt.UnixNano(), 10)}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		if !s.store.ExpireAt(args[1], expireAt) {
			return w.Int(0)
		}
		return w.Int(1)
	case "PEXPIRE":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		ms, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if s.store.TTL(args[1]) == -2 {
			return w.Int(0)
		}
		expireAt := time.Now().Add(time.Duration(ms) * time.Millisecond)
		persist := []string{"EXPIREATABS", args[1], strconv.FormatInt(expireAt.UnixNano(), 10)}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		if !s.store.ExpireAt(args[1], expireAt) {
			return w.Int(0)
		}
		return w.Int(1)
	case "TTL":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		return w.Int(s.store.TTL(args[1]))
	case "PTTL":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		return w.Int(s.store.PTTL(args[1]))
	case "HSET":
		return s.hset(w, args)
	case "HGET":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		value, ok, err := s.store.HGet(args[1], args[2])
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		return w.Bulk(value)
	case "HGETALL":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		pairs, err := s.store.HGetAll(args[1])
		if err != nil {
			return err
		}
		return writeHashPairs(w, pairs)
	case "HSEARCH":
		return s.hsearch(w, args)
	case "HDEL":
		return s.hdel(w, args)
	case "HINCRBY":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		delta, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return err
		}
		if err := s.schemas.Validate(args[1], map[string]string{args[2]: args[3]}); err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n, err := s.store.HIncrBy(args[1], args[2], delta)
		if err != nil {
			return err
		}
		return w.Int(n)
	case "HSETNX":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		if err := s.schemas.Validate(args[1], map[string]string{args[2]: args[3]}); err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		ok, err := s.store.HSetNX(args[1], args[2], args[3])
		if err != nil {
			return err
		}
		if ok {
			if err := s.aof.Append([]string{"HSET", args[1], args[2], args[3]}); err != nil {
				return err
			}
			return w.Int(1)
		}
		return w.Int(0)
	case "LPUSH":
		return s.listPush(w, args, true)
	case "RPUSH":
		return s.listPush(w, args, false)
	case "LPOP":
		return s.listPop(w, args, true)
	case "RPOP":
		return s.listPop(w, args, false)
	case "LRANGE":
		return s.lrange(w, args)
	case "LINDEX":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		index, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		val, ok, err := s.store.LIndex(args[1], index)
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		return w.Bulk(val)
	case "LSET":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		index, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		if err := s.store.LSet(args[1], index, args[3]); err != nil {
			return err
		}
		return w.Simple("OK")
	case "LINSERT":
		if len(args) != 5 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		before := strings.EqualFold(args[2], "BEFORE")
		if !before && !strings.EqualFold(args[2], "AFTER") {
			return wrongArgs(cmd)
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n, err := s.store.LInsert(args[1], before, args[3], args[4])
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "LPOS":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		pos, ok, err := s.store.LPos(args[1], args[2])
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		return w.Int(pos)
	case "LREM":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		removed, err := s.store.LRem(args[1], count, args[3])
		if err != nil {
			return err
		}
		return w.Int(int64(removed))
	case "SADD":
		return s.sadd(w, args)
	case "SREM":
		return s.srem(w, args)
	case "SMEMBERS":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		members, err := s.store.SMembers(args[1])
		if err != nil {
			return err
		}
		return writeBulkStrings(w, members)
	case "SISMEMBER":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		ok, err := s.store.SIsMember(args[1], args[2])
		if err != nil {
			return err
		}
		if ok {
			return w.Int(1)
		}
		return w.Int(0)
	case "SCARD":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		n, err := s.store.SCard(args[1])
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "SRANDMEMBER":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count := 1
		returnArray := len(args) == 3
		if len(args) == 3 {
			n, err := strconv.Atoi(args[2])
			if err != nil {
				return err
			}
			count = n
		}
		members, err := s.store.SRandMember(args[1], count)
		if err != nil {
			return err
		}
		if !returnArray {
			if len(members) == 0 {
				return w.Null()
			}
			return w.Bulk(members[0])
		}
		return writeBulkStrings(w, members)
	case "SMOVE":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		if ok, err := s.ensureOwns(w, args[2]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		moved, err := s.store.SMove(args[1], args[2], args[3])
		if err != nil {
			return err
		}
		if moved {
			return w.Int(1)
		}
		return w.Int(0)
	case "SPOP":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count := 1
		returnArray := len(args) == 3
		if len(args) == 3 {
			n, err := strconv.Atoi(args[2])
			if err != nil {
				return err
			}
			count = n
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		members, err := s.store.SPop(args[1], count)
		if err != nil {
			return err
		}
		// Log individual SREM commands for AOF replay
		if len(members) > 0 {
			remArgs := append([]string{"SREM", args[1]}, members...)
			_ = s.aof.Append(remArgs)
		}
		if !returnArray {
			if len(members) == 0 {
				return w.Null()
			}
			return w.Bulk(members[0])
		}
		return writeBulkStrings(w, members)
	case "ZADD":
		return s.zadd(w, args)
	case "ZRANGE":
		return s.zrange(w, args)
	case "ZREM":
		return s.zrem(w, args)
	case "ZSCORE":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		score, ok, err := s.store.ZScore(args[1], args[2])
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		return w.Bulk(formatScore(score))
	case "ZCARD":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		n, err := s.store.ZCard(args[1])
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "PUBLISH":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		return w.Int(int64(s.pubsub.Publish(args[1], args[2])))
	case "XADD":
		return s.xadd(w, args)
	case "XRANGE":
		return s.xrange(w, args)
	case "XREAD":
		return s.xread(w, args)
	case "XGROUP":
		return s.xgroup(w, args)
	case "XREADGROUP":
		return s.xreadgroup(w, args)
	case "XACK":
		return s.xack(w, args)
	case "PFADD":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		changed, err := s.store.PFAdd(args[1], args[2:]...)
		if err != nil {
			return err
		}
		if err := s.aof.Append(args); err != nil {
			return err
		}
		if changed {
			return w.Int(1)
		}
		return w.Int(0)
	case "PFCOUNT":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		n, err := s.store.PFCount(args[1:]...)
		if err != nil {
			return err
		}
		return w.Int(n)
	case "PFMERGE":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		allKeys := args[1:]
		if ok, err := s.ensureOwns(w, allKeys...); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		if err := s.store.PFMerge(args[1], args[2:]...); err != nil {
			return err
		}
		return w.Simple("OK")
	case "CLUSTER":
		return s.clusterCommand(w, args)
	case "INFO":
		return s.info(w)
	case "REPLICAOF", "SLAVEOF":
		if len(args) == 3 && strings.EqualFold(args[1], "NO") && strings.EqualFold(args[2], "ONE") {
			s.repl.StopFollowing()
			return w.Simple("OK")
		}
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		addr := net.JoinHostPort(args[1], args[2])
		if err := s.repl.StartFollowing(context.Background(), addr, s.store); err != nil {
			return err
		}
		return w.Simple("OK")
	case "SAVE":
		if len(args) != 1 {
			return wrongArgs(cmd)
		}
		if s.snapshotPath == "" {
			return errors.New("snapshot path is not configured")
		}
		if err := s.saveSnapshot(true); err != nil {
			return err
		}
		return w.Simple("OK")
	case "BGSAVE":
		if len(args) != 1 {
			return wrongArgs(cmd)
		}
		if s.snapshotPath == "" {
			return errors.New("snapshot path is not configured")
		}
		go func() {
			if err := s.saveSnapshot(true); err != nil {
				log.Printf("bgsave failed: %v", err)
			}
		}()
		return w.Simple("Background saving started")
	case "LASTSAVE":
		if len(args) != 1 {
			return wrongArgs(cmd)
		}
		return w.Int(s.lastSave.Unix())
	case "DUMP":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		data, ok := s.store.DumpKey(args[1])
		if !ok {
			return w.Null()
		}
		return w.Bulk(data)
	case "RESTORE":
		// Restore is a simplified stub that returns OK
		// Full wire-compatible RESTORE requires RDB format parsing
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		return w.Error("ERR RESTORE is not supported in Vole")
	case "SWAPDB":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		return w.Error("ERR SWAPDB is not supported (Vole uses namespaces instead of numbered databases)")
	case "PERSIST":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if !s.store.Persist(args[1]) {
			return w.Int(0)
		}
		persist := []string{"EXPIREATABS", args[1], "0"}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		return w.Int(1)
	case "PEXPIREAT":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		ms, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		expireAt := time.Unix(0, ms*int64(time.Millisecond))
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		persist := []string{"EXPIREATABS", args[1], strconv.FormatInt(expireAt.UnixNano(), 10)}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		if !s.store.ExpireAt(args[1], expireAt) {
			return w.Int(0)
		}
		return w.Int(1)
	case "DBSIZE":
		if len(args) != 1 {
			return wrongArgs(cmd)
		}
		return w.Int(int64(s.store.DBSize()))
	case "RENAME":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1], args[2]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		if err := s.store.Rename(args[1], args[2]); err != nil {
			return err
		}
		return w.Simple("OK")
	case "FLUSHDB", "FLUSHALL":
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		s.store.FlushDB()
		if err := s.aof.Reset(); err != nil {
			return err
		}
		return w.Simple("OK")
	case "RANDOMKEY":
		if len(args) != 1 {
			return wrongArgs(cmd)
		}
		key, ok := s.store.RandomKey()
		if !ok {
			return w.Null()
		}
		return w.Bulk(key)
	case "OBJECT":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "ENCODING":
			if len(args) != 3 {
				return wrongArgs("OBJECT")
			}
			typ := s.store.Type(args[2])
			var encoding string
			switch typ {
			case "string":
				encoding = "embstr"
			case "hash":
				encoding = "hashtable"
			case "list":
				encoding = "listpack"
			case "set":
				encoding = "hashtable"
			case "zset":
				encoding = "skiplist"
			case "stream":
				encoding = "stream"
			case "json":
				encoding = "json"
			default:
				return w.Null()
			}
			return w.Bulk(encoding)
		case "REFCOUNT":
			if len(args) != 3 {
				return wrongArgs("OBJECT")
			}
			if s.store.Type(args[2]) == "none" {
				return w.Null()
			}
			return w.Int(1)
		case "IDLETIME":
			if len(args) != 3 {
				return wrongArgs("OBJECT")
			}
			if s.store.Type(args[2]) == "none" {
				return w.Null()
			}
			return w.Int(0)
		case "FREQ":
			if len(args) != 3 {
				return wrongArgs("OBJECT")
			}
			if s.store.Type(args[2]) == "none" {
				return w.Null()
			}
			return w.Int(0)
		case "HELP":
			helpLines := []string{
				"OBJECT ENCODING <key> - Return the encoding of a key.",
				"OBJECT REFCOUNT <key> - Return the reference count of a key.",
				"OBJECT IDLETIME <key> - Return the idle time of a key.",
				"OBJECT FREQ <key> - Return the access frequency of a key.",
				"OBJECT HELP - Return this help text.",
			}
			return writeBulkStrings(w, helpLines)
		default:
			return fmt.Errorf("unsupported OBJECT subcommand %q", args[1])
		}
	case "COMMAND":
		if len(args) == 1 {
			return w.ArrayLen(0)
		}
		switch strings.ToUpper(args[1]) {
		case "COUNT":
			return w.Int(60)
		case "DOCS":
			return w.ArrayLen(0)
		default:
			return fmt.Errorf("unsupported COMMAND subcommand %q", args[1])
		}
	case "CONFIG":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "GET":
			if len(args) != 3 {
				return wrongArgs("CONFIG")
			}
			pattern := strings.ToLower(args[2])
			var pairs []string
			if pattern == "maxmemory" || pattern == "*" {
				pairs = append(pairs, "maxmemory", strconv.FormatInt(s.store.GetMaxMemory(), 10))
			}
			if pattern == "maxmemory-policy" || pattern == "*" {
				pairs = append(pairs, "maxmemory-policy", s.store.GetEvictPolicy())
			}
			return writeBulkStrings(w, pairs)
		case "SET":
			if len(args) < 4 || len(args)%2 != 0 {
				return wrongArgs("CONFIG")
			}
			for i := 2; i < len(args); i += 2 {
				param := strings.ToLower(args[i])
				val := args[i+1]
				switch param {
				case "maxmemory":
					n, err := strconv.ParseInt(val, 10, 64)
					if err != nil {
						return fmt.Errorf("invalid maxmemory value: %s", val)
					}
					s.store.SetMaxMemory(n)
				case "maxmemory-policy":
					switch val {
					case "noeviction", "allkeys-random", "volatile-random", "allkeys-lru":
						s.store.SetEvictPolicy(val)
					default:
						return fmt.Errorf("unsupported maxmemory-policy: %s", val)
					}
				}
			}
			return w.Simple("OK")
		default:
			return fmt.Errorf("unsupported CONFIG subcommand %q", args[1])
		}
	case "WAIT":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		return w.Int(0)
	case "TIME":
		if len(args) != 1 {
			return wrongArgs(cmd)
		}
		now := time.Now()
		if err := w.ArrayLen(2); err != nil {
			return err
		}
		if err := w.Bulk(strconv.FormatInt(now.Unix(), 10)); err != nil {
			return err
		}
		return w.Bulk(strconv.FormatInt(int64(now.Nanosecond()/1000), 10))
	case "STRLEN":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		n, err := s.store.Strlen(args[1])
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "APPEND":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		n, err := s.store.Append(args[1], args[2])
		if err != nil {
			return err
		}
		if err := s.aof.Append([]string{"APPEND", args[1], args[2]}); err != nil {
			return err
		}
		return w.Int(int64(n))
	case "GETDEL":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		v, ok := s.store.GetDel(args[1])
		if !ok {
			return w.Null()
		}
		if err := s.aof.Append([]string{"DEL", args[1]}); err != nil {
			return err
		}
		return w.Bulk(v)
	case "SETBIT":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		offset, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		val, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		old, err := s.store.SetBit(args[1], offset, val)
		if err != nil {
			return err
		}
		// Persist the full string value
		v, _ := s.store.Get(args[1])
		persist := []string{"SETABS", args[1], v, "0"}
		pttl := s.store.PTTL(args[1])
		if pttl > 0 {
			expireAt := time.Now().Add(time.Duration(pttl) * time.Millisecond)
			persist[3] = strconv.FormatInt(expireAt.UnixNano(), 10)
		}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		return w.Int(int64(old))
	case "GETBIT":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		offset, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		val, err := s.store.GetBit(args[1], offset)
		if err != nil {
			return err
		}
		return w.Int(int64(val))
	case "BITCOUNT":
		if len(args) != 2 && len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		hasBounds := len(args) == 4
		start, end := 0, 0
		if hasBounds {
			var err error
			start, err = strconv.Atoi(args[2])
			if err != nil {
				return err
			}
			end, err = strconv.Atoi(args[3])
			if err != nil {
				return err
			}
		}
		n, err := s.store.BitCount(args[1], start, end, hasBounds)
		if err != nil {
			return err
		}
		return w.Int(n)
	case "BITOP":
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		op := args[1]
		dest := args[2]
		keys := args[3:]
		allKeys := append([]string{dest}, keys...)
		if ok, err := s.ensureOwns(w, allKeys...); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		n, err := s.store.BitOp(op, dest, keys)
		if err != nil {
			return err
		}
		// Persist dest
		v, _ := s.store.Get(dest)
		persist := []string{"SETABS", dest, v, "0"}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		return w.Int(int64(n))
	case "BITPOS":
		if len(args) < 3 || len(args) > 5 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		bit, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		hasBounds := len(args) >= 4
		start, end := 0, 0
		if len(args) >= 4 {
			start, err = strconv.Atoi(args[3])
			if err != nil {
				return err
			}
		}
		if len(args) == 5 {
			end, err = strconv.Atoi(args[4])
			if err != nil {
				return err
			}
		} else if hasBounds {
			// Only start given, end defaults to last byte
			end = -1
		}
		pos, err := s.store.BitPos(args[1], bit, start, end, hasBounds)
		if err != nil {
			return err
		}
		return w.Int(pos)
	case "LLEN":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		n, err := s.store.LLen(args[1])
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "HLEN":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		n, err := s.store.HLen(args[1])
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "HEXISTS":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		exists, err := s.store.HExists(args[1], args[2])
		if err != nil {
			return err
		}
		if exists {
			return w.Int(1)
		}
		return w.Int(0)
	case "SINTER":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		members, err := s.store.SInter(args[1:]...)
		if err != nil {
			return err
		}
		return writeBulkStrings(w, members)
	case "SUNION":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		members, err := s.store.SUnion(args[1:]...)
		if err != nil {
			return err
		}
		return writeBulkStrings(w, members)
	case "SDIFF":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		members, err := s.store.SDiff(args[1:]...)
		if err != nil {
			return err
		}
		return writeBulkStrings(w, members)
	case "BLPOP":
		return s.blpop(w, args, true)
	case "BRPOP":
		return s.blpop(w, args, false)
	case "ZRANGEBYSCORE":
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		min, max, err := parseScoreRange(args[2], args[3])
		if err != nil {
			return err
		}
		offset, count := 0, 0
		withScores := false
		for i := 4; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "WITHSCORES":
				withScores = true
			case "LIMIT":
				if i+2 >= len(args) {
					return wrongArgs(cmd)
				}
				offset, err = strconv.Atoi(args[i+1])
				if err != nil {
					return err
				}
				count, err = strconv.Atoi(args[i+2])
				if err != nil {
					return err
				}
				i += 2
			}
		}
		items, err := s.store.ZRangeByScore(args[1], min, max, offset, count)
		if err != nil {
			return err
		}
		if !withScores {
			values := make([]string, len(items))
			for i, item := range items {
				values[i] = item.Member
			}
			return writeBulkStrings(w, values)
		}
		if err := w.ArrayLen(len(items) * 2); err != nil {
			return err
		}
		for _, item := range items {
			if err := w.Bulk(item.Member); err != nil {
				return err
			}
			if err := w.Bulk(formatScore(item.Score)); err != nil {
				return err
			}
		}
		return nil
	case "ZREVRANGE":
		if len(args) != 4 && len(args) != 5 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		start, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		stop, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		withScores := false
		if len(args) == 5 {
			if !strings.EqualFold(args[4], "WITHSCORES") {
				return wrongArgs(cmd)
			}
			withScores = true
		}
		items, err := s.store.ZRevRange(args[1], start, stop)
		if err != nil {
			return err
		}
		if !withScores {
			values := make([]string, len(items))
			for i, item := range items {
				values[i] = item.Member
			}
			return writeBulkStrings(w, values)
		}
		if err := w.ArrayLen(len(items) * 2); err != nil {
			return err
		}
		for _, item := range items {
			if err := w.Bulk(item.Member); err != nil {
				return err
			}
			if err := w.Bulk(formatScore(item.Score)); err != nil {
				return err
			}
		}
		return nil
	case "ZRANK":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		rank, ok, err := s.store.ZRank(args[1], args[2])
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		return w.Int(rank)
	case "ZREVRANK":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		rank, ok, err := s.store.ZRevRank(args[1], args[2])
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		return w.Int(rank)
	case "ZCOUNT":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		min, max, err := parseScoreRange(args[2], args[3])
		if err != nil {
			return err
		}
		n, err := s.store.ZCount(args[1], min, max)
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "ZPOPMIN":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count := 1
		if len(args) == 3 {
			n, err := strconv.Atoi(args[2])
			if err != nil {
				return err
			}
			count = n
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		items, err := s.store.ZPopMin(args[1], count)
		if err != nil {
			return err
		}
		if len(items) > 0 {
			remArgs := []string{"ZREM", args[1]}
			for _, m := range items {
				remArgs = append(remArgs, m.Member)
			}
			_ = s.aof.Append(remArgs)
		}
		if err := w.ArrayLen(len(items) * 2); err != nil {
			return err
		}
		for _, m := range items {
			if err := w.Bulk(m.Member); err != nil {
				return err
			}
			if err := w.Bulk(formatScore(m.Score)); err != nil {
				return err
			}
		}
		return nil
	case "ZPOPMAX":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count := 1
		if len(args) == 3 {
			n, err := strconv.Atoi(args[2])
			if err != nil {
				return err
			}
			count = n
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		items, err := s.store.ZPopMax(args[1], count)
		if err != nil {
			return err
		}
		if len(items) > 0 {
			remArgs := []string{"ZREM", args[1]}
			for _, m := range items {
				remArgs = append(remArgs, m.Member)
			}
			_ = s.aof.Append(remArgs)
		}
		if err := w.ArrayLen(len(items) * 2); err != nil {
			return err
		}
		for _, m := range items {
			if err := w.Bulk(m.Member); err != nil {
				return err
			}
			if err := w.Bulk(formatScore(m.Score)); err != nil {
				return err
			}
		}
		return nil
	case "ZINCRBY":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		incr, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		newScore, err := s.store.ZIncrBy(args[1], incr, args[3])
		if err != nil {
			return err
		}
		// Persist as ZADD with the final score
		persist := []string{"ZADD", args[1], formatScore(newScore), args[3]}
		_ = s.aof.Append(persist)
		return w.Bulk(formatScore(newScore))
	case "ZRANGEBYLEX":
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		offset, count := 0, 0
		for i := 4; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "LIMIT":
				if i+2 >= len(args) {
					return wrongArgs(cmd)
				}
				var parseErr error
				offset, parseErr = strconv.Atoi(args[i+1])
				if parseErr != nil {
					return parseErr
				}
				count, parseErr = strconv.Atoi(args[i+2])
				if parseErr != nil {
					return parseErr
				}
				i += 2
			}
		}
		items, err := s.store.ZRangeByLex(args[1], args[2], args[3], offset, count)
		if err != nil {
			return err
		}
		values := make([]string, len(items))
		for i, item := range items {
			values[i] = item.Member
		}
		return writeBulkStrings(w, values)
	case "GEOADD":
		// GEOADD key longitude latitude member [longitude latitude member ...]
		if len(args) < 5 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		idx := 2
		if (len(args)-idx)%3 != 0 {
			return wrongArgs(cmd)
		}
		members := make([]store.GeoMember, 0, (len(args)-idx)/3)
		for idx < len(args) {
			lon, err := strconv.ParseFloat(args[idx], 64)
			if err != nil {
				return err
			}
			lat, err := strconv.ParseFloat(args[idx+1], 64)
			if err != nil {
				return err
			}
			members = append(members, store.GeoMember{Longitude: lon, Latitude: lat, Name: args[idx+2]})
			idx += 3
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		// Persist as ZADD with encoded scores
		persist := []string{"ZADD", args[1]}
		for _, m := range members {
			score := store.GeoEncode(m.Longitude, m.Latitude)
			persist = append(persist, strconv.FormatFloat(score, 'f', -1, 64), m.Name)
		}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		n, err := s.store.GeoAdd(args[1], members)
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "GEOPOS":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		points, found := s.store.GeoPos(args[1], args[2:]...)
		if err := w.ArrayLen(len(points)); err != nil {
			return err
		}
		for i := range points {
			if !found[i] {
				if err := w.NullArray(); err != nil {
					return err
				}
			} else {
				if err := w.ArrayLen(2); err != nil {
					return err
				}
				if err := w.Bulk(strconv.FormatFloat(points[i].Longitude, 'f', -1, 64)); err != nil {
					return err
				}
				if err := w.Bulk(strconv.FormatFloat(points[i].Latitude, 'f', -1, 64)); err != nil {
					return err
				}
			}
		}
		return nil
	case "GEODIST":
		if len(args) < 4 || len(args) > 5 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		dist, ok := s.store.GeoDist(args[1], args[2], args[3])
		if !ok {
			return w.Null()
		}
		unit := "m"
		if len(args) == 5 {
			unit = strings.ToLower(args[4])
		}
		dist = convertGeoUnit(dist, unit)
		return w.Bulk(strconv.FormatFloat(dist, 'f', 4, 64))
	case "GEOSEARCH":
		return s.geoSearch(w, args)
	case "XLEN":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		n, err := s.store.XLen(args[1])
		if err != nil {
			return err
		}
		return w.Int(int64(n))
	case "COPY":
		if len(args) < 3 || len(args) > 5 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1], args[2]); !ok {
			return err
		}
		replace := false
		for i := 3; i < len(args); i++ {
			if strings.EqualFold(args[i], "REPLACE") {
				replace = true
			}
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		ok, err := s.store.Copy(args[1], args[2], replace)
		if err != nil {
			return err
		}
		if !ok {
			return w.Int(0)
		}
		if err := s.aof.Append(args); err != nil {
			return err
		}
		return w.Int(1)
	case "GETSET":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		old, had, err := s.store.GetSet(args[1], args[2])
		if err != nil {
			return err
		}
		persist := []string{"SETABS", args[1], args[2], "0"}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		if !had {
			return w.Null()
		}
		return w.Bulk(old)
	case "GETEX":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		var ttl time.Duration
		persist := false
		for i := 2; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "EX":
				if i+1 >= len(args) {
					return wrongArgs(cmd)
				}
				n, err := strconv.ParseInt(args[i+1], 10, 64)
				if err != nil {
					return err
				}
				ttl = time.Duration(n) * time.Second
				i++
			case "PX":
				if i+1 >= len(args) {
					return wrongArgs(cmd)
				}
				n, err := strconv.ParseInt(args[i+1], 10, 64)
				if err != nil {
					return err
				}
				ttl = time.Duration(n) * time.Millisecond
				i++
			case "PERSIST":
				persist = true
			default:
				return wrongArgs(cmd)
			}
		}
		v, ok := s.store.GetEx(args[1], ttl, persist)
		if !ok {
			return w.Null()
		}
		// Persist expiry change to AOF if needed
		if ttl > 0 || persist {
			s.persistMu.Lock()
			if persist {
				_ = s.aof.Append([]string{"EXPIREATABS", args[1], "0"})
			} else if ttl > 0 {
				expireAt := time.Now().Add(ttl)
				_ = s.aof.Append([]string{"EXPIREATABS", args[1], strconv.FormatInt(expireAt.UnixNano(), 10)})
			}
			s.persistMu.Unlock()
		}
		return w.Bulk(v)
	case "SETRANGE":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		offset, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		n, err := s.store.SetRange(args[1], offset, args[3])
		if err != nil {
			return err
		}
		// Persist the full resulting value
		v, _ := s.store.Get(args[1])
		persistCmd := []string{"SETABS", args[1], v, "0"}
		// Preserve expiry if exists
		pttl := s.store.PTTL(args[1])
		if pttl > 0 {
			expireAt := time.Now().Add(time.Duration(pttl) * time.Millisecond)
			persistCmd[3] = strconv.FormatInt(expireAt.UnixNano(), 10)
		}
		if err := s.aof.Append(persistCmd); err != nil {
			return err
		}
		return w.Int(int64(n))
	case "GETRANGE":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		start, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		end, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		v, err := s.store.GetRange(args[1], start, end)
		if err != nil {
			return err
		}
		return w.Bulk(v)
	case "SUBSTR":
		// SUBSTR is a legacy alias for GETRANGE
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		start, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		end, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		v, err := s.store.GetRange(args[1], start, end)
		if err != nil {
			return err
		}
		return w.Bulk(v)
	case "MSETNX":
		if len(args) < 3 || len(args)%2 != 1 {
			return wrongArgs(cmd)
		}
		keys := make([]string, 0, (len(args)-1)/2)
		values := make([]string, 0, (len(args)-1)/2)
		for i := 1; i < len(args); i += 2 {
			keys = append(keys, args[i])
			values = append(values, args[i+1])
		}
		if ok, err := s.ensureOwns(w, keys...); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if !s.store.MSetNX(keys, values) {
			return w.Int(0)
		}
		persistCmd := []string{"MSETABS"}
		for i := 0; i < len(keys); i++ {
			persistCmd = append(persistCmd, keys[i], values[i], "0")
		}
		if err := s.aof.Append(persistCmd); err != nil {
			return err
		}
		return w.Int(1)
	case "SORT":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		alpha := false
		desc := false
		offset, count := 0, 0
		storeKey := ""
		for i := 2; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "ALPHA":
				alpha = true
			case "ASC":
				desc = false
			case "DESC":
				desc = true
			case "LIMIT":
				if i+2 >= len(args) {
					return wrongArgs(cmd)
				}
				var err error
				offset, err = strconv.Atoi(args[i+1])
				if err != nil {
					return err
				}
				count, err = strconv.Atoi(args[i+2])
				if err != nil {
					return err
				}
				i += 2
			case "STORE":
				if i+1 >= len(args) {
					return wrongArgs(cmd)
				}
				storeKey = args[i+1]
				i++
			default:
				return fmt.Errorf("unsupported SORT option %q", args[i])
			}
		}
		if storeKey != "" {
			if ok, err := s.ensureOwns(w, storeKey); !ok {
				return err
			}
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		result, err := s.store.Sort(args[1], alpha, desc, offset, count, storeKey)
		if err != nil {
			return err
		}
		if storeKey != "" {
			if len(result) > 0 {
				_ = s.aof.Append([]string{"DEL", storeKey})
				_ = s.aof.Append(append([]string{"RPUSH", storeKey}, result...))
			}
			return w.Int(int64(len(result)))
		}
		return writeBulkStrings(w, result)
	case "TOUCH":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		return w.Int(int64(s.store.Exists(args[1:]...)))
	case "UNLINK":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1:]...); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(append([]string{"DEL"}, args[1:]...)); err != nil {
			return err
		}
		return w.Int(int64(s.store.Del(args[1:]...)))
	case "SINTERCARD":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		numkeys, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		if numkeys < 1 || len(args) < 2+numkeys {
			return wrongArgs(cmd)
		}
		keys := args[2 : 2+numkeys]
		limit := 0
		if len(args) > 2+numkeys {
			if strings.ToUpper(args[2+numkeys]) == "LIMIT" && len(args) > 3+numkeys {
				limit, _ = strconv.Atoi(args[3+numkeys])
			}
		}
		if ok, err := s.ensureOwns(w, keys...); !ok {
			return err
		}
		members, err := s.store.SInter(keys...)
		if err != nil {
			return err
		}
		cnt := len(members)
		if limit > 0 && cnt > limit {
			cnt = limit
		}
		return w.Int(int64(cnt))
	case "SINTERSTORE":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		dest := args[1]
		keys := args[2:]
		allKeys := append([]string{dest}, keys...)
		if ok, err := s.ensureOwns(w, allKeys...); !ok {
			return err
		}
		members, err := s.store.SInter(keys...)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		s.store.Del(dest)
		if len(members) > 0 {
			s.store.SAdd(dest, members...)
		}
		_ = s.aof.Append([]string{"DEL", dest})
		if len(members) > 0 {
			_ = s.aof.Append(append([]string{"SADD", dest}, members...))
		}
		return w.Int(int64(len(members)))
	case "SUNIONSTORE":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		dest := args[1]
		keys := args[2:]
		allKeys := append([]string{dest}, keys...)
		if ok, err := s.ensureOwns(w, allKeys...); !ok {
			return err
		}
		members, err := s.store.SUnion(keys...)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		s.store.Del(dest)
		if len(members) > 0 {
			s.store.SAdd(dest, members...)
		}
		_ = s.aof.Append([]string{"DEL", dest})
		if len(members) > 0 {
			_ = s.aof.Append(append([]string{"SADD", dest}, members...))
		}
		return w.Int(int64(len(members)))
	case "SDIFFSTORE":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		dest := args[1]
		keys := args[2:]
		allKeys := append([]string{dest}, keys...)
		if ok, err := s.ensureOwns(w, allKeys...); !ok {
			return err
		}
		members, err := s.store.SDiff(keys...)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		s.store.Del(dest)
		if len(members) > 0 {
			s.store.SAdd(dest, members...)
		}
		_ = s.aof.Append([]string{"DEL", dest})
		if len(members) > 0 {
			_ = s.aof.Append(append([]string{"SADD", dest}, members...))
		}
		return w.Int(int64(len(members)))
	case "EXPIRETIME":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		return w.Int(s.store.ExpireTime(args[1]))
	case "PEXPIRETIME":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		return w.Int(s.store.PExpireTime(args[1]))
	case "HVALS":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		vals, err := s.store.HVals(args[1])
		if err != nil {
			return err
		}
		return writeBulkStrings(w, vals)
	case "HKEYS":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		keys, err := s.store.HKeys(args[1])
		if err != nil {
			return err
		}
		return writeBulkStrings(w, keys)
	case "HRANDFIELD":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count := 1
		returnArray := len(args) == 3
		if len(args) == 3 {
			n, err := strconv.Atoi(args[2])
			if err != nil {
				return err
			}
			count = n
		}
		fields, err := s.store.HRandField(args[1], count)
		if err != nil {
			return err
		}
		if !returnArray {
			if len(fields) == 0 {
				return w.Null()
			}
			return w.Bulk(fields[0])
		}
		return writeBulkStrings(w, fields)
	case "RPOPLPUSH":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1], args[2]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		val, ok, err := s.store.LMove(args[1], args[2], false, true)
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		_ = s.aof.Append([]string{"RPOP", args[1]})
		_ = s.aof.Append([]string{"LPUSH", args[2], val})
		return w.Bulk(val)
	case "LMOVE":
		if len(args) != 5 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1], args[2]); !ok {
			return err
		}
		srcLeft := strings.EqualFold(args[3], "LEFT")
		dstLeft := strings.EqualFold(args[4], "LEFT")
		if !srcLeft && !strings.EqualFold(args[3], "RIGHT") {
			return wrongArgs(cmd)
		}
		if !dstLeft && !strings.EqualFold(args[4], "RIGHT") {
			return wrongArgs(cmd)
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		val, ok, err := s.store.LMove(args[1], args[2], srcLeft, dstLeft)
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		popCmd := "RPOP"
		if srcLeft {
			popCmd = "LPOP"
		}
		pushCmd := "RPUSH"
		if dstLeft {
			pushCmd = "LPUSH"
		}
		_ = s.aof.Append([]string{popCmd, args[1]})
		_ = s.aof.Append([]string{pushCmd, args[2], val})
		return w.Bulk(val)
	case "MEMORY":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "USAGE":
			if len(args) != 3 {
				return wrongArgs("MEMORY")
			}
			typ := s.store.Type(args[2])
			if typ == "none" {
				return w.Null()
			}
			return w.Int(64)
		case "DOCTOR":
			return w.Bulk("Sam, I have no memory problems")
		case "HELP":
			return writeBulkStrings(w, []string{"MEMORY USAGE <key>", "MEMORY DOCTOR", "MEMORY HELP"})
		default:
			return fmt.Errorf("unsupported MEMORY subcommand %q", args[1])
		}
	case "DEBUG":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "SLEEP":
			if len(args) != 3 {
				return wrongArgs("DEBUG")
			}
			sec, err := strconv.ParseFloat(args[2], 64)
			if err != nil {
				return err
			}
			time.Sleep(time.Duration(sec * float64(time.Second)))
			return w.Simple("OK")
		case "SET-ACTIVE-EXPIRE":
			return w.Simple("OK")
		default:
			return fmt.Errorf("unsupported DEBUG subcommand %q", args[1])
		}
	case "HELLO":
		if err := w.ArrayLen(14); err != nil {
			return err
		}
		pairs := []string{"server", "vole", "version", "0.1.0", "proto", "2", "id", "1", "mode", "standalone", "role", "master", "modules"}
		for _, p := range pairs {
			if err := w.Bulk(p); err != nil {
				return err
			}
		}
		return w.ArrayLen(0)
	case "WEBHOOK":
		return s.webhookCmd(w, args)

	// ----- Rate Limiting -----
	case "RATELIMIT":
		// RATELIMIT key maxRequests windowSeconds
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		max, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return err
		}
		windowSec, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return err
		}
		window := time.Duration(windowSec) * time.Second

		allowed, remaining, retryAfterMs, resetAtMs := s.store.RateLimit(args[1], max, window)

		if err := w.ArrayLen(4); err != nil {
			return err
		}
		if allowed {
			if err := w.Int(1); err != nil {
				return err
			}
		} else {
			if err := w.Int(0); err != nil {
				return err
			}
		}
		if err := w.Int(remaining); err != nil {
			return err
		}
		if err := w.Int(retryAfterMs); err != nil {
			return err
		}
		return w.Int(resetAtMs)

	case "RATELIMIT.PEEK":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		remaining, resetAtMs := s.store.RateLimitPeek(args[1])
		if err := w.ArrayLen(2); err != nil {
			return err
		}
		if err := w.Int(remaining); err != nil {
			return err
		}
		return w.Int(resetAtMs)

	case "RATELIMIT.RESET":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		if s.store.RateLimitReset(args[1]) {
			return w.Int(1)
		}
		return w.Int(0)

	// ----- Delayed/Scheduled Keys -----
	case "SETDELAYED":
		// SETDELAYED key value delaySeconds [EX ttlSeconds]
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		delaySec, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			return err
		}
		var ttl time.Duration
		if len(args) >= 6 && strings.EqualFold(args[4], "EX") {
			n, err := strconv.ParseInt(args[5], 10, 64)
			if err != nil {
				return err
			}
			ttl = time.Duration(n) * time.Second
		}
		delay := time.Duration(delaySec) * time.Second
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		s.store.SetDelayed(args[1], args[2], delay, ttl)
		return w.Simple("OK")

	// ----- Reliable Queue -----
	case "ENQUEUE":
		// ENQUEUE queue message [DELAY seconds]
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		var delay time.Duration
		if len(args) >= 5 && strings.EqualFold(args[3], "DELAY") {
			n, err := strconv.ParseInt(args[4], 10, 64)
			if err != nil {
				return err
			}
			delay = time.Duration(n) * time.Second
		}
		s.persistMu.Lock()
		id := s.store.Enqueue(args[1], args[2], delay)
		if s.aof != nil {
			if err := s.aof.Append(args); err != nil {
				s.persistMu.Unlock()
				return err
			}
		}
		s.persistMu.Unlock()
		return w.Bulk(id)

	case "DEQUEUE":
		// DEQUEUE queue [TIMEOUT seconds]
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		timeout := time.Duration(0)
		if len(args) >= 4 && strings.EqualFold(args[2], "TIMEOUT") {
			n, err := strconv.ParseFloat(args[3], 64)
			if err != nil {
				return err
			}
			timeout = time.Duration(n * float64(time.Second))
		}
		msg, ok := s.store.Dequeue(args[1], timeout)
		if !ok {
			return w.NullArray()
		}
		// Return [id, body, retries]
		if err := w.ArrayLen(3); err != nil {
			return err
		}
		if err := w.Bulk(msg.ID); err != nil {
			return err
		}
		if err := w.Bulk(msg.Body); err != nil {
			return err
		}
		return w.Int(int64(msg.Retries))

	case "QACK":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		if s.aof != nil {
			if err := s.aof.Append(args); err != nil {
				s.persistMu.Unlock()
				return err
			}
		}
		s.persistMu.Unlock()
		if s.store.QAck(args[1], args[2]) {
			return w.Int(1)
		}
		return w.Int(0)

	case "QNACK":
		if len(args) != 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		if s.aof != nil {
			if err := s.aof.Append(args); err != nil {
				s.persistMu.Unlock()
				return err
			}
		}
		s.persistMu.Unlock()
		if s.store.QNack(args[1], args[2]) {
			return w.Int(1)
		}
		return w.Int(0)

	case "QPEEK":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count := 1
		if len(args) >= 4 && strings.EqualFold(args[2], "COUNT") {
			count, _ = strconv.Atoi(args[3])
		}
		msgs := s.store.QPeek(args[1], count)
		return writeQueueMessages(w, msgs)

	case "QLEN":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		return w.Int(int64(s.store.QLen(args[1])))

	case "QINFO":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		pending, processing, dead := s.store.QInfo(args[1])
		if err := w.ArrayLen(6); err != nil {
			return err
		}
		_ = w.Bulk("pending")
		_ = w.Int(int64(pending))
		_ = w.Bulk("processing")
		_ = w.Int(int64(processing))
		_ = w.Bulk("dead_letter")
		return w.Int(int64(dead))

	case "QDEAD":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count := 10
		if len(args) >= 4 && strings.EqualFold(args[2], "COUNT") {
			count, _ = strconv.Atoi(args[3])
		}
		msgs := s.store.QDead(args[1], count)
		return writeQueueMessages(w, msgs)

	// ----- JSON Document Commands -----
	case "JSON.SET":
		// JSON.SET key path value
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		if err := s.store.JSONSet(args[1], args[2], args[3]); err != nil {
			return err
		}
		return w.Simple("OK")

	case "JSON.GET":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		path := "$"
		if len(args) == 3 {
			path = args[2]
		}
		val, ok, err := s.store.JSONGet(args[1], path)
		if err != nil {
			return err
		}
		if !ok {
			return w.Null()
		}
		return w.Bulk(val)

	case "JSON.DEL":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		path := "$"
		if len(args) == 3 {
			path = args[2]
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		deleted, err := s.store.JSONDel(args[1], path)
		if err != nil {
			return err
		}
		if deleted {
			return w.Int(1)
		}
		return w.Int(0)

	case "JSON.TYPE":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		path := "$"
		if len(args) == 3 {
			path = args[2]
		}
		t, err := s.store.JSONType(args[1], path)
		if err != nil {
			return err
		}
		return w.Bulk(t)

	case "JSON.NUMINCRBY":
		if len(args) != 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		delta, err := strconv.ParseFloat(args[3], 64)
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		result, err := s.store.JSONNumIncrBy(args[1], args[2], delta)
		if err != nil {
			return err
		}
		return w.Bulk(strconv.FormatFloat(result, 'f', -1, 64))

	case "JSON.ARRAPPEND":
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n, err := s.store.JSONArrAppend(args[1], args[2], args[3:]...)
		if err != nil {
			return err
		}
		return w.Int(int64(n))

	case "JSON.ARRLEN":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		path := "$"
		if len(args) == 3 {
			path = args[2]
		}
		n, err := s.store.JSONArrLen(args[1], path)
		if err != nil {
			return err
		}
		return w.Int(int64(n))

	case "JSON.KEYS":
		if len(args) < 2 || len(args) > 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		path := "$"
		if len(args) == 3 {
			path = args[2]
		}
		keys, err := s.store.JSONKeys(args[1], path)
		if err != nil {
			return err
		}
		return writeBulkStrings(w, keys)

	// ----- Key Tagging / Metadata -----
	case "TAG":
		// TAG key field1=value1 field2=value2
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		tags := make(map[string]string)
		for _, arg := range args[2:] {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid tag format: %q (use key=value)", arg)
			}
			tags[parts[0]] = parts[1]
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		if err := s.store.TagSet(args[1], tags); err != nil {
			return err
		}
		return w.Simple("OK")

	case "TAGGET":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		tags := s.store.TagGet(args[1])
		if tags == nil {
			return w.NullArray()
		}
		// Return as flat array [field, value, field, value, ...]
		pairs := make([]string, 0, len(tags)*2)
		tagKeys := make([]string, 0, len(tags))
		for k := range tags {
			tagKeys = append(tagKeys, k)
		}
		sort.Strings(tagKeys)
		for _, k := range tagKeys {
			pairs = append(pairs, k, tags[k])
		}
		return writeBulkStrings(w, pairs)

	case "TAGDEL":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		n := s.store.TagDel(args[1], args[2:])
		return w.Int(int64(n))

	case "TAGQUERY":
		// TAGQUERY field=value [AND field=value ...] [LIMIT n]
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		criteria := make(map[string]string)
		limit := 100
		for i := 1; i < len(args); i++ {
			if strings.EqualFold(args[i], "AND") {
				continue
			}
			if strings.EqualFold(args[i], "LIMIT") {
				if i+1 < len(args) {
					limit, _ = strconv.Atoi(args[i+1])
					i++
				}
				continue
			}
			parts := strings.SplitN(args[i], "=", 2)
			if len(parts) == 2 {
				criteria[parts[0]] = parts[1]
			}
		}
		results := s.store.TagQuery(criteria, limit)
		return writeBulkStrings(w, results)

	// ----- Time-Series -----
	case "TS.ADD":
		// TS.ADD key timestamp value [LABELS field=value ...]
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		var timestamp int64
		if args[2] == "*" {
			timestamp = 0 // will use current time in store
		} else {
			var err error
			timestamp, err = strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return err
			}
		}
		value, err := strconv.ParseFloat(args[3], 64)
		if err != nil {
			return err
		}
		labels := make(map[string]string)
		if len(args) > 4 {
			labelStart := 4
			if strings.EqualFold(args[4], "LABELS") {
				labelStart = 5
			}
			for i := labelStart; i < len(args); i++ {
				parts := strings.SplitN(args[i], "=", 2)
				if len(parts) == 2 {
					labels[parts[0]] = parts[1]
				}
			}
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		if err := s.aof.Append(args); err != nil {
			return err
		}
		if err := s.store.TSAdd(args[1], timestamp, value, labels); err != nil {
			return err
		}
		return w.Simple("OK")

	case "TS.RANGE":
		// TS.RANGE key from to [COUNT n]
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		var from, to int64
		if args[2] == "-" {
			from = 0
		} else {
			from, _ = strconv.ParseInt(args[2], 10, 64)
		}
		if args[3] == "+" {
			to = 1 << 62
		} else {
			to, _ = strconv.ParseInt(args[3], 10, 64)
		}
		count := 0
		if len(args) >= 6 && strings.EqualFold(args[4], "COUNT") {
			count, _ = strconv.Atoi(args[5])
		}
		samples, err := s.store.TSRange(args[1], from, to, count)
		if err != nil {
			return err
		}
		if err := w.ArrayLen(len(samples)); err != nil {
			return err
		}
		for _, sample := range samples {
			if err := w.ArrayLen(2); err != nil {
				return err
			}
			if err := w.Int(sample.Timestamp); err != nil {
				return err
			}
			if err := w.Bulk(strconv.FormatFloat(sample.Value, 'f', -1, 64)); err != nil {
				return err
			}
		}
		return nil

	case "TS.GET":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		sample, ok, err := s.store.TSGet(args[1])
		if err != nil {
			return err
		}
		if !ok {
			return w.NullArray()
		}
		if err := w.ArrayLen(2); err != nil {
			return err
		}
		if err := w.Int(sample.Timestamp); err != nil {
			return err
		}
		return w.Bulk(strconv.FormatFloat(sample.Value, 'f', -1, 64))

	case "TS.INFO":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		count, labels, firstTS, lastTS, err := s.store.TSInfo(args[1])
		if err != nil {
			return err
		}
		if err := w.ArrayLen(10); err != nil {
			return err
		}
		_ = w.Bulk("total_samples")
		_ = w.Int(int64(count))
		_ = w.Bulk("first_timestamp")
		_ = w.Int(firstTS)
		_ = w.Bulk("last_timestamp")
		_ = w.Int(lastTS)
		_ = w.Bulk("labels")
		pairs := make([]string, 0, len(labels)*2)
		for k, v := range labels {
			pairs = append(pairs, k, v)
		}
		_ = writeBulkStrings(w, pairs)
		_ = w.Bulk("retention")
		return w.Int(0)

	case "TS.DOWNSAMPLE":
		// TS.DOWNSAMPLE srcKey dstKey aggregation from to bucketSizeMs
		if len(args) != 7 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1], args[2]); !ok {
			return err
		}
		from, _ := strconv.ParseInt(args[4], 10, 64)
		to, _ := strconv.ParseInt(args[5], 10, 64)
		bucketSize, _ := strconv.ParseInt(args[6], 10, 64)
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		n, err := s.store.TSDownsample(args[1], args[2], strings.ToLower(args[3]), from, to, bucketSize)
		if err != nil {
			return err
		}
		return w.Int(int64(n))

	// ---- Cron commands ----
	case "CRON.ADD":
		// CRON.ADD name schedule COMMAND arg1 arg2 ...
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		name, schedule := args[1], args[2]
		cmdIdx := 3
		// Skip optional "COMMAND" keyword
		if len(args) > 3 && strings.EqualFold(args[3], "COMMAND") {
			cmdIdx = 4
		}
		if cmdIdx >= len(args) {
			return wrongArgs(cmd)
		}
		command := args[cmdIdx:]
		if err := s.cron.Add(name, schedule, command); err != nil {
			return err
		}
		return w.Simple("OK")

	case "CRON.DEL":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		if s.cron.Del(args[1]) {
			return w.Int(1)
		}
		return w.Int(0)

	case "CRON.LIST":
		jobs := s.cron.List()
		if err := w.ArrayLen(len(jobs)); err != nil {
			return err
		}
		for _, j := range jobs {
			if err := w.ArrayLen(6); err != nil {
				return err
			}
			_ = w.Bulk(j.Name)
			_ = w.Bulk(j.Schedule)
			_ = w.Bulk(strings.Join(j.Command, " "))
			_ = w.Int(j.RunCount)
			_ = w.Bulk(j.LastRun.Format(time.RFC3339))
			_ = w.Bulk(j.LastError)
		}
		return nil

	case "CRON.INFO":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		j, ok := s.cron.Get(args[1])
		if !ok {
			return w.Null()
		}
		if err := w.ArrayLen(12); err != nil {
			return err
		}
		_ = w.Bulk("name")
		_ = w.Bulk(j.Name)
		_ = w.Bulk("schedule")
		_ = w.Bulk(j.Schedule)
		_ = w.Bulk("command")
		_ = w.Bulk(strings.Join(j.Command, " "))
		_ = w.Bulk("run_count")
		_ = w.Int(j.RunCount)
		_ = w.Bulk("last_run")
		_ = w.Bulk(j.LastRun.Format(time.RFC3339))
		_ = w.Bulk("last_error")
		return w.Bulk(j.LastError)

	// ---- Audit commands ----
	case "AUDIT":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		count := 10
		if len(args) >= 4 && strings.EqualFold(args[2], "COUNT") {
			count, _ = strconv.Atoi(args[3])
		}
		entries := s.audit.ForKey(args[1], count)
		return writeAuditEntries(w, entries)

	case "AUDIT.SEARCH":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		count := 50
		if len(args) >= 4 && strings.EqualFold(args[2], "COUNT") {
			count, _ = strconv.Atoi(args[3])
		}
		entries := s.audit.Search(args[1], count)
		return writeAuditEntries(w, entries)

	case "AUDIT.ENABLE":
		s.audit.Enable()
		return w.Simple("OK")

	case "AUDIT.DISABLE":
		s.audit.Disable()
		return w.Simple("OK")

	case "AUDIT.CLEAR":
		s.audit.Clear()
		return w.Simple("OK")

	case "AUDIT.SIZE":
		return w.Int(int64(s.audit.Size()))

	// ----- Schema Enforcement -----
	case "SCHEMA.SET":
		return s.handleSchemaSet(w, args)
	case "SCHEMA.GET":
		return s.handleSchemaGet(w, args)
	case "SCHEMA.DEL":
		return s.handleSchemaDel(w, args)
	case "SCHEMA.LIST":
		return s.handleSchemaList(w, args)

	case "EVAL":
		// EVAL script numkeys key [key ...] arg [arg ...]
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		numKeys, err := strconv.Atoi(args[2])
		if err != nil {
			return fmt.Errorf("ERR value is not an integer or out of range")
		}
		if len(args) < 3+numKeys {
			return wrongArgs(cmd)
		}
		keys := args[3 : 3+numKeys]
		argv := args[3+numKeys:]

		// Store the script for future EVALSHA
		script := args[1]
		s.scripts.Load(script)

		sc := &ScriptContext{
			Keys:   keys,
			Args:   argv,
			server: s,
		}
		result, execErr := sc.Execute(script)
		if execErr != nil {
			return execErr
		}
		return writeValue(w, result)

	case "EVALSHA":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		sha := args[1]
		script, ok := s.scripts.Get(sha)
		if !ok {
			return errors.New("NOSCRIPT No matching script. Use EVAL.")
		}
		numKeys, err := strconv.Atoi(args[2])
		if err != nil {
			return fmt.Errorf("ERR value is not an integer or out of range")
		}
		if len(args) < 3+numKeys {
			return wrongArgs(cmd)
		}
		keys := args[3 : 3+numKeys]
		argv := args[3+numKeys:]

		sc := &ScriptContext{
			Keys:   keys,
			Args:   argv,
			server: s,
		}
		result, execErr := sc.Execute(script)
		if execErr != nil {
			return execErr
		}
		return writeValue(w, result)

	case "SCRIPT":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "LOAD":
			if len(args) != 3 {
				return wrongArgs("SCRIPT")
			}
			sha := s.scripts.Load(args[2])
			return w.Bulk(sha)
		case "EXISTS":
			if len(args) < 3 {
				return wrongArgs("SCRIPT")
			}
			results := s.scripts.Exists(args[2:])
			if err := w.ArrayLen(len(results)); err != nil {
				return err
			}
			for _, exists := range results {
				if exists {
					_ = w.Int(1)
				} else {
					_ = w.Int(0)
				}
			}
			return nil
		case "FLUSH":
			s.scripts.Flush()
			return w.Simple("OK")
		default:
			return fmt.Errorf("unsupported SCRIPT subcommand %q", args[1])
		}

	case "SLOWLOG":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "GET":
			count := 10
			if len(args) >= 3 {
				count, _ = strconv.Atoi(args[2])
			}
			entries := s.slowlog.Get(count)
			if err := w.ArrayLen(len(entries)); err != nil {
				return err
			}
			for _, e := range entries {
				if err := w.ArrayLen(6); err != nil {
					return err
				}
				_ = w.Int(e.ID)
				_ = w.Int(e.Timestamp.Unix())
				_ = w.Int(e.Duration.Microseconds())
				if err := writeBulkStrings(w, e.Command); err != nil {
					return err
				}
				_ = w.Bulk(e.Client)
				_ = w.Bulk("")
			}
			return nil
		case "LEN":
			return w.Int(int64(s.slowlog.Len()))
		case "RESET":
			s.slowlog.Reset()
			return w.Simple("OK")
		default:
			return fmt.Errorf("unsupported SLOWLOG subcommand %q", args[1])
		}

	case "XINFO":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[2]); !ok {
			return err
		}
		switch strings.ToUpper(args[1]) {
		case "STREAM":
			length, firstID, lastID, groups, exists := s.store.XInfoStream(args[2])
			if !exists && length == 0 {
				return fmt.Errorf("ERR no such key")
			}
			if err := w.ArrayLen(14); err != nil {
				return err
			}
			_ = w.Bulk("length")
			_ = w.Int(int64(length))
			_ = w.Bulk("radix-tree-keys")
			_ = w.Int(0)
			_ = w.Bulk("radix-tree-nodes")
			_ = w.Int(0)
			_ = w.Bulk("last-generated-id")
			_ = w.Bulk(lastID)
			_ = w.Bulk("groups")
			_ = w.Int(int64(groups))
			_ = w.Bulk("first-entry")
			if length > 0 {
				_ = w.Bulk(firstID)
			} else {
				_ = w.Null()
			}
			_ = w.Bulk("last-entry")
			if length > 0 {
				_ = w.Bulk(lastID)
			} else {
				_ = w.Null()
			}
			return nil
		case "GROUPS":
			groupInfos := s.store.XInfoGroups(args[2])
			if err := w.ArrayLen(len(groupInfos)); err != nil {
				return err
			}
			for _, gi := range groupInfos {
				if err := w.ArrayLen(8); err != nil {
					return err
				}
				_ = w.Bulk("name")
				_ = w.Bulk(gi["name"].(string))
				_ = w.Bulk("consumers")
				_ = w.Int(int64(gi["consumers"].(int)))
				_ = w.Bulk("pending")
				_ = w.Int(int64(gi["pending"].(int)))
				_ = w.Bulk("last-delivered-id")
				_ = w.Bulk(gi["last-delivered-id"].(string))
			}
			return nil
		default:
			return fmt.Errorf("unsupported XINFO subcommand %q", args[1])
		}

	case "LATENCY":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "LATEST":
			return w.ArrayLen(0)
		case "HISTORY":
			return w.ArrayLen(0)
		case "RESET":
			return w.Simple("OK")
		default:
			return fmt.Errorf("unsupported LATENCY subcommand %q", args[1])
		}

	case "ACL":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "WHOAMI":
			return w.Bulk("default")
		case "LIST":
			return writeBulkStrings(w, []string{"user default on ~* +@all"})
		case "GETUSER":
			if len(args) != 3 {
				return wrongArgs("ACL")
			}
			if args[2] == "default" {
				if err := w.ArrayLen(2); err != nil {
					return err
				}
				_ = w.Bulk("flags")
				return writeBulkStrings(w, []string{"on"})
			}
			return w.Null()
		case "CAT":
			return writeBulkStrings(w, []string{"read", "write", "set", "sortedset", "list", "hash", "string", "stream", "pubsub", "admin", "fast", "slow", "generic", "server", "connection"})
		default:
			return fmt.Errorf("unsupported ACL subcommand %q", args[1])
		}

	case "SELECT":
		if len(args) != 2 {
			return wrongArgs(cmd)
		}
		db, err := strconv.Atoi(args[1])
		if err != nil {
			return err
		}
		if db != 0 {
			return errors.New("ERR SELECT is not supported, use NAMESPACE instead")
		}
		return w.Simple("OK")

	case "RESET":
		return w.Simple("RESET")

	case "MODULE":
		if len(args) < 2 {
			return wrongArgs(cmd)
		}
		switch strings.ToUpper(args[1]) {
		case "LIST":
			return w.ArrayLen(0)
		case "LOAD", "UNLOAD":
			return errors.New("ERR modules are not supported")
		default:
			return fmt.Errorf("unsupported MODULE subcommand %q", args[1])
		}

	case "XCLAIM":
		if len(args) < 6 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		minIdle, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return err
		}
		ids := args[5:]
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		claimed, err := s.store.XClaim(args[1], args[2], args[3], minIdle, ids)
		if err != nil {
			return err
		}
		return writeEntries(w, claimed)

	case "XAUTOCLAIM":
		// XAUTOCLAIM key group consumer min-idle-time start [COUNT count]
		if len(args) < 6 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		minIdle, err := strconv.ParseInt(args[4], 10, 64)
		if err != nil {
			return err
		}
		startID := args[5]
		count := 100
		for i := 6; i < len(args)-1; i++ {
			if strings.EqualFold(args[i], "COUNT") {
				count, err = strconv.Atoi(args[i+1])
				if err != nil {
					return err
				}
			}
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		nextID, claimed, err := s.store.XAutoClaim(args[1], args[2], args[3], minIdle, startID, count)
		if err != nil {
			return err
		}
		if err := w.ArrayLen(3); err != nil {
			return err
		}
		_ = w.Bulk(nextID)
		if err := writeEntries(w, claimed); err != nil {
			return err
		}
		// Empty array of deleted IDs
		return w.ArrayLen(0)

	case "XPENDING":
		if len(args) < 3 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		total, minID, maxID, consumers := s.store.XPendingSummary(args[1], args[2])
		if total == 0 {
			if err := w.ArrayLen(4); err != nil {
				return err
			}
			_ = w.Int(0)
			_ = w.Null()
			_ = w.Null()
			return w.ArrayLen(0)
		}
		if err := w.ArrayLen(4); err != nil {
			return err
		}
		_ = w.Int(int64(total))
		_ = w.Bulk(minID)
		_ = w.Bulk(maxID)
		if err := w.ArrayLen(len(consumers)); err != nil {
			return err
		}
		for consumer, cnt := range consumers {
			if err := w.ArrayLen(2); err != nil {
				return err
			}
			_ = w.Bulk(consumer)
			_ = w.Bulk(strconv.Itoa(cnt))
		}
		return nil

	case "XTRIM":
		if len(args) < 4 {
			return wrongArgs(cmd)
		}
		if ok, err := s.ensureOwns(w, args[1]); !ok {
			return err
		}
		idx := 2
		// Skip ~ (approximate) flag if present
		if args[idx] == "~" {
			idx++
		}
		if !strings.EqualFold(args[idx], "MAXLEN") {
			return wrongArgs(cmd)
		}
		idx++
		if idx >= len(args) {
			return wrongArgs(cmd)
		}
		// Skip ~ after MAXLEN too
		if args[idx] == "~" {
			idx++
		}
		if idx >= len(args) {
			return wrongArgs(cmd)
		}
		maxLen, err := strconv.Atoi(args[idx])
		if err != nil {
			return err
		}
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		n, err := s.store.XTrim(args[1], maxLen)
		if err != nil {
			return err
		}
		return w.Int(int64(n))

	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func writeAuditEntries(w *resp.Writer, entries []AuditEntry) error {
	if err := w.ArrayLen(len(entries)); err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.ArrayLen(8); err != nil {
			return err
		}
		_ = w.Bulk("timestamp")
		_ = w.Bulk(e.Timestamp.Format(time.RFC3339Nano))
		_ = w.Bulk("key")
		_ = w.Bulk(e.Key)
		_ = w.Bulk("command")
		_ = w.Bulk(e.Command)
		_ = w.Bulk("client")
		_ = w.Bulk(e.Client)
	}
	return nil
}

func (s *Server) set(w *resp.Writer, args []string) error {
	if len(args) < 3 {
		return wrongArgs("SET")
	}
	key, value := args[1], args[2]

	var ttl time.Duration
	var delay time.Duration
	var useNX, useXX bool

	// Parse optional flags after key and value in any order.
	i := 3
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "NX":
			useNX = true
			i++
		case "XX":
			useXX = true
			i++
		case "EX":
			if i+1 >= len(args) {
				return wrongArgs("SET")
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return err
			}
			ttl = time.Duration(n) * time.Second
			i += 2
		case "PX":
			if i+1 >= len(args) {
				return wrongArgs("SET")
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return err
			}
			ttl = time.Duration(n) * time.Millisecond
			i += 2
		case "AFTER":
			if i+1 >= len(args) {
				return wrongArgs("SET")
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return err
			}
			delay = time.Duration(n) * time.Second
			i += 2
		default:
			return errors.New("unsupported SET option")
		}
	}

	if useNX && useXX {
		return errors.New("NX and XX options are mutually exclusive")
	}

	if ok, err := s.ensureOwns(w, key); !ok {
		return err
	}

	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	// Delayed/scheduled SET — key becomes visible after delay
	if delay > 0 {
		delaySec := strconv.FormatInt(int64(delay/time.Second), 10)
		persist := []string{"SETDELAYED", key, value, delaySec}
		if ttl > 0 {
			persist = append(persist, "EX", strconv.FormatInt(int64(ttl/time.Second), 10))
		}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		s.store.SetDelayed(key, value, delay, ttl)
		return w.Simple("OK")
	}

	if useNX {
		ok := s.store.SetNX(key, value, ttl)
		if !ok {
			return w.Null()
		}
		var expireAt time.Time
		if ttl > 0 {
			expireAt = time.Now().Add(ttl)
		}
		persist := []string{"SETABS", key, value, "0"}
		if !expireAt.IsZero() {
			persist[3] = strconv.FormatInt(expireAt.UnixNano(), 10)
		}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		return w.Simple("OK")
	}

	if useXX {
		ok := s.store.SetXX(key, value, ttl)
		if !ok {
			return w.Null()
		}
		var expireAt time.Time
		if ttl > 0 {
			expireAt = time.Now().Add(ttl)
		}
		persist := []string{"SETABS", key, value, "0"}
		if !expireAt.IsZero() {
			persist[3] = strconv.FormatInt(expireAt.UnixNano(), 10)
		}
		if err := s.aof.Append(persist); err != nil {
			return err
		}
		return w.Simple("OK")
	}

	var expireAt time.Time
	if ttl > 0 {
		expireAt = time.Now().Add(ttl)
	}
	persist := []string{"SETABS", key, value, "0"}
	if !expireAt.IsZero() {
		persist[3] = strconv.FormatInt(expireAt.UnixNano(), 10)
	}
	if err := s.aof.Append(persist); err != nil {
		return err
	}
	s.store.SetAbsolute(key, value, expireAt)
	return w.Simple("OK")
}

func (s *Server) hset(w *resp.Writer, args []string) error {
	if len(args) < 4 || len(args)%2 != 0 {
		return wrongArgs("HSET")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	pairs := make([]store.HashPair, 0, (len(args)-2)/2)
	persist := []string{"HSET", args[1]}
	for i := 2; i < len(args); i += 2 {
		pairs = append(pairs, store.HashPair{Field: args[i], Value: args[i+1]})
		persist = append(persist, args[i], args[i+1])
	}
	// Schema validation
	fieldMap := make(map[string]string, len(pairs))
	for _, p := range pairs {
		fieldMap[p.Field] = p.Value
	}
	if err := s.schemas.Validate(args[1], fieldMap); err != nil {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ != "none" && typ != "hash" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(persist); err != nil {
		return err
	}
	added, err := s.store.HSet(args[1], pairs)
	if err != nil {
		return err
	}
	return w.Int(int64(added))
}

func (s *Server) hsearch(w *resp.Writer, args []string) error {
	// HSEARCH pattern WHERE field op value [AND field op value ...] [LIMIT offset count]
	if len(args) < 5 {
		return wrongArgs("HSEARCH")
	}

	pattern := args[1]

	if !strings.EqualFold(args[2], "WHERE") {
		return wrongArgs("HSEARCH")
	}

	var conditions []store.SearchCondition
	offset, count := 0, 0

	i := 3
	for i < len(args) {
		if strings.EqualFold(args[i], "LIMIT") {
			if i+2 >= len(args) {
				return wrongArgs("HSEARCH")
			}
			var err error
			offset, err = strconv.Atoi(args[i+1])
			if err != nil {
				return err
			}
			count, err = strconv.Atoi(args[i+2])
			if err != nil {
				return err
			}
			i += 3
			continue
		}

		if strings.EqualFold(args[i], "AND") {
			i++
			continue
		}

		// Parse condition: field op value
		if i+2 >= len(args) {
			return wrongArgs("HSEARCH")
		}

		conditions = append(conditions, store.SearchCondition{
			Field: args[i],
			Op:    args[i+1],
			Value: args[i+2],
		})
		i += 3
	}

	if len(conditions) == 0 {
		return wrongArgs("HSEARCH")
	}

	results := s.store.HSearch(pattern, conditions, offset, count)

	// Return as array of [key, [field, value, ...]] pairs
	if err := w.ArrayLen(len(results)); err != nil {
		return err
	}
	for _, result := range results {
		if err := w.ArrayLen(2); err != nil {
			return err
		}
		if err := w.Bulk(result.Key); err != nil {
			return err
		}
		// Write fields as flat array [field1, val1, field2, val2, ...]
		fieldNames := make([]string, 0, len(result.Fields))
		for f := range result.Fields {
			fieldNames = append(fieldNames, f)
		}
		sort.Strings(fieldNames)
		fields := make([]string, 0, len(result.Fields)*2)
		for _, f := range fieldNames {
			fields = append(fields, f, result.Fields[f])
		}
		if err := writeBulkStrings(w, fields); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) hdel(w *resp.Writer, args []string) error {
	if len(args) < 3 {
		return wrongArgs("HDEL")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ == "none" {
		return w.Int(0)
	} else if typ != "hash" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	deleted, err := s.store.HDel(args[1], args[2:]...)
	if err != nil {
		return err
	}
	return w.Int(int64(deleted))
}

func (s *Server) listPush(w *resp.Writer, args []string, left bool) error {
	if len(args) < 3 {
		return wrongArgs(args[0])
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ != "none" && typ != "list" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	var n int
	var err error
	if left {
		n, err = s.store.LPush(args[1], args[2:]...)
	} else {
		n, err = s.store.RPush(args[1], args[2:]...)
	}
	if err != nil {
		return err
	}
	return w.Int(int64(n))
}

func (s *Server) listPop(w *resp.Writer, args []string, left bool) error {
	if len(args) != 2 {
		return wrongArgs(args[0])
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ == "none" {
		return w.Null()
	} else if typ != "list" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	var value string
	var ok bool
	var err error
	if left {
		value, ok, err = s.store.LPop(args[1])
	} else {
		value, ok, err = s.store.RPop(args[1])
	}
	if err != nil {
		return err
	}
	if !ok {
		return w.Null()
	}
	return w.Bulk(value)
}

func (s *Server) blpop(w *resp.Writer, args []string, left bool) error {
	if len(args) < 3 {
		return wrongArgs(args[0])
	}
	timeout, err := strconv.ParseFloat(args[len(args)-1], 64)
	if err != nil {
		return err
	}
	keys := args[1 : len(args)-1]
	if ok, err := s.ensureOwns(w, keys...); !ok {
		return err
	}

	dur := time.Duration(timeout * float64(time.Second))
	key, val, ok := s.store.BPop(keys, dur, left)
	if !ok {
		return w.NullArray()
	}
	// Persist the pop
	s.persistMu.Lock()
	popCmd := "LPOP"
	if !left {
		popCmd = "RPOP"
	}
	_ = s.aof.Append([]string{popCmd, key})
	s.persistMu.Unlock()

	if err := w.ArrayLen(2); err != nil {
		return err
	}
	if err := w.Bulk(key); err != nil {
		return err
	}
	return w.Bulk(val)
}

func (s *Server) lrange(w *resp.Writer, args []string) error {
	if len(args) != 4 {
		return wrongArgs("LRANGE")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}
	stop, err := strconv.Atoi(args[3])
	if err != nil {
		return err
	}
	values, err := s.store.LRange(args[1], start, stop)
	if err != nil {
		return err
	}
	return writeBulkStrings(w, values)
}

func (s *Server) sadd(w *resp.Writer, args []string) error {
	if len(args) < 3 {
		return wrongArgs("SADD")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ != "none" && typ != "set" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	added, err := s.store.SAdd(args[1], args[2:]...)
	if err != nil {
		return err
	}
	return w.Int(int64(added))
}

func (s *Server) srem(w *resp.Writer, args []string) error {
	if len(args) < 3 {
		return wrongArgs("SREM")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ == "none" {
		return w.Int(0)
	} else if typ != "set" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	removed, err := s.store.SRem(args[1], args[2:]...)
	if err != nil {
		return err
	}
	return w.Int(int64(removed))
}

func (s *Server) zadd(w *resp.Writer, args []string) error {
	if len(args) < 4 || len(args)%2 != 0 {
		return wrongArgs("ZADD")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	members := make([]store.ZMember, 0, (len(args)-2)/2)
	for i := 2; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return err
		}
		members = append(members, store.ZMember{Score: score, Member: args[i+1]})
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ != "none" && typ != "zset" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	added, err := s.store.ZAdd(args[1], members)
	if err != nil {
		return err
	}
	return w.Int(int64(added))
}

func (s *Server) zrange(w *resp.Writer, args []string) error {
	if len(args) != 4 && len(args) != 5 {
		return wrongArgs("ZRANGE")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	start, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}
	stop, err := strconv.Atoi(args[3])
	if err != nil {
		return err
	}
	withScores := false
	if len(args) == 5 {
		if !strings.EqualFold(args[4], "WITHSCORES") {
			return wrongArgs("ZRANGE")
		}
		withScores = true
	}
	items, err := s.store.ZRange(args[1], start, stop)
	if err != nil {
		return err
	}
	if !withScores {
		values := make([]string, len(items))
		for i, item := range items {
			values[i] = item.Member
		}
		return writeBulkStrings(w, values)
	}
	if err := w.ArrayLen(len(items) * 2); err != nil {
		return err
	}
	for _, item := range items {
		if err := w.Bulk(item.Member); err != nil {
			return err
		}
		if err := w.Bulk(formatScore(item.Score)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) zrem(w *resp.Writer, args []string) error {
	if len(args) < 3 {
		return wrongArgs("ZREM")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if typ := s.store.Type(args[1]); typ == "none" {
		return w.Int(0)
	} else if typ != "zset" {
		return errors.New("wrong type")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	removed, err := s.store.ZRem(args[1], args[2:]...)
	if err != nil {
		return err
	}
	return w.Int(int64(removed))
}

func (s *Server) mset(w *resp.Writer, args []string) error {
	if len(args) < 3 || len(args)%2 != 1 {
		return wrongArgs("MSET")
	}
	keys := make([]string, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		keys = append(keys, args[i])
	}
	if ok, err := s.ensureOwns(w, keys...); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	persist := []string{"MSETABS"}
	for i := 1; i < len(args); i += 2 {
		persist = append(persist, args[i], args[i+1], "0")
	}
	if err := s.aof.Append(persist); err != nil {
		return err
	}
	for i := 1; i < len(args); i += 2 {
		s.store.SetAbsolute(args[i], args[i+1], time.Time{})
	}
	return w.Simple("OK")
}

func (s *Server) xadd(w *resp.Writer, args []string) error {
	if len(args) < 5 {
		return wrongArgs("XADD")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	id, err := s.store.NextStreamID(args[1], args[2])
	if err != nil {
		return err
	}
	persist := append([]string{"XADD", args[1], id}, args[3:]...)
	if err := s.aof.Append(persist); err != nil {
		return err
	}
	if err := s.store.XAddPrepared(args[1], id, args[3:]); err != nil {
		return err
	}
	return w.Bulk(id)
}

func (s *Server) xrange(w *resp.Writer, args []string) error {
	if len(args) != 4 && len(args) != 6 {
		return wrongArgs("XRANGE")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	count := 0
	if len(args) == 6 {
		if strings.ToUpper(args[4]) != "COUNT" {
			return wrongArgs("XRANGE")
		}
		n, err := strconv.Atoi(args[5])
		if err != nil {
			return err
		}
		count = n
	}
	return writeEntries(w, s.store.XRange(args[1], args[2], args[3], count))
}

func (s *Server) scan(w *resp.Writer, args []string) error {
	if len(args) < 2 {
		return wrongArgs("SCAN")
	}
	cursor, err := strconv.Atoi(args[1])
	if err != nil {
		return err
	}
	pattern := "*"
	count := 10
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return wrongArgs("SCAN")
		}
		switch strings.ToUpper(args[i]) {
		case "MATCH":
			pattern = args[i+1]
		case "COUNT":
			n, err := strconv.Atoi(args[i+1])
			if err != nil {
				return err
			}
			count = n
		default:
			return fmt.Errorf("unsupported SCAN option %q", args[i])
		}
	}
	next, keys := s.store.Scan(cursor, count, pattern)
	if err := w.ArrayLen(2); err != nil {
		return err
	}
	if err := w.Bulk(strconv.Itoa(next)); err != nil {
		return err
	}
	return writeBulkStrings(w, keys)
}

func (s *Server) xread(w *resp.Writer, args []string) error {
	block := time.Duration(-1)
	pos := 1
	if pos < len(args) && strings.ToUpper(args[pos]) == "BLOCK" {
		if pos+1 >= len(args) {
			return wrongArgs("XREAD")
		}
		ms, err := strconv.ParseInt(args[pos+1], 10, 64)
		if err != nil {
			return err
		}
		block = time.Duration(ms) * time.Millisecond
		pos += 2
	}
	if pos >= len(args) || strings.ToUpper(args[pos]) != "STREAMS" {
		return wrongArgs("XREAD")
	}
	parts := args[pos+1:]
	if len(parts) == 0 || len(parts)%2 != 0 {
		return wrongArgs("XREAD")
	}
	half := len(parts) / 2
	streams := parts[:half]
	ids := parts[half:]
	if ok, err := s.ensureOwns(w, streams...); !ok {
		return err
	}
	ids = append([]string(nil), ids...)
	for i, id := range ids {
		if id == "$" {
			ids[i] = s.store.LastID(streams[i])
		}
	}
	results := s.store.XRead(streams, ids)
	if len(results) == 0 && block >= 0 {
		s.store.Wait(streams, block)
		results = s.store.XRead(streams, ids)
	}
	if len(results) == 0 {
		return w.NullArray()
	}
	return writeStreamReadResults(w, results)
}

func (s *Server) xgroup(w *resp.Writer, args []string) error {
	if len(args) != 5 && len(args) != 6 {
		return wrongArgs("XGROUP")
	}
	if !strings.EqualFold(args[1], "CREATE") {
		return fmt.Errorf("unsupported XGROUP subcommand %q", args[1])
	}
	mkstream := false
	if len(args) == 6 {
		if !strings.EqualFold(args[5], "MKSTREAM") {
			return wrongArgs("XGROUP")
		}
		mkstream = true
	}
	stream, group, id := args[2], args[3], args[4]
	if ok, err := s.ensureOwns(w, stream); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	typ := s.store.Type(stream)
	if typ != "none" && typ != "stream" {
		return errors.New("wrong type")
	}
	if typ == "none" && !mkstream {
		return errors.New("stream does not exist")
	}
	if s.store.XGroupExists(stream, group) {
		return errors.New("consumer group already exists")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	if err := s.store.XGroupCreate(stream, group, id, mkstream); err != nil {
		return err
	}
	return w.Simple("OK")
}

func (s *Server) xreadgroup(w *resp.Writer, args []string) error {
	if len(args) < 7 || !strings.EqualFold(args[1], "GROUP") {
		return wrongArgs("XREADGROUP")
	}
	group, consumer := args[2], args[3]
	count := 10
	block := time.Duration(-1)
	pos := 4
	for pos < len(args) && !strings.EqualFold(args[pos], "STREAMS") {
		if pos+1 >= len(args) {
			return wrongArgs("XREADGROUP")
		}
		switch strings.ToUpper(args[pos]) {
		case "COUNT":
			n, err := strconv.Atoi(args[pos+1])
			if err != nil {
				return err
			}
			count = n
		case "BLOCK":
			ms, err := strconv.ParseInt(args[pos+1], 10, 64)
			if err != nil {
				return err
			}
			block = time.Duration(ms) * time.Millisecond
		default:
			return fmt.Errorf("unsupported XREADGROUP option %q", args[pos])
		}
		pos += 2
	}
	if pos >= len(args) || !strings.EqualFold(args[pos], "STREAMS") {
		return wrongArgs("XREADGROUP")
	}
	parts := args[pos+1:]
	if len(parts) == 0 || len(parts)%2 != 0 {
		return wrongArgs("XREADGROUP")
	}
	half := len(parts) / 2
	streams := parts[:half]
	ids := parts[half:]
	if ok, err := s.ensureOwns(w, streams...); !ok {
		return err
	}

	s.persistMu.Lock()
	results, err := s.store.XReadGroupPlan(group, consumer, streams, ids, count)
	if err != nil {
		s.persistMu.Unlock()
		return err
	}
	if len(results) == 0 && block >= 0 {
		s.persistMu.Unlock()
		s.store.Wait(streams, block)
		s.persistMu.Lock()
		results, err = s.store.XReadGroupPlan(group, consumer, streams, ids, count)
		if err != nil {
			s.persistMu.Unlock()
			return err
		}
	}
	if len(results) == 0 {
		s.persistMu.Unlock()
		return w.NullArray()
	}
	deliveredAt := time.Now()
	for _, stream := range store.SortStreamNames(results) {
		delivered := make([]string, len(results[stream]))
		for i, entry := range results[stream] {
			delivered[i] = entry.ID
		}
		record := append([]string{"XGROUPDELIVER", stream, group, consumer, strconv.FormatInt(deliveredAt.UnixNano(), 10)}, delivered...)
		if err := s.aof.Append(record); err != nil {
			s.persistMu.Unlock()
			return err
		}
		if err := s.store.XGroupDeliver(stream, group, consumer, delivered, deliveredAt); err != nil {
			s.persistMu.Unlock()
			return err
		}
	}
	s.persistMu.Unlock()
	return writeStreamReadResults(w, results)
}

func (s *Server) xack(w *resp.Writer, args []string) error {
	if len(args) < 4 {
		return wrongArgs("XACK")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if !s.store.XGroupExists(args[1], args[2]) {
		return errors.New("consumer group does not exist")
	}
	if err := s.aof.Append(args); err != nil {
		return err
	}
	n, err := s.store.XAck(args[1], args[2], args[3:]...)
	if err != nil {
		return err
	}
	return w.Int(int64(n))
}

func (s *Server) ensureOwns(w *resp.Writer, keys ...string) (bool, error) {
	if len(keys) > 1 && s.cluster.NodeCount() > 1 {
		slot := Slot(keys[0])
		for _, key := range keys[1:] {
			if Slot(key) != slot {
				return false, w.Error("CROSSSLOT Keys in request don't hash to the same slot")
			}
		}
	}
	for _, key := range keys {
		if !s.cluster.SelfOwns(key) {
			return false, moved(w, s.cluster.Owner(key), key)
		}
	}
	return true, nil
}

func writeStreamReadResults(w *resp.Writer, results map[string][]store.StreamEntry) error {
	names := store.SortStreamNames(results)
	if err := w.ArrayLen(len(names)); err != nil {
		return err
	}
	for _, name := range names {
		if err := w.ArrayLen(2); err != nil {
			return err
		}
		if err := w.Bulk(name); err != nil {
			return err
		}
		if err := writeEntries(w, results[name]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) subscribe(conn net.Conn, w *resp.Writer, channels []string) {
	if len(channels) == 0 {
		_ = w.Error("ERR wrong number of arguments for SUBSCRIBE")
		_ = w.Flush()
		return
	}

	type subMsg struct {
		channel string
		data    string
	}

	merged := make(chan subMsg, 128)
	done := make(chan struct{})
	connDead := make(chan struct{})

	subs := make([]chan string, len(channels))
	for i, channel := range channels {
		ch := s.pubsub.Subscribe(channel)
		subs[i] = ch
		_ = w.ArrayLen(3)
		_ = w.Bulk("subscribe")
		_ = w.Bulk(channel)
		_ = w.Int(int64(i + 1))

		go func(name string, ch chan string) {
			for {
				select {
				case m, ok := <-ch:
					if !ok {
						return
					}
					select {
					case merged <- subMsg{channel: name, data: m}:
					case <-done:
						return
					}
				case <-done:
					return
				}
			}
		}(channel, ch)
	}
	_ = w.Flush()

	// Detect client disconnect
	go func() {
		buf := make([]byte, 1)
		for {
			select {
			case <-done:
				return
			default:
			}
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Read(buf); err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				close(connDead)
				return
			}
		}
	}()

	defer func() {
		close(done)
		for i, channel := range channels {
			s.pubsub.Unsubscribe(channel, subs[i])
		}
	}()

	for {
		select {
		case m := <-merged:
			_ = w.ArrayLen(3)
			_ = w.Bulk("message")
			_ = w.Bulk(m.channel)
			_ = w.Bulk(m.data)
			if err := w.Flush(); err != nil {
				return
			}
		case <-connDead:
			return
		}
	}
}

func (s *Server) psubscribe(conn net.Conn, w *resp.Writer, patterns []string) {
	if len(patterns) == 0 {
		_ = w.Error("ERR wrong number of arguments for PSUBSCRIBE")
		_ = w.Flush()
		return
	}

	type pMsg struct {
		pattern string
		channel string
		data    string
	}

	merged := make(chan pMsg, 128)
	done := make(chan struct{})
	connDead := make(chan struct{})

	subs := make([]chan PSubMessage, len(patterns))
	for i, pattern := range patterns {
		ch := s.pubsub.PSubscribe(pattern)
		subs[i] = ch
		_ = w.ArrayLen(3)
		_ = w.Bulk("psubscribe")
		_ = w.Bulk(pattern)
		_ = w.Int(int64(i + 1))

		go func(pat string, ch chan PSubMessage) {
			for {
				select {
				case m, ok := <-ch:
					if !ok {
						return
					}
					select {
					case merged <- pMsg{pattern: m.Pattern, channel: m.Channel, data: m.Data}:
					case <-done:
						return
					}
				case <-done:
					return
				}
			}
		}(pattern, ch)
	}
	_ = w.Flush()

	// Detect client disconnect
	go func() {
		buf := make([]byte, 1)
		for {
			select {
			case <-done:
				return
			default:
			}
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Read(buf); err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				close(connDead)
				return
			}
		}
	}()

	defer func() {
		close(done)
		for i, pattern := range patterns {
			s.pubsub.PUnsubscribe(pattern, subs[i])
		}
	}()

	for {
		select {
		case m := <-merged:
			_ = w.ArrayLen(4)
			_ = w.Bulk("pmessage")
			_ = w.Bulk(m.pattern)
			_ = w.Bulk(m.channel)
			_ = w.Bulk(m.data)
			if err := w.Flush(); err != nil {
				return
			}
		case <-connDead:
			return
		}
	}
}

func (s *Server) clusterCommand(w *resp.Writer, args []string) error {
	if len(args) < 2 {
		return wrongArgs("CLUSTER")
	}
	switch strings.ToUpper(args[1]) {
	case "NODES":
		var b strings.Builder
		for _, n := range s.cluster.Nodes() {
			flags := "master"
			if n.Self {
				flags = "myself,master"
			}
			state := string(n.State)
			if state == "" {
				state = "online"
			}
			fmt.Fprintf(&b, "%s %s@0 %s %s - 0 0 connected %d-%d\n", n.ID, n.Addr, flags, state, n.Start, n.End)
		}
		return w.Bulk(b.String())
	case "SLOTS":
		nodes := s.cluster.Nodes()
		if err := w.ArrayLen(len(nodes)); err != nil {
			return err
		}
		for _, n := range nodes {
			host, portText, err := net.SplitHostPort(n.Addr)
			if err != nil {
				host = n.Addr
				portText = "0"
			}
			port, _ := strconv.Atoi(portText)
			if err := w.ArrayLen(3); err != nil {
				return err
			}
			_ = w.Int(int64(n.Start))
			_ = w.Int(int64(n.End))
			_ = w.ArrayLen(3)
			_ = w.Bulk(host)
			_ = w.Int(int64(port))
			_ = w.Bulk(n.ID)
		}
		return nil
	case "MEET":
		if len(args) != 3 {
			return wrongArgs("CLUSTER")
		}
		if err := s.cluster.Meet(args[2]); err != nil {
			return fmt.Errorf("ERR %s", err.Error())
		}
		return w.Simple("OK")
	case "FORGET":
		if len(args) != 3 {
			return wrongArgs("CLUSTER")
		}
		if err := s.cluster.Forget(args[2]); err != nil {
			return fmt.Errorf("ERR %s", err.Error())
		}
		return w.Simple("OK")
	case "INFO":
		return w.Bulk(s.cluster.Info())
	case "MYID":
		return w.Bulk(s.cluster.Self().ID)
	case "RESET":
		s.cluster.Reset()
		return w.Simple("OK")
	case "REPLICATE":
		return w.Simple("OK")
	case "FAILOVER":
		return w.Simple("OK")
	case "SAVECONFIG":
		return w.Simple("OK")
	case "KEYSLOT":
		if len(args) != 3 {
			return wrongArgs("CLUSTER")
		}
		return w.Int(int64(Slot(args[2])))
	case "COUNTKEYSINSLOT":
		if len(args) != 3 {
			return wrongArgs("CLUSTER")
		}
		slot, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		count := 0
		keys := s.store.Keys("*")
		for _, key := range keys {
			if Slot(key) == slot {
				count++
			}
		}
		return w.Int(int64(count))
	case "GETKEYSINSLOT":
		if len(args) != 4 {
			return wrongArgs("CLUSTER")
		}
		slot, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}
		maxKeys, err := strconv.Atoi(args[3])
		if err != nil {
			return err
		}
		keys := s.store.Keys("*")
		var matching []string
		for _, key := range keys {
			if Slot(key) == slot {
				matching = append(matching, key)
				if len(matching) >= maxKeys {
					break
				}
			}
		}
		return writeBulkStrings(w, matching)
	default:
		return fmt.Errorf("unsupported CLUSTER subcommand %q", args[1])
	}
}

func (s *Server) info(w *resp.Writer) error {
	keys, streams := s.store.Stats()
	self := s.cluster.Self()
	appendOnly := 0
	if s.aof != nil {
		appendOnly = 1
	}
	role := s.repl.Role()
	replCount := s.repl.ReplicaCount()
	replInfo := fmt.Sprintf("\r\n# Replication\r\nrole:%s\r\nconnected_slaves:%d\r\n", role, replCount)
	if role == RoleReplica {
		replInfo += fmt.Sprintf("master_host:%s\r\n", s.repl.LeaderAddr())
	}
	return w.Bulk(fmt.Sprintf("# Server\r\nvole_version:0.1.0\r\nnode_id:%s\r\naddr:%s\r\n\r\n# Persistence\r\nappendonly:%d\r\nsnapshot_path:%s\r\n\r\n# Keyspace\r\nkeys:%d\r\nstreams:%d\r\n%s", self.ID, self.Addr, appendOnly, s.snapshotPath, keys, streams, replInfo))
}

func (s *Server) snapshotLoop(ctx context.Context) {
	ticker := time.NewTicker(s.snapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.saveSnapshot(true); err != nil {
				log.Printf("snapshot failed: %v", err)
			}
		}
	}
}

func (s *Server) saveSnapshot(resetAOF bool) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if err := SaveSnapshot(s.snapshotPath, s.store); err != nil {
		return err
	}
	s.lastSave = time.Now()
	if resetAOF {
		return s.aof.Reset()
	}
	return nil
}

func writeEntries(w *resp.Writer, entries []store.StreamEntry) error {
	if err := w.ArrayLen(len(entries)); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := w.ArrayLen(2); err != nil {
			return err
		}
		if err := w.Bulk(entry.ID); err != nil {
			return err
		}
		if err := w.ArrayLen(len(entry.Fields)); err != nil {
			return err
		}
		for _, field := range entry.Fields {
			if err := w.Bulk(field); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeQueueMessages(w *resp.Writer, msgs []*store.QueueMessage) error {
	if err := w.ArrayLen(len(msgs)); err != nil {
		return err
	}
	for _, msg := range msgs {
		if err := w.ArrayLen(4); err != nil {
			return err
		}
		if err := w.Bulk(msg.ID); err != nil {
			return err
		}
		if err := w.Bulk(msg.Body); err != nil {
			return err
		}
		if err := w.Int(int64(msg.Retries)); err != nil {
			return err
		}
		if err := w.Bulk(msg.CreatedAt.Format(time.RFC3339)); err != nil {
			return err
		}
	}
	return nil
}

func writeBulkStrings(w *resp.Writer, values []string) error {
	if err := w.ArrayLen(len(values)); err != nil {
		return err
	}
	for _, value := range values {
		if err := w.Bulk(value); err != nil {
			return err
		}
	}
	return nil
}

func writeHashPairs(w *resp.Writer, pairs []store.HashPair) error {
	if err := w.ArrayLen(len(pairs) * 2); err != nil {
		return err
	}
	for _, pair := range pairs {
		if err := w.Bulk(pair.Field); err != nil {
			return err
		}
		if err := w.Bulk(pair.Value); err != nil {
			return err
		}
	}
	return nil
}

func formatScore(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
}

func moved(w *resp.Writer, node Node, key string) error {
	return w.Error(fmt.Sprintf("MOVED %d %s", Slot(key), node.Addr))
}

func wrongArgs(cmd string) error {
	return fmt.Errorf("wrong number of arguments for %s", cmd)
}

// isWriteCommand returns true for commands that may add or modify data,
// used to trigger eviction checks before writes.
func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "SETNX", "SETEX", "PSETEX", "MSET", "MSETNX", "APPEND",
		"INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
		"DEL", "RENAME", "COPY", "EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT", "PERSIST",
		"HSET", "HSETNX", "HMSET", "HINCRBY", "HINCRBYFLOAT", "HDEL",
		"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET", "LINSERT", "LREM",
		"SADD", "SREM", "SMOVE", "SPOP", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
		"ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX", "ZUNIONSTORE", "ZINTERSTORE",
		"XADD", "XGROUP", "XACK",
		"PFADD", "PFMERGE",
		"SETBIT", "SETRANGE",
		"GETSET", "GETDEL",
		"RATELIMIT", "RATELIMIT.RESET", "SETDELAYED",
		"ENQUEUE", "QACK", "QNACK",
		"JSON.SET", "JSON.DEL", "JSON.NUMINCRBY", "JSON.ARRAPPEND",
		"TAG", "TAGDEL",
		"TS.ADD", "TS.DOWNSAMPLE":
		return true
	}
	return false
}

func parseScoreRange(minStr, maxStr string) (float64, float64, error) {
	min := math.Inf(-1)
	max := math.Inf(1)
	if minStr != "-inf" {
		exclusive := false
		if strings.HasPrefix(minStr, "(") {
			exclusive = true
			minStr = minStr[1:]
		}
		v, err := strconv.ParseFloat(minStr, 64)
		if err != nil {
			return 0, 0, err
		}
		min = v
		if exclusive {
			min = math.Nextafter(v, math.Inf(1))
		}
	}
	if maxStr != "+inf" {
		exclusive := false
		if strings.HasPrefix(maxStr, "(") {
			exclusive = true
			maxStr = maxStr[1:]
		}
		v, err := strconv.ParseFloat(maxStr, 64)
		if err != nil {
			return 0, 0, err
		}
		max = v
		if exclusive {
			max = math.Nextafter(v, math.Inf(-1))
		}
	}
	return min, max, nil
}

func convertGeoUnit(meters float64, unit string) float64 {
	switch unit {
	case "km":
		return meters / 1000
	case "mi":
		return meters / 1609.344
	case "ft":
		return meters / 0.3048
	default:
		return meters
	}
}

func geoUnitToMeters(val float64, unit string) float64 {
	switch unit {
	case "km":
		return val * 1000
	case "mi":
		return val * 1609.344
	case "ft":
		return val * 0.3048
	default:
		return val
	}
}

func (s *Server) geoSearch(w *resp.Writer, args []string) error {
	// GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius m|km|mi|ft [ASC|DESC] [COUNT count] [WITHCOORD] [WITHDIST]
	if len(args) < 6 {
		return wrongArgs("GEOSEARCH")
	}
	if ok, err := s.ensureOwns(w, args[1]); !ok {
		return err
	}

	var centerLon, centerLat float64
	var fromMember string
	var radius float64
	asc := true
	count := 0
	withCoord := false
	withDist := false

	i := 2
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "FROMMEMBER":
			if i+1 >= len(args) {
				return wrongArgs("GEOSEARCH")
			}
			fromMember = args[i+1]
			i += 2
		case "FROMLONLAT":
			if i+2 >= len(args) {
				return wrongArgs("GEOSEARCH")
			}
			var err error
			centerLon, err = strconv.ParseFloat(args[i+1], 64)
			if err != nil {
				return err
			}
			centerLat, err = strconv.ParseFloat(args[i+2], 64)
			if err != nil {
				return err
			}
			i += 3
		case "BYRADIUS":
			if i+2 >= len(args) {
				return wrongArgs("GEOSEARCH")
			}
			var err error
			radius, err = strconv.ParseFloat(args[i+1], 64)
			if err != nil {
				return err
			}
			radius = geoUnitToMeters(radius, strings.ToLower(args[i+2]))
			i += 3
		case "ASC":
			asc = true
			i++
		case "DESC":
			asc = false
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return wrongArgs("GEOSEARCH")
			}
			var err error
			count, err = strconv.Atoi(args[i+1])
			if err != nil {
				return err
			}
			i += 2
		case "WITHCOORD":
			withCoord = true
			i++
		case "WITHDIST":
			withDist = true
			i++
		default:
			i++
		}
	}

	var results []store.GeoResult
	if fromMember != "" {
		var err error
		results, err = s.store.GeoSearchByMember(args[1], fromMember, radius, count, asc)
		if err != nil {
			return err
		}
	} else {
		results = s.store.GeoSearchByRadius(args[1], centerLon, centerLat, radius, count, asc)
	}

	if err := w.ArrayLen(len(results)); err != nil {
		return err
	}
	for _, r := range results {
		extras := 0
		if withDist {
			extras++
		}
		if withCoord {
			extras++
		}

		if extras == 0 {
			if err := w.Bulk(r.Member); err != nil {
				return err
			}
		} else {
			if err := w.ArrayLen(1 + extras); err != nil {
				return err
			}
			if err := w.Bulk(r.Member); err != nil {
				return err
			}
			if withDist {
				if err := w.Bulk(strconv.FormatFloat(r.Dist, 'f', 4, 64)); err != nil {
					return err
				}
			}
			if withCoord {
				if err := w.ArrayLen(2); err != nil {
					return err
				}
				if err := w.Bulk(strconv.FormatFloat(r.Longitude, 'f', -1, 64)); err != nil {
					return err
				}
				if err := w.Bulk(strconv.FormatFloat(r.Latitude, 'f', -1, 64)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func LogStartup(addr string, cluster *Cluster, opts Options) {
	self := cluster.Self()
	log.Printf("vole listening on %s node=%s slots=%d-%d", addr, self.ID, self.Start, self.End)
	if opts.AppendOnly {
		log.Printf("  persistence: AOF (%s) fsync=%s", opts.AOFPath, opts.AppendFsync)
	}
	if opts.SnapshotPath != "" {
		log.Printf("  persistence: snapshot (%s) interval=%s", opts.SnapshotPath, opts.SnapshotInterval)
	}
	if opts.HTTPAddr != "" {
		log.Printf("  HTTP API: %s", opts.HTTPAddr)
	}
	if opts.TLSCert != "" {
		log.Printf("  TLS: enabled")
	}
	if opts.Password != "" {
		log.Printf("  auth: password required")
	}
}
