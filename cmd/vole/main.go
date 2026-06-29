package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"vole/internal/config"
	"vole/internal/server"
)

func main() {
	configPath := flag.String("config", "", "path to config file (e.g. /etc/vole/vole.conf)")
	addr := flag.String("addr", "127.0.0.1:7379", "TCP listen address")
	data := flag.String("data", "", "compatibility alias for -appendfilename")
	appendOnly := flag.Bool("appendonly", true, "enable append-only persistence")
	appendFilename := flag.String("appendfilename", "vole.aof", "append-only persistence file")
	appendFsync := flag.String("appendfsync", server.FsyncEverySec, "append-only fsync policy: always, everysec, or no")
	snapshot := flag.String("snapshot", "vole.rdb.json", "snapshot file path")
	snapshotInterval := flag.Duration("snapshot-interval", 0, "automatic snapshot interval, for example 60s; 0 disables periodic snapshots")
	nodeID := flag.String("node-id", "", "stable cluster node ID")
	peers := flag.String("peers", "", "comma-separated peer list as nodeID@host:port")
	maxMemory := flag.Int64("maxmemory", 0, "max memory in bytes (0 = unlimited)")
	maxMemoryPolicy := flag.String("maxmemory-policy", "noeviction", "eviction policy: noeviction, allkeys-random, volatile-random, allkeys-lru")
	httpAddr := flag.String("http-addr", "", "HTTP API listen address (e.g. :8080), empty to disable")
	password := flag.String("requirepass", "", "require password for client connections")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file path")
	tlsKey := flag.String("tls-key", "", "TLS private key file path")
	replicaOf := flag.String("replicaof", "", "replicate from this address (host:port)")
	multimaster := flag.Bool("multimaster", false, "enable multi-master replication")
	flag.Parse()

	// Load config file if given. CLI flags always win.
	if *configPath != "" {
		cfg, err := config.Load(*configPath)
		if err != nil {
			log.Fatalf("failed to load config %s: %v", *configPath, err)
		}
		applyDefaults(cfg, addr, data, appendOnly, appendFilename, appendFsync,
			snapshot, snapshotInterval, nodeID, peers, maxMemory, maxMemoryPolicy,
			httpAddr, password, tlsCert, tlsKey, replicaOf)
		log.Printf("loaded config from %s", *configPath)
	}

	if *data != "" {
		*appendFilename = *data
	}
	if *snapshotInterval < 0 {
		log.Fatal("snapshot-interval must be >= 0")
	}
	if *appendFsync != server.FsyncAlways && *appendFsync != server.FsyncEverySec && *appendFsync != server.FsyncNo {
		log.Fatal("appendfsync must be always, everysec, or no")
	}
	opts := server.Options{
		Addr:             *addr,
		AOFPath:          *appendFilename,
		AppendOnly:       *appendOnly,
		AppendFsync:      *appendFsync,
		SnapshotPath:     *snapshot,
		SnapshotInterval: time.Duration(*snapshotInterval),
		NodeID:           *nodeID,
		Peers:            *peers,
		MaxMemory:        *maxMemory,
		MaxMemoryPolicy:  *maxMemoryPolicy,
		HTTPAddr:         *httpAddr,
		Password:         *password,
		TLSCert:          *tlsCert,
		TLSKey:           *tlsKey,
	}
	srv, err := server.NewWithOptions(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *replicaOf != "" {
		parts := strings.SplitN(*replicaOf, ":", 2)
		if len(parts) != 2 {
			log.Fatal("replicaof must be host:port")
		}
		if err := srv.ReplicaOf(ctx, *replicaOf); err != nil {
			log.Fatalf("failed to start replication: %v", err)
		}
	}

	if *multimaster {
		srv.EnableMultiMaster()
	}

	if *httpAddr != "" {
		httpSrv := server.NewHTTPServer(srv, *httpAddr)
		go func() {
			log.Printf("vole HTTP API listening on %s", *httpAddr)
			if err := httpSrv.ListenAndServe(ctx); err != nil {
				log.Printf("HTTP server error: %v", err)
			}
		}()
	}

	server.LogStartup(*addr, srv.Cluster(), opts)
	if err := srv.ListenAndServe(ctx); err != nil {
		log.Fatal(err)
	}
}

// applyDefaults fills in flag values from a config file. Flags that were
// explicitly passed on the command line are left alone.
func applyDefaults(cfg map[string]string,
	addr, data *string, appendOnly *bool, appendFilename, appendFsync *string,
	snapshot *string, snapshotInterval *time.Duration,
	nodeID, peers *string, maxMemory *int64, maxMemoryPolicy *string,
	httpAddr, password, tlsCert, tlsKey, replicaOf *string,
) {
	set := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { set[f.Name] = true })

	str := func(name string, dst *string) {
		if v, ok := cfg[name]; ok && !set[name] {
			*dst = v
		}
	}

	str("addr", addr)
	str("data", data)
	str("appendfilename", appendFilename)
	str("appendfsync", appendFsync)
	str("snapshot", snapshot)
	str("node-id", nodeID)
	str("peers", peers)
	str("maxmemory-policy", maxMemoryPolicy)
	str("http-addr", httpAddr)
	str("requirepass", password)
	str("tls-cert", tlsCert)
	str("tls-key", tlsKey)
	str("replicaof", replicaOf)

	if v, ok := cfg["appendonly"]; ok && !set["appendonly"] {
		lower := strings.ToLower(v)
		*appendOnly = lower == "true" || lower == "yes" || lower == "1"
	}
	if v, ok := cfg["maxmemory"]; ok && !set["maxmemory"] {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			*maxMemory = n
		}
	}
	if v, ok := cfg["snapshot-interval"]; ok && !set["snapshot-interval"] {
		if v == "0" || v == "" {
			*snapshotInterval = 0
		} else if d, err := time.ParseDuration(v); err == nil {
			*snapshotInterval = d
		}
	}
}
