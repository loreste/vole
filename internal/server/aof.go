package server

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"vole/internal/store"
)

type AOF struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	fsync    string
	lastSync time.Time
}

const (
	FsyncAlways   = "always"
	FsyncEverySec = "everysec"
	FsyncNo       = "no"
)

func OpenAOF(path, fsync string) (*AOF, error) {
	if path == "" {
		return nil, nil
	}
	if fsync == "" {
		fsync = FsyncEverySec
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &AOF{file: f, path: path, fsync: fsync}, nil
}

func (a *AOF) Close() error {
	if a == nil || a.file == nil {
		return nil
	}
	return a.file.Close()
}

func (a *AOF) Append(args []string) error {
	if a == nil || a.file == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	// Build the line content
	var line strings.Builder
	for i, arg := range args {
		if i > 0 {
			line.WriteByte('\t')
		}
		line.WriteString(escape(arg))
	}
	content := line.String()

	// Write content + checksum
	checksum := crc32.ChecksumIEEE([]byte(content))
	if _, err := fmt.Fprintf(a.file, "%s\t#%08x\n", content, checksum); err != nil {
		return err
	}
	switch a.fsync {
	case FsyncAlways:
		a.lastSync = time.Now()
		return a.file.Sync()
	case FsyncEverySec:
		if time.Since(a.lastSync) >= time.Second {
			a.lastSync = time.Now()
			return a.file.Sync()
		}
	}
	return nil
}

func (a *AOF) Reset() error {
	if a == nil || a.file == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.file.Truncate(0); err != nil {
		return err
	}
	if _, err := a.file.Seek(0, 0); err != nil {
		return err
	}
	a.lastSync = time.Now()
	return a.file.Sync()
}

func ReplayAOF(path string, st *store.Store) error {
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		args, valid := verifyAndParseLine(line)
		if !valid {
			log.Printf("aof: skipping corrupted entry")
			continue
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "SETABS":
			if len(args) == 4 {
				var ns int64
				_, _ = fmt.Sscan(args[3], &ns)
				var expireAt time.Time
				if ns > 0 {
					expireAt = time.Unix(0, ns)
				}
				st.SetAbsolute(args[1], args[2], expireAt)
			}
		case "MSETABS":
			if len(args) >= 4 && (len(args)-1)%3 == 0 {
				for i := 1; i < len(args); i += 3 {
					var ns int64
					_, _ = fmt.Sscan(args[i+2], &ns)
					var expireAt time.Time
					if ns > 0 {
						expireAt = time.Unix(0, ns)
					}
					st.SetAbsolute(args[i], args[i+1], expireAt)
				}
			}
		case "SET":
			var ttl time.Duration
			if len(args) >= 5 {
				switch strings.ToUpper(args[3]) {
				case "PX":
					var ms int64
					_, _ = fmt.Sscan(args[4], &ms)
					ttl = time.Duration(ms) * time.Millisecond
				case "EX":
					var sec int64
					_, _ = fmt.Sscan(args[4], &sec)
					ttl = time.Duration(sec) * time.Second
				}
			}
			if len(args) >= 3 {
				st.Set(args[1], args[2], ttl)
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
				_, _ = st.HSet(args[1], pairs)
			}
		case "HDEL":
			if len(args) > 2 {
				_, _ = st.HDel(args[1], args[2:]...)
			}
		case "LPUSH":
			if len(args) > 2 {
				_, _ = st.LPush(args[1], args[2:]...)
			}
		case "RPUSH":
			if len(args) > 2 {
				_, _ = st.RPush(args[1], args[2:]...)
			}
		case "LPOP":
			if len(args) == 2 {
				_, _, _ = st.LPop(args[1])
			}
		case "RPOP":
			if len(args) == 2 {
				_, _, _ = st.RPop(args[1])
			}
		case "LSET":
			if len(args) == 4 {
				index, _ := strconv.Atoi(args[2])
				_ = st.LSet(args[1], index, args[3])
			}
		case "LINSERT":
			if len(args) == 5 {
				before := strings.EqualFold(args[2], "BEFORE")
				_, _ = st.LInsert(args[1], before, args[3], args[4])
			}
		case "SADD":
			if len(args) > 2 {
				_, _ = st.SAdd(args[1], args[2:]...)
			}
		case "SREM":
			if len(args) > 2 {
				_, _ = st.SRem(args[1], args[2:]...)
			}
		case "SMOVE":
			if len(args) == 4 {
				_, _ = st.SMove(args[1], args[2], args[3])
			}
		case "LREM":
			if len(args) == 4 {
				count, _ := strconv.Atoi(args[2])
				_, _ = st.LRem(args[1], count, args[3])
			}
		case "ZADD":
			if len(args) >= 4 && len(args)%2 == 0 {
				members := make([]store.ZMember, 0, (len(args)-2)/2)
				for i := 2; i < len(args); i += 2 {
					var score float64
					_, _ = fmt.Sscan(args[i], &score)
					members = append(members, store.ZMember{Score: score, Member: args[i+1]})
				}
				_, _ = st.ZAdd(args[1], members)
			}
		case "ZREM":
			if len(args) > 2 {
				_, _ = st.ZRem(args[1], args[2:]...)
			}
		case "INCR":
			if len(args) == 2 {
				_, _ = st.Incr(args[1])
			}
		case "INCRBY":
			if len(args) == 3 {
				var delta int64
				_, _ = fmt.Sscan(args[2], &delta)
				_, _ = st.IncrBy(args[1], delta)
			}
		case "DECRBY":
			if len(args) == 3 {
				var delta int64
				_, _ = fmt.Sscan(args[2], &delta)
				_, _ = st.DecrBy(args[1], delta)
			}
		case "DECR":
			if len(args) == 2 {
				_, _ = st.IncrBy(args[1], -1)
			}
		case "INCRBYFLOAT":
			if len(args) == 3 {
				var delta float64
				_, _ = fmt.Sscan(args[2], &delta)
				_, _ = st.IncrByFloat(args[1], delta)
			}
		case "HINCRBY":
			if len(args) == 4 {
				var delta int64
				_, _ = fmt.Sscan(args[3], &delta)
				_, _ = st.HIncrBy(args[1], args[2], delta)
			}
		case "APPEND":
			if len(args) == 3 {
				_, _ = st.Append(args[1], args[2])
			}
		case "EXPIRE":
			if len(args) == 3 {
				var sec int64
				_, _ = fmt.Sscan(args[2], &sec)
				st.Expire(args[1], time.Duration(sec)*time.Second)
			}
		case "EXPIREATABS":
			if len(args) == 3 {
				var ns int64
				_, _ = fmt.Sscan(args[2], &ns)
				if ns == 0 {
					st.ExpireAt(args[1], time.Time{})
				} else {
					st.ExpireAt(args[1], time.Unix(0, ns))
				}
			}
		case "RENAME":
			if len(args) == 3 {
				_ = st.Rename(args[1], args[2])
			}
		case "XADD":
			if len(args) >= 5 {
				_, _ = st.XAdd(args[1], args[2], args[3:])
			}
		case "XGROUP":
			if len(args) >= 5 && strings.EqualFold(args[1], "CREATE") {
				mkstream := len(args) == 6 && strings.EqualFold(args[5], "MKSTREAM")
				_ = st.XGroupCreate(args[2], args[3], args[4], mkstream)
			}
		case "XGROUPDELIVER":
			if len(args) >= 6 {
				var ns int64
				_, _ = fmt.Sscan(args[4], &ns)
				_ = st.XGroupDeliver(args[1], args[2], args[3], args[5:], time.Unix(0, ns))
			}
		case "XACK":
			if len(args) >= 4 {
				_, _ = st.XAck(args[1], args[2], args[3:]...)
			}
		case "PFADD":
			if len(args) >= 2 {
				_, _ = st.PFAdd(args[1], args[2:]...)
			}
		case "PFMERGE":
			if len(args) >= 2 {
				_ = st.PFMerge(args[1], args[2:]...)
			}
		case "COPY":
			if len(args) >= 3 {
				replace := false
				for _, a := range args[3:] {
					if strings.EqualFold(a, "REPLACE") {
						replace = true
					}
				}
				_, _ = st.Copy(args[1], args[2], replace)
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
				// On replay, the delay may have already passed. Set with remaining delay.
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
				_ = st.Enqueue(args[1], args[2], delay)
			}
		case "QACK":
			if len(args) == 3 {
				_ = st.QAck(args[1], args[2])
			}
		case "QNACK":
			if len(args) == 3 {
				_ = st.QNack(args[1], args[2])
			}
		case "JSON.SET":
			if len(args) == 4 {
				_ = st.JSONSet(args[1], args[2], args[3])
			}
		case "JSON.DEL":
			if len(args) >= 2 {
				path := "$"
				if len(args) == 3 {
					path = args[2]
				}
				_, _ = st.JSONDel(args[1], path)
			}
		case "JSON.NUMINCRBY":
			if len(args) == 4 {
				delta, _ := strconv.ParseFloat(args[3], 64)
				_, _ = st.JSONNumIncrBy(args[1], args[2], delta)
			}
		case "JSON.ARRAPPEND":
			if len(args) >= 4 {
				_, _ = st.JSONArrAppend(args[1], args[2], args[3:]...)
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
				_ = st.TagSet(args[1], tags)
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
				_ = st.TSAdd(args[1], ts, val, labels)
			}
		}
	}
	return scanner.Err()
}

func verifyAndParseLine(line string) ([]string, bool) {
	args := unescapeLine(line)
	if len(args) == 0 {
		return nil, true // empty line, skip
	}
	// Check for checksum in last field
	last := args[len(args)-1]
	if len(last) == 9 && last[0] == '#' {
		// Has checksum - verify it
		// Reconstruct the content portion (everything before the last tab-separated field)
		contentEnd := strings.LastIndex(line, "\t")
		if contentEnd < 0 {
			return nil, false
		}
		content := line[:contentEnd]
		expected, err := strconv.ParseUint(last[1:], 16, 32)
		if err != nil {
			return nil, false
		}
		actual := crc32.ChecksumIEEE([]byte(content))
		if uint32(expected) != actual {
			return nil, false
		}
		return args[:len(args)-1], true
	}
	// No checksum (old format) - accept as-is
	return args, true
}

func escape(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\t", "\\t")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func unescapeLine(line string) []string {
	raw := strings.Split(line, "\t")
	out := make([]string, len(raw))
	for i, part := range raw {
		part = strings.ReplaceAll(part, "\\n", "\n")
		part = strings.ReplaceAll(part, "\\t", "\t")
		part = strings.ReplaceAll(part, "\\\\", "\\")
		out[i] = part
	}
	return out
}
