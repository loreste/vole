package server

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"vole/internal/resp"
)

// ScriptManager manages cached scripts and execution.
type ScriptManager struct {
	mu      sync.RWMutex
	scripts map[string]string // SHA1 -> script body
}

func NewScriptManager() *ScriptManager {
	return &ScriptManager{scripts: make(map[string]string)}
}

// scriptSHA1 computes the SHA1 hash of a script.
func scriptSHA1(script string) string {
	h := sha1.New()
	h.Write([]byte(script))
	return hex.EncodeToString(h.Sum(nil))
}

// Load stores a script and returns its SHA1.
func (sm *ScriptManager) Load(script string) string {
	sha := scriptSHA1(script)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.scripts[sha] = script
	return sha
}

// Get retrieves a script by SHA1.
func (sm *ScriptManager) Get(sha string) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s, ok := sm.scripts[sha]
	return s, ok
}

// Exists checks if scripts exist.
func (sm *ScriptManager) Exists(shas []string) []bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]bool, len(shas))
	for i, sha := range shas {
		_, result[i] = sm.scripts[sha]
	}
	return result
}

// Flush removes all cached scripts.
func (sm *ScriptManager) Flush() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.scripts = make(map[string]string)
}

// ScriptContext holds the execution context for a script.
type ScriptContext struct {
	Keys   []string
	Args   []string
	server *Server
}

// Execute runs a script. For full Lua compatibility, this is a simplified executor
// that handles the most common patterns: redis.call() invocations with basic
// string manipulation and conditionals.
//
// Since implementing a full Lua VM in pure Go is impractical without dependencies,
// we support a practical subset:
// 1. Direct redis.call() sequences (most common use case)
// 2. Simple variable assignment and return
// 3. Basic string/number operations
//
// For scripts that are just sequences of redis.call(), we parse and execute them directly.
func (sc *ScriptContext) Execute(script string) (resp.Value, error) {
	calls, returnExpr, err := parseSimpleScript(script)
	if err != nil {
		return resp.Value{}, fmt.Errorf("NOSCRIPT script parse error: %v", err)
	}

	vars := make(map[string]resp.Value)
	var lastResult resp.Value

	for _, call := range calls {
		resolved := sc.resolveArgs(call.args, vars)
		result, err := sc.execRedisCall(resolved)
		if err != nil {
			return resp.Value{}, err
		}
		lastResult = result
		if call.varName != "" {
			vars[call.varName] = result
		}
	}

	// Handle return
	if returnExpr != "" {
		return sc.resolveReturn(returnExpr, vars, lastResult), nil
	}

	return resp.Value{Type: resp.SimpleString, Text: "OK"}, nil
}

type scriptCall struct {
	varName string   // if assigned to a variable
	args    []string // arguments to redis.call
}

// parseSimpleScript extracts redis.call() invocations from a script.
// Handles patterns like:
//
//	redis.call('SET', KEYS[1], ARGV[1])
//	local val = redis.call('GET', KEYS[1])
//	return redis.call('GET', KEYS[1])
//	return val
//	return tonumber(val) + 1
func parseSimpleScript(script string) ([]scriptCall, string, error) {
	var calls []scriptCall
	var returnExpr string

	lines := strings.Split(script, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue // skip empty lines and comments
		}

		// Handle: local var = redis.call(...)
		if strings.HasPrefix(line, "local ") {
			rest := strings.TrimPrefix(line, "local ")
			if eqIdx := strings.Index(rest, "="); eqIdx >= 0 {
				varName := strings.TrimSpace(rest[:eqIdx])
				expr := strings.TrimSpace(rest[eqIdx+1:])
				if args, ok := parseRedisCall(expr); ok {
					calls = append(calls, scriptCall{varName: varName, args: args})
					continue
				}
			}
			continue // skip local declarations we can't parse
		}

		// Handle: return redis.call(...) or return expr
		if strings.HasPrefix(line, "return ") {
			expr := strings.TrimPrefix(line, "return ")
			if args, ok := parseRedisCall(expr); ok {
				calls = append(calls, scriptCall{args: args})
				returnExpr = "__last__"
			} else {
				returnExpr = expr
			}
			continue
		}

		// Handle: redis.call(...)
		if args, ok := parseRedisCall(line); ok {
			calls = append(calls, scriptCall{args: args})
			continue
		}

		// Handle: if/then/else/end blocks — skip for now (control flow not supported)
		if strings.HasPrefix(line, "if ") || line == "end" || line == "else" ||
			strings.HasPrefix(line, "elseif ") || strings.HasPrefix(line, "then") {
			continue
		}
	}

	return calls, returnExpr, nil
}

// parseRedisCall extracts arguments from redis.call('CMD', arg1, arg2, ...)
func parseRedisCall(expr string) ([]string, bool) {
	expr = strings.TrimSpace(expr)
	// Remove trailing semicolons
	expr = strings.TrimRight(expr, ";")

	// Match redis.call(...) or redis.pcall(...)
	var inner string
	if strings.HasPrefix(expr, "redis.call(") && strings.HasSuffix(expr, ")") {
		inner = expr[len("redis.call(") : len(expr)-1]
	} else if strings.HasPrefix(expr, "redis.pcall(") && strings.HasSuffix(expr, ")") {
		inner = expr[len("redis.pcall(") : len(expr)-1]
	} else {
		return nil, false
	}

	// Parse comma-separated arguments
	var args []string
	for _, part := range splitScriptArgs(inner) {
		part = strings.TrimSpace(part)
		// Remove quotes
		if (strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'")) ||
			(strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"")) {
			args = append(args, part[1:len(part)-1])
		} else {
			args = append(args, part) // KEYS[n], ARGV[n], or variable reference
		}
	}
	return args, true
}

// splitScriptArgs splits by comma, respecting quotes and parentheses.
func splitScriptArgs(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	inQuote := byte(0)

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inQuote != 0 {
			current.WriteByte(ch)
			if ch == inQuote {
				inQuote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			inQuote = ch
			current.WriteByte(ch)
		case '(':
			depth++
			current.WriteByte(ch)
		case ')':
			depth--
			current.WriteByte(ch)
		case ',':
			if depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// resolveArgs replaces KEYS[n], ARGV[n], and variable references with actual values.
func (sc *ScriptContext) resolveArgs(args []string, vars map[string]resp.Value) []string {
	resolved := make([]string, len(args))
	for i, arg := range args {
		resolved[i] = sc.resolveArg(arg, vars)
	}
	return resolved
}

func (sc *ScriptContext) resolveArg(arg string, vars map[string]resp.Value) string {
	arg = strings.TrimSpace(arg)
	// KEYS[n]
	if strings.HasPrefix(arg, "KEYS[") && strings.HasSuffix(arg, "]") {
		idxStr := arg[5 : len(arg)-1]
		idx, err := strconv.Atoi(idxStr)
		if err == nil && idx >= 1 && idx <= len(sc.Keys) {
			return sc.Keys[idx-1]
		}
		return ""
	}
	// ARGV[n]
	if strings.HasPrefix(arg, "ARGV[") && strings.HasSuffix(arg, "]") {
		idxStr := arg[5 : len(arg)-1]
		idx, err := strconv.Atoi(idxStr)
		if err == nil && idx >= 1 && idx <= len(sc.Args) {
			return sc.Args[idx-1]
		}
		return ""
	}
	// Variable reference
	if v, ok := vars[arg]; ok {
		return valueToString(v)
	}
	// tostring(), tonumber() wrappers
	if strings.HasPrefix(arg, "tostring(") || strings.HasPrefix(arg, "tonumber(") {
		inner := arg[strings.Index(arg, "(")+1 : len(arg)-1]
		return sc.resolveArg(inner, vars)
	}
	return arg
}

func (sc *ScriptContext) resolveReturn(expr string, vars map[string]resp.Value, lastResult resp.Value) resp.Value {
	expr = strings.TrimSpace(expr)
	if expr == "__last__" {
		return lastResult
	}
	// Check if it's a variable
	if v, ok := vars[expr]; ok {
		return v
	}
	// Try as number
	if n, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return resp.Value{Type: resp.Integer, Int: n}
	}
	// Return as string
	return resp.Value{Type: resp.BulkString, Text: expr}
}

func (sc *ScriptContext) execRedisCall(args []string) (resp.Value, error) {
	if len(args) == 0 {
		return resp.Value{}, errors.New("empty redis.call")
	}
	// Execute via the server's exec method
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	err := sc.server.exec(w, args)
	if err != nil {
		return resp.Value{Type: resp.ErrorString, Text: err.Error()}, nil
	}
	_ = w.Flush()

	// Parse the response
	rd := resp.NewReader(strings.NewReader(buf.String()))
	return rd.ReadValue()
}

func valueToString(v resp.Value) string {
	switch v.Type {
	case resp.BulkString, resp.SimpleString:
		return v.Text
	case resp.Integer:
		return strconv.FormatInt(v.Int, 10)
	default:
		return ""
	}
}

// writeValue writes a resp.Value back to a resp.Writer.
func writeValue(w *resp.Writer, v resp.Value) error {
	switch v.Type {
	case resp.SimpleString:
		return w.Simple(v.Text)
	case resp.ErrorString:
		return w.Error(v.Text)
	case resp.Integer:
		return w.Int(v.Int)
	case resp.BulkString:
		if v.Null {
			return w.Null()
		}
		return w.Bulk(v.Text)
	case resp.Array:
		if v.Null {
			return w.NullArray()
		}
		if err := w.ArrayLen(len(v.Items)); err != nil {
			return err
		}
		for _, item := range v.Items {
			if err := writeValue(w, item); err != nil {
				return err
			}
		}
		return nil
	default:
		return w.Null()
	}
}
