package main

import (
	"bytes"
	"testing"

	"vole/internal/resp"
)

func TestSplitArgs(t *testing.T) {
	got, err := splitArgs(`SET message "hello world" 'again'`)
	if err != nil {
		t.Fatalf("split failed: %v", err)
	}
	want := []string{"SET", "message", "hello world", "again"}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

func TestPrintValue(t *testing.T) {
	var buf bytes.Buffer
	printValue(&buf, resp.Value{
		Type: resp.Array,
		Items: []resp.Value{
			{Type: resp.BulkString, Text: "events"},
			{Type: resp.Integer, Int: 2},
		},
	}, false, 0)
	want := "1) \"events\"\n2) (integer) 2\n"
	if got := buf.String(); got != want {
		t.Fatalf("want %q, got %q", want, got)
	}
}

func TestIsStreamingCommand(t *testing.T) {
	if !isStreamingCommand([]string{"subscribe", "events"}) {
		t.Fatal("expected subscribe to be streaming")
	}
	if !isStreamingCommand([]string{"PSUBSCRIBE", "__keyevent__:*"}) {
		t.Fatal("expected psubscribe to be streaming")
	}
	if isStreamingCommand([]string{"xread", "block", "0", "streams", "events", "$"}) {
		t.Fatal("xread returns a single response per command")
	}
}

func TestHelpDoesNotPanic(t *testing.T) {
	// Just make sure printHelp doesn't crash.
	printHelp()
}

func TestMovedAddress(t *testing.T) {
	addr, ok := movedAddress(resp.Value{Type: resp.ErrorString, Text: "MOVED 12182 127.0.0.1:7380"})
	if !ok || addr != "127.0.0.1:7380" {
		t.Fatalf("expected MOVED address, got addr=%q ok=%v", addr, ok)
	}
	if addr, ok := movedAddress(resp.Value{Type: resp.ErrorString, Text: "ERR wrong type"}); ok || addr != "" {
		t.Fatalf("expected non-MOVED error to be ignored, got addr=%q ok=%v", addr, ok)
	}
	if addr, ok := movedAddress(resp.Value{Type: resp.BulkString, Text: "MOVED 1 host:1"}); ok || addr != "" {
		t.Fatalf("expected non-error response to be ignored, got addr=%q ok=%v", addr, ok)
	}
}

func TestParseOptions(t *testing.T) {
	opts, err := parseOptions([]string{"-h", "localhost", "-p", "6380", "--raw", "GET", "k"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if opts.addr != "localhost:6380" {
		t.Fatalf("expected localhost:6380, got %q", opts.addr)
	}
	if !opts.raw {
		t.Fatal("expected raw option")
	}
	if len(opts.args) != 2 || opts.args[0] != "GET" || opts.args[1] != "k" {
		t.Fatalf("unexpected args: %v", opts.args)
	}
}

func TestParseOptionsAddrOverridesHostPort(t *testing.T) {
	opts, err := parseOptions([]string{"--addr", "10.0.0.1:7000", "-h", "ignored", "-p", "1"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if opts.addr != "10.0.0.1:7000" {
		t.Fatalf("expected addr override, got %q", opts.addr)
	}
}

func TestParseOptionsRejectsDatabaseSelection(t *testing.T) {
	if _, err := parseOptions([]string{"-n", "1"}); err == nil {
		t.Fatal("expected unsupported db selection error")
	}
}
