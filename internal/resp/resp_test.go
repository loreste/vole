package resp

import (
	"bytes"
	"testing"
)

func TestCommandWritesArrayOfBulkStrings(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.Command([]string{"SET", "name", "vole"}); err != nil {
		t.Fatalf("command failed: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	const want = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$4\r\nvole\r\n"
	if got := buf.String(); got != want {
		t.Fatalf("unexpected command encoding:\nwant %q\n got %q", want, got)
	}
}

func TestReadNestedValue(t *testing.T) {
	input := "*2\r\n$6\r\nevents\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$4\r\ntype\r\n$7\r\ncreated\r\n"
	v, err := NewReader(bytes.NewBufferString(input)).ReadValue()
	if err != nil {
		t.Fatalf("read value failed: %v", err)
	}
	if v.Type != Array || len(v.Items) != 2 {
		t.Fatalf("unexpected top-level value: %#v", v)
	}
	if v.Items[0].Text != "events" {
		t.Fatalf("expected stream name, got %#v", v.Items[0])
	}
	if got := v.Items[1].Items[0].Items[0].Text; got != "1-1" {
		t.Fatalf("expected stream ID, got %q", got)
	}
}
