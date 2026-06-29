package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Type byte

const (
	SimpleString Type = '+'
	ErrorString  Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

type Value struct {
	Type  Type
	Text  string
	Int   int64
	Items []Value
	Null  bool
}

type Reader struct {
	r *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(rd)}
}

func (r *Reader) Buffered() int {
	return r.r.Buffered()
}

func (r *Reader) ReadCommand() ([]string, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != '*' {
		line, err := r.r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return splitInline(string(append([]byte{b}, line...))), nil
	}

	n, err := r.readLen()
	if err != nil {
		return nil, err
	}
	parts := make([]string, 0, n)
	for i := 0; i < n; i++ {
		t, err := r.r.ReadByte()
		if err != nil {
			return nil, err
		}
		if t != '$' {
			return nil, fmt.Errorf("expected bulk string, got %q", t)
		}
		l, err := r.readLen()
		if err != nil {
			return nil, err
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, err
		}
		if buf[l] != '\r' || buf[l+1] != '\n' {
			return nil, errors.New("invalid bulk terminator")
		}
		parts = append(parts, string(buf[:l]))
	}
	return parts, nil
}

func (r *Reader) ReadValue() (Value, error) {
	b, err := r.r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Type(b) {
	case SimpleString:
		line, err := r.readLine()
		return Value{Type: SimpleString, Text: line}, err
	case ErrorString:
		line, err := r.readLine()
		return Value{Type: ErrorString, Text: line}, err
	case Integer:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: Integer, Int: n}, nil
	case BulkString:
		l, err := r.readLen()
		if err != nil {
			return Value{}, err
		}
		if l == -1 {
			return Value{Type: BulkString, Null: true}, nil
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return Value{}, err
		}
		if buf[l] != '\r' || buf[l+1] != '\n' {
			return Value{}, errors.New("invalid bulk terminator")
		}
		return Value{Type: BulkString, Text: string(buf[:l])}, nil
	case Array:
		n, err := r.readLen()
		if err != nil {
			return Value{}, err
		}
		if n == -1 {
			return Value{Type: Array, Null: true}, nil
		}
		items := make([]Value, 0, n)
		for i := 0; i < n; i++ {
			item, err := r.ReadValue()
			if err != nil {
				return Value{}, err
			}
			items = append(items, item)
		}
		return Value{Type: Array, Items: items}, nil
	default:
		return Value{}, fmt.Errorf("unknown RESP type %q", b)
	}
}

func (r *Reader) readLen() (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(line)
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

func splitInline(line string) []string {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil
	}
	return strings.Fields(line)
}

type Writer struct {
	w *bufio.Writer
}

func NewWriter(wr io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(wr)}
}

func (w *Writer) Flush() error {
	return w.w.Flush()
}

func (w *Writer) Command(args []string) error {
	if err := w.ArrayLen(len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if err := w.Bulk(arg); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Simple(s string) error {
	if err := w.w.WriteByte('+'); err != nil {
		return err
	}
	if _, err := w.w.WriteString(s); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) Error(s string) error {
	if err := w.w.WriteByte('-'); err != nil {
		return err
	}
	if _, err := w.w.WriteString(s); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) Int(n int64) error {
	if err := w.w.WriteByte(':'); err != nil {
		return err
	}
	var buf [20]byte
	b := strconv.AppendInt(buf[:0], n, 10)
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) Bulk(s string) error {
	if err := w.w.WriteByte('$'); err != nil {
		return err
	}
	var buf [20]byte
	b := strconv.AppendInt(buf[:0], int64(len(s)), 10)
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	if _, err := w.w.WriteString("\r\n"); err != nil {
		return err
	}
	if _, err := w.w.WriteString(s); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) Null() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

func (w *Writer) ArrayLen(n int) error {
	if err := w.w.WriteByte('*'); err != nil {
		return err
	}
	var buf [20]byte
	b := strconv.AppendInt(buf[:0], int64(n), 10)
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	_, err := w.w.WriteString("\r\n")
	return err
}

func (w *Writer) NullArray() error {
	_, err := w.w.WriteString("*-1\r\n")
	return err
}

func (w *Writer) WriteRaw(data []byte) error {
	_, err := w.w.Write(data)
	return err
}
