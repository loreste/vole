package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"vole/internal/resp"
)

type options struct {
	host string
	port string
	addr string
	raw  bool
	args []string
}

func main() {
	opts, err := parseOptions(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if len(opts.args) > 0 {
		if err := runOne(opts.addr, opts.args, opts.raw); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := repl(opts.addr, opts.raw); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(args []string) (options, error) {
	opts := options{host: "127.0.0.1", port: "7379"}
	fs := flag.NewFlagSet("vole-cli", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.addr, "addr", "", "Vole server address")
	fs.StringVar(&opts.host, "h", opts.host, "server host")
	fs.StringVar(&opts.port, "p", opts.port, "server port")
	fs.BoolVar(&opts.raw, "raw", false, "print bulk strings without quotes")
	db := fs.Int("n", 0, "database number, accepted for redis-cli compatibility")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if *db != 0 {
		return options{}, errors.New("database selection is not supported yet")
	}
	if opts.addr == "" {
		opts.addr = net.JoinHostPort(opts.host, opts.port)
	}
	opts.args = fs.Args()
	return opts, nil
}

func runOne(addr string, args []string, raw bool) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	reader := resp.NewReader(conn)
	if isStreamingCommand(args) {
		return sendAndStream(conn, reader, args, raw)
	}
	for redirects := 0; redirects < 8; redirects++ {
		v, err := sendAndRead(conn, reader, args)
		if err != nil {
			return err
		}
		if next, ok := movedAddress(v); ok {
			_ = conn.Close()
			conn, reader, err = dial(next)
			if err != nil {
				return err
			}
			defer conn.Close()
			continue
		}
		printValue(os.Stdout, v, raw, 0)
		return nil
	}
	return errors.New("too many MOVED redirects")
}

func repl(addr string, raw bool) error {
	conn, reader, err := dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	in := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("%s> ", addr)
		if !in.Scan() {
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "quit") || strings.EqualFold(line, "exit") {
			return nil
		}
		args, err := splitArgs(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "(error) %v\n", err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		if isStreamingCommand(args) {
			if err := sendAndStream(conn, reader, args, raw); err != nil {
				return err
			}
			return nil
		}
		handled := false
		for redirects := 0; redirects < 8; redirects++ {
			v, err := sendAndRead(conn, reader, args)
			if err != nil {
				return err
			}
			if next, ok := movedAddress(v); ok {
				_ = conn.Close()
				addr = next
				conn, reader, err = dial(addr)
				if err != nil {
					return err
				}
				continue
			}
			printValue(os.Stdout, v, raw, 0)
			handled = true
			break
		}
		if !handled {
			return errors.New("too many MOVED redirects")
		}
	}
	return in.Err()
}

func dial(addr string) (net.Conn, *resp.Reader, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	return conn, resp.NewReader(conn), nil
}

func sendAndRead(conn net.Conn, reader *resp.Reader, args []string) (resp.Value, error) {
	w := resp.NewWriter(conn)
	if err := w.Command(args); err != nil {
		return resp.Value{}, err
	}
	if err := w.Flush(); err != nil {
		return resp.Value{}, err
	}
	return reader.ReadValue()
}

func movedAddress(v resp.Value) (string, bool) {
	if v.Type != resp.ErrorString {
		return "", false
	}
	parts := strings.Fields(v.Text)
	if len(parts) != 3 || parts[0] != "MOVED" {
		return "", false
	}
	return parts[2], true
}

func sendAndStream(conn net.Conn, reader *resp.Reader, args []string, raw bool) error {
	w := resp.NewWriter(conn)
	if err := w.Command(args); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		v, err := reader.ReadValue()
		if err != nil {
			return err
		}
		printValue(os.Stdout, v, raw, 0)
	}
}

func isStreamingCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch strings.ToUpper(args[0]) {
	case "SUBSCRIBE", "PSUBSCRIBE":
		return true
	}
	return false
}

func isBlockingCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch strings.ToUpper(args[0]) {
	case "BLPOP", "BRPOP", "DEQUEUE":
		return true
	}
	return false
}

func printValue(w io.Writer, v resp.Value, raw bool, indent int) {
	pad := strings.Repeat("  ", indent)
	switch v.Type {
	case resp.SimpleString:
		fmt.Fprintln(w, v.Text)
	case resp.ErrorString:
		fmt.Fprintf(w, "(error) %s\n", v.Text)
	case resp.Integer:
		fmt.Fprintf(w, "(integer) %d\n", v.Int)
	case resp.BulkString:
		if v.Null {
			fmt.Fprintln(w, "(nil)")
			return
		}
		if raw {
			fmt.Fprintln(w, v.Text)
			return
		}
		fmt.Fprintf(w, "%q\n", v.Text)
	case resp.Array:
		if v.Null {
			fmt.Fprintln(w, "(nil)")
			return
		}
		if len(v.Items) == 0 {
			fmt.Fprintln(w, "(empty array)")
			return
		}
		for i, item := range v.Items {
			fmt.Fprintf(w, "%s%d) ", pad, i+1)
			if item.Type == resp.Array && !item.Null {
				fmt.Fprintln(w)
				printValue(w, item, raw, indent+1)
				continue
			}
			printValue(w, item, raw, 0)
		}
	}
}

func splitArgs(line string) ([]string, error) {
	var args []string
	var b strings.Builder
	var quote rune
	escaped := false
	for _, r := range line {
		if escaped {
			switch r {
			case 'n':
				b.WriteRune('\n')
			case 'r':
				b.WriteRune('\r')
			case 't':
				b.WriteRune('\t')
			default:
				b.WriteRune(r)
			}
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				b.WriteRune(r)
			}
			continue
		}
		switch r {
		case '\'', '"':
			quote = r
		case ' ', '\t':
			if b.Len() > 0 {
				args = append(args, b.String())
				b.Reset()
			}
		default:
			b.WriteRune(r)
		}
	}
	if escaped {
		b.WriteRune('\\')
	}
	if quote != 0 {
		return nil, errors.New("unterminated " + strconv.QuoteRune(quote) + " string")
	}
	if b.Len() > 0 {
		args = append(args, b.String())
	}
	return args, nil
}
