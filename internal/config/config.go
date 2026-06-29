package config

import (
	"bufio"
	"os"
	"strings"
)

// Load reads a vole config file and returns its key-value pairs.
// Lines starting with # are comments. Blank lines are skipped.
// Format: "key value" separated by whitespace. The value is everything
// after the first run of whitespace, so values can contain spaces.
func Load(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Split into key and value at the first whitespace.
		key, value := splitDirective(line)
		if key != "" {
			cfg[key] = value
		}
	}
	return cfg, scanner.Err()
}

func splitDirective(line string) (string, string) {
	i := strings.IndexAny(line, " \t")
	if i < 0 {
		return line, ""
	}
	return line[:i], strings.TrimSpace(line[i+1:])
}
