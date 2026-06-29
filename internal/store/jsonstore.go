package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// JSONDoc wraps a parsed JSON document.
type JSONDoc struct {
	data interface{} // can be map, slice, string, float64, bool, nil
}

func NewJSONDoc(raw string) (*JSONDoc, error) {
	var data interface{}
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return nil, err
	}
	return &JSONDoc{data: data}, nil
}

func (d *JSONDoc) String() string {
	b, _ := json.Marshal(d.data)
	return string(b)
}

// Get retrieves a value at the given path. Path uses dot notation: $.field.subfield
// $ is the root. $.name, $.address.city, $.tags[0]
func (d *JSONDoc) Get(path string) (interface{}, error) {
	if path == "$" || path == "." || path == "" {
		return d.data, nil
	}
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")
	return navigate(d.data, parsePath(path))
}

// Set sets a value at the given path.
func (d *JSONDoc) Set(path string, value interface{}) error {
	if path == "$" || path == "." || path == "" {
		d.data = value
		return nil
	}
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")
	parts := parsePath(path)
	return setPath(d, parts, value)
}

// Del deletes a value at the given path.
func (d *JSONDoc) Del(path string) error {
	if path == "$" || path == "." || path == "" {
		d.data = nil
		return nil
	}
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")
	parts := parsePath(path)
	return delPath(d, parts)
}

// Type returns the JSON type at the given path.
func (d *JSONDoc) Type(path string) string {
	val, err := d.Get(path)
	if err != nil || val == nil {
		return "null"
	}
	switch val.(type) {
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	default:
		return "null"
	}
}

// Keys returns the keys of an object at the given path.
func (d *JSONDoc) Keys(path string) ([]string, error) {
	val, err := d.Get(path)
	if err != nil {
		return nil, err
	}
	obj, ok := val.(map[string]interface{})
	if !ok {
		return nil, errors.New("not an object")
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// NumIncrBy increments a number at the given path.
func (d *JSONDoc) NumIncrBy(path string, delta float64) (float64, error) {
	val, err := d.Get(path)
	if err != nil {
		return 0, err
	}
	num, ok := val.(float64)
	if !ok {
		return 0, errors.New("not a number")
	}
	result := num + delta
	if err := d.Set(path, result); err != nil {
		return 0, err
	}
	return result, nil
}

// ArrAppend appends values to an array at the given path.
func (d *JSONDoc) ArrAppend(path string, values ...interface{}) (int, error) {
	val, err := d.Get(path)
	if err != nil {
		return 0, err
	}
	arr, ok := val.([]interface{})
	if !ok {
		return 0, errors.New("not an array")
	}
	arr = append(arr, values...)
	if err := d.Set(path, arr); err != nil {
		return 0, err
	}
	return len(arr), nil
}

// ArrLen returns the length of an array at the given path.
func (d *JSONDoc) ArrLen(path string) (int, error) {
	val, err := d.Get(path)
	if err != nil {
		return 0, err
	}
	arr, ok := val.([]interface{})
	if !ok {
		return 0, errors.New("not an array")
	}
	return len(arr), nil
}

// parsePath splits "field.subfield[0].name" into ["field", "subfield", "0", "name"]
func parsePath(path string) []string {
	var parts []string
	current := ""
	for i := 0; i < len(path); i++ {
		switch path[i] {
		case '.':
			if current != "" {
				parts = append(parts, current)
			}
			current = ""
		case '[':
			if current != "" {
				parts = append(parts, current)
			}
			current = ""
		case ']':
			if current != "" {
				parts = append(parts, current)
			}
			current = ""
		default:
			current += string(path[i])
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func navigate(data interface{}, parts []string) (interface{}, error) {
	current := data
	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			val, ok := v[part]
			if !ok {
				return nil, fmt.Errorf("key %q not found", part)
			}
			current = val
		case []interface{}:
			idx, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid array index %q", part)
			}
			if idx < 0 {
				idx = len(v) + idx
			}
			if idx < 0 || idx >= len(v) {
				return nil, fmt.Errorf("index %d out of range", idx)
			}
			current = v[idx]
		default:
			return nil, fmt.Errorf("cannot navigate into %T", current)
		}
	}
	return current, nil
}

func setPath(doc *JSONDoc, parts []string, value interface{}) error {
	if len(parts) == 0 {
		doc.data = value
		return nil
	}
	if len(parts) == 1 {
		// Set on root
		switch v := doc.data.(type) {
		case map[string]interface{}:
			v[parts[0]] = value
			return nil
		case []interface{}:
			idx, err := strconv.Atoi(parts[0])
			if err != nil {
				return err
			}
			if idx < 0 || idx >= len(v) {
				return fmt.Errorf("index out of range")
			}
			v[idx] = value
			return nil
		default:
			return errors.New("cannot set on this type")
		}
	}
	// Navigate to parent
	parent, err := navigate(doc.data, parts[:len(parts)-1])
	if err != nil {
		return err
	}
	last := parts[len(parts)-1]
	switch p := parent.(type) {
	case map[string]interface{}:
		p[last] = value
	case []interface{}:
		idx, err := strconv.Atoi(last)
		if err != nil {
			return err
		}
		if idx < 0 || idx >= len(p) {
			return fmt.Errorf("index out of range")
		}
		p[idx] = value
	default:
		return errors.New("parent is not an object or array")
	}
	return nil
}

func delPath(doc *JSONDoc, parts []string) error {
	if len(parts) == 0 {
		doc.data = nil
		return nil
	}
	if len(parts) == 1 {
		switch v := doc.data.(type) {
		case map[string]interface{}:
			delete(v, parts[0])
			return nil
		default:
			return errors.New("cannot delete from this type")
		}
	}
	parent, err := navigate(doc.data, parts[:len(parts)-1])
	if err != nil {
		return err
	}
	last := parts[len(parts)-1]
	switch p := parent.(type) {
	case map[string]interface{}:
		delete(p, last)
	default:
		return errors.New("parent is not an object")
	}
	return nil
}
