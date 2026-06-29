package server

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"vole/internal/resp"
	"vole/internal/store"
)

// FieldSchema describes a single field's name and expected type.
type FieldSchema struct {
	Name string
	Type string // string, int, float, bool, email, required
}

// SchemaRule maps a glob pattern to a set of typed field constraints.
type SchemaRule struct {
	Pattern string
	Fields  map[string]string // field name -> type
}

// SchemaManager manages schema rules for hash keys.
type SchemaManager struct {
	mu    sync.RWMutex
	rules []*SchemaRule
}

// NewSchemaManager creates a new SchemaManager.
func NewSchemaManager() *SchemaManager {
	return &SchemaManager{}
}

// Set defines or updates a schema for the given pattern.
func (sm *SchemaManager) Set(pattern string, fields map[string]string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, rule := range sm.rules {
		if rule.Pattern == pattern {
			rule.Fields = fields
			return
		}
	}
	sm.rules = append(sm.rules, &SchemaRule{Pattern: pattern, Fields: fields})
}

// Get returns the schema rule for an exact pattern match, or nil.
func (sm *SchemaManager) Get(pattern string) *SchemaRule {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, rule := range sm.rules {
		if rule.Pattern == pattern {
			return rule
		}
	}
	return nil
}

// Del removes the schema for the given pattern. Returns true if found.
func (sm *SchemaManager) Del(pattern string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for i, rule := range sm.rules {
		if rule.Pattern == pattern {
			sm.rules = append(sm.rules[:i], sm.rules[i+1:]...)
			return true
		}
	}
	return false
}

// List returns a copy of all schema rules.
func (sm *SchemaManager) List() []*SchemaRule {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	out := make([]*SchemaRule, len(sm.rules))
	copy(out, sm.rules)
	return out
}

// Validate checks if the given hash fields comply with any matching schema.
// Returns nil if valid or no schema matches, error if validation fails.
func (sm *SchemaManager) Validate(key string, fields map[string]string) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, rule := range sm.rules {
		if !store.MatchGlob(rule.Pattern, key) {
			continue
		}
		// Found a matching schema — validate
		for fieldName, fieldType := range rule.Fields {
			value, exists := fields[fieldName]

			if !exists {
				if fieldType == "required" {
					return fmt.Errorf("schema violation: field %q is required", fieldName)
				}
				continue
			}

			if err := validateFieldType(fieldName, value, fieldType); err != nil {
				return err
			}
		}
		break // only apply first matching schema
	}
	return nil
}

func validateFieldType(field, value, typ string) error {
	switch typ {
	case "string", "required":
		return nil
	case "int":
		if _, err := strconv.ParseInt(value, 10, 64); err != nil {
			return fmt.Errorf("schema violation: %q must be int, got %q", field, value)
		}
	case "float":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return fmt.Errorf("schema violation: %q must be float, got %q", field, value)
		}
	case "bool":
		v := strings.ToLower(value)
		if v != "true" && v != "false" && v != "0" && v != "1" {
			return fmt.Errorf("schema violation: %q must be bool, got %q", field, value)
		}
	case "email":
		if !strings.Contains(value, "@") || !strings.Contains(value, ".") {
			return fmt.Errorf("schema violation: %q must be email, got %q", field, value)
		}
	}
	return nil
}

// handleSchemaSet handles SCHEMA.SET pattern field:type [field:type ...]
func (s *Server) handleSchemaSet(w *resp.Writer, args []string) error {
	if len(args) < 3 {
		return wrongArgs("SCHEMA.SET")
	}
	fields := make(map[string]string)
	for _, arg := range args[2:] {
		parts := strings.SplitN(arg, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid field schema %q (use field:type)", arg)
		}
		validTypes := map[string]bool{"string": true, "int": true, "float": true, "bool": true, "email": true, "required": true}
		if !validTypes[parts[1]] {
			return fmt.Errorf("unknown schema type %q (valid: string, int, float, bool, email, required)", parts[1])
		}
		fields[parts[0]] = parts[1]
	}
	s.schemas.Set(args[1], fields)
	return w.Simple("OK")
}

// handleSchemaGet handles SCHEMA.GET pattern
func (s *Server) handleSchemaGet(w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return wrongArgs("SCHEMA.GET")
	}
	rule := s.schemas.Get(args[1])
	if rule == nil {
		return w.NullArray()
	}
	fieldNames := make([]string, 0, len(rule.Fields))
	for f := range rule.Fields {
		fieldNames = append(fieldNames, f)
	}
	sort.Strings(fieldNames)
	pairs := make([]string, 0, len(fieldNames)*2)
	for _, f := range fieldNames {
		pairs = append(pairs, f, rule.Fields[f])
	}
	return writeBulkStrings(w, pairs)
}

// handleSchemaDel handles SCHEMA.DEL pattern
func (s *Server) handleSchemaDel(w *resp.Writer, args []string) error {
	if len(args) != 2 {
		return wrongArgs("SCHEMA.DEL")
	}
	if s.schemas.Del(args[1]) {
		return w.Int(1)
	}
	return w.Int(0)
}

// handleSchemaList handles SCHEMA.LIST
func (s *Server) handleSchemaList(w *resp.Writer, args []string) error {
	rules := s.schemas.List()
	if err := w.ArrayLen(len(rules)); err != nil {
		return err
	}
	for _, rule := range rules {
		if err := w.ArrayLen(2); err != nil {
			return err
		}
		_ = w.Bulk(rule.Pattern)
		fieldNames := make([]string, 0, len(rule.Fields))
		for f := range rule.Fields {
			fieldNames = append(fieldNames, f)
		}
		sort.Strings(fieldNames)
		pairs := make([]string, 0, len(fieldNames)*2)
		for _, f := range fieldNames {
			pairs = append(pairs, f+":"+rule.Fields[f])
		}
		if err := writeBulkStrings(w, pairs); err != nil {
			return err
		}
	}
	return nil
}
