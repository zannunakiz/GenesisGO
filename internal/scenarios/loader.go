package scenarios

import (
	"fmt"
	"path/filepath"
)

// Loader reads built-in scenario fixtures from a directory and validates them.
type Loader struct {
	dir string
}

// NewLoader creates a loader that reads fixtures from dir.
func NewLoader(dir string) *Loader {
	return &Loader{dir: dir}
}

// Load reads every *.json fixture from the loader's directory, validates each,
// and returns them keyed by scenario key. Duplicate keys are rejected.
func (l *Loader) Load() (map[string]*Scenario, error) {
	raw, err := loadDir(l.dir)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*Scenario, len(raw))
	for i := range raw {
		s := raw[i]
		if err := Validate(s); err != nil {
			return nil, fmt.Errorf("validate fixture %s: %w", s.Key, err)
		}
		if _, exists := out[s.Key]; exists {
			return nil, fmt.Errorf("duplicate scenario key %q", s.Key)
		}
		out[s.Key] = &s
	}
	return out, nil
}

// FixturePath returns the on-disk path for a scenario's fixture file.
func (l *Loader) FixturePath(key string) string {
	return filepath.Join(l.dir, key+".json")
}
