package auth

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Store resolves digests to records. Implementations must be safe for
// concurrent use and must never persist plaintext secrets.
type Store interface {
	Lookup(digest string) (Record, bool)
	List() []Record
	Upsert(record Record) error
	Revoke(keyID string) error
}

// MemoryStore keeps digests in memory. It is suitable for tests and for
// development when no key file is configured.
type MemoryStore struct {
	mu      sync.RWMutex
	records map[string]Record
	byID    map[string]string
}

// NewMemoryStore builds a store seeded with digest-only records.
func NewMemoryStore(seeds ...Record) (*MemoryStore, error) {
	s := &MemoryStore{records: make(map[string]Record), byID: make(map[string]string)}
	for _, record := range seeds {
		if err := s.Upsert(record); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Lookup finds a non-revoked record by digest.
func (s *MemoryStore) Lookup(digest string) (Record, bool) {
	digest = strings.TrimSpace(digest)
	if digest == "" {
		return Record{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[digest]
	if !ok || record.Revoked {
		return Record{}, false
	}
	return record, true
}

// List returns a stable copy of stored records.
func (s *MemoryStore) List() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Record, 0, len(s.records))
	for _, record := range s.records {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Upsert stores a digest-only record and supports rotation by key ID.
func (s *MemoryStore) Upsert(record Record) error {
	if strings.TrimSpace(record.ID) == "" {
		return errors.New("key id is required")
	}
	if strings.TrimSpace(record.Digest) == "" {
		return errors.New("key digest is required")
	}
	scopes, err := NormaliseScopes(record.Scopes)
	if err != nil {
		return err
	}
	record.Scopes = scopes
	if record.CreatedAt.IsZero() {
		record.CreatedAt = nowFunc().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]Record)
	}
	if s.byID == nil {
		s.byID = make(map[string]string)
	}
	if oldDigest, ok := s.byID[record.ID]; ok && oldDigest != record.Digest {
		delete(s.records, oldDigest)
	}
	s.records[record.Digest] = record
	s.byID[record.ID] = record.Digest
	return nil
}

// Revoke marks a key ID revoked without deleting its digest history.
func (s *MemoryStore) Revoke(keyID string) error {
	id := strings.TrimSpace(keyID)
	if id == "" {
		return errors.New("key id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	digest, ok := s.byID[id]
	if !ok {
		return errors.New("api key not found")
	}
	record := s.records[digest]
	record.Revoked = true
	s.records[digest] = record
	return nil
}

// FileStore persists digest-only records as JSON. The file must never
// contain plaintext secrets; callers store GeneratedKey.Record only.
type FileStore struct {
	path string
	mu   sync.RWMutex
	mem  *MemoryStore
}

// NewFileStore loads digests from path, creating the parent directory when
// needed. A missing file starts empty so bootstrap can create the first key.
func NewFileStore(path string) (*FileStore, error) {
	clean := strings.TrimSpace(path)
	if clean == "" {
		return nil, errors.New("api keys file path is required")
	}
	store := &FileStore{path: filepath.Clean(clean)}
	mem, err := NewMemoryStore()
	if err != nil {
		return nil, err
	}
	store.mem = mem
	raw, err := os.ReadFile(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return store, nil
		}
		return nil, errors.New("read api keys file")
	}
	if len(raw) == 0 {
		return store, nil
	}
	var records []Record
	if err := json.Unmarshal(raw, &records); err != nil {
		return nil, errors.New("api keys file must contain a JSON array of key records")
	}
	for _, record := range records {
		if strings.TrimSpace(record.Digest) == "" || strings.Contains(record.Digest, KeyPrefix) {
			return nil, errors.New("api keys file must store digests only")
		}
		if err := store.mem.Upsert(record); err != nil {
			return nil, err
		}
	}
	return store, nil
}

// Lookup delegates to the in-memory index.
func (s *FileStore) Lookup(digest string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Lookup(digest)
}

// List delegates to the in-memory index.
func (s *FileStore) List() []Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.List()
}

// Upsert updates memory and rewrites the file atomically.
func (s *FileStore) Upsert(record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Upsert(record); err != nil {
		return err
	}
	return s.persistLocked()
}

// Revoke marks revoked and rewrites the file atomically.
func (s *FileStore) Revoke(keyID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Revoke(keyID); err != nil {
		return err
	}
	return s.persistLocked()
}

func (s *FileStore) persistLocked() error {
	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return errors.New("create api keys directory")
		}
	}
	records := s.mem.List()
	raw, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return errors.New("encode api keys file")
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return errors.New("write api keys file")
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return errors.New("replace api keys file")
	}
	_ = os.Chmod(s.path, 0o600)
	return nil
}

// IssuedAt returns the creation time truncated for stable output.
func IssuedAt(t time.Time) time.Time {
	return t.UTC().Truncate(time.Second)
}
