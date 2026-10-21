package portalconfig

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"unicode/utf8"
)

const Format = "helianthus.portal-config/v1"
const MaxBytes = 64 * 1024

type Error struct {
	Code     string `json:"code"`
	Field    string `json:"field"`
	NextStep string `json:"next_step"`
}

func (e *Error) Error() string { return e.Code }
func failure(code string) *Error {
	steps := map[string]string{"invalid_record": "repair structure", "unsupported_format": "use supported format", "unsupported_selection": "choose a provisioned option", "revision_conflict": "reload", "persistence_failed": "check local storage", "revision_exhausted": "use a future supported format migration"}
	return &Error{code, "record", steps[code]}
}

type Driver struct {
	ID      string `json:"driver_id"`
	Enabled bool   `json:"enabled"`
}
type Sharing struct {
	GraphQL bool `json:"graphql"`
	MCP     bool `json:"mcp"`
}
type Outputs struct {
	Matter bool `json:"matter"`
}
type Record struct {
	Format         string   `json:"format"`
	Revision       string   `json:"revision"`
	Drivers        []Driver `json:"drivers"`
	Sharing        Sharing  `json:"sharing"`
	AdapterProxy   bool     `json:"adapter_proxy"`
	OutputBindings Outputs  `json:"output_bindings"`
}
type Constraints struct {
	Drivers                            []string
	GraphQL, MCP, AdapterProxy, Matter bool
}

// Decode accepts only the bounded, closed record shape, including duplicate-key rejection.
func Decode(data []byte) (Record, error) {
	var r Record
	if len(data) > MaxBytes || !utf8.Valid(data) {
		return r, failure("invalid_record")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	var value func(int) (any, error)
	value = func(depth int) (any, error) {
		if depth > 5 {
			return nil, failure("invalid_record")
		}
		t, e := d.Token()
		if e != nil || t == nil {
			return nil, failure("invalid_record")
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				m := map[string]any{}
				for d.More() {
					k, e := d.Token()
					if e != nil {
						return nil, e
					}
					key, ok := k.(string)
					if !ok {
						return nil, failure("invalid_record")
					}
					if _, ok = m[key]; ok {
						return nil, failure("invalid_record")
					}
					v, e := value(depth + 1)
					if e != nil {
						return nil, e
					}
					m[key] = v
				}
				_, e = d.Token()
				return m, e
			case '[':
				a := []any{}
				for d.More() {
					v, e := value(depth + 1)
					if e != nil {
						return nil, e
					}
					a = append(a, v)
					if len(a) > 64 {
						return nil, failure("invalid_record")
					}
				}
				_, e = d.Token()
				return a, e
			}
			return nil, failure("invalid_record")
		}
		return t, nil
	}
	v, e := value(0)
	if e != nil {
		return r, failure("invalid_record")
	}
	if _, e = d.Token(); e != io.EOF {
		return r, failure("invalid_record")
	}
	shape := func(v any, keys ...string) bool {
		m, ok := v.(map[string]any)
		if !ok || len(m) != len(keys) {
			return false
		}
		for _, k := range keys {
			if _, ok := m[k]; !ok {
				return false
			}
		}
		return true
	}
	if !shape(v, "format", "revision", "drivers", "sharing", "adapter_proxy", "output_bindings") {
		return r, failure("invalid_record")
	}
	m := v.(map[string]any)
	if !shape(m["sharing"], "graphql", "mcp") || !shape(m["output_bindings"], "matter") {
		return r, failure("invalid_record")
	}
	a, ok := m["drivers"].([]any)
	if !ok {
		return r, failure("invalid_record")
	}
	for _, v := range a {
		if !shape(v, "driver_id", "enabled") {
			return r, failure("invalid_record")
		}
	}
	if json.Unmarshal(data, &r) != nil {
		return Record{}, failure("invalid_record")
	}
	if r.Format != Format {
		return Record{}, failure("unsupported_format")
	}
	n, e := strconv.ParseUint(r.Revision, 10, 64)
	if e != nil || strconv.FormatUint(n, 10) != r.Revision {
		return Record{}, failure("invalid_record")
	}
	ids := map[string]bool{}
	for _, d := range r.Drivers {
		if d.ID == "" || ids[d.ID] {
			return Record{}, failure("invalid_record")
		}
		ids[d.ID] = true
	}
	sort.Slice(r.Drivers, func(i, j int) bool { return r.Drivers[i].ID < r.Drivers[j].ID })
	return r, nil
}

type Store struct {
	mu          sync.Mutex
	root        string
	lock        *os.File
	constraints Constraints
	current     Record
	accepted    []byte
	pending     []byte
	blocked     error
	uncertain   bool
	// fault is a package-private deterministic persistence boundary used by offline tests.
	fault func(string) error
}

// Open owns an exclusive writer until Close. On failure the returned store keeps a bounded error view.
func Open(root string, initial Record, c Constraints) (*Store, error) {
	s := &Store{root: root, constraints: c}
	s.constraints.Drivers = append([]string(nil), c.Drivers...)
	fail := func(e error) (*Store, error) { s.blocked = e; return s, e }
	abs, e := filepath.Abs(root)
	if e != nil {
		return fail(failure("persistence_failed"))
	}
	resolved, e := filepath.EvalSymlinks(abs)
	if e != nil {
		return fail(failure("persistence_failed"))
	}
	// The injected root is trusted host configuration; canonicalize filesystem aliases.
	abs = resolved
	s.root = abs
	f, e := os.OpenFile(filepath.Join(abs, "portal-config.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if e != nil {
		return fail(failure("persistence_failed"))
	}
	info, statErr := f.Stat()
	if statErr != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return fail(failure("persistence_failed"))
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		_ = f.Close()
		return fail(failure("persistence_failed"))
	}
	s.lock = f
	data, e := s.readDisk()
	if os.IsNotExist(e) {
		if initial.Revision != "0" || initial.Sharing != (Sharing{}) || initial.AdapterProxy || initial.OutputBindings != (Outputs{}) {
			return fail(failure("invalid_record"))
		}
		r, e := s.validate(initial)
		if e != nil {
			return fail(e)
		}
		if e = s.persist(r); e != nil {
			return fail(e)
		}
		return s, nil
	}
	if e != nil {
		return fail(failure("persistence_failed"))
	}
	r, e := Decode(data)
	if e != nil {
		return fail(e)
	}
	r, e = s.validate(r)
	if e != nil {
		return fail(e)
	}
	if s.syncDir() != nil {
		return fail(failure("persistence_failed"))
	}
	s.current = r
	s.accepted = data
	return s, nil
}
func (s *Store) validate(r Record) (Record, error) {
	b, e := json.Marshal(r)
	if e != nil {
		return Record{}, failure("invalid_record")
	}
	r, e = Decode(b)
	if e != nil {
		return Record{}, e
	}
	ids := map[string]bool{}
	for _, id := range s.constraints.Drivers {
		if id == "" || ids[id] {
			return Record{}, failure("unsupported_selection")
		}
		ids[id] = true
	}
	if len(ids) != len(r.Drivers) {
		return Record{}, failure("unsupported_selection")
	}
	for _, d := range r.Drivers {
		if !ids[d.ID] {
			return Record{}, failure("unsupported_selection")
		}
	}
	c := s.constraints
	if r.Sharing.GraphQL && !c.GraphQL || r.Sharing.MCP && !c.MCP || r.AdapterProxy && !c.AdapterProxy || r.OutputBindings.Matter && !c.Matter {
		return Record{}, failure("unsupported_selection")
	}
	return r, nil
}
func clone(r Record) Record { r.Drivers = append([]Driver{}, r.Drivers...); return r }
func (s *Store) readDisk() ([]byte, error) {
	f, e := os.OpenFile(filepath.Join(s.root, "portal-config.json"), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, e
	}
	// Closing a read-only descriptor cannot change the bytes or read error.
	defer func() { _ = f.Close() }()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		return nil, failure("persistence_failed")
	}
	return io.ReadAll(io.LimitReader(f, MaxBytes+1))
}
func (s *Store) check() error {
	if s.blocked != nil {
		return s.blocked
	}
	b, e := s.readDisk()
	if e != nil {
		s.blocked = failure("persistence_failed")
	} else if !bytes.Equal(b, s.accepted) {
		_, e = Decode(b)
		if e == nil {
			e = failure("revision_conflict")
		}
		s.blocked = e
	}
	return s.blocked
}
func (s *Store) Read() (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.check()
	return clone(s.current), e
}

// Update uses the record revision as the expected revision, never as a successor.
func (s *Store) Update(r Record) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return clone(s.current), e
	}
	r, e := s.validate(r)
	if e != nil {
		return clone(s.current), e
	}
	if r.Revision != s.current.Revision {
		return clone(s.current), failure("revision_conflict")
	}
	if reflect.DeepEqual(r, s.current) {
		return clone(s.current), nil
	}
	n, _ := strconv.ParseUint(s.current.Revision, 10, 64)
	if n == math.MaxUint64 {
		return clone(s.current), failure("revision_exhausted")
	}
	r.Revision = strconv.FormatUint(n+1, 10)
	r, e = s.validate(r)
	if e != nil {
		return clone(s.current), e
	}
	if e = s.persist(r); e != nil {
		return clone(s.current), e
	}
	return clone(s.current), nil
}
func (s *Store) stage(name string) error {
	if s.fault != nil {
		return s.fault(name)
	}
	return nil
}
func (s *Store) persist(r Record) error {
	fail := func() error { return failure("persistence_failed") }
	b, e := json.Marshal(r)
	if e != nil {
		return fail()
	}
	if s.stage("create") != nil {
		return fail()
	}
	f, e := os.CreateTemp(s.root, ".portal-config-*")
	if e != nil {
		return fail()
	}
	// Cleanup is best effort: incomplete temporary files are ignored on load,
	// and a successful rename has already removed this temporary path.
	defer func() { _ = os.Remove(f.Name()) }()
	if s.stage("write") != nil {
		_ = f.Close()
		return fail()
	}
	_, e = f.Write(b)
	if e != nil {
		_ = f.Close()
		return fail()
	}
	if s.stage("file_sync") != nil {
		_ = f.Close()
		return fail()
	}
	if e = f.Sync(); e != nil {
		_ = f.Close()
		return fail()
	}
	if s.stage("close") != nil {
		_ = f.Close()
		return fail()
	}
	if e = f.Close(); e != nil {
		return fail()
	}
	if s.stage("rename") != nil {
		return fail()
	}
	if e = os.Rename(f.Name(), filepath.Join(s.root, "portal-config.json")); e != nil {
		return fail()
	}
	if s.stage("directory_sync") != nil || s.syncDir() != nil {
		s.pending = b
		s.uncertain = true
		s.blocked = fail()
		return s.blocked
	}
	s.current = clone(r)
	s.accepted = b
	return nil
}
func (s *Store) syncDir() error {
	d, e := os.Open(s.root)
	if e != nil {
		return e
	}
	// Durability is determined by Sync; closing the read-only directory is cleanup.
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// RecoverDurability validates and flushes an uncertain replacement before publishing its revision.
func (s *Store) RecoverDurability() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lock == nil {
		return failure("persistence_failed")
	}
	if !s.uncertain {
		return s.blocked
	}
	b, e := s.readDisk()
	if e != nil || !bytes.Equal(b, s.pending) {
		return failure("persistence_failed")
	}
	r, e := Decode(b)
	if e != nil {
		return e
	}
	r, e = s.validate(r)
	if e != nil {
		return e
	}
	if s.stage("directory_sync") != nil || s.syncDir() != nil {
		return failure("persistence_failed")
	}
	s.current = r
	s.accepted = b
	s.blocked = nil
	s.uncertain = false
	return nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocked = failure("persistence_failed")
	if s.lock == nil {
		return nil
	}
	e := s.lock.Close()
	s.lock = nil
	return e
}
