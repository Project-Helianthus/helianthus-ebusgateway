package semanticmetrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"reflect"
	"sync"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
)

const o21MaxBytes = 256 * 1024

// O21Persistence is an injected, exclusive per-target durable object adapter.
// Load must return fs.ErrNotExist only for a definitely absent object. Commit
// must atomically replace the entire object and acknowledge durable completion;
// any error is treated as uncertain, including errors after replacement. The
// adapter must fence other writers for the store's lifetime. No path is chosen
// here. Implementations must not retain or mutate supplied bytes.
type O21Persistence interface {
	Load(target string) ([]byte, error)
	Commit(target string, document []byte) error
}

type o21Document struct {
	Version  int
	Target   string
	Bindings []Binding
}

// O21SlotStore owns at most 32 assignments for one opaque scrape target.
// Export returns detached state; failures retain assignments but withhold values.
type O21SlotStore struct {
	operation   sync.Mutex
	mu          sync.RWMutex
	persistence O21Persistence
	document    o21Document
	validated   bool
	uncertain   bool
}

func o21Clone[T any](v T) T {
	raw, _ := json.Marshal(v)
	var result T
	_ = json.Unmarshal(raw, &result)
	return result
}

func o21Check(d o21Document, target string) error {
	if target == "" || len(target) > 256 || d.Target != target || d.Version != 1 || len(d.Bindings) > 32 {
		return fmt.Errorf("invalid slot store identity or bounds")
	}
	_, err := Render(semanticview.View{}, Registry{Bindings: d.Bindings})
	return err
}

// O21Open loads a bounded document without trusting prior owner validation.
// Missing, malformed and cross-target objects fail closed.
func O21Open(p O21Persistence, target string) (*O21SlotStore, error) {
	if p == nil {
		return nil, fmt.Errorf("missing persistence")
	}
	raw, err := p.Load(target)
	if err != nil {
		return nil, err
	}
	if len(raw) > o21MaxBytes {
		return nil, fmt.Errorf("slot store too large")
	}
	var d o21Document
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&d); err != nil {
		return nil, err
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing slot store data")
	}
	if err = o21Check(d, target); err != nil {
		return nil, err
	}
	return &O21SlotStore{persistence: p, document: d}, nil
}

// O21Create explicitly provisions a definitely absent store; it never repairs
// a missing or corrupt object implicitly during scrape or restart.
func O21Create(p O21Persistence, target string) (*O21SlotStore, error) {
	d := o21Document{Version: 1, Target: target, Bindings: []Binding{}}
	if err := o21Check(d, target); err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("missing persistence")
	}
	if _, err := p.Load(target); !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("slot store not definitely absent")
	}
	raw, _ := json.Marshal(d)
	if err := p.Commit(target, raw); err != nil {
		return nil, err
	}
	return &O21SlotStore{persistence: p, document: d}, nil
}

// Export returns last accepted assignments, with values withheld until revalidation.
func (s *O21SlotStore) Export() Registry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Registry{Validated: s.validated && !s.uncertain, Bindings: o21Clone(s.document.Bindings)}
}

// Revalidate is an explicit current-owner attestation of the entire exact
// registry, including reservations. It cannot recover an uncertain writer.
func (s *O21SlotStore) Revalidate(target string, bindings []Binding) error {
	s.operation.Lock()
	defer s.operation.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.validated = false
	if s.uncertain || target != s.document.Target || !reflect.DeepEqual(bindings, s.document.Bindings) {
		return fmt.Errorf("owner revalidation mismatch or uncertain persistence")
	}
	s.validated = true
	return nil
}

// Assign reserves a caller-selected unused slot. Assignments are immutable;
// changed identities require a fresh slot and explicit retirement of the old.
func (s *O21SlotStore) Assign(target string, binding Binding) (Registry, error) {
	return s.o21Update(target, binding, false)
}

// Retire requires the full original identity and permanently reserves its slot.
func (s *O21SlotStore) Retire(target string, binding Binding) (Registry, error) {
	return s.o21Update(target, binding, true)
}

func (s *O21SlotStore) o21Update(target string, binding Binding, retire bool) (Registry, error) {
	s.operation.Lock()
	defer s.operation.Unlock()
	s.mu.RLock()
	d, uncertain := o21Clone(s.document), s.uncertain
	s.mu.RUnlock()
	if uncertain || target != d.Target || binding.Retired {
		return Registry{}, fmt.Errorf("invalid slot operation")
	}
	binding = o21Clone(binding)
	if retire {
		found := false
		for i, old := range d.Bindings {
			if reflect.DeepEqual(old, binding) {
				d.Bindings[i].Retired = true
				found = true
				break
			}
		}
		if !found {
			return Registry{}, fmt.Errorf("retirement identity mismatch")
		}
	} else {
		d.Bindings = append(d.Bindings, binding)
	}
	if err := o21Check(d, target); err != nil {
		return Registry{}, err
	}
	raw, err := json.Marshal(d)
	if err != nil || len(raw) > o21MaxBytes {
		return Registry{}, fmt.Errorf("slot store too large or invalid")
	}
	s.mu.Lock()
	s.validated = false
	s.mu.Unlock()
	err = s.persistence.Commit(target, raw)
	s.mu.Lock()
	if err != nil {
		s.uncertain = true
	} else {
		s.document = d
	}
	s.mu.Unlock()
	if err != nil {
		return Registry{}, err
	}
	return s.Export(), nil
}
