package semgraph

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The stored document: one file in the store directory, a format name and a
// version that are compared exactly, and no migration from any other form.
const (
	storeFileName     = "semgraph-anchors.json"
	storeFormat       = "helianthus.semgraph.anchors"
	storeVersion      = 1
	maxStoreFileBytes = 8 << 20
	maxStoreSlotBytes = 128
)

// Errors of the file store.
var (
	ErrStoreDamaged   = errors.New("semgraph: anchor store file is damaged")
	ErrStoreForeign   = errors.New("semgraph: anchor store file is of another format or version")
	ErrStoreUncertain = errors.New("semgraph: anchor store write is uncertain; reopen the store")
)

// FileStore is the Store kept in one file, so that a superseded anchor keeps
// its identifier across a restart. Each write replaces the whole file
// atomically; the table changes only after the write succeeded. A failure
// after the replacement leaves the store uncertain, and every later operation
// is refused until the store is opened again. One FileStore owns its
// directory's file; two stores over one directory are not coordinated.
type FileStore struct {
	mu        sync.Mutex
	dir, path string
	entries   map[AnchorKey]Entry
	homes     map[semreg.ResourceID]semreg.AssetID
	uncertain error
	// fault is a test seam called before each write step.
	fault func(step string) error
}

var _ Store = (*FileStore)(nil)

// OpenStore returns the supersession table kept in dir, or the in-memory
// table when dir is empty. The directory must exist. An absent file is an
// empty table, and opening never writes. A damaged file is refused with
// ErrStoreDamaged and a file of another format or version with
// ErrStoreForeign; neither is changed.
func OpenStore(dir string) (Store, error) {
	if dir == "" {
		return NewMemoryStore(), nil
	}
	store, err := openFileStore(dir)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func openFileStore(dir string) (*FileStore, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("semgraph: anchor store directory: %w", err)
	}
	if !info.IsDir() {
		return nil, errors.New("semgraph: anchor store directory is not a directory")
	}
	store := &FileStore{dir: dir, path: filepath.Join(dir, storeFileName), entries: make(map[AnchorKey]Entry), homes: make(map[semreg.ResourceID]semreg.AssetID)}
	info, err = os.Lstat(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("semgraph: anchor store file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: not a regular file", ErrStoreDamaged)
	}
	file, err := os.Open(store.path)
	if err != nil {
		return nil, fmt.Errorf("semgraph: anchor store file: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxStoreFileBytes+1))
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, fmt.Errorf("semgraph: anchor store file: %w", err)
	}
	if len(data) > maxStoreFileBytes {
		return nil, fmt.Errorf("%w: larger than %d bytes", ErrStoreDamaged, maxStoreFileBytes)
	}
	if store.entries, store.homes, err = decodeStore(data); err != nil {
		return nil, err
	}
	return store, nil
}

// Lookup returns the recorded entry of key.
func (s *FileStore) Lookup(key AnchorKey) (Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return Entry{}, false, s.uncertain
	}
	entry, ok := s.entries[key]
	return entry, ok, nil
}

// Commit records one supersession in the file and then in the table. A record
// the file could not load again is refused with ErrStoreConflict.
func (s *FileStore) Commit(record Supersession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return s.uncertain
	}
	entries := maps.Clone(s.entries)
	if err := commitTo(entries, record); err != nil {
		return err
	}
	for _, key := range []AnchorKey{record.Previous, record.Next} {
		if err := entryError(key, entries[key]); err != nil {
			return fmt.Errorf("%w: %v", ErrStoreConflict, err)
		}
	}
	if err := s.write(entries, s.homes); err != nil {
		return err
	}
	s.entries = entries
	return nil
}

// RecordHome records asset as the home of resource unless one is recorded,
// and returns the recorded home.
func (s *FileStore) RecordHome(resource semreg.ResourceID, asset semreg.AssetID) (semreg.AssetID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uncertain != nil {
		return "", s.uncertain
	}
	if home, ok := s.homes[resource]; ok {
		return home, nil
	}
	if err := homeError(resource, asset); err != nil {
		return "", err
	}
	homes := maps.Clone(s.homes)
	homes[resource] = asset
	if err := s.write(s.entries, homes); err != nil {
		return "", err
	}
	s.homes = homes
	return asset, nil
}

// write replaces the file with the document of entries and homes: a new file
// in the same directory is written, synced and renamed over the old one, and
// the directory is synced.
func (s *FileStore) write(entries map[AnchorKey]Entry, homes map[semreg.ResourceID]semreg.AssetID) (err error) {
	data, err := encodeStore(entries, homes)
	if err != nil {
		return err
	}
	if err := s.step("create"); err != nil {
		return err
	}
	temp, err := os.CreateTemp(s.dir, "."+storeFileName+".*.tmp")
	if err != nil {
		return fmt.Errorf("semgraph: anchor store write: %w", err)
	}
	name, closed, renamed := temp.Name(), false, false
	defer func() {
		if !closed {
			_ = temp.Close()
		}
		if !renamed {
			_ = os.Remove(name)
		}
	}()
	if err := s.step("write"); err != nil {
		return err
	}
	if _, err := temp.Write(data); err != nil {
		return fmt.Errorf("semgraph: anchor store write: %w", err)
	}
	if err := s.step("sync"); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("semgraph: anchor store write: %w", err)
	}
	if err := s.step("close"); err != nil {
		return err
	}
	closed = true
	if err := temp.Close(); err != nil {
		return fmt.Errorf("semgraph: anchor store write: %w", err)
	}
	if err := s.step("rename"); err != nil {
		return err
	}
	if err := os.Rename(name, s.path); err != nil {
		return fmt.Errorf("semgraph: anchor store write: %w", err)
	}
	renamed = true
	if err := s.syncDir(); err != nil {
		s.uncertain = fmt.Errorf("%w: %w", ErrStoreUncertain, err)
		return s.uncertain
	}
	return nil
}

func (s *FileStore) syncDir() error {
	if err := s.step("sync directory"); err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (s *FileStore) step(name string) error {
	if s.fault == nil {
		return nil
	}
	if err := s.fault(name); err != nil {
		return fmt.Errorf("semgraph: anchor store %s: %w", name, err)
	}
	return nil
}

// storeDocument is the stored form. An anchor holds the key members (Key for
// a native identity, otherwise Parent and Slot) and the recorded entry; the
// Parent and Slot of a native identity are those of its anchor.
type storeDocument struct {
	Format  string        `json:"format"`
	Version int           `json:"version"`
	Anchors []storeAnchor `json:"anchors"`
	Homes   []storeHome   `json:"homes"`
}

type storeAnchor struct {
	Kind       semreg.DefinitionID `json:"kind"`
	Key        semreg.Digest       `json:"key,omitempty"`
	Parent     semreg.ResourceID   `json:"parent,omitempty"`
	Slot       string              `json:"slot,omitempty"`
	Resource   semreg.ResourceID   `json:"resource"`
	Class      semreg.AnchorClass  `json:"class"`
	Basis      semreg.AnchorBasis  `json:"basis"`
	Superseded bool                `json:"superseded"`
}

type storeHome struct {
	Resource semreg.ResourceID `json:"resource"`
	Asset    semreg.AssetID    `json:"asset"`
}

func encodeStore(entries map[AnchorKey]Entry, homes map[semreg.ResourceID]semreg.AssetID) ([]byte, error) {
	document := storeDocument{Format: storeFormat, Version: storeVersion, Anchors: make([]storeAnchor, 0, len(entries)), Homes: make([]storeHome, 0, len(homes))}
	for key, entry := range entries {
		document.Anchors = append(document.Anchors, storeAnchor{Kind: key.Kind, Key: key.Key, Parent: entry.Parent, Slot: entry.Slot, Resource: entry.Resource, Class: entry.Class, Basis: entry.Basis, Superseded: entry.Superseded})
	}
	sort.Slice(document.Anchors, func(i, j int) bool {
		a, b := document.Anchors[i], document.Anchors[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		if a.Parent != b.Parent {
			return a.Parent < b.Parent
		}
		return a.Slot < b.Slot
	})
	for resource, asset := range homes {
		document.Homes = append(document.Homes, storeHome{Resource: resource, Asset: asset})
	}
	sort.Slice(document.Homes, func(i, j int) bool { return document.Homes[i].Resource < document.Homes[j].Resource })
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("semgraph: anchor store encode: %w", err)
	}
	return append(data, '\n'), nil
}

// decodeStore reads a stored document. The format and version are compared
// first, so a document of another format or version is foreign whatever its
// members; then every member is decoded strictly and validated.
func decodeStore(data []byte) (map[AnchorKey]Entry, map[semreg.ResourceID]semreg.AssetID, error) {
	var header struct {
		Format  string `json:"format"`
		Version int    `json:"version"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrStoreDamaged, err)
	}
	if header.Format != storeFormat || header.Version != storeVersion {
		return nil, nil, ErrStoreForeign
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document storeDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrStoreDamaged, err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, nil, fmt.Errorf("%w: data after the document", ErrStoreDamaged)
	}
	entries := make(map[AnchorKey]Entry, len(document.Anchors))
	for _, anchor := range document.Anchors {
		key := AnchorKey{Kind: anchor.Kind, Key: anchor.Key}
		if anchor.Key == "" {
			key.Parent, key.Slot = anchor.Parent, anchor.Slot
		}
		entry := Entry{Resource: anchor.Resource, Class: anchor.Class, Basis: anchor.Basis, Parent: anchor.Parent, Slot: anchor.Slot, Superseded: anchor.Superseded}
		if err := entryError(key, entry); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrStoreDamaged, err)
		}
		if _, duplicate := entries[key]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate anchor key", ErrStoreDamaged)
		}
		entries[key] = entry
	}
	homes := make(map[semreg.ResourceID]semreg.AssetID, len(document.Homes))
	for _, home := range document.Homes {
		if err := homeError(home.Resource, home.Asset); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrStoreDamaged, err)
		}
		if _, duplicate := homes[home.Resource]; duplicate {
			return nil, nil, fmt.Errorf("%w: duplicate home", ErrStoreDamaged)
		}
		homes[home.Resource] = home.Asset
	}
	return entries, homes, nil
}

// entryError refuses an entry the stored form cannot hold: an invalid kind,
// resource, class or basis, a native key without the native identity basis or
// a slot key with it, a slot key without a slot or whose recorded parent and
// slot differ from the key's, and an invalid parent or slot.
func entryError(key AnchorKey, entry Entry) error {
	if err := key.Kind.Validate(); err != nil {
		return fmt.Errorf("kind: %v", err)
	}
	if err := entry.Resource.Validate(); err != nil {
		return fmt.Errorf("resource: %v", err)
	}
	if err := entry.Class.Validate(); err != nil {
		return fmt.Errorf("class: %v", err)
	}
	if err := entry.Basis.Validate(); err != nil {
		return fmt.Errorf("basis: %v", err)
	}
	if key.Key != "" {
		if err := key.Key.Validate(); err != nil {
			return fmt.Errorf("key: %v", err)
		}
		if key.Parent != "" || key.Slot != "" || entry.Basis != semreg.AnchorNativeIdentity {
			return errors.New("a native key needs the native identity basis and no key parent or slot")
		}
	} else if entry.Basis == semreg.AnchorNativeIdentity || key.Slot == "" || entry.Parent != key.Parent || entry.Slot != key.Slot {
		return errors.New("a slot key needs a slot basis, a slot, and the key's parent and slot")
	}
	if entry.Parent != "" {
		if err := entry.Parent.Validate(); err != nil {
			return fmt.Errorf("parent: %v", err)
		}
	}
	if entry.Slot != "" && !validSlot(entry.Slot) {
		return errors.New("slot")
	}
	return nil
}

func homeError(resource semreg.ResourceID, asset semreg.AssetID) error {
	if err := resource.Validate(); err != nil {
		return fmt.Errorf("home resource: %v", err)
	}
	if err := asset.Validate(); err != nil {
		return fmt.Errorf("home asset: %v", err)
	}
	return nil
}

// validSlot admits 1 through 128 bytes of valid UTF-8 without control
// characters.
func validSlot(slot string) bool {
	if slot == "" || len(slot) > maxStoreSlotBytes || !utf8.ValidString(slot) {
		return false
	}
	return strings.IndexFunc(slot, unicode.IsControl) < 0
}
