package semgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// resourceDomain separates resource identifiers from every other identifier
// the gateway hashes.
const resourceDomain = "helianthus.semgraph.resource/v1"

// Errors of allocation, supersession and home recording.
var (
	ErrNilStore             = errors.New("semgraph: nil supersession store")
	ErrStore                = errors.New("semgraph: supersession store")
	ErrStoreConflict        = errors.New("semgraph: supersession store conflict")
	ErrAnchorSuperseded     = errors.New("semgraph: anchor superseded")
	ErrDowngrade            = errors.New("semgraph: anchor downgrade")
	ErrNotUpgrade           = errors.New("semgraph: anchor change is not an allowed upgrade")
	ErrKindChange           = errors.New("semgraph: resource kind change")
	ErrKeyHeld              = errors.New("semgraph: anchor key already held")
	ErrPreviousAnchor       = errors.New("semgraph: previous anchor is not the current anchor")
	ErrContinuity           = errors.New("semgraph: continuity evidence")
	ErrUnknownResource      = errors.New("semgraph: resource not allocated by this allocator")
	ErrSecondHome           = errors.New("semgraph: resource already has a home asset")
	ErrSupersessionRequired = errors.New("semgraph: anchor basis rises only by supersession")
	ErrClassConflict        = errors.New("semgraph: anchor key is held under another class")
)

// ContinuityBasis names the kind of continuity evidence of a supersession.
type ContinuityBasis string

// The three kinds of continuity evidence. An operator declaration is
// continuity evidence only when the previous anchor is an operator slot.
const (
	// ContinuityBinding: a current binding of the home asset observes the
	// unit under both anchors.
	ContinuityBinding ContinuityBasis = "binding"
	// ContinuityNativeMapping: the native owner's documented mapping from the
	// old token form to the new.
	ContinuityNativeMapping ContinuityBasis = "native_mapping"
	// ContinuityOperatorDeclaration: an operator declaration with audit
	// evidence.
	ContinuityOperatorDeclaration ContinuityBasis = "operator_declaration"
)

// Continuity is the continuity evidence of one supersession.
type Continuity struct {
	Basis    ContinuityBasis
	Evidence []semreg.EvidenceRef
}

// Claim is a resource kind with one anchor.
type Claim struct {
	Kind   semreg.DefinitionID
	Anchor Anchor
}

// Entry is the recorded state of one anchor key. A superseded key is never
// resolved again; a current key resolves to Resource for an anchor of its
// class and basis, and refuses an anchor of another class or basis.
type Entry struct {
	Resource   semreg.ResourceID
	Class      semreg.AnchorClass
	Basis      semreg.AnchorBasis
	Superseded bool
}

// Supersession is one recorded anchor change of Resource, whose anchor class
// is Class. Previous and Next are equal when only the basis rises (an operator
// slot to a native slot at the same parent and slot).
type Supersession struct {
	Resource      semreg.ResourceID
	Class         semreg.AnchorClass
	Previous      AnchorKey
	PreviousBasis semreg.AnchorBasis
	Next          AnchorKey
	NextBasis     semreg.AnchorBasis
}

// Store is the supersession table. Commit is atomic: it records the previous
// key as superseded (unless it equals the next) and the next key as current,
// and refuses a record that contradicts what it holds, including a class
// other than the previous key's recorded one.
type Store interface {
	Lookup(key AnchorKey) (Entry, bool, error)
	Commit(record Supersession) error
}

// MemoryStore is the in-memory Store.
type MemoryStore struct {
	mu      sync.Mutex
	entries map[AnchorKey]Entry
}

// NewMemoryStore returns an empty in-memory supersession table.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[AnchorKey]Entry)}
}

// Lookup returns the recorded entry of key.
func (s *MemoryStore) Lookup(key AnchorKey) (Entry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	return entry, ok, nil
}

// Commit records one supersession.
func (s *MemoryStore) Commit(record Supersession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, ok := s.entries[record.Previous]; ok && (previous.Superseded || previous.Resource != record.Resource || previous.Class != record.Class) {
		return ErrStoreConflict
	}
	if record.Next != record.Previous {
		if _, ok := s.entries[record.Next]; ok {
			return ErrStoreConflict
		}
		s.entries[record.Previous] = Entry{Resource: record.Resource, Class: record.Class, Basis: record.PreviousBasis, Superseded: true}
	}
	s.entries[record.Next] = Entry{Resource: record.Resource, Class: record.Class, Basis: record.NextBasis}
	return nil
}

// Allocator allocates resource identifiers. It is safe for concurrent use.
type Allocator struct {
	mu    sync.Mutex
	store Store
	// held maps each anchor key resolved or superseded to in this
	// incarnation to its resource.
	held map[AnchorKey]heldAnchor
	// kinds holds every resource this allocator returned.
	kinds map[semreg.ResourceID]semreg.DefinitionID
	homes map[semreg.ResourceID]semreg.AssetID
}

// heldAnchor is the resource, class and basis an anchor key was resolved
// under.
type heldAnchor struct {
	resource semreg.ResourceID
	class    semreg.AnchorClass
	basis    semreg.AnchorBasis
}

// storeMu serializes every allocator operation, so that resolving a key is
// atomic relative to a supersession commit even when allocators share a store.
var storeMu sync.Mutex

// NewAllocator returns an allocator over a supersession table.
func NewAllocator(store Store) (*Allocator, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	return &Allocator{
		store: store,
		held:  make(map[AnchorKey]heldAnchor),
		kinds: make(map[semreg.ResourceID]semreg.DefinitionID),
		homes: make(map[semreg.ResourceID]semreg.AssetID),
	}, nil
}

// Resolve returns the identifier and the SemReg anchor record of a resource
// of kind with anchor. Without a recorded supersession the identifier is a
// domain-separated hash over the kind and the anchor key, so the same anchor
// yields the same identifier in every incarnation. A superseded anchor key,
// an anchor of another class than the key's recorded one, an anchor of a lower
// basis (a downgrade) or a higher basis (which rises only by Supersede) and a
// parent this allocator did not return are refused.
func (a *Allocator) Resolve(kind semreg.DefinitionID, anchor Anchor) (semreg.ResourceID, semreg.ResourceAnchor, error) {
	key, err := anchor.Key(kind)
	if err != nil {
		return "", semreg.ResourceAnchor{}, err
	}
	record, err := anchor.record()
	if err != nil {
		return "", semreg.ResourceAnchor{}, err
	}
	storeMu.Lock()
	defer storeMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.parentKnown(anchor); err != nil {
		return "", semreg.ResourceAnchor{}, err
	}
	entry, recorded, err := a.lookup(key)
	if err != nil {
		return "", semreg.ResourceAnchor{}, err
	}
	id := resourceID(key)
	if recorded {
		if entry.Superseded {
			return "", semreg.ResourceAnchor{}, ErrAnchorSuperseded
		}
		if err := currentError(anchor, entry.Class, entry.Basis); err != nil {
			return "", semreg.ResourceAnchor{}, err
		}
		id = entry.Resource
	} else {
		if held, ok := a.held[key]; ok {
			if err := currentError(anchor, held.class, held.basis); err != nil {
				return "", semreg.ResourceAnchor{}, err
			}
			id = held.resource
		}
		// Record the resolved key in the store, so that every allocator over
		// the store sees it as held.
		if err := a.store.Commit(Supersession{Resource: id, Class: anchor.Class, Previous: key, PreviousBasis: anchor.Basis, Next: key, NextBasis: anchor.Basis}); err != nil {
			return "", semreg.ResourceAnchor{}, fmt.Errorf("%w: %w", ErrStore, err)
		}
	}
	a.held[key] = heldAnchor{resource: id, class: anchor.Class, basis: anchor.Basis}
	a.kinds[id] = kind
	return id, record, nil
}

// Supersede keeps the identifier of the resource anchored by previous while
// its anchor becomes next, and returns the supersedes record: the exact
// previous anchor and the continuity evidence. It refuses a kind change, a
// class change, a downgrade, any change that is not an allowed upgrade, a
// previous anchor that is superseded or not current, and a next key already
// held. The allowed upgrades are an operator slot to a native slot or native
// identity, a native slot to a native identity, and a native identity to a
// revised token of the same scheme and issuer.
func (a *Allocator) Supersede(previous, next Claim, continuity Continuity) (semreg.ResourceID, semreg.AnchorSupersession, error) {
	if previous.Kind != next.Kind {
		return "", semreg.AnchorSupersession{}, ErrKindChange
	}
	previousKey, err := previous.Anchor.Key(previous.Kind)
	if err != nil {
		return "", semreg.AnchorSupersession{}, err
	}
	nextKey, err := next.Anchor.Key(next.Kind)
	if err != nil {
		return "", semreg.AnchorSupersession{}, err
	}
	if err := upgradeError(previous.Anchor, next.Anchor, previousKey, nextKey); err != nil {
		return "", semreg.AnchorSupersession{}, err
	}
	previousRecord, err := previous.Anchor.record()
	if err != nil {
		return "", semreg.AnchorSupersession{}, err
	}
	supersedes := semreg.AnchorSupersession{Previous: previousRecord}
	if continuity.Evidence != nil {
		supersedes.Evidence = append(make([]semreg.EvidenceRef, 0, len(continuity.Evidence)), continuity.Evidence...)
	}
	if err := continuityError(previous.Anchor, continuity, supersedes); err != nil {
		return "", semreg.AnchorSupersession{}, err
	}

	storeMu.Lock()
	defer storeMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.parentKnown(next.Anchor); err != nil {
		return "", semreg.AnchorSupersession{}, err
	}
	entry, recorded, err := a.lookup(previousKey)
	if err != nil {
		return "", semreg.AnchorSupersession{}, err
	}
	// Only a key the table records as current can be superseded: a key no
	// allocator over the store has resolved has no history to continue.
	if !recorded {
		return "", semreg.AnchorSupersession{}, fmt.Errorf("%w: key is not resolved", ErrPreviousAnchor)
	}
	if entry.Superseded {
		return "", semreg.AnchorSupersession{}, ErrAnchorSuperseded
	}
	if entry.Class != previous.Anchor.Class || entry.Basis != previous.Anchor.Basis {
		return "", semreg.AnchorSupersession{}, fmt.Errorf("%w: key is held as %s under %s", ErrPreviousAnchor, entry.Class, entry.Basis)
	}
	id := entry.Resource
	if nextKey != previousKey {
		if _, held, err := a.lookup(nextKey); err != nil {
			return "", semreg.AnchorSupersession{}, err
		} else if held {
			return "", semreg.AnchorSupersession{}, ErrKeyHeld
		}
		if holder, held := a.held[nextKey]; held && holder.resource != id {
			return "", semreg.AnchorSupersession{}, ErrKeyHeld
		}
	}
	if err := a.store.Commit(Supersession{Resource: id, Class: next.Anchor.Class, Previous: previousKey, PreviousBasis: previous.Anchor.Basis, Next: nextKey, NextBasis: next.Anchor.Basis}); err != nil {
		return "", semreg.AnchorSupersession{}, fmt.Errorf("%w: %w", ErrStore, err)
	}
	delete(a.held, previousKey)
	a.held[nextKey] = heldAnchor{resource: id, class: next.Anchor.Class, basis: next.Anchor.Basis}
	a.kinds[id] = next.Kind
	return id, supersedes, nil
}

// Home records the one home asset of a resource this allocator returned and
// returns it. Recording the same asset again is accepted; a second asset is
// refused with the recorded home.
func (a *Allocator) Home(resource semreg.ResourceID, asset semreg.AssetID) (semreg.AssetID, error) {
	if err := resource.Validate(); err != nil {
		return "", fmt.Errorf("%w: resource id", ErrUnknownResource)
	}
	if err := asset.Validate(); err != nil {
		return "", fmt.Errorf("semgraph: invalid asset id: %w", err)
	}
	storeMu.Lock()
	defer storeMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, known := a.kinds[resource]; !known {
		return "", ErrUnknownResource
	}
	if home, recorded := a.homes[resource]; recorded {
		if home != asset {
			return home, ErrSecondHome
		}
		return home, nil
	}
	a.homes[resource] = asset
	return asset, nil
}

func (a *Allocator) parentKnown(anchor Anchor) error {
	if anchor.Parent == "" {
		return nil
	}
	if _, known := a.kinds[anchor.Parent]; !known {
		return fmt.Errorf("%w: parent", ErrUnknownResource)
	}
	return nil
}

func (a *Allocator) lookup(key AnchorKey) (Entry, bool, error) {
	entry, ok, err := a.store.Lookup(key)
	if err != nil {
		return Entry{}, false, fmt.Errorf("%w: %w", ErrStore, err)
	}
	return entry, ok, nil
}

// currentError compares an anchor with the class and basis its key is current
// under: another class is refused, a lower basis is a downgrade and a higher
// basis rises only through Supersede.
func currentError(anchor Anchor, class semreg.AnchorClass, basis semreg.AnchorBasis) error {
	if anchor.Class != class {
		return fmt.Errorf("%w: key is held as %s", ErrClassConflict, class)
	}
	switch rank, held := basisRank(anchor.Basis), basisRank(basis); {
	case rank < held:
		return fmt.Errorf("%w: key is held under %s", ErrDowngrade, basis)
	case rank > held:
		return fmt.Errorf("%w: key is held under %s", ErrSupersessionRequired, basis)
	}
	return nil
}

// upgradeError checks the anchor change of a supersession.
func upgradeError(previous, next Anchor, previousKey, nextKey AnchorKey) error {
	if previous.Class != next.Class {
		return fmt.Errorf("%w: class change", ErrNotUpgrade)
	}
	if basisRank(next.Basis) < basisRank(previous.Basis) {
		return ErrDowngrade
	}
	switch previous.Basis {
	case semreg.AnchorOperatorSlot:
		if next.Basis == semreg.AnchorNativeSlot || next.Basis == semreg.AnchorNativeIdentity {
			return nil
		}
	case semreg.AnchorNativeSlot:
		if next.Basis == semreg.AnchorNativeIdentity {
			return nil
		}
	case semreg.AnchorNativeIdentity:
		if previous.Scheme == next.Scheme && previous.Issuer == next.Issuer && previousKey != nextKey {
			return nil
		}
	}
	return fmt.Errorf("%w: %s to %s", ErrNotUpgrade, previous.Basis, next.Basis)
}

// continuityError checks the continuity evidence: a known kind, an operator
// declaration only after an operator slot, and 1 through 32 unique, sorted
// references.
func continuityError(previous Anchor, continuity Continuity, supersedes semreg.AnchorSupersession) error {
	switch continuity.Basis {
	case ContinuityBinding, ContinuityNativeMapping:
	case ContinuityOperatorDeclaration:
		if previous.Basis != semreg.AnchorOperatorSlot {
			return fmt.Errorf("%w: an operator declaration continues only an operator slot", ErrContinuity)
		}
	default:
		return fmt.Errorf("%w: unknown basis", ErrContinuity)
	}
	if err := supersedes.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrContinuity, err)
	}
	return nil
}

func basisRank(basis semreg.AnchorBasis) int {
	switch basis {
	case semreg.AnchorOperatorSlot:
		return 1
	case semreg.AnchorNativeSlot:
		return 2
	case semreg.AnchorNativeIdentity:
		return 3
	}
	return 0
}

// resourceID is "resource:" and the first 32 lowercase hexadecimal characters
// of sha256 over the domain followed by the kind and the anchor key members,
// each preceded by one 0x00 byte. No member can contain 0x00.
func resourceID(key AnchorKey) semreg.ResourceID {
	fields := []string{string(key.Kind), "slot", string(key.Parent), key.Slot}
	if key.Key != "" {
		fields = []string{string(key.Kind), "key", string(key.Key)}
	}
	digest := sha256.New()
	digest.Write([]byte(resourceDomain))
	for _, field := range fields {
		digest.Write([]byte{0})
		digest.Write([]byte(field))
	}
	return semreg.ResourceID("resource:" + hex.EncodeToString(digest.Sum(nil))[:32])
}
