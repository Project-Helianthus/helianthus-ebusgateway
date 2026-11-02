package runtimecontrol

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// Registration errors. Every refusal wraps exactly one of these and is
// returned before any adapter runs or any registry state changes.
var (
	ErrEmptyRegistration        = errors.New("runtimecontrol: empty registration")
	ErrUnsupportedFormat        = errors.New("runtimecontrol: unsupported descriptor format")
	ErrNoncanonicalDriverID     = errors.New("runtimecontrol: noncanonical driver id")
	ErrDuplicateDriverID        = errors.New("runtimecontrol: duplicate driver id")
	ErrInvalidDirection         = errors.New("runtimecontrol: invalid direction")
	ErrInvalidDisposition       = errors.New("runtimecontrol: invalid disposition")
	ErrInvalidNativeContractRef = errors.New("runtimecontrol: invalid native contract reference")
	ErrMissingAdapter           = errors.New("runtimecontrol: missing lifecycle adapter")
	ErrInvalidSourceID          = errors.New("runtimecontrol: invalid source id")
	ErrDuplicateSourceID        = errors.New("runtimecontrol: duplicate source ownership")
	ErrHubSourceCollision       = errors.New("runtimecontrol: source already owned in the hub")
	ErrUnknownHook              = errors.New("runtimecontrol: unknown hook")
	ErrHookUnimplemented        = errors.New("runtimecontrol: hook not implemented")
	ErrNotFound                 = errors.New("runtimecontrol: driver not found")
)

const (
	maxDriverIDBytes    = 128
	maxSourceIDBytes    = 256
	maxNativeFieldBytes = 256
)

var (
	driverIDPattern    = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)
	sourceIDPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@-]*$`)
	nativeFieldPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/+-]*$`)
)

// RegistrationError reports one refused descriptor. Index is the position in
// the submitted batch. DriverID is set only when it is canonical; refused
// native references and source material are never echoed.
type RegistrationError struct {
	Index    int
	DriverID DriverID
	Err      error
}

func (e *RegistrationError) Error() string {
	if e.DriverID != "" {
		return fmt.Sprintf("descriptor %d (%s): %v", e.Index, e.DriverID, e.Err)
	}
	return fmt.Sprintf("descriptor %d: %v", e.Index, e.Err)
}

func (e *RegistrationError) Unwrap() error { return e.Err }

type registered struct {
	entry   DriverEntry
	adapter LifecycleAdapter
}

// Registry is the frozen S0 driver registry. Reads return detached copies and
// never construct a driver.
type Registry struct {
	hub HubSourceOwnership

	mu       sync.RWMutex
	revision CatalogRevision
	entries  map[DriverID]registered
	sources  map[SourceID]DriverID
}

// NewRegistry returns an empty registry that checks source ownership against
// the given hub view. A nil view, including a typed nil, means the hub owns no source.
func NewRegistry(hub HubSourceOwnership) *Registry {
	if value := reflect.ValueOf(hub); value.IsValid() {
		switch value.Kind() {
		case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan:
			if value.IsNil() {
				hub = nil
			}
		}
	}
	return &Registry{hub: hub, entries: make(map[DriverID]registered), sources: make(map[SourceID]DriverID)}
}

// Register validates the complete batch, including disabled entries, against
// itself, the installed registry and the hub, then installs detached frozen
// copies in one step. Any refusal installs nothing. No adapter is invoked.
func (r *Registry) Register(descriptors ...Descriptor) error {
	if len(descriptors) == 0 {
		return ErrEmptyRegistration
	}
	frozen := make([]registered, len(descriptors))
	for index, descriptor := range descriptors {
		entry, err := validateDescriptor(descriptor)
		if err != nil {
			return refusal(index, descriptor.DriverID, err)
		}
		frozen[index] = registered{entry: entry, adapter: descriptor.Adapter}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Complete passes in the documented order; each pass covers the whole
	// batch before the next begins, so the first refusal class does not
	// depend on descriptor order.
	batchDrivers := make(map[DriverID]struct{}, len(frozen))
	for index, candidate := range frozen {
		id := candidate.entry.DriverID
		if _, exists := r.entries[id]; exists {
			return refusal(index, id, ErrDuplicateDriverID)
		}
		if _, exists := batchDrivers[id]; exists {
			return refusal(index, id, ErrDuplicateDriverID)
		}
		batchDrivers[id] = struct{}{}
	}
	batchSources := make(map[SourceID]struct{})
	for index, candidate := range frozen {
		for _, source := range candidate.entry.Sources {
			if _, exists := r.sources[source]; exists {
				return refusal(index, candidate.entry.DriverID, ErrDuplicateSourceID)
			}
			if _, exists := batchSources[source]; exists {
				return refusal(index, candidate.entry.DriverID, ErrDuplicateSourceID)
			}
			batchSources[source] = struct{}{}
		}
	}
	if r.hub != nil {
		for index, candidate := range frozen {
			id := candidate.entry.DriverID
			for _, source := range candidate.entry.Sources {
				if owner, owned := r.hub.SourceOwner(source); owned && owner != id {
					return refusal(index, id, ErrHubSourceCollision)
				}
			}
		}
	}
	for _, candidate := range frozen {
		r.entries[candidate.entry.DriverID] = candidate
		for _, source := range candidate.entry.Sources {
			r.sources[source] = candidate.entry.DriverID
		}
	}
	r.revision++
	return nil
}

// List returns every registered driver sorted bytewise by DriverID with the
// catalog revision of the same read.
func (r *Registry) List() ListResult {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.entries))
	for id := range r.entries {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	drivers := make([]DriverEntry, 0, len(ids))
	for _, id := range ids {
		drivers = append(drivers, r.entries[DriverID(id)].entry.clone())
	}
	return ListResult{CatalogRevision: r.revision, Drivers: drivers}
}

// Get returns one registered driver with the catalog revision of the same
// read, or ErrNotFound. It never constructs a driver.
func (r *Registry) Get(id DriverID) (GetResult, error) {
	if !canonicalDriverID(id) {
		return GetResult{}, ErrNotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	stored, ok := r.entries[id]
	if !ok {
		return GetResult{}, ErrNotFound
	}
	return GetResult{CatalogRevision: r.revision, Driver: stored.entry.clone()}, nil
}

func refusal(index int, id DriverID, err error) error {
	if !canonicalDriverID(id) {
		id = ""
	}
	return &RegistrationError{Index: index, DriverID: id, Err: err}
}

func validateDescriptor(descriptor Descriptor) (DriverEntry, error) {
	if descriptor.Format != DescriptorFormatV1 {
		return DriverEntry{}, ErrUnsupportedFormat
	}
	if !canonicalDriverID(descriptor.DriverID) {
		return DriverEntry{}, ErrNoncanonicalDriverID
	}
	switch descriptor.Direction {
	case DirectionSouthbound, DirectionNorthbound, DirectionBoth:
	default:
		return DriverEntry{}, ErrInvalidDirection
	}
	switch descriptor.Disposition {
	case DispositionEnabled, DispositionDisabled:
	default:
		return DriverEntry{}, ErrInvalidDisposition
	}
	if !safeNativeContractRef(descriptor.NativeContract) {
		return DriverEntry{}, ErrInvalidNativeContractRef
	}
	if !adapterUsable(descriptor.Adapter) {
		return DriverEntry{}, ErrMissingAdapter
	}
	sources := make([]SourceID, 0, len(descriptor.Sources))
	seen := make(map[SourceID]struct{}, len(descriptor.Sources))
	for _, source := range descriptor.Sources {
		if !validSourceID(source) {
			return DriverEntry{}, ErrInvalidSourceID
		}
		if _, exists := seen[source]; exists {
			return DriverEntry{}, ErrDuplicateSourceID
		}
		seen[source] = struct{}{}
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	for _, hook := range descriptor.Hooks {
		if err := hookError(hook.Name); err != nil {
			return DriverEntry{}, err
		}
	}
	entry := DriverEntry{
		Format:         descriptor.Format,
		DriverID:       descriptor.DriverID,
		Direction:      descriptor.Direction,
		Disposition:    descriptor.Disposition,
		NativeContract: descriptor.NativeContract,
		Sources:        sources,
	}
	return entry.clone(), nil
}

func canonicalDriverID(id DriverID) bool {
	return len(id) > 0 && len(id) <= maxDriverIDBytes && driverIDPattern.MatchString(string(id))
}

func validSourceID(source SourceID) bool {
	return len(source) > 0 && len(source) <= maxSourceIDBytes && sourceIDPattern.MatchString(string(source))
}

func safeNativeContractRef(ref NativeContractRef) bool {
	for _, field := range []string{ref.Owner, ref.Contract, ref.Version} {
		if len(field) == 0 || len(field) > maxNativeFieldBytes || !nativeFieldPattern.MatchString(field) {
			return false
		}
		if strings.Contains(field, "..") || strings.Contains(field, "//") || net.ParseIP(field) != nil {
			return false
		}
	}
	return true
}

// hookError classifies a hook name. This host implements no hook, so every
// declaration fails: reserved names as unimplemented, all others as unknown.
func hookError(name HookName) error {
	switch name {
	case HookMCP, HookSetup, HookPortal, HookSemantic, HookEvidence:
		return fmt.Errorf("%w: %s is reserved", ErrHookUnimplemented, name)
	default:
		return ErrUnknownHook
	}
}
