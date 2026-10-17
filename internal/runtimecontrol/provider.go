package runtimecontrol

import (
	"context"
	"errors"
	"reflect"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

// ErrNilRuntime reports a factory that returned no error and no usable
// runtime, for example a disabled optional driver returning a nil pointer.
var ErrNilRuntime = errors.New("runtimecontrol: factory returned a nil runtime")

// Construction is the frozen registration material a lifecycle adapter
// receives when a later composition step constructs its runtime.
type Construction struct {
	DriverID       DriverID
	Direction      Direction
	NativeContract NativeContractRef
	Sources        []SourceID
}

// LifecycleAdapter is the typed factory plus DriverManager runtime binding of
// one driver. Registration stores it without invoking it.
type LifecycleAdapter interface {
	Construct(context.Context, Construction) (drivermanager.Runtime, error)
}

// NewAdapter wraps a typed runtime factory in the common adapter seam, so that
// unrelated driver packages with distinct runtime types register through the
// same path.
func NewAdapter[R drivermanager.Runtime](factory func(context.Context, Construction) (R, error)) LifecycleAdapter {
	return typedAdapter[R]{factory: factory}
}

type typedAdapter[R drivermanager.Runtime] struct {
	factory func(context.Context, Construction) (R, error)
}

func (adapter typedAdapter[R]) Construct(ctx context.Context, construction Construction) (drivermanager.Runtime, error) {
	runtime, err := adapter.factory(ctx, construction)
	if err != nil {
		return nil, err
	}
	// Check the generic value before the interface conversion: a typed nil
	// pointer converted to drivermanager.Runtime is a non-nil interface.
	if value := reflect.ValueOf(runtime); !value.IsValid() {
		return nil, ErrNilRuntime
	} else {
		switch value.Kind() {
		case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan:
			if value.IsNil() {
				return nil, ErrNilRuntime
			}
		}
	}
	return runtime, nil
}

func (adapter typedAdapter[R]) usable() bool { return adapter.factory != nil }

func adapterUsable(adapter LifecycleAdapter) bool {
	if adapter == nil {
		return false
	}
	value := reflect.ValueOf(adapter)
	switch value.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan:
		if value.IsNil() {
			return false
		}
	}
	if checked, ok := adapter.(interface{ usable() bool }); ok {
		return checked.usable()
	}
	return true
}

// Binding is the read-only pairing of one frozen construction with its
// stored adapter. Obtaining a binding constructs nothing.
type Binding struct {
	Construction Construction
	Adapter      LifecycleAdapter
}

// HubSourceOwnership is the read-only view of source ownership already held
// by the semantic publication hub. Registration consults it and never
// mutates hub ownership.
type HubSourceOwnership interface {
	SourceOwner(SourceID) (DriverID, bool)
}

// ResolveHook reports whether a hook of a registered driver is available.
// No hook is implemented by this host: every reserved name fails as
// unimplemented, evidence included, and any other name is unknown.
func (r *Registry) ResolveHook(id DriverID, name HookName) error {
	if _, err := r.Get(id); err != nil {
		return err
	}
	return hookError(name)
}

// Binding returns the frozen construction and adapter of a registered driver
// without invoking the adapter.
func (r *Registry) Binding(id DriverID) (Binding, error) {
	if !canonicalDriverID(id) {
		return Binding{}, ErrNotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	stored, ok := r.entries[id]
	if !ok {
		return Binding{}, ErrNotFound
	}
	return Binding{
		Construction: Construction{
			DriverID:       stored.entry.DriverID,
			Direction:      stored.entry.Direction,
			NativeContract: stored.entry.NativeContract,
			Sources:        append([]SourceID(nil), stored.entry.Sources...),
		},
		Adapter: stored.adapter,
	}, nil
}
