// Package eebusoutput is the gateway side of the eeBUS output binding
// (docs/architecture/eebus-output-binding-design.md). The binding itself lives
// in the eeBUS registry; this package only relays a peer request, unchanged, to
// the injected semantic operation route. It serves no eeBUS feature, registers
// no SHIP endpoint, constructs no native frame and owns no mapping.
package eebusoutput

import (
	"context"
	"reflect"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
)

// Operations is the semantic operation route, satisfied by
// *semanticoperations.Router.
type Operations interface {
	Route(context.Context, semanticoperations.Request) (semanticoperations.Result, error)
}

// Config composes a Binding. A nil Operations leaves the relay unwired.
type Config struct {
	Operations Operations
}

// Binding is the gateway relay. It is safe for concurrent use.
type Binding struct {
	operations Operations
}

// NewBinding composes a binding. A typed-nil route counts as absent.
func NewBinding(config Config) (*Binding, error) {
	binding := &Binding{}
	if !isNil(config.Operations) {
		binding.operations = config.Operations
	}
	return binding, nil
}

// Relay hands one request to the injected operation route and returns its
// Result and error unchanged. It adds no retry, reroute or readback and never
// reports an eeBUS-side acknowledgement as native execution. Without a route
// the relay is unwired and answers UNAVAILABLE with no send.
func (b *Binding) Relay(ctx context.Context, request semanticoperations.Request) (semanticoperations.Result, error) {
	if b == nil || b.operations == nil {
		return semanticoperations.Result{}, &semanticoperations.Error{Code: semanticoperations.CodeUnavailable, Detail: "eebus output relay unwired"}
	}
	return b.operations.Route(ctx, request)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}
