// Package nm_runtime — RED stub. Real impl lands in GREEN commit.
package nm_runtime

import (
	"context"
	"errors"

	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

type EmitEvent string

const (
	EventResetStatus EmitEvent = "nm_reset_status_broadcast"
	EventFailure     EmitEvent = "nm_failure_broadcast"
)

var ErrNoCatalogEntry = errors.New("nm_runtime: stub no entry")

type Emitter interface {
	EmitBroadcast(ctx context.Context, source, pb, sb byte, payload []byte) error
}

type Runtime struct{}

func NewRuntime(_ ebusstd.Catalog, _ Emitter) *Runtime { return &Runtime{} }

func (r *Runtime) Emit(_ context.Context, _ EmitEvent, _ []byte) error {
	return ErrNoCatalogEntry
}
