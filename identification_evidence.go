package ebusgateway

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/ebus/evidence/identification"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// IdentificationEvidenceRingPerAddress bounds retained exchanges per target.
const IdentificationEvidenceRingPerAddress = identification.RingPerAddress

// IdentificationExchange is a raw, undecoded directed identification exchange.
type IdentificationExchange = identification.Exchange

// IdentificationEvidenceSnapshot is ordered by address, then capture sequence.
type IdentificationEvidenceSnapshot = identification.Snapshot

// IdentificationEvidenceStore preserves the gateway observer and snapshot facade.
// Retention, session identity and byte ownership belong to the native eBUS store.
// The observer session identifies this store instance, not a driver generation.
type IdentificationEvidenceStore struct {
	store *identification.Store
}

// NewIdentificationEvidenceStore returns an empty store. A nil clock uses time.Now.
func NewIdentificationEvidenceStore(now func() time.Time) *IdentificationEvidenceStore {
	return &IdentificationEvidenceStore{store: identification.NewStore(now)}
}

// OnBusEvent delegates capture to the native eBUS store.
func (store *IdentificationEvidenceStore) OnBusEvent(event protocol.BusEvent) error {
	if store == nil {
		return nil
	}
	return store.store.OnBusEvent(event)
}

// Snapshot returns a detached snapshot from the native eBUS store.
func (store *IdentificationEvidenceStore) Snapshot() IdentificationEvidenceSnapshot {
	if store == nil {
		return IdentificationEvidenceSnapshot{}
	}
	return store.store.Snapshot()
}
