package main

import (
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const eebusPendingNormativeRevision semreg.DefinitionID = "unknown_pending_std_01"

// This is the eeBUS-local input hook for the shared publication consumer. It
// carries no canonical FactKey, admitted capability, operation or output binding:
// the native source ledger has not resolved the required normative revisions.
// A consumer must preserve prior per-item evidence on partial refreshes.
// Hooks run synchronously and must not reenter adapter or slot lifecycle methods.
type eebusSemregPublicationHook func(eebusSemregObservation) error

// eebusSemregItem references a documented semantic catalog entry; the reference
// is neither a native pairing identity nor a qualified canonical semantic key.
type eebusSemregItem struct {
	CatalogRef  string
	Evidence    []semreg.EvidenceRef
	Quality     semreg.Quality
	Disposition projection.ProjectionOutcome
	Loss        []projection.LossDetail
}
