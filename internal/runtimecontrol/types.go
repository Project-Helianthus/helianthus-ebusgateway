// Package runtimecontrol is the protocol-neutral runtime control package of
// the runtime driver/provider contract. It holds the S0 driver registry
// (descriptor types, validation before effects, a frozen registry with
// coherent list and get views, and the typed lifecycle-adapter seam) and the
// S1/D3 desired-state service: an internal ControlService that decides
// START, STOP and RESTART through an injected durable store and reconciles
// each driver through an injected lifecycle executor and recovery adapter.
// The package performs no construction, endpoint I/O or semantic declaration
// of its own, has no production registration and no MCP surface, and carries
// no receipts, idempotency keys, principals or authorization adapters.
package runtimecontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// DescriptorFormat names the gateway-owned descriptor format.
type DescriptorFormat string

// DescriptorFormatV1 is the only descriptor format this host accepts.
const DescriptorFormatV1 DescriptorFormat = "v1"

// Identity and ordering types. Each is a distinct defined type so that no
// counter or identifier can stand in for another without an explicit
// conversion. None of them is an alias, and none is shared with the semantic
// publication hub.

// DriverID is the stable configured runtime identity of one driver.
type DriverID string

// SourceID is one stable configured source identity owned by a driver.
type SourceID string

// SourceEpoch identifies one source epoch.
type SourceEpoch string

// DriverGeneration counts lifecycle generations inside one source epoch.
type DriverGeneration uint64

// NativeGeneration is a native transport connection generation.
type NativeGeneration uint64

// DesiredRevision is the revision of one persistent desired-state record.
type DesiredRevision uint64

// CatalogRevision is the revision of the coherent registry list view.
type CatalogRevision uint64

// LifecycleSequence is the monotonic per-driver lifecycle sequence.
type LifecycleSequence uint64

// HubSeq is a publication-hub commit sequence as seen by runtime control.
type HubSeq uint64

// Direction is the supported role set of one driver instance.
type Direction string

const (
	DirectionSouthbound Direction = "southbound"
	DirectionNorthbound Direction = "northbound"
	DirectionBoth       Direction = "both"
)

// Disposition is the explicit configured eligibility of one driver. It is
// separate from durable desired state and from observed state.
type Disposition string

const (
	DispositionEnabled  Disposition = "enabled"
	DispositionDisabled Disposition = "disabled"
)

// NativeContractRef names the public native owner, the owner-defined contract
// and the exact supported version or revision. It never carries an endpoint,
// credential, raw payload or handle.
type NativeContractRef struct {
	Owner    string
	Contract string
	Version  string
}

// HookName names a driver hook declaration.
type HookName string

// Reserved future hook names. Reserving a name does not implement it.
const (
	HookMCP      HookName = "mcp"
	HookSetup    HookName = "setup"
	HookPortal   HookName = "portal"
	HookSemantic HookName = "semantic"
	HookEvidence HookName = "evidence"
)

// HookDeclaration is one optional versioned hook declaration.
type HookDeclaration struct {
	Name    HookName
	Version string
}

// Descriptor is the caller-owned registration material of one driver.
// Registration validates it and stores a detached frozen copy.
type Descriptor struct {
	Format         DescriptorFormat
	DriverID       DriverID
	Direction      Direction
	Disposition    Disposition
	NativeContract NativeContractRef
	Adapter        LifecycleAdapter
	Sources        []SourceID
	Hooks          []HookDeclaration
}

// DriverEntry is the immutable registry view of one driver. Every read
// returns a fresh copy; mutating it never alters the registry.
type DriverEntry struct {
	Format         DescriptorFormat
	DriverID       DriverID
	Direction      Direction
	Disposition    Disposition
	NativeContract NativeContractRef
	Sources        []SourceID
	Hooks          []HookDeclaration
}

// ListResult is one coherent registry read: every entry sorted bytewise by
// DriverID together with the catalog revision of the same read.
type ListResult struct {
	CatalogRevision CatalogRevision
	Drivers         []DriverEntry
}

// GetResult is one coherent registry read of a single entry.
type GetResult struct {
	CatalogRevision CatalogRevision
	Driver          DriverEntry
}

// LifecycleCorrelation identifies one lifecycle generation of one driver
// source. It carries identity only.
type LifecycleCorrelation struct {
	DriverID   DriverID
	SourceID   SourceID
	Epoch      SourceEpoch
	Generation DriverGeneration
	Sequence   LifecycleSequence
}

func (entry DriverEntry) clone() DriverEntry {
	entry.Sources = append([]SourceID(nil), entry.Sources...)
	entry.Hooks = append([]HookDeclaration(nil), entry.Hooks...)
	return entry
}

// --- S1/D3 desired-state service types ---
//
// The types below are the protocol-neutral surface of the desired-state
// service (service.go, reconcile.go, events.go). They mirror the durable
// control store's decision and work vocabulary without importing it: the
// service reaches the store only through the Store interface, so this package
// keeps depending on the standard library and internal/drivermanager alone.

// DesiredState is durable driver intent: RUNNING or STOPPED.
type DesiredState string

const (
	DesiredRunning DesiredState = "RUNNING"
	DesiredStopped DesiredState = "STOPPED"
)

// Method selects a lifecycle decision.
type Method string

const (
	MethodStart   Method = "START"
	MethodStop    Method = "STOP"
	MethodRestart Method = "RESTART"
)

// TransitionKind is the exact stored kind of one typed work slot. KindCancel
// is the cancellation synthesized for an untyped attempt.
type TransitionKind string

const (
	KindStart   TransitionKind = "START"
	KindStop    TransitionKind = "STOP"
	KindRestart TransitionKind = "RESTART"
	KindCancel  TransitionKind = "CANCEL_ATTEMPT"
)

// TransitionPhase is the persisted phase of one typed work slot. It only
// moves forward: PENDING, ACTIVE, CLEANUP.
type TransitionPhase string

const (
	PhasePending TransitionPhase = "PENDING"
	PhaseActive  TransitionPhase = "ACTIVE"
	PhaseCleanup TransitionPhase = "CLEANUP"
)

// Transition is one bounded typed in-flight work slot. Sequence is internal
// recovery bookkeeping, never a client operation handle or receipt.
type Transition struct {
	Kind                   TransitionKind
	DesiredRevision        DesiredRevision
	Sequence               LifecycleSequence
	Phase                  TransitionPhase
	Epoch                  SourceEpoch
	Generation             DriverGeneration
	NativeClosePending     bool
	SemanticCleanupPending bool
	AttemptsUsed           int
	DeadlineUTC            time.Time
}

// ControlRecord is the store's coherent read of one visible driver.
type ControlRecord struct {
	DriverID        DriverID
	Enabled         bool
	Failed          bool
	ReadOnly        bool
	HasRecord       bool
	DesiredState    DesiredState
	DesiredRevision DesiredRevision
	Current         *Transition
	Reserved        *Transition
}

// ControlCatalog is the store's coherent read of every visible driver,
// sorted bytewise by DriverID.
type ControlCatalog struct {
	ReadOnly        bool
	IssuedMark      DesiredRevision
	CatalogRevision CatalogRevision
	Drivers         []ControlRecord
}

// PlanInput is what the slot planner sees under the store's namespace-wide
// serialization point.
type PlanInput struct {
	Method    Method
	DriverID  DriverID
	HasRecord bool
	Current   *Transition
	Reserved  *Transition
}

// Plan is the complete replacement of a driver's two typed slots. A new
// transition carries Sequence 0 and DesiredRevision 0; the store assigns the
// accepted revision and the next lifecycle sequences. A retained transition
// equals the stored one exactly.
type Plan struct {
	Current  *Transition
	Reserved *Transition
}

// Planner decides slot placement. It runs under the store's serialization
// point, must not block or perform I/O, and may refuse only with
// refusal-table rows 7 (FAILED_PRECONDITION) or 8 (TRANSITION_BUSY).
type Planner func(PlanInput) (Plan, *Refusal)

// RecoveryMaterial is the D5 recovery envelope content. The payload is opaque
// to the service and the store.
type RecoveryMaterial struct {
	DriverIDs          []DriverID
	MaxDesiredRevision DesiredRevision
	Payload            []byte
}

// RefusalCode is one closed lifecycle refusal code. Lifecycle refusals never
// rename a semantic error; see SemanticError.
type RefusalCode string

const (
	RefusalInvalidArgument    RefusalCode = "INVALID_ARGUMENT"
	RefusalNotFound           RefusalCode = "NOT_FOUND"
	RefusalFailedPrecondition RefusalCode = "FAILED_PRECONDITION"
	RefusalUnavailable        RefusalCode = "UNAVAILABLE"
	RefusalDeadlineExceeded   RefusalCode = "DEADLINE_EXCEEDED"
	RefusalRevisionConflict   RefusalCode = "REVISION_CONFLICT"
	RefusalTransitionBusy     RefusalCode = "TRANSITION_BUSY"
	RefusalResourceExhausted  RefusalCode = "RESOURCE_EXHAUSTED"
	RefusalCounterExhausted   RefusalCode = "COUNTER_EXHAUSTED"
)

// Refusal is a refused lifecycle request. Row is the refusal-table order, or
// 0 for the pre-commit write failure outside the ordered checks. View is set
// only for REVISION_CONFLICT. A refusal leaves durable state unchanged.
type Refusal struct {
	Code   RefusalCode
	Row    int
	View   *DesiredView
	Reason string
}

func (r *Refusal) Error() string {
	return fmt.Sprintf("runtimecontrol: %s (row %d): %s", r.Code, r.Row, r.Reason)
}

// Retryable reports the refusal table's retryable column.
func (r *Refusal) Retryable() bool {
	switch r.Code {
	case RefusalUnavailable, RefusalDeadlineExceeded, RefusalRevisionConflict,
		RefusalTransitionBusy, RefusalResourceExhausted:
		return true
	}
	return false
}

// DesiredView is the closed decision result: desired state and desired
// revision only. It carries no operation ID, sequence, disposition or
// completion claim.
type DesiredView struct {
	DesiredState    DesiredState
	DesiredRevision DesiredRevision
}

// MarshalJSON encodes exactly {desired_state, desired_revision}, with the
// revision as a canonical decimal string.
func (v DesiredView) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		DesiredState    DesiredState `json:"desired_state"`
		DesiredRevision string       `json:"desired_revision"`
	}{v.DesiredState, strconv.FormatUint(uint64(v.DesiredRevision), 10)})
}

// Request is the closed two-field lifecycle request.
type Request struct {
	DriverID         DriverID
	ExpectedRevision DesiredRevision
}

// Store is the durable control store seen by the service: the J09
// runtime_control namespace behind a protocol-neutral seam. Every write is
// durable before it returns; reads return committed state only and never wait
// for an in-flight write.
type Store interface {
	// Decide validates, decides and durably commits one request under the
	// namespace-wide serialization point, consulting the planner for slot
	// placement. It returns the current view for a same-desired no-op.
	Decide(ctx context.Context, method Method, req Request, planner Planner) (DesiredView, error)
	// CommitWork durably replaces a driver's typed slots without changing
	// desired state, its revision or the mark.
	CommitWork(ctx context.Context, id DriverID, current, reserved *Transition) error
	// AdvanceCatalog durably advances the catalog view revision once.
	AdvanceCatalog(ctx context.Context) (CatalogRevision, error)
	// CommitRecovery durably stores the D5 recovery envelope.
	CommitRecovery(ctx context.Context, material *RecoveryMaterial) error
	// Recovery returns a detached copy of the committed D5 envelope.
	Recovery() *RecoveryMaterial
	// Catalog returns one coherent read of committed control state.
	Catalog() (ControlCatalog, error)
	// Driver returns a coherent read of one visible driver.
	Driver(id DriverID) (ControlRecord, error)
	// AdmissionOpen reports whether decisions can be admitted.
	AdmissionOpen() bool
	// ResolveReadback runs the readback after ambiguous persistence.
	ResolveReadback(ctx context.Context) error
}

// LifecycleExecutor is the injectable native lifecycle seam of one driver.
// The service never constructs it and never performs endpoint I/O itself.
type LifecycleExecutor interface {
	// Start opens one generation under the given correlation. A nil error
	// means the generation is admitted. A failed Start leaves nothing open.
	// An error whose Retryable method reports true schedules a bounded retry.
	Start(ctx context.Context, correlation LifecycleCorrelation) error
	// Stop withdraws and closes the generation of the given correlation. It
	// must accept a correlation whose generation never opened.
	Stop(ctx context.Context, correlation LifecycleCorrelation) StopOutcome
}

// StopOutcome reports the two independent cleanup obligations of a stop.
type StopOutcome struct {
	NativeClosed    bool
	SemanticCleaned bool
}

// RecoveryAdapter is the injectable D5 seam. There is no default
// implementation: a service without one refuses to start.
type RecoveryAdapter interface {
	// Restore replays committed recovery material before any admission.
	Restore(ctx context.Context, material *RecoveryMaterial) error
	// Secure returns the recovery material that must be durable before the
	// given terminal transition is pruned, or nil when nothing new must be
	// retained.
	Secure(ctx context.Context, id DriverID, terminal Transition) (*RecoveryMaterial, error)
}
