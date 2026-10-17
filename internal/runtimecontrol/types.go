// Package runtimecontrol is the protocol-neutral S0 driver registry of the
// runtime driver/provider contract: descriptor types, validation before
// effects, a frozen registry with coherent list and get views, and the typed
// lifecycle-adapter seam. It performs no construction, endpoint I/O, semantic
// declaration, desired-state persistence or lifecycle control, and it carries
// no receipts, idempotency keys, principals or authorization adapters.
package runtimecontrol

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
