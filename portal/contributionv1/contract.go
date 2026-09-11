// Package contributionv1 defines the closed Portal driver contribution v1 wire
// model. It intentionally contains presentation metadata and typed references
// only: the host resolves every reference and owns rendering and authority.
package contributionv1

const Contract = "helianthus.gateway.portal-contribution/v1"
const SemanticKernel = "helianthus.semantic.kernel/v1"

const (
	MaxManifestBytes       = 256 << 10
	MaxGroups              = 64
	MaxViews               = 64
	MaxFields              = 512
	MaxActions             = 64
	MaxDiagnostics         = 128
	MinOrder         int32 = -2147483648
	MaxOrder         int32 = 2147483647
)

type PackRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

type DefinitionRef struct {
	Pack    PackRef `json:"pack"`
	ID      string  `json:"id"`
	Version string  `json:"version"`
}

type NativeContractRef struct {
	Owner    string `json:"owner"`
	Contract string `json:"contract"`
	Version  string `json:"version"`
}

type Label struct {
	Key     string `json:"key"`
	Default string `json:"default"`
}

type Contributor struct {
	DriverID       string            `json:"driver_id"`
	NativeContract NativeContractRef `json:"native_contract"`
}

type Requirements struct {
	SemanticKernel string    `json:"semantic_kernel"`
	Packs          []PackRef `json:"packs"`
}

type Group struct {
	ID              string `json:"id"`
	Label           Label  `json:"label"`
	ResourceContext string `json:"resource_context"`
	Order           int32  `json:"order"`
}

type Field struct {
	ID            string        `json:"id"`
	Group         string        `json:"group"`
	Label         Label         `json:"label"`
	Ref           DefinitionRef `json:"ref"`
	ServiceRef    DefinitionRef `json:"service_ref"`
	CapabilityRef DefinitionRef `json:"capability_ref"`
	UnitRef       DefinitionRef `json:"unit_ref"`
	Order         int32         `json:"order"`
}

type View struct {
	ID            string   `json:"id"`
	Group         string   `json:"group"`
	Label         Label    `json:"label"`
	Renderer      string   `json:"renderer"`
	Slot          string   `json:"slot"`
	FieldIDs      []string `json:"field_ids"`
	DiagnosticIDs []string `json:"diagnostic_ids"`
	Order         int32    `json:"order"`
}

type Action struct {
	ID            string        `json:"id"`
	Group         string        `json:"group"`
	Label         Label         `json:"label"`
	OperationRef  DefinitionRef `json:"operation_ref"`
	CapabilityRef DefinitionRef `json:"capability_ref"`
	ServiceRef    DefinitionRef `json:"service_ref"`
	ArgumentRef   DefinitionRef `json:"argument_ref"`
	EffectRef     DefinitionRef `json:"effect_ref"`
	Order         int32         `json:"order"`
}

type Diagnostic struct {
	ID       string `json:"id"`
	Group    string `json:"group"`
	Label    Label  `json:"label"`
	MemberID string `json:"member_id"`
	Kind     string `json:"kind"`
	Order    int32  `json:"order"`
}

type Manifest struct {
	Contract        string       `json:"contract"`
	ManifestID      string       `json:"manifest_id"`
	ManifestVersion string       `json:"manifest_version"`
	Contributor     Contributor  `json:"contributor"`
	Requires        Requirements `json:"requires"`
	Groups          []Group      `json:"groups"`
	Fields          []Field      `json:"fields"`
	Views           []View       `json:"views"`
	Actions         []Action     `json:"actions"`
	Diagnostics     []Diagnostic `json:"diagnostics"`
}

// SemanticIndex is supplied by the SemReg catalog/admission owner. It is a
// read-only resolver, never an opportunity for a manifest to define semantics.
type SemanticIndex interface {
	HasPack(PackRef) bool
	HasDefinition(DefinitionRef) bool
	CanonicalUnit(DefinitionRef) (DefinitionRef, bool)
	ServiceOwnsCapability(DefinitionRef, DefinitionRef) bool
	FieldMatches(DefinitionRef, DefinitionRef, DefinitionRef) bool
	OperationMatches(DefinitionRef, DefinitionRef, DefinitionRef, DefinitionRef, DefinitionRef) bool
	HasNativeMember(NativeContractRef, string, string) bool
}

// StaticIndex is deliberately small and useful for deterministic fixtures.
// Production composition must provide a current SemReg-backed SemanticIndex.
type StaticIndex struct {
	Packs               map[packKey]bool
	Definitions         map[definitionKey]bool
	Units               map[definitionKey]DefinitionRef
	ServiceCapabilities map[serviceCapabilityKey]bool
	FieldRelations      map[fieldRelationKey]bool
	Operations          map[operationKey]bool
	NativeMembers       map[nativeMemberKey]bool
}

type packKey struct{ ID, Version string }
type definitionKey struct {
	Pack        packKey
	ID, Version string
}
type serviceCapabilityKey struct{ Service, Capability definitionKey }
type fieldRelationKey struct{ Field, Service, Capability definitionKey }
type operationKey struct{ Operation, Capability, Service, Argument, Effect definitionKey }
type nativeContractKey struct{ Owner, Contract, Version string }
type nativeMemberKey struct {
	Contract nativeContractKey
	Kind, ID string
}

func asPackKey(p PackRef) packKey { return packKey(p) }
func asDefinitionKey(r DefinitionRef) definitionKey {
	return definitionKey{asPackKey(r.Pack), r.ID, r.Version}
}
func asNativeContractKey(r NativeContractRef) nativeContractKey {
	return nativeContractKey(r)
}

func (s StaticIndex) HasPack(p PackRef) bool             { return s.Packs[asPackKey(p)] }
func (s StaticIndex) HasDefinition(r DefinitionRef) bool { return s.Definitions[asDefinitionKey(r)] }
func (s StaticIndex) CanonicalUnit(r DefinitionRef) (DefinitionRef, bool) {
	u, ok := s.Units[asDefinitionKey(r)]
	return u, ok
}
func (s StaticIndex) ServiceOwnsCapability(service, capability DefinitionRef) bool {
	return s.ServiceCapabilities[serviceCapabilityKey{asDefinitionKey(service), asDefinitionKey(capability)}]
}
func (s StaticIndex) FieldMatches(field, service, capability DefinitionRef) bool {
	return s.FieldRelations[fieldRelationKey{asDefinitionKey(field), asDefinitionKey(service), asDefinitionKey(capability)}]
}
func (s StaticIndex) OperationMatches(operation, capability, service, argument, effect DefinitionRef) bool {
	return s.Operations[operationKey{asDefinitionKey(operation), asDefinitionKey(capability), asDefinitionKey(service), asDefinitionKey(argument), asDefinitionKey(effect)}]
}
func (s StaticIndex) HasNativeMember(contract NativeContractRef, kind, id string) bool {
	return s.NativeMembers[nativeMemberKey{asNativeContractKey(contract), kind, id}]
}
