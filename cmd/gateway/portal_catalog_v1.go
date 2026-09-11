package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portalgraphql"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
)

// newPortalCatalogV1Handler starts with an intentionally empty, truthful
// registry. Gateway composition may publish only accepted descriptors and
// detached sources; no legacy/public API is consulted as a fallback.
func newPortalCatalogV1Handler(source catalogv1.SourceCapture) http.Handler {
	index, err := newGatewayPortalSemanticIndex()
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "portal semantic registry unavailable", http.StatusServiceUnavailable)
		})
	}
	composer := catalogv1.New(index, source, nil, nil)
	registry := contributionv1.NewRegistry(index)
	byDriver := map[string][]contributionv1.Manifest{}
	for _, m := range acceptedPortalCatalogDescriptors() {
		byDriver[m.Contributor.DriverID] = append(byDriver[m.Contributor.DriverID], m)
	}
	for driver, manifests := range byDriver {
		if err := registry.ReplaceGeneration(contributionv1.DriverGeneration{DriverID: driver, Generation: 1}, manifests); err != nil {
			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "portal descriptor registry unavailable", http.StatusServiceUnavailable)
			})
		}
	}
	for _, descriptor := range registry.Snapshot().Accepted {
		m := descriptor.Manifest
		if err := composer.Publish(m); err != nil {
			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "portal descriptor registry unavailable", http.StatusServiceUnavailable)
			})
		}
	}
	return portalgraphql.Handler{Catalog: composer}
}

// gatewayPortalSemanticIndex is deliberately thin: SemReg owns every semantic
// relation, while Gateway keeps the native-contract membership boundary.
type gatewayPortalSemanticIndex struct{ metadata *semreg.PackMetadataRegistry }

func newGatewayPortalSemanticIndex() (*gatewayPortalSemanticIndex, error) {
	r, err := packs.NewMetadataRegistry()
	if err != nil {
		return nil, err
	}
	return &gatewayPortalSemanticIndex{metadata: r}, nil
}
func semanticPack(p contributionv1.PackRef) semreg.PackRef {
	return semreg.PackRef{ID: semreg.DefinitionID(p.ID), Version: semreg.SemanticVersion(p.Version)}
}
func semanticRef(r contributionv1.DefinitionRef) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: semanticPack(r.Pack), ID: semreg.DefinitionID(r.ID), Version: semreg.SemanticVersion(r.Version)}
}
func (i *gatewayPortalSemanticIndex) HasPack(p contributionv1.PackRef) bool {
	if i == nil || i.metadata == nil {
		return false
	}
	for _, got := range i.metadata.Packs() {
		if got == semanticPack(p) {
			return true
		}
	}
	return false
}
func (i *gatewayPortalSemanticIndex) HasDefinition(r contributionv1.DefinitionRef) bool {
	return i != nil && i.metadata != nil && i.metadata.HasDefinition(semanticRef(r))
}
func (i *gatewayPortalSemanticIndex) CanonicalUnit(r contributionv1.DefinitionRef) (contributionv1.DefinitionRef, bool) {
	if i == nil || i.metadata == nil {
		return contributionv1.DefinitionRef{}, false
	}
	u, ok := i.metadata.CanonicalUnit(semanticRef(r))
	return contributionv1.DefinitionRef{Pack: contributionv1.PackRef{ID: string(u.Pack.ID), Version: string(u.Pack.Version)}, ID: string(u.ID), Version: string(u.Version)}, ok
}
func (i *gatewayPortalSemanticIndex) ServiceOwnsCapability(s, c contributionv1.DefinitionRef) bool {
	return i != nil && i.metadata != nil && i.metadata.ServiceOwnsCapability(semanticRef(s), semanticRef(c))
}
func (i *gatewayPortalSemanticIndex) FieldMatches(f, s, c contributionv1.DefinitionRef) bool {
	return i != nil && i.metadata != nil && i.metadata.FieldMatches(semanticRef(f), semanticRef(s), semanticRef(c))
}
func (i *gatewayPortalSemanticIndex) OperationMatches(o, c, s, a, e contributionv1.DefinitionRef) bool {
	return i != nil && i.metadata != nil && i.metadata.OperationMatches(semanticRef(o), semanticRef(c), semanticRef(s), semanticRef(a), semanticRef(e))
}

// No Portal descriptor currently exposes native diagnostics.  Failing closed
// here prevents a future descriptor from inventing a native member.
func (*gatewayPortalSemanticIndex) HasNativeMember(contributionv1.NativeContractRef, string, string) bool {
	return false
}

func acceptedPortalCatalogDescriptors() []contributionv1.Manifest {
	items := []struct {
		driver, id, group, field, service, capability, unit string
		pack                                                contributionv1.PackRef
	}{
		{"pv.primary", "portal.pv", "inverter", "pv.ac.frequency", "pv.service.inverter", "pv.capability.read.inverter", "unit.hertz", catalogv1.FivePacks[1]},
		{"storage.primary", "portal.storage", "pack", "storage.state.soc", "storage.service.pack", "storage.capability.read.pack", "unit.percent", catalogv1.FivePacks[2]},
		{"evse.primary", "portal.evse", "evse", "evse.limit.configured_current", "evse.service.evse", "evse.capability.read.evse", "unit.ampere", catalogv1.FivePacks[3]},
	}
	out := make([]contributionv1.Manifest, 0, len(items))
	for _, x := range items {
		ref := func(id string) contributionv1.DefinitionRef {
			return contributionv1.DefinitionRef{Pack: x.pack, ID: id, Version: x.pack.Version}
		}
		out = append(out, contributionv1.Manifest{Contract: contributionv1.Contract, ManifestID: x.id, ManifestVersion: "1.0.0", Contributor: contributionv1.Contributor{DriverID: x.driver, NativeContract: contributionv1.NativeContractRef{Owner: "helianthus", Contract: "portal-catalog", Version: "1.0.0"}}, Requires: contributionv1.Requirements{SemanticKernel: contributionv1.SemanticKernel, Packs: []contributionv1.PackRef{x.pack}}, Groups: []contributionv1.Group{{ID: x.group, Label: contributionv1.Label{Key: x.id + ".group", Default: x.group}, ResourceContext: x.group}}, Fields: []contributionv1.Field{{ID: x.field, Group: x.group, Label: contributionv1.Label{Key: x.id + ".field", Default: x.field}, Ref: ref(x.field), ServiceRef: ref(x.service), CapabilityRef: ref(x.capability), UnitRef: ref(x.unit)}}, Views: []contributionv1.View{{ID: x.id + ".summary", Group: x.group, Label: contributionv1.Label{Key: x.id + ".summary", Default: "Summary"}, Renderer: "summary", Slot: "lens", FieldIDs: []string{x.field}, DiagnosticIDs: []string{}}}, Actions: []contributionv1.Action{}, Diagnostics: []contributionv1.Diagnostic{}})
	}
	return out
}

type gatewayPortalCatalogSource struct {
	pv      *modbusadapter.Adapter
	storage *growattStoragePublication
	evse    *teslaHSCRetainedOwner
}

func newGatewayPortalCatalogSource(pv *modbusadapter.Adapter, storage *growattStoragePublication, evse *teslaHSCRetainedOwner) catalogv1.SourceCapture {
	return &gatewayPortalCatalogSource{pv: pv, storage: storage, evse: evse}
}

func gatewayPortalStoragePublication(provider *growattBMSRS485ProductionProvider) *growattStoragePublication {
	if provider == nil {
		return nil
	}
	return provider.storage
}
func (s *gatewayPortalCatalogSource) Capture(at time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	resources := []catalogv1.Resource{}
	fields := []catalogv1.Field{}
	if s == nil {
		return resources, fields, nil
	}
	if s.pv != nil {
		if v, ok := s.pv.SemanticPVCurrentSingleAt(at); ok {
			resources, fields = appendPortalPV(resources, fields, v)
		}
	}
	if s.storage != nil {
		if v, ok := s.storage.CurrentAt(string(s.storage.assetID), at); ok {
			resources, fields = appendPortalStorage(resources, fields, v)
		}
	}
	if s.evse != nil {
		if v, ok := s.evse.SemanticEVSECurrentAt(at); ok {
			resources, fields = appendPortalEVSE(resources, fields, v)
		}
	}
	return resources, fields, nil
}
func rawPortal(v any) (json.RawMessage, string) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, ""
	}
	return json.RawMessage(b), string(b)
}
func portalDigest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func appendPortalSnapshot(resources []catalogv1.Resource, fields []catalogv1.Field, domain, driver, manifest string, snapshot semreg.Snapshot, evaluation any, selections any, projection any) ([]catalogv1.Resource, []catalogv1.Field) {
	snap, _ := rawPortal(snapshot)
	eval, evalText := rawPortal(evaluation)
	sels, _ := rawPortal(selections)
	proj, _ := rawPortal(projection)
	revisions, revisionText := rawPortal(snapshot.Revisions)
	bindings, bindingText := rawPortal(snapshot.Bindings)
	sources, sourceText := rawPortal(snapshot.Sources)
	generation := uint64(0)
	for _, service := range snapshot.Services {
		if n, err := strconv.ParseUint(string(service.DriverGeneration), 10, 64); err == nil && n > generation {
			generation = n
		}
	}
	for _, capability := range snapshot.Capabilities {
		if n, err := strconv.ParseUint(string(capability.DriverGeneration), 10, 64); err == nil && n > generation {
			generation = n
		}
	}
	source := catalogv1.Source{AssetID: string(snapshot.AssetID), SnapshotID: string(snapshot.SnapshotID), Revision: revisionText, EvaluationDigest: portalDigest(evaluation), BindingID: bindingText, SourceEpoch: sourceText, DriverGeneration: generation, Snapshot: snap, Evaluation: eval, Selections: sels, Projection: proj}
	r := catalogv1.Resource{ID: string(snapshot.AssetID), Domain: domain, State: "CURRENT", Source: source, ContributionDriverID: driver, ContributionManifestID: manifest, ContributionManifestVersion: "1.0.0"}
	if len(revisions) == 0 || len(evalText) == 0 || len(bindings) == 0 || len(sources) == 0 {
		return resources, fields
	}
	// A source snapshot is the complete fact/projection envelope.  Do not make
	// a second, lossy field table with null values or reconstructed quality.
	return append(resources, r), fields
}
func appendPortalPV(resources []catalogv1.Resource, fields []catalogv1.Field, v modbusadapter.SemanticPVCurrent) ([]catalogv1.Resource, []catalogv1.Field) {
	return appendPortalSnapshot(resources, fields, "helianthus.pack.pv", "pv.primary", "portal.pv", v.Snapshot, v.Evaluation, v.Selections, v.Projection)
}
func appendPortalStorage(resources []catalogv1.Resource, fields []catalogv1.Field, v growattStorageCurrent) ([]catalogv1.Resource, []catalogv1.Field) {
	return appendPortalSnapshot(resources, fields, "helianthus.pack.storage", "storage.primary", "portal.storage", v.Snapshot, v.Evaluation, []semreg.Selection{}, v.Projection)
}
func appendPortalEVSE(resources []catalogv1.Resource, fields []catalogv1.Field, v mcp.SemanticEVSECurrent) ([]catalogv1.Resource, []catalogv1.Field) {
	return appendPortalSnapshot(resources, fields, "helianthus.pack.evse", "evse.primary", "portal.evse", v.Snapshot, v.Evaluation, v.Selections, v.Projection)
}
