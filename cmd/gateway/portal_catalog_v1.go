package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/catalogv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal/contributionv1"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portalgraphql"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
)

type gatewayPortalCatalogContributionOwner interface {
	PortalCatalogContributions() *gatewayPortalCatalogContributions
}

// newPortalCatalogV1Handler captures the long-lived Gateway contribution
// registry on every request. Gateway composition may publish only accepted
// descriptors and detached sources; no legacy/public API is consulted as a
// fallback.
func newPortalCatalogV1Handler(source catalogv1.SourceCapture) http.Handler {
	owner, ok := source.(gatewayPortalCatalogContributionOwner)
	if !ok || owner.PortalCatalogContributions() == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "portal contribution registry unavailable", http.StatusServiceUnavailable)
		})
	}
	contributions := owner.PortalCatalogContributions()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		composer, err := contributions.composer(source)
		if err != nil {
			http.Error(w, "portal descriptor registry unavailable", http.StatusServiceUnavailable)
			return
		}
		portalgraphql.Handler{Catalog: composer}.ServeHTTP(w, r)
	})
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

// Only the Huawei instance contribution exposes native diagnostics, and only
// its closed status members. Every other member fails closed, so a descriptor
// cannot invent a native member.
func (*gatewayPortalSemanticIndex) HasNativeMember(contract contributionv1.NativeContractRef, kind, member string) bool {
	return huaweiPortalNativeMember(contract, kind, member)
}

func gatewayPortalPVDescriptor() contributionv1.Manifest {
	return newGatewayPortalDescriptor("pv.primary", "portal.pv", "inverter", "pv.ac.frequency", "pv.service.inverter", "pv.capability.read.inverter", "unit.hertz", catalogv1.FivePacks[1])
}

func gatewayPortalStorageDescriptor() contributionv1.Manifest {
	return newGatewayPortalDescriptor("storage.primary", "portal.storage", "pack", "storage.state.soc", "storage.service.pack", "storage.capability.read.pack", "unit.percent", catalogv1.FivePacks[2])
}

func gatewayPortalEVSEDescriptor() contributionv1.Manifest {
	return newGatewayPortalDescriptor("evse.primary", "portal.evse", "evse", "evse.limit.configured_current", "evse.service.evse", "evse.capability.read.evse", "unit.ampere", catalogv1.FivePacks[3])
}

func newGatewayPortalDescriptor(driver, id, group, field, service, capability, unit string, pack contributionv1.PackRef) contributionv1.Manifest {
	ref := func(id string) contributionv1.DefinitionRef {
		return contributionv1.DefinitionRef{Pack: pack, ID: id, Version: pack.Version}
	}
	return contributionv1.Manifest{Contract: contributionv1.Contract, ManifestID: id, ManifestVersion: "1.0.0", Contributor: contributionv1.Contributor{DriverID: driver, NativeContract: contributionv1.NativeContractRef{Owner: "helianthus", Contract: "portal-catalog", Version: "1.0.0"}}, Requires: contributionv1.Requirements{SemanticKernel: contributionv1.SemanticKernel, Packs: []contributionv1.PackRef{pack}}, Groups: []contributionv1.Group{{ID: group, Label: contributionv1.Label{Key: id + ".group", Default: group}, ResourceContext: group}}, Fields: []contributionv1.Field{{ID: field, Group: group, Label: contributionv1.Label{Key: id + ".field", Default: field}, Ref: ref(field), ServiceRef: ref(service), CapabilityRef: ref(capability), UnitRef: ref(unit)}}, Views: []contributionv1.View{{ID: id + ".summary", Group: group, Label: contributionv1.Label{Key: id + ".summary", Default: "Summary"}, Renderer: "summary", Slot: "lens", FieldIDs: []string{field}, DiagnosticIDs: []string{}}}, Actions: []contributionv1.Action{}, Diagnostics: []contributionv1.Diagnostic{}}
}

type gatewayPortalCatalogSource struct {
	managed       *managedSidecarBindings
	pv            *modbusadapter.Adapter
	storage       *growattStoragePublication
	evse          *teslaHSCRetainedOwner
	contributions *gatewayPortalCatalogContributions
	// drivers resolves the driver of a binding's source. Without it a
	// resource is kept only while its asset holds one current source.
	drivers gatewayPortalSourceDrivers
	// dropped is the last logged set of dropped-resource diagnostics, so an
	// unchanged set is logged once and not on every capture.
	droppedMu sync.Mutex
	dropped   string
}

func newGatewayPortalCatalogSource(pv *modbusadapter.Adapter, storage *growattStoragePublication, evse *teslaHSCRetainedOwner, contributions *gatewayPortalCatalogContributions, managed ...*managedSidecarBindings) catalogv1.SourceCapture {
	source := &gatewayPortalCatalogSource{pv: pv, storage: storage, evse: evse, contributions: contributions}
	if len(managed) == 1 {
		source.managed = managed[0]
	}
	return source
}
func (s *gatewayPortalCatalogSource) PortalCatalogContributions() *gatewayPortalCatalogContributions {
	if s == nil {
		return nil
	}
	return s.contributions
}

func gatewayPortalStoragePublication(provider *growattBMSRS485ProductionProvider) *growattStoragePublication {
	if provider == nil {
		return nil
	}
	return provider.storage
}
func (s *gatewayPortalCatalogSource) Capture(at time.Time) ([]catalogv1.Resource, []catalogv1.Field, error) {
	resources, fields, diagnostics, err := s.CaptureDiagnostics(at)
	s.logDropped(diagnostics)
	return resources, fields, err
}

// logDropped reports every dropped resource with its reason whenever the set
// changes, so a dropped resource is visible outside the diagnostics method.
func (s *gatewayPortalCatalogSource) logDropped(diagnostics []gatewayPortalCatalogDiagnostic) {
	if s == nil {
		return
	}
	encoded, err := json.Marshal(diagnostics)
	if err != nil {
		return
	}
	s.droppedMu.Lock()
	changed := s.dropped != string(encoded)
	s.dropped = string(encoded)
	s.droppedMu.Unlock()
	if !changed || len(diagnostics) == 0 {
		return
	}
	for _, d := range diagnostics {
		log.Printf("portal catalog: resource %q of asset %q not kept: %s (driver %q, manifest %q)", d.ResourceID, d.AssetID, d.Reason, d.DriverID, d.ManifestID)
	}
}

// CaptureDiagnostics is Capture plus one diagnostic for every resource the
// capture could not keep, so a dropped resource always carries its reason.
func (s *gatewayPortalCatalogSource) CaptureDiagnostics(at time.Time) ([]catalogv1.Resource, []catalogv1.Field, []gatewayPortalCatalogDiagnostic, error) {
	out := portalCapture{resources: []catalogv1.Resource{}, fields: []catalogv1.Field{}, diagnostics: []gatewayPortalCatalogDiagnostic{}}
	if s == nil {
		return out.resources, out.fields, out.diagnostics, nil
	}
	if s.managed != nil {
		_ = s.managed.modbus(context.Background(), func(adapter *modbusadapter.Adapter) error {
			source := &gatewayPortalCatalogSource{pv: adapter, contributions: s.contributions, drivers: s.drivers}
			r, f, d, err := source.CaptureDiagnostics(at)
			out.merge(r, f, d)
			return err
		})
		_ = s.managed.growatt(context.Background(), func(provider *growattBMSRS485ProductionProvider) error {
			source := &gatewayPortalCatalogSource{storage: provider.storage, contributions: s.contributions, drivers: s.drivers}
			r, f, d, err := source.CaptureDiagnostics(at)
			out.merge(r, f, d)
			return err
		})
	}
	attribution := s.attribution()
	if s.pv != nil && s.contributions != nil && s.contributions.accepts("pv.primary", "portal.pv") {
		if v, ok := s.pv.SemanticPVCurrentSingleAt(at); ok {
			out.add(attribution, gatewayPortalPVDescriptor(), v.Snapshot, v.Evaluation, v.Selections, v.Projection)
		}
	}
	if s.storage != nil && s.contributions != nil && s.contributions.accepts("storage.primary", "portal.storage") {
		if v, ok := s.storage.CurrentAt(string(s.storage.assetID), at); ok {
			out.add(attribution, gatewayPortalStorageDescriptor(), v.Snapshot, v.Evaluation, []semreg.Selection{}, v.Projection)
		}
	}
	if s.evse != nil && s.contributions != nil && s.contributions.accepts("evse.primary", "portal.evse") {
		if v, ok := s.evse.SemanticEVSECurrentAt(at); ok {
			out.add(attribution, gatewayPortalEVSEDescriptor(), v.Snapshot, v.Evaluation, v.Selections, v.Projection)
		}
	}
	return out.resources, out.fields, out.diagnostics, nil
}

// attribution captures what a capture needs to name a binding's driver: the
// source-to-driver resolution and the admitted descriptors.
func (s *gatewayPortalCatalogSource) attribution() portalAttribution {
	if s == nil || s.drivers == nil {
		return portalAttribution{}
	}
	return portalAttribution{drivers: s.drivers, accepted: s.contributions.snapshot().Accepted}
}

// gatewayPortalSourceDrivers resolves the driver that holds a source. The
// publication hub satisfies it.
type gatewayPortalSourceDrivers interface {
	Source(semreg.SourceID) (semhub.SourceInfo, bool)
}

var _ gatewayPortalSourceDrivers = (*semhub.Hub)(nil)

// gatewayPortalCatalogDiagnostic names a resource the catalog source did not
// keep and why. DriverID and ManifestID name the descriptor that asked for it.
type gatewayPortalCatalogDiagnostic struct {
	Domain     string `json:"domain"`
	AssetID    string `json:"asset_id"`
	ResourceID string `json:"resource_id"`
	DriverID   string `json:"driver_id"`
	ManifestID string `json:"manifest_id"`
	Reason     string `json:"reason"`
}

// Reasons a resource is not kept.
const (
	// The descriptor names no single pack, field or resource context.
	portalDropDescriptorUnusable = "descriptor_unusable"
	// A service instance names a binding the snapshot does not hold.
	portalDropBindingUnresolved = "binding_unresolved"
	// No source holds a complete current service and capability path.
	portalDropCapabilityUnbound = "capability_unbound"
	// One source holds several instances, or several owner or contributor
	// paths qualify and none is the owner's.
	portalDropCapabilityAmbiguous = "capability_ambiguous"
	// The asset holds several sources and the driver of a path's source is
	// not known.
	portalDropDriverUnresolved = "contributor_driver_unresolved"
	// The contributor that carries the capability has no admitted descriptor
	// for the resource.
	portalDropContributorUnadmitted = "contributor_unadmitted"
	// The snapshot's revisions or evaluation cannot be recorded.
	portalDropSourceIncomplete = "source_record_incomplete"
)

type portalAttribution struct {
	drivers  gatewayPortalSourceDrivers
	accepted []contributionv1.AcceptedDescriptor
}

type portalCapture struct {
	resources   []catalogv1.Resource
	fields      []catalogv1.Field
	diagnostics []gatewayPortalCatalogDiagnostic
}

func (c *portalCapture) merge(resources []catalogv1.Resource, fields []catalogv1.Field, diagnostics []gatewayPortalCatalogDiagnostic) {
	c.resources = append(c.resources, resources...)
	c.fields = append(c.fields, fields...)
	c.diagnostics = append(c.diagnostics, diagnostics...)
}

func (c *portalCapture) add(attribution portalAttribution, descriptor contributionv1.Manifest, snapshot semreg.Snapshot, evaluation any, selections any, projection any) {
	resource, reason := portalSnapshotResource(attribution, descriptor, snapshot, evaluation, selections, projection)
	if reason != "" {
		diagnostic := gatewayPortalCatalogDiagnostic{AssetID: string(snapshot.AssetID), ResourceID: portalResourceContext(descriptor), DriverID: descriptor.Contributor.DriverID, ManifestID: descriptor.ManifestID, Reason: reason}
		if len(descriptor.Requires.Packs) == 1 {
			diagnostic.Domain = descriptor.Requires.Packs[0].ID
		}
		c.diagnostics = append(c.diagnostics, diagnostic)
		return
	}
	c.resources = append(c.resources, resource)
}

// portalSnapshotCapture captures one asset snapshot for one descriptor.
func portalSnapshotCapture(attribution portalAttribution, descriptor contributionv1.Manifest, snapshot semreg.Snapshot, evaluation any, selections any, projection any) ([]catalogv1.Resource, []catalogv1.Field, []gatewayPortalCatalogDiagnostic) {
	out := portalCapture{resources: []catalogv1.Resource{}, fields: []catalogv1.Field{}, diagnostics: []gatewayPortalCatalogDiagnostic{}}
	out.add(attribution, descriptor, snapshot, evaluation, selections, projection)
	return out.resources, out.fields, out.diagnostics
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
func appendPortalSnapshot(resources []catalogv1.Resource, fields []catalogv1.Field, descriptor contributionv1.Manifest, snapshot semreg.Snapshot, evaluation any, selections any, projection any) ([]catalogv1.Resource, []catalogv1.Field) {
	r, f, _ := portalSnapshotCapture(portalAttribution{}, descriptor, snapshot, evaluation, selections, projection)
	return append(resources, r...), append(fields, f...)
}

func portalResourceContext(descriptor contributionv1.Manifest) string {
	if len(descriptor.Fields) == 0 {
		return ""
	}
	for _, group := range descriptor.Groups {
		if group.ID == descriptor.Fields[0].Group {
			return group.ResourceContext
		}
	}
	return ""
}

func portalSnapshotResource(attribution portalAttribution, descriptor contributionv1.Manifest, snapshot semreg.Snapshot, evaluation any, selections any, projection any) (catalogv1.Resource, string) {
	resourceID := portalResourceContext(descriptor)
	if len(descriptor.Requires.Packs) != 1 || resourceID == "" {
		return catalogv1.Resource{}, portalDropDescriptorUnusable
	}
	paths, reason := portalSnapshotPaths(snapshot, descriptor.Fields[0].ServiceRef, descriptor.Fields[0].CapabilityRef)
	if reason != "" {
		return catalogv1.Resource{}, reason
	}
	path, contributor, reason := attribution.choose(descriptor, resourceID, snapshot, paths)
	if reason != "" {
		return catalogv1.Resource{}, reason
	}
	snap, _ := rawPortal(snapshot)
	eval, evalText := rawPortal(evaluation)
	sels, _ := rawPortal(selections)
	proj, _ := rawPortal(projection)
	revisions, revisionText := rawPortal(snapshot.Revisions)
	if len(revisions) == 0 || len(evalText) == 0 {
		return catalogv1.Resource{}, portalDropSourceIncomplete
	}
	source := catalogv1.Source{AssetID: string(snapshot.AssetID), SnapshotID: string(snapshot.SnapshotID), Revision: revisionText, EvaluationDigest: portalDigest(evaluation), BindingID: string(path.capability.BindingID), SourceEpoch: string(path.capability.SourceEpochID), DriverGeneration: path.generation, Snapshot: snap, Evaluation: eval, Selections: sels, Projection: proj}
	// A source snapshot is the complete fact/projection envelope.  Do not make
	// a second, lossy field table with null values or reconstructed quality.
	return catalogv1.Resource{ID: resourceID, Domain: descriptor.Requires.Packs[0].ID, ServiceID: string(path.service.Definition.ID), CapabilityID: string(path.capability.Definition.ID), State: "CURRENT", Source: source, ContributionDriverID: contributor.driver, ContributionManifestID: contributor.manifest, ContributionManifestVersion: contributor.version}, ""
}

// portalPath is one source's complete current service and capability path.
type portalPath struct {
	source     semreg.SourceID
	service    semreg.ServiceInstance
	capability semreg.CapabilityInstance
	generation uint64
}

type portalContributor struct{ driver, manifest, version string }

// portalSnapshotPaths groups the service instances of the descriptor's
// definition by the source of their binding and returns each source's path.
// Within one source the rules are those of a single-source asset: exactly one
// service instance, exactly one capability instance on it, and a current
// binding and source epoch that agree with both.
func portalSnapshotPaths(snapshot semreg.Snapshot, serviceRef, capabilityRef contributionv1.DefinitionRef) ([]portalPath, string) {
	wantService, wantCapability := semanticRef(serviceRef), semanticRef(capabilityRef)
	bindingSource := make(map[semreg.NativeBindingID]semreg.SourceID, len(snapshot.Bindings))
	for _, binding := range snapshot.Bindings {
		if binding.AssetID != snapshot.AssetID {
			continue
		}
		if known, ok := bindingSource[binding.BindingID]; ok && known != binding.SourceID {
			return nil, portalDropBindingUnresolved
		}
		bindingSource[binding.BindingID] = binding.SourceID
	}
	services := make(map[semreg.SourceID][]semreg.ServiceInstance)
	for _, candidate := range snapshot.Services {
		if candidate.AssetID != snapshot.AssetID || candidate.Definition != wantService {
			continue
		}
		source, ok := bindingSource[candidate.BindingID]
		if !ok || source == "" {
			return nil, portalDropBindingUnresolved
		}
		services[source] = append(services[source], candidate)
	}
	sources := make([]semreg.SourceID, 0, len(services))
	for source := range services {
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
	paths := make([]portalPath, 0, len(sources))
	for _, source := range sources {
		if len(services[source]) != 1 {
			return nil, portalDropCapabilityAmbiguous
		}
		path, reason := portalSourcePath(snapshot, source, services[source][0], wantCapability)
		if reason == portalDropCapabilityAmbiguous {
			return nil, reason
		}
		if reason == "" {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return nil, portalDropCapabilityUnbound
	}
	return paths, ""
}

func portalSourcePath(snapshot semreg.Snapshot, source semreg.SourceID, service semreg.ServiceInstance, wantCapability semreg.DefinitionRef) (portalPath, string) {
	var capability semreg.CapabilityInstance
	capabilityMatches := 0
	for _, candidate := range snapshot.Capabilities {
		if candidate.AssetID == snapshot.AssetID && candidate.Definition == wantCapability && candidate.ServiceInstance == service.InstanceID && candidate.BindingID == service.BindingID && candidate.SourceEpochID == service.SourceEpochID && candidate.DriverGeneration == service.DriverGeneration {
			capability, capabilityMatches = candidate, capabilityMatches+1
		}
	}
	if capabilityMatches > 1 {
		return portalPath{}, portalDropCapabilityAmbiguous
	}
	if capabilityMatches != 1 {
		return portalPath{}, portalDropCapabilityUnbound
	}
	generation, err := strconv.ParseUint(string(capability.DriverGeneration), 10, 64)
	if err != nil || generation == 0 {
		return portalPath{}, portalDropCapabilityUnbound
	}
	if service.BindingID == "" || service.SourceEpochID == "" || capability.BindingID == "" || capability.SourceEpochID == "" || service.BindingID != capability.BindingID || service.SourceEpochID != capability.SourceEpochID || service.DriverGeneration != capability.DriverGeneration {
		return portalPath{}, portalDropCapabilityUnbound
	}
	bindingMatches := 0
	for _, candidate := range snapshot.Bindings {
		if candidate.BindingID == capability.BindingID && candidate.AssetID == snapshot.AssetID && candidate.SourceID == source && candidate.SourceEpochID == capability.SourceEpochID && candidate.DriverGeneration == capability.DriverGeneration && candidate.State == semreg.BindingCurrent {
			bindingMatches++
		}
	}
	if bindingMatches != 1 {
		return portalPath{}, portalDropCapabilityUnbound
	}
	sourceMatches := 0
	for _, candidate := range snapshot.Sources {
		if candidate.SourceID == source && candidate.SourceEpochID == capability.SourceEpochID && candidate.State == semreg.SourceCurrent {
			sourceMatches++
		}
	}
	if sourceMatches != 1 {
		return portalPath{}, portalDropCapabilityUnbound
	}
	return portalPath{source: source, service: service, capability: capability, generation: generation}, ""
}

// choose keeps the owner's path, or the path of the one contributor whose
// binding carries the capability, and names the driver of that binding. The
// owner is the source whose driver published the descriptor.
func (a portalAttribution) choose(descriptor contributionv1.Manifest, resourceID string, snapshot semreg.Snapshot, paths []portalPath) (portalPath, portalContributor, string) {
	owner := portalContributor{driver: descriptor.Contributor.DriverID, manifest: descriptor.ManifestID, version: descriptor.ManifestVersion}
	if a.drivers == nil {
		// Without driver resolution only a single-source asset can be
		// attributed: its one source is the descriptor's driver.
		if portalCurrentSources(snapshot) > 1 {
			return portalPath{}, portalContributor{}, portalDropDriverUnresolved
		}
		if len(paths) != 1 {
			return portalPath{}, portalContributor{}, portalDropCapabilityAmbiguous
		}
		return paths[0], owner, ""
	}
	drivers := make([]string, len(paths))
	owners := make([]int, 0, 1)
	for index, path := range paths {
		info, ok := a.drivers.Source(path.source)
		if !ok || info.Driver == "" {
			return portalPath{}, portalContributor{}, portalDropDriverUnresolved
		}
		drivers[index] = info.Driver
		if info.Driver == owner.driver {
			owners = append(owners, index)
		}
	}
	if len(owners) == 1 {
		return paths[owners[0]], owner, ""
	}
	if len(owners) > 1 || len(paths) != 1 {
		return portalPath{}, portalContributor{}, portalDropCapabilityAmbiguous
	}
	contributor, ok := a.descriptorOf(drivers[0], descriptor, resourceID)
	if !ok {
		return portalPath{}, portalContributor{}, portalDropContributorUnadmitted
	}
	return paths[0], contributor, ""
}

// descriptorOf returns the one admitted descriptor of driver that declares the
// same pack, resource context, service and capability as descriptor.
func (a portalAttribution) descriptorOf(driver string, descriptor contributionv1.Manifest, resourceID string) (portalContributor, bool) {
	var found portalContributor
	matches := 0
	for _, accepted := range a.accepted {
		if accepted.Key.DriverID != driver || len(accepted.Manifest.Requires.Packs) != 1 || accepted.Manifest.Requires.Packs[0] != descriptor.Requires.Packs[0] {
			continue
		}
		groups := make(map[string]bool, len(accepted.Manifest.Groups))
		for _, group := range accepted.Manifest.Groups {
			if group.ResourceContext == resourceID {
				groups[group.ID] = true
			}
		}
		for _, field := range accepted.Manifest.Fields {
			if groups[field.Group] && field.ServiceRef == descriptor.Fields[0].ServiceRef && field.CapabilityRef == descriptor.Fields[0].CapabilityRef {
				found, matches = portalContributor{driver: accepted.Key.DriverID, manifest: accepted.Key.ManifestID, version: accepted.Key.ManifestVersion}, matches+1
				break
			}
		}
	}
	return found, matches == 1
}

// portalCurrentSources counts the distinct sources with a current epoch.
func portalCurrentSources(snapshot semreg.Snapshot) int {
	sources := make(map[semreg.SourceID]struct{}, len(snapshot.Sources))
	for _, source := range snapshot.Sources {
		if source.State == semreg.SourceCurrent {
			sources[source.SourceID] = struct{}{}
		}
	}
	return len(sources)
}
