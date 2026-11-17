package main

import (
	"context"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
)

// The infrastructure graph fixture is synthetic. One site publisher, declared
// with graph enforcement on, observes a site, the site's grid connection and
// one meter in its own asset; the configuration provider is its declared
// contributor. No production publisher of the infrastructure pack exists, so
// this fixture is the pack's only graph-enforced publisher. Every identity is
// synthetic and names no device family, model or unit.
const (
	igtAsset      semreg.AssetID    = "asset:igt-site"
	igtSource     semreg.SourceID   = "source:igt-site"
	igtDriver                       = "driver-igt-site"
	igtSite       semreg.ResourceID = "resource:igt:site-1"
	igtConnection semreg.ResourceID = "resource:igt:connection-1"
	igtMeter      semreg.ResourceID = "resource:igt:meter-1"

	igtMeterService    semreg.ServiceInstanceID    = "service:igt:meter-read"
	igtMeterCapability semreg.CapabilityInstanceID = "capability:igt:meter-read"
	igtConnectionLimit semreg.CapabilityInstanceID = "capability:igt:connection-export-limit"
)

// igtDocument is the operator-record section of the site: the site contains
// its grid connection, and the meter's role is the grid connection's
// measures relation. Both are configuration published into the asset that
// homes their starting ends.
const igtDocument = `{
  "format": "helianthus.portal-config.operator-records/v1",
  "revision": "0",
  "records": [
    {"form": "relation", "home": "asset:igt-site",
     "definition": {"pack": {"id": "helianthus.pack.infrastructure", "version": "1.0.0"}, "id": "infrastructure.relation.site_grid_connection", "version": "1.0.0"},
     "from": "resource:igt:site-1", "to": "resource:igt:connection-1", "basis_class": "configuration"},
    {"form": "relation", "home": "asset:igt-site",
     "definition": {"pack": {"id": "helianthus.pack.infrastructure", "version": "1.0.0"}, "id": "infrastructure.relation.grid_connection_meter", "version": "1.0.0"},
     "from": "resource:igt:connection-1", "to": "resource:igt:meter-1", "basis_class": "configuration"}
  ]
}`

var igtPack = infrastructure.New().Pack()

func igtDefinition(id semreg.DefinitionID) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: igtPack, ID: id, Version: igtPack.Version}
}

// The two configured relations of the document.
var (
	igtContains = igtDefinition("infrastructure.relation.site_grid_connection")
	igtMeasures = igtDefinition("infrastructure.relation.grid_connection_meter")
)

// igtMeterBinding is the publisher's second binding: it observes the meter,
// while the path's own binding observes the grid connection.
func igtMeterBinding(t *testing.T, p *oraPath) semreg.NativeBindingID {
	t.Helper()
	binding, err := semhub.NativeBindingID("igt_meter", p.asset, p.source, "igttest.profile.v1", p.handle.Epoch(), p.handle.Generation())
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

// igtSitePath is the site publisher. Its first batch holds the site, the
// grid connection and the meter, each under its own native slot, a second
// binding with its identity link naming the meter, and a qualified read
// capability of the meter.
func igtSitePath() *oraPath {
	return &oraPath{asset: igtAsset, source: igtSource, driver: igtDriver, kind: "resource.grid_connection", class: semreg.AnchorFunction, resource: igtConnection, slot: "connection-1",
		first: func(t *testing.T, p *oraPath, batch *semreg.PublicationBatch) {
			meter := igtMeter
			binding := igtMeterBinding(t, p)
			evidence := oraEvidence("oratest.observation", string(igtMeter))
			batch.ResourceUpserts = []semreg.Resource{
				batch.ResourceUpserts[0],
				p.nativeResource(t, igtMeter, "resource.meter", semreg.AnchorEquipment, "", "meter-1"),
				p.nativeResource(t, igtSite, "resource.site", semreg.AnchorFunction, "", "site-1"),
			}
			batch.BindingUpserts = append(batch.BindingUpserts, semreg.NativeBinding{BindingID: binding, AssetID: p.asset, SourceID: p.source, SourceEpochID: p.handle.Epoch(), DriverGeneration: p.generation(),
				NativeResource: oraEvidence("oratest.native", string(igtMeter)), State: semreg.BindingCurrent, Revision: "1"})
			batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, semreg.IdentityLink{AssetID: p.asset, BindingID: binding, Resource: &meter, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Revision: "1"})
			batch.ServiceUpserts = []semreg.ServiceInstance{{InstanceID: igtMeterService, AssetID: p.asset, Definition: igtDefinition("infrastructure.service.meter"), BindingID: binding, SourceEpochID: p.handle.Epoch(), DriverGeneration: p.generation(),
				Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Resource: &meter, Revision: "1"}}
			batch.CapabilityUpserts = []semreg.CapabilityInstance{{InstanceID: igtMeterCapability, AssetID: p.asset, ServiceInstance: igtMeterService, Definition: igtDefinition("infrastructure.capability.read.meter"), BindingID: binding,
				SourceEpochID: p.handle.Epoch(), DriverGeneration: p.generation(), Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable,
				Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{oraEvidence("oratest.activation", "meter-read")}, Revision: "1"}}
			sort.Slice(batch.BindingUpserts, func(i, j int) bool { return batch.BindingUpserts[i].BindingID < batch.BindingUpserts[j].BindingID })
			sort.Slice(batch.IdentityLinkUpserts, func(i, j int) bool {
				return batch.IdentityLinkUpserts[i].BindingID < batch.IdentityLinkUpserts[j].BindingID
			})
		}}
}

// igtQuantity is a non-negative quantity in unit.
func igtQuantity(coefficient string, exponent int32, unit semreg.DefinitionID) semreg.Value {
	return semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}, Unit: unit}}
}

// igtFact is one observed, promoted candidate of an infrastructure fact whose
// one dimension names subject, published through binding.
func igtFact(t *testing.T, f *oraFixture, p *oraPath, binding semreg.NativeBindingID, id, dimension semreg.DefinitionID, subject semreg.ResourceID, value semreg.Value) semreg.FactCandidate {
	t.Helper()
	text := string(subject)
	epoch, source, generation := p.handle.Epoch(), p.source, p.generation()
	wall, mono := f.now()
	evidence := oraEvidence("oratest.observation", string(id)+"/"+text)
	candidate := semreg.CandidateID("candidate:igt:" + oraHash("candidate", string(binding), string(id), text)[:32])
	return semreg.FactCandidate{CandidateID: candidate,
		Key:             semreg.FactKey{PackID: igtPack.ID, PackVersion: igtPack.Version, FactID: id, Dimensions: []semreg.Dimension{{ID: dimension, Value: semreg.Value{Kind: semreg.ValueText, Text: &text}}}},
		Value:           &value,
		Quality:         semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times:           semreg.Times{ReceivedAt: wall, ReceiptMonotonic: mono, EvaluatedAt: wall, EvaluateMonotonic: mono},
		FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:igt", Version: "1.0.0", FreshForNS: "600000000000", RetainForNS: "3600000000000", MaxWallUncertaintyNS: "0"},
		BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
		Origin:   semreg.OriginRef{OriginID: semreg.OriginID("origin:igt:" + oraHash("origin", string(candidate))[:24]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
		Evidence: []semreg.EvidenceRef{evidence}, Revision: "1"}
}

// igtGateFacts are the facts the gate places: direction-separated power on
// the grid connection, through the connection's binding, and the energy
// counters on the meter, through the meter's binding.
func igtGateFacts(t *testing.T, f *oraFixture, p *oraPath) []semreg.FactCandidate {
	t.Helper()
	connection, meter := p.binding(t), igtMeterBinding(t, p)
	return []semreg.FactCandidate{
		igtFact(t, f, p, connection, "infrastructure.power.import_active", "infrastructure.dimension.grid_connection", igtConnection, igtQuantity("18", 2, "unit.watt")),
		igtFact(t, f, p, connection, "infrastructure.power.export_active", "infrastructure.dimension.grid_connection", igtConnection, igtQuantity("0", 0, "unit.watt")),
		igtFact(t, f, p, meter, "infrastructure.energy.import", "infrastructure.dimension.meter", igtMeter, igtQuantity("12345", -1, "unit.kilowatt_hour")),
		igtFact(t, f, p, meter, "infrastructure.energy.export", "infrastructure.dimension.meter", igtMeter, igtQuantity("678", -1, "unit.kilowatt_hour")),
	}
}

// igtPublishFacts publishes facts in one batch of the site publisher.
func igtPublishFacts(t *testing.T, f *oraFixture, p *oraPath, facts ...semreg.FactCandidate) error {
	t.Helper()
	return f.publish(t, p, func(_ *semhub.Tx, batch *semreg.PublicationBatch) {
		batch.FactUpserts = append([]semreg.FactCandidate{}, facts...)
		sort.Slice(batch.FactUpserts, func(i, j int) bool { return batch.FactUpserts[i].CandidateID < batch.FactUpserts[j].CandidateID })
	})
}

// igtFactKeys returns the snapshot's fact keys by fact identifier.
func igtFactKeys(snapshot semreg.Snapshot) map[semreg.DefinitionID]semreg.FactKey {
	out := map[semreg.DefinitionID]semreg.FactKey{}
	for _, envelope := range snapshot.Facts {
		out[envelope.Key.FactID] = envelope.Key
	}
	return out
}

// newIGTFixture declares the site publisher's asset with graph enforcement on
// and publishes its first batch and the gate's facts.
func newIGTFixture(t *testing.T) (*oraFixture, *oraPath) {
	t.Helper()
	site := igtSitePath()
	f := newOraFixture(t, infrastructure.New, []*oraPath{site})
	state, err := f.hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	if state.CatalogDigest != graph.PinnedDigest || len(state.Assets) != 1 || state.Assets[0].Asset != igtAsset || state.Assets[0].Enforcement != semhub.GraphEnforced {
		t.Fatalf("graph state=%+v, want the site asset enforced under the pinned element catalog", state)
	}
	if err := igtPublishFacts(t, f, site, igtGateFacts(t, f, site)...); err != nil {
		t.Fatalf("publish the gate's facts: %v", err)
	}
	return f, site
}

// The infrastructure pack runs graph-enforced on synthetic input: the site,
// the grid connection and the meter are current resources of the enforced
// asset; the configuration provider publishes the meter role as a qualified
// measures instance, and the site's contains instance, into that asset; both
// are coherent edges of a complete cut; power sits on the grid connection and
// energy on the meter, and the role leaves every fact key as it was.
func TestInfrastructureGraphPublishesTheMeterRoleAsAConfiguredMeasuresInstance(t *testing.T) {
	f, _ := newIGTFixture(t)
	before := f.inspect(t, igtAsset).Snapshot
	for id, kind := range map[semreg.ResourceID]semreg.DefinitionID{igtSite: "resource.site", igtConnection: "resource.grid_connection", igtMeter: "resource.meter"} {
		resource, ok := oprResourceIn(before, id)
		if !ok || resource.State != semreg.ResourceCurrent || resource.Kind != kind {
			t.Fatalf("resource %s=%+v present=%v, want a current %s", id, resource, ok, kind)
		}
	}
	keys := igtFactKeys(before)
	for id, subject := range map[semreg.DefinitionID]struct {
		dimension semreg.DefinitionID
		resource  semreg.ResourceID
	}{
		"infrastructure.power.import_active": {"infrastructure.dimension.grid_connection", igtConnection},
		"infrastructure.power.export_active": {"infrastructure.dimension.grid_connection", igtConnection},
		"infrastructure.energy.import":       {"infrastructure.dimension.meter", igtMeter},
		"infrastructure.energy.export":       {"infrastructure.dimension.meter", igtMeter},
	} {
		key, ok := keys[id]
		if !ok || len(key.Dimensions) != 1 || key.Dimensions[0].ID != subject.dimension || key.Dimensions[0].Value.Text == nil || *key.Dimensions[0].Value.Text != string(subject.resource) {
			t.Fatalf("fact %s key=%+v present=%v, want its subject %s under %s", id, key, ok, subject.resource, subject.dimension)
		}
	}
	if len(keys) != 4 {
		t.Fatalf("facts=%+v, want the gate's four", keys)
	}

	saved := f.replace(t, oraDecode(t, igtDocument), "published:", "published:")
	after := f.inspect(t, igtAsset).Snapshot
	audit := portalOperatorAudit(igtAsset, saved.Records)
	held, ok := oprRelationIn(after, operatorRelation{Definition: igtMeasures, From: igtConnection, To: igtMeter})
	if !ok || held.State != semreg.LinkQualified || held.BasisClass != semreg.RelationConfiguration ||
		!reflect.DeepEqual(held.Basis, []semreg.EvidenceRef{audit.evidence()}) {
		t.Fatalf("meter role=%+v present=%v, want a qualified configuration instance on the grid connection's home asset", held, ok)
	}
	oprRequireOperatorOrigin(t, held.Origin, audit)
	contains, ok := oprRelationIn(after, operatorRelation{Definition: igtContains, From: igtSite, To: igtConnection})
	if !ok || contains.State != semreg.LinkQualified || contains.BasisClass != semreg.RelationConfiguration || !reflect.DeepEqual(contains.Basis, []semreg.EvidenceRef{audit.evidence()}) {
		t.Fatalf("site relation=%+v present=%v, want a qualified configuration instance", contains, ok)
	}
	oprRequireOperatorOrigin(t, contains.Origin, audit)
	if !reflect.DeepEqual(after.Facts, before.Facts) {
		t.Fatalf("the meter role changed the facts:\nbefore %+v\nafter  %+v", before.Facts, after.Facts)
	}
	if !reflect.DeepEqual(igtFactKeys(after), keys) {
		t.Fatalf("the meter role changed a fact key: %+v, want %+v", igtFactKeys(after), keys)
	}

	cut := f.cut(t)
	oraRequireCoherent(t, cut, igtAsset, igtMeasures, igtConnection, igtMeter)
	oraRequireCoherent(t, cut, igtAsset, igtContains, igtSite, igtConnection)
	if relations := cut.Relations(); len(relations) != 2 {
		t.Fatalf("cut relations=%+v, want the two configured instances only", relations)
	}
	if findings := cut.Findings(); len(findings) != 0 {
		t.Fatalf("cut findings=%+v, want none", findings)
	}
}

// The role never moves a fact: with the meter role configured, power keyed to
// the meter and energy keyed to the grid connection are refused, each from
// the binding that observes the resource it names, and the snapshot stays
// byte-identical. A field under another field's dimension is invalid_value;
// a field's own dimension naming a resource of another kind is identity_drift.
func TestInfrastructureGraphRoleNeverChangesAFactDefinition(t *testing.T) {
	f, site := newIGTFixture(t)
	f.replace(t, oraDecode(t, igtDocument), "published:", "published:")
	before := f.inspect(t, igtAsset).Canonical
	for name, negative := range map[string]struct {
		fact semreg.FactCandidate
		want semreg.ErrorID
	}{
		"power under the meter dimension":             {igtFact(t, f, site, igtMeterBinding(t, site), "infrastructure.power.import_active", "infrastructure.dimension.meter", igtMeter, igtQuantity("18", 2, "unit.watt")), semreg.InvalidValue},
		"power whose subject is the meter":            {igtFact(t, f, site, igtMeterBinding(t, site), "infrastructure.power.export_active", "infrastructure.dimension.grid_connection", igtMeter, igtQuantity("1", 2, "unit.watt")), semreg.IdentityDrift},
		"energy under the grid connection dimension":  {igtFact(t, f, site, site.binding(t), "infrastructure.energy.import", "infrastructure.dimension.grid_connection", igtConnection, igtQuantity("1", 0, "unit.kilowatt_hour")), semreg.InvalidValue},
		"energy whose subject is the grid connection": {igtFact(t, f, site, site.binding(t), "infrastructure.energy.export", "infrastructure.dimension.meter", igtConnection, igtQuantity("1", 0, "unit.kilowatt_hour")), semreg.IdentityDrift},
	} {
		err := igtPublishFacts(t, f, site, negative.fact)
		if got := semreg.ErrorIdentifier(err); err == nil || got != negative.want {
			t.Fatalf("%s: err=%v (%s), want refused with %s", name, err, got, negative.want)
		}
		if after := f.inspect(t, igtAsset).Canonical; string(after) != string(before) {
			t.Fatalf("%s: the refused batch changed the asset", name)
		}
	}
}

// igtExportLimit is an operation the infrastructure pack does not define: the
// pack indexes no operation, so the grid connection has no capability of its
// own that could carry one.
var igtExportLimit = igtDefinition("infrastructure.operation.set_export_limit")

// igtOwner is a fake runtime intent owner: it requires the grid connection's
// own capability and expects the grid connection's export power.
type igtOwner struct{ calls atomic.Int32 }

func (o *igtOwner) CompleteIntent(_ context.Context, draft operation.Intent, budget time.Time) (operation.Intent, operation.AuthorityResolver, error) {
	n := strconv.Itoa(int(o.calls.Add(1)))
	intent := draft
	intent.IntentID = semreg.IntentID("intent:igt-" + n)
	intent.IdempotencyKey = semreg.IdempotencyKey("idempotency:igt-" + n)
	intent.RequiredCapability = operation.CapabilityRequirement{Pack: igtPack, DefinitionID: "infrastructure.capability.read.grid_connection", Versions: semreg.VersionRange{Minimum: "1.0.0", MaximumExclusive: "2.0.0"}, InstanceID: draft.RequiredCapability.InstanceID}
	connection := string(igtConnection)
	fact := semreg.FactKey{PackID: igtPack.ID, PackVersion: igtPack.Version, FactID: "infrastructure.power.export_active", Dimensions: []semreg.Dimension{{ID: "infrastructure.dimension.grid_connection", Value: semreg.Value{Kind: semreg.ValueText, Text: &connection}}}}
	intent.ExpectedEffect = operation.ExpectedEffect{Rule: igtDefinition("infrastructure.effect.set_export_limit"), Fact: fact, Operator: semreg.PredicateLessEqual, Expected: igtQuantity("0", 0, "unit.watt")}
	intent.Authority = oraEvidence("oratest.authority", "operator")
	now := time.Now()
	intent.Causal = semreg.CausalContext{Origin: semreg.OriginRef{OriginID: "origin:igt-operator", Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{oraEvidence("oratest.operator", "operator")}},
		CorrelationID: semreg.CorrelationID("correlation:igt-" + n), MaxHops: 8, FirstSeenAt: semoptest.TimePoint(now), ExpiresAt: semoptest.TimePoint(now.Add(2 * time.Minute)), Path: []semreg.TargetID{}}
	intent.Deadline = semoptest.TimePoint(budget)
	return intent, operation.AuthorityResolverFunc(func(operation.Intent, operation.Route, semreg.EvaluationContext) error { return nil }), nil
}

// igtRequest asks for an intent on the grid connection through capability,
// against the site asset's current publication.
func igtRequest(t *testing.T, f *oraFixture, p *oraPath, binding semreg.NativeBindingID, capability semreg.CapabilityInstanceID) semanticoperations.Request {
	t.Helper()
	snapshot := f.inspect(t, p.asset).Snapshot
	revision := semreg.Uint64("1")
	for _, held := range snapshot.Capabilities {
		if held.InstanceID == capability {
			revision = held.Revision
		}
	}
	return semanticoperations.Request{
		AssetID: p.asset, BindingID: binding, CapabilityInstanceID: capability, Operation: igtExportLimit,
		Arguments:                          []semreg.TypedField{},
		ExpectedSemanticRevision:           snapshot.Revisions.Semantic,
		ExpectedCapabilityRevision:         snapshot.Revisions.Capabilities,
		ExpectedCapabilityInstanceRevision: revision,
		ExpectedSourceEpochID:              p.handle.Epoch(),
		ExpectedDriverGeneration:           p.generation(),
		Preconditions:                      []operation.Precondition{},
		Timeout:                            2 * time.Second,
	}
}

// The infrastructure pack defines no operation, so an intent on the grid
// connection has no owner and is refused without a send, whichever capability
// it names: one of the grid connection that the publisher does not publish, or
// the meter's qualified read capability. Both refusals are
// definition_owner_missing and come from the missing operation owner before
// capability admission. This test therefore covers the pack's lack of
// operations, not capability isolation across the role; it does not show that
// the role transfers or withholds a capability. The role, the snapshot and the
// executor are unchanged.
func TestInfrastructureGraphPackDefinesNoOperationSoAnIntentOnTheGridConnectionIsRefused(t *testing.T) {
	f, site := newIGTFixture(t)
	f.replace(t, oraDecode(t, igtDocument), "published:", "published:")
	oraRequireCoherent(t, f.cut(t), igtAsset, igtMeasures, igtConnection, igtMeter)
	snapshot := f.inspect(t, igtAsset)
	for _, held := range snapshot.Snapshot.Capabilities {
		if held.Definition.ID == "infrastructure.capability.read.grid_connection" {
			t.Fatalf("the publisher published a capability of the grid connection: %+v", held)
		}
	}

	kernel, err := semanticoperations.NewBuiltinKernel()
	if err != nil {
		t.Fatal(err)
	}
	executor := &oraExecutor{}
	lookup := &semoptest.Lookup{}
	lookup.Register(operation.Route{CapabilityInstance: igtConnectionLimit, ServiceInstance: "service:igt:connection", BindingID: site.binding(t), SourceID: site.source, SourceEpochID: site.handle.Epoch(), DriverGeneration: site.generation()}, igtExportLimit, executor)
	lookup.Register(operation.Route{CapabilityInstance: igtMeterCapability, ServiceInstance: igtMeterService, BindingID: igtMeterBinding(t, site), SourceID: site.source, SourceEpochID: site.handle.Epoch(), DriverGeneration: site.generation()}, igtExportLimit, executor)
	router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: &hubSemanticSnapshotProvider{hub: f.hub, now: time.Now}, Kernel: kernel, Intents: &igtOwner{}, Executors: lookup})

	for name, request := range map[string]semanticoperations.Request{
		"the grid connection's own capability": igtRequest(t, f, site, site.binding(t), igtConnectionLimit),
		"the meter's capability via the role":  igtRequest(t, f, site, igtMeterBinding(t, site), igtMeterCapability),
	} {
		result, err := router.Route(context.Background(), request)
		if err != nil {
			t.Fatalf("%s: route: %v", name, err)
		}
		if result.Status != semanticoperations.StatusFailed || result.Delivery == nil || *result.Delivery != operation.DeliveryNotSent || oraReason(result) != string(semreg.DefinitionOwnerMissing) {
			t.Fatalf("%s: result=%+v (reason %s), want refused without a send", name, result, oraReason(result))
		}
	}
	if routes := executor.received(); len(routes) != 0 {
		t.Fatalf("a refused intent reached the executor: %+v", routes)
	}
	if after := f.inspect(t, igtAsset); string(after.Canonical) != string(snapshot.Canonical) {
		t.Fatal("the refused intents changed the site asset")
	}
	if _, ok := oprRelationIn(f.inspect(t, igtAsset).Snapshot, operatorRelation{Definition: igtMeasures, From: igtConnection, To: igtMeter}); !ok {
		t.Fatal("the meter role is gone after the refused intents")
	}
}
