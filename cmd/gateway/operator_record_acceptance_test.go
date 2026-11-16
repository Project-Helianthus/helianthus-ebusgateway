package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
)

// The operator-record acceptance fixtures are synthetic. A site driver
// observes one grid connection natively; the operator declares, in the Portal
// configuration document, a meter seen through two configuration-only homes
// and the meter's measures relation to the driver's grid connection. A
// charging driver observes a charging station, its EVSE and the EVSE's
// connector; the operator declares a meter that measures the EVSE. Every
// identity is synthetic and names no device family, model or unit.
const (
	oraSiteAsset  semreg.AssetID    = "asset:ora-site"
	oraSiteSource semreg.SourceID   = "source:ora-site"
	oraSiteDriver                   = "driver-ora-site"
	oraConnection semreg.ResourceID = "resource:ora:connection-1"
	oraMeterHomeA semreg.AssetID    = "asset:ora-meter-a"
	oraMeterHomeB semreg.AssetID    = "asset:ora-meter-b"
	oraMeterA     semreg.ResourceID = "resource:ora:meter-a"
	oraMeterB     semreg.ResourceID = "resource:ora:meter-b"
	oraSubMeter   semreg.ResourceID = "resource:ora:meter-sub"
	oraMeterSlot                    = "meter-main"

	oraChargingAsset     semreg.AssetID              = "asset:ora-charging"
	oraChargingMeterHome semreg.AssetID              = "asset:ora-charging-meter"
	oraChargingMeter     semreg.ResourceID           = "resource:ora:charging-meter"
	oraStation           semreg.ResourceID           = "resource:ora:station-1"
	oraEVSE              semreg.ResourceID           = "resource:ora:evse-1"
	oraConnector         semreg.ResourceID           = "resource:ora:connector-1"
	oraReadService       semreg.ServiceInstanceID    = "service:ora:evse-read"
	oraReadCapability    semreg.CapabilityInstanceID = "capability:ora:evse-read"
	oraControlService    semreg.ServiceInstanceID    = "service:ora:connector-control"
	oraControlCapability semreg.CapabilityInstanceID = "capability:ora:connector-allocated-current"
)

// oraDocument is the operator-record section of the site's configuration
// document: one pairing of two views of the meter and the configured measures
// relation from the driver's grid connection to the first view.
const oraDocument = `{
  "format": "helianthus.portal-config.operator-records/v1",
  "revision": "0",
  "records": [
    {"form": "pairing", "kind": "resource.meter", "slot": "meter-main", "views": [
      {"home": "asset:ora-meter-a", "resource_id": "resource:ora:meter-a"},
      {"home": "asset:ora-meter-b", "resource_id": "resource:ora:meter-b"}
    ]},
    {"form": "relation", "home": "asset:ora-site",
     "definition": {"pack": {"id": "helianthus.pack.infrastructure", "version": "1.0.0"}, "id": "infrastructure.relation.grid_connection_meter", "version": "1.0.0"},
     "from": "resource:ora:connection-1", "to": "resource:ora:meter-a", "basis_class": "configuration"}
  ]
}`

// oraChargingDocument declares a meter that measures the charging driver's
// EVSE.
const oraChargingDocument = `{
  "format": "helianthus.portal-config.operator-records/v1",
  "revision": "0",
  "records": [
    {"form": "anchor", "home": "asset:ora-charging-meter", "resource_id": "resource:ora:charging-meter", "kind": "resource.meter", "slot": "meter-1"},
    {"form": "relation", "home": "asset:ora-charging",
     "definition": {"pack": {"id": "helianthus.pack.evse", "version": "1.0.0"}, "id": "evse.relation.evse_meter", "version": "1.0.0"},
     "from": "resource:ora:evse-1", "to": "resource:ora:charging-meter", "basis_class": "configuration"}
  ]
}`

// The measures-type relation definitions the documents name.
var (
	oraMeasures         = semreg.DefinitionRef{Pack: infrastructure.New().Pack(), ID: "infrastructure.relation.grid_connection_meter", Version: "1.0.0"}
	oraChargingMeasures = semreg.DefinitionRef{Pack: evse.New().Pack(), ID: "evse.relation.evse_meter", Version: "1.0.0"}
)

// oraHash is a domain-separated digest of parts, so the fixture holds no
// literal digest.
func oraHash(parts ...string) string {
	sum := sha256.Sum256([]byte("oratest\x00" + strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func oraEvidence(kind semreg.DefinitionID, label string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "helianthus.gateway.oratest", Kind: kind, Digest: semreg.Digest("sha256:" + oraHash(string(kind), label)), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

// oraPath is one synthetic driver: the asset it owns, with the configuration
// provider as a declared contributor, and the resource its binding observes
// under a native slot. first adds the driver's other records to its first
// batch; unlinked publishes the driver's identity link without naming the
// resource.
type oraPath struct {
	asset    semreg.AssetID
	source   semreg.SourceID
	driver   string
	kind     semreg.DefinitionID
	class    semreg.AnchorClass
	resource semreg.ResourceID
	parent   semreg.ResourceID
	slot     string
	unlinked bool
	first    func(*testing.T, *oraPath, *semreg.PublicationBatch)
	handle   *semhub.Handle
}

func oraSitePath() *oraPath {
	return &oraPath{asset: oraSiteAsset, source: oraSiteSource, driver: oraSiteDriver, kind: "resource.grid_connection", class: semreg.AnchorFunction, resource: oraConnection, slot: "connection-1"}
}

func (p *oraPath) generation() semreg.Uint64 {
	return semreg.Uint64(strconv.FormatUint(p.handle.Generation(), 10))
}

func (p *oraPath) binding(t *testing.T) semreg.NativeBindingID {
	t.Helper()
	binding, err := semhub.NativeBindingID("ora_driver", p.asset, p.source, "oratest.profile.v1", p.handle.Epoch(), p.handle.Generation())
	if err != nil {
		t.Fatal(err)
	}
	return binding
}

func (p *oraPath) native() semreg.EvidenceRef {
	return oraEvidence("oratest.native", string(p.resource))
}

// nativeResource is one resource the driver observes under a native slot,
// optionally within a parent.
func (p *oraPath) nativeResource(t *testing.T, id semreg.ResourceID, kind semreg.DefinitionID, class semreg.AnchorClass, parent semreg.ResourceID, slot string) semreg.Resource {
	t.Helper()
	evidence := oraEvidence("oratest.observation", string(id))
	source, epoch, binding := p.source, p.handle.Epoch(), p.binding(t)
	anchor := semreg.ResourceAnchor{Class: class, Basis: semreg.AnchorNativeSlot, Slot: &slot, Evidence: []semreg.EvidenceRef{evidence}}
	if parent != "" {
		anchor.Parent = &parent
	}
	return semreg.Resource{ResourceID: id, Kind: kind, Anchor: anchor, State: semreg.ResourceCurrent,
		Origin: semreg.OriginRef{OriginID: semreg.OriginID("origin:ora:" + oraHash("origin", string(id))[:24]), Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}}, Revision: "1"}
}

type oraFixture struct {
	hub     *semhub.Hub
	start   time.Time
	records *portal.ConfigOperatorRecords

	mu    sync.Mutex
	ticks int64
}

// newOraFixture declares every driver asset with the configuration provider
// as a contributor and the provider's configuration-only homes, all with the
// one pack, admits each driver and publishes its first batch, and binds a
// Portal configuration runtime to the hub: the runtime's own configuration
// provider publishes the accepted operator-record section.
func newOraFixture(t *testing.T, pack func() semreg.PackValidator, paths []*oraPath, homes ...semreg.AssetID) *oraFixture {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f := &oraFixture{hub: hub, start: time.Now()}
	declaring, err := newOperatorRecordPublisher(hub, operatorRecordOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if _, err := hub.Declare(semhub.AssetSpec{AssetID: path.asset, SourceID: path.source, Driver: path.driver, Validators: []semreg.PackValidator{pack()}, Graph: true,
			Contributors: []semhub.ContributorSpec{declaring.Contributor(pack())}}); err != nil {
			t.Fatalf("declare %s: %v", path.asset, err)
		}
	}
	for _, home := range homes {
		if err := declaring.DeclareHome(home, pack()); err != nil {
			t.Fatalf("declare home %s: %v", home, err)
		}
	}
	for _, path := range paths {
		if path.handle, err = hub.AdmitSource(semhub.Admission{Source: path.source, Driver: path.driver}); err != nil {
			t.Fatal(err)
		}
		if err := f.publish(t, path, nil); err != nil {
			t.Fatalf("first publication of %s: %v", path.asset, err)
		}
	}
	runtime := opr27Runtime(t)
	f.records = runtime.callbacks(ebusgateway.Config{}, &gatewayRuntimeControl{hub: hub}).OperatorRecords
	return f
}

// now is the fixture's wall and monotonic clock. The wall is real time, so a
// capture evaluated at the current instant never precedes an observation.
func (f *oraFixture) now() (semreg.TimePoint, semreg.MonotonicPoint) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ticks++
	return semoptest.TimePoint(time.Now()), semreg.MonotonicPoint{ClockEpochID: "clock:ora-test", Nanoseconds: semreg.Uint64(strconv.FormatInt(time.Since(f.start).Nanoseconds()+f.ticks, 10))}
}

// publish runs one driver transaction on the path's asset. The first batch of
// an incarnation carries the source, the binding, its identity link and the
// natively anchored resource, plus the records path.first adds; edit then
// changes the batch.
func (f *oraFixture) publish(t *testing.T, path *oraPath, edit func(*semhub.Tx, *semreg.PublicationBatch)) error {
	t.Helper()
	tx, err := f.hub.Begin(path.handle, path.asset)
	if err != nil {
		return err
	}
	defer tx.Abort()
	inc := tx.Incarnation()
	current, exists := tx.Current()
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	wall, mono := f.now()
	generation, sequence, epoch, source := path.generation(), tx.NextSequence(), inc.Epoch, inc.Source
	binding := path.binding(t)
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:ora:" + oraHash(string(inc.Asset), string(epoch), string(generation), string(sequence))), AssetID: inc.Asset,
		SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: sequence, ExpectedSemanticRevision: expected, ObservedAt: wall,
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{},
		FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{},
		CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !exists {
		resource := path.resource
		link := semreg.IdentityLink{AssetID: inc.Asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{oraEvidence("oratest.observation", string(path.resource))}, Revision: "1"}
		if !path.unlinked {
			link.Resource = &resource
		}
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "oratest_protocol", ProfileID: "oratest.profile.v1", ProfileVersion: "1", RegistryEvidence: oraEvidence("oratest.registry", string(source)), StartedAt: wall, State: semreg.SourceCurrent, Revision: "1"}}
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: path.native(), State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{link}
		batch.ResourceUpserts = []semreg.Resource{path.nativeResource(t, path.resource, path.kind, path.class, path.parent, path.slot)}
		if path.first != nil {
			path.first(t, path, &batch)
		}
	}
	if edit != nil {
		edit(tx, &batch)
	}
	batch.BatchDigest = ""
	if batch.BatchDigest, err = batch.ComputedDigest(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Apply(batch, mono); err != nil {
		return err
	}
	outcome, err := tx.Commit(nil)
	if err == nil && outcome != semhub.Accepted {
		err = errors.New("driver batch not accepted: " + string(outcome))
	}
	return err
}

// fence fences the driver's current generation.
func (f *oraFixture) fence(t *testing.T, path *oraPath) {
	t.Helper()
	wall, mono := f.now()
	if _, err := f.hub.Fence(semhub.FenceRequest{Source: path.source, Epoch: path.handle.Epoch(), Generation: path.handle.Generation(), Reason: "lifecycle.generation_replaced",
		Evidence: []semreg.EvidenceRef{oraEvidence("oratest.fence", string(path.source))}, ObservedAt: wall, Monotonic: mono}); err != nil {
		t.Fatalf("fence %s: %v", path.source, err)
	}
}

// stop retires the driver's source epoch, as a stopped driver does.
func (f *oraFixture) stop(t *testing.T, path *oraPath) {
	t.Helper()
	wall, mono := f.now()
	if _, err := f.hub.RetireEpoch(semhub.RetireRequest{Source: path.source, Epoch: path.handle.Epoch(), Reason: "lifecycle.source_stopped",
		Evidence: []semreg.EvidenceRef{oraEvidence("oratest.stop", string(path.source))}, ObservedAt: wall, Monotonic: mono}); err != nil {
		t.Fatalf("stop %s: %v", path.source, err)
	}
	if info, ok := f.hub.Source(path.source); !ok || info.Live {
		t.Fatalf("driver %s still live after it stopped: %+v", path.source, info)
	}
}

func (f *oraFixture) inspect(t *testing.T, id semreg.AssetID) semhub.InspectView {
	t.Helper()
	view, err := f.hub.Inspect(id)
	if err != nil || !view.Present {
		t.Fatalf("inspect %s: present=%v err=%v", id, view.Present, err)
	}
	return view
}

// replace submits one operator-record section through the Portal
// configuration callbacks and waits for the statuses of its records.
func (f *oraFixture) replace(t *testing.T, set portalconfig.OperatorRecordSet, want ...string) portalconfig.OperatorRecordSet {
	t.Helper()
	saved, err := f.records.Replace(context.Background(), set)
	if err != nil {
		t.Fatalf("replace the operator-record section: %v", err)
	}
	opr27Await(t, f.records, saved, want)
	return saved
}

// cut assembles one graph cut over every declared asset and requires it to
// include every one of them.
func (f *oraFixture) cut(t *testing.T) *graph.Cut {
	t.Helper()
	cut, err := f.hub.CaptureCut(context.Background())
	if err != nil {
		t.Fatalf("capture cut: %v", err)
	}
	if len(cut.NotIncluded) != 0 || cut.Cut.Completeness() != graph.CutComplete {
		t.Fatalf("cut not included=%+v completeness=%s, want every declared asset in a complete cut", cut.NotIncluded, cut.Cut.Completeness())
	}
	return cut.Cut
}

// oraRequireCoherent asserts that the cut holds the configured relation as a
// qualified, coherent edge of the asset that homes its starting end.
func oraRequireCoherent(t *testing.T, cut *graph.Cut, holder semreg.AssetID, definition semreg.DefinitionRef, from, to semreg.ResourceID) {
	t.Helper()
	held, ok := cut.Relation(semreg.RelationKey{Definition: definition, From: from, To: to})
	if !ok || held.AssetID != holder || held.State != semreg.LinkQualified || held.Coherence != graph.CoherenceCoherent {
		t.Fatalf("cut relation %s to %s=%+v present=%v, want a qualified coherent instance held by %s", from, to, held, ok, holder)
	}
}

// oraSameUnit returns the cut's same-unit findings.
func oraSameUnit(cut *graph.Cut) []graph.CutFinding {
	var out []graph.CutFinding
	for _, finding := range cut.Findings() {
		if finding.Kind == graph.CutSameUnit {
			out = append(out, finding)
		}
	}
	return out
}

// oraDecode decodes and validates a configuration document section as the
// Portal does before it stores it.
func oraDecode(t *testing.T, document string) portalconfig.OperatorRecordSet {
	t.Helper()
	set, err := portalconfig.DecodeOperatorRecords([]byte(document))
	if err != nil {
		t.Fatalf("decode the configuration document: %v", err)
	}
	if err := portalconfig.ValidateOperatorRecords(set); err != nil {
		t.Fatalf("validate the configuration document: %v", err)
	}
	return set
}

// oraSubMeterRecords are a second meter and its measures relation, added to
// the site's section.
func oraSubMeterRecords() []portalconfig.OperatorRecord {
	return []portalconfig.OperatorRecord{
		{Form: portalconfig.OperatorFormAnchor, Home: string(oraMeterHomeA), ResourceID: string(oraSubMeter), Kind: "resource.meter", Slot: "meter-sub"},
		{Form: portalconfig.OperatorFormRelation, Home: string(oraSiteAsset), Definition: &oraMeasures, From: string(oraConnection), To: string(oraSubMeter), BasisClass: string(semreg.RelationConfiguration)},
	}
}

// oraSiteRelation reports whether the site asset holds the configured
// measures relation to the meter view.
func oraSiteRelation(t *testing.T, f *oraFixture, to semreg.ResourceID) bool {
	t.Helper()
	_, ok := oprRelationIn(f.inspect(t, oraSiteAsset).Snapshot, operatorRelation{Definition: oraMeasures, From: oraConnection, To: to})
	return ok
}

// One declaration goes from the configuration document to the snapshot and
// to a graph cut: the pairing homes one operator-slot view of the meter in
// each configuration-only home and the cut flags the pair as one unit; the
// configured measures relation is published on the driver's asset by the
// configuration provider and is a coherent edge of the cut.
func TestOperatorRecordAcceptanceFromConfigurationDocumentToCut(t *testing.T) {
	f := newOraFixture(t, infrastructure.New, []*oraPath{oraSitePath()}, oraMeterHomeA, oraMeterHomeB)

	saved := f.replace(t, oraDecode(t, oraDocument), "published:", "published:")
	held, ok := oprRelationIn(f.inspect(t, oraSiteAsset).Snapshot, operatorRelation{Definition: oraMeasures, From: oraConnection, To: oraMeterA})
	if !ok || held.State != semreg.LinkQualified || held.BasisClass != semreg.RelationConfiguration {
		t.Fatalf("relation=%+v present=%v, want a qualified configuration instance on the driver's asset", held, ok)
	}
	audit := portalOperatorAudit(oraSiteAsset, []portalconfig.OperatorRecord{saved.Records[1]})
	if !reflect.DeepEqual(held.Basis, []semreg.EvidenceRef{audit.evidence()}) {
		t.Fatalf("relation basis=%+v, want the audit of the document's declaration", held.Basis)
	}
	oprRequireOperatorOrigin(t, held.Origin, audit)
	for home, id := range map[semreg.AssetID]semreg.ResourceID{oraMeterHomeA: oraMeterA, oraMeterHomeB: oraMeterB} {
		snapshot := f.inspect(t, home).Snapshot
		resource, ok := oprResourceIn(snapshot, id)
		if !ok || resource.State != semreg.ResourceCurrent || resource.Kind != "resource.meter" || resource.Anchor.Basis != semreg.AnchorOperatorSlot ||
			resource.Anchor.Class != semreg.AnchorEquipment || resource.Anchor.Slot == nil || *resource.Anchor.Slot != oraMeterSlot || resource.Anchor.Parent != nil {
			t.Fatalf("view %s in %s=%+v present=%v, want a current operator-slot meter", id, home, resource, ok)
		}
		oprRequireOperatorOrigin(t, resource.Origin, portalOperatorAudit(home, []portalconfig.OperatorRecord{saved.Records[0]}))
		if len(snapshot.Sources) != 1 || snapshot.Sources[0].SourceID != operatorRecordSource {
			t.Fatalf("home %s sources=%+v, want the configuration provider only", home, snapshot.Sources)
		}
	}

	cut := f.cut(t)
	oraRequireCoherent(t, cut, oraSiteAsset, oraMeasures, oraConnection, oraMeterA)
	want := []graph.CutFinding{{Kind: graph.CutSameUnit, Resources: []graph.CutResourceRef{{AssetID: oraMeterHomeA, ResourceID: oraMeterA}, {AssetID: oraMeterHomeB, ResourceID: oraMeterB}}}}
	if findings := cut.Findings(); !reflect.DeepEqual(findings, want) {
		t.Fatalf("cut findings=%+v, want only the declared pair as one unit", findings)
	}
	if relations := cut.Relations(); len(relations) != 1 {
		t.Fatalf("cut relations=%+v, want the configured relation only", relations)
	}
}

// A record added to the section while the driver's generation is fenced is
// published by the configuration provider; the driver's resource stays
// current and both relations are coherent edges of the cut.
func TestOperatorRecordAcceptancePublishedWhileTheDriverIsFenced(t *testing.T) {
	site := oraSitePath()
	f := newOraFixture(t, infrastructure.New, []*oraPath{site}, oraMeterHomeA, oraMeterHomeB)
	saved := f.replace(t, oraDecode(t, oraDocument), "published:", "published:")

	f.fence(t, site)
	if !oraSiteRelation(t, f, oraMeterA) {
		t.Fatal("the configured relation did not survive the driver's fence")
	}
	fenced := saved
	fenced.Records = append(append([]portalconfig.OperatorRecord{}, saved.Records...), oraSubMeterRecords()...)
	f.replace(t, fenced, "published:", "published:", "published:", "published:")
	snapshot := f.inspect(t, oraSiteAsset).Snapshot
	for _, to := range []semreg.ResourceID{oraMeterA, oraSubMeter} {
		if !oraSiteRelation(t, f, to) {
			t.Fatalf("relation to %s missing while the driver is fenced", to)
		}
	}
	driverFenced := false
	for _, cursor := range snapshot.Cursors {
		driverFenced = driverFenced || cursor.SourceID == oraSiteSource && cursor.Fenced
	}
	if !driverFenced {
		t.Fatalf("driver cursor not fenced: %+v", snapshot.Cursors)
	}
	if resource, ok := oprResourceIn(snapshot, oraConnection); !ok || resource.State != semreg.ResourceCurrent {
		t.Fatalf("driver resource=%+v present=%v, want current after the fence", resource, ok)
	}
	cut := f.cut(t)
	oraRequireCoherent(t, cut, oraSiteAsset, oraMeasures, oraConnection, oraMeterA)
	oraRequireCoherent(t, cut, oraSiteAsset, oraMeasures, oraConnection, oraSubMeter)
}

// A record removed from the section while the driver is stopped is withdrawn
// by the configuration provider: the configured records survive the stop,
// the removed relation leaves the snapshot and the cut, its meter is
// withdrawn, the relation that stays configured stays coherent and the
// driver stays stopped.
func TestOperatorRecordAcceptanceWithdrawnWhileTheDriverIsStopped(t *testing.T) {
	site := oraSitePath()
	f := newOraFixture(t, infrastructure.New, []*oraPath{site}, oraMeterHomeA, oraMeterHomeB)
	set := oraDecode(t, oraDocument)
	set.Records = append(set.Records, oraSubMeterRecords()...)
	saved := f.replace(t, set, "published:", "published:", "published:", "published:")

	f.stop(t, site)
	for _, to := range []semreg.ResourceID{oraMeterA, oraSubMeter} {
		if !oraSiteRelation(t, f, to) {
			t.Fatalf("relation to %s did not survive the driver's stop", to)
		}
	}
	withdrawn := saved
	withdrawn.Records = append([]portalconfig.OperatorRecord{}, saved.Records[:2]...)
	f.replace(t, withdrawn, "published:", "published:")
	if oraSiteRelation(t, f, oraSubMeter) {
		t.Fatal("the removed relation is still published while the driver is stopped")
	}
	if !oraSiteRelation(t, f, oraMeterA) {
		t.Fatal("the withdrawal removed the relation that stays configured")
	}
	if resource, _ := oprResourceIn(f.inspect(t, oraMeterHomeA).Snapshot, oraSubMeter); resource.State != semreg.ResourceWithdrawn {
		t.Fatalf("removed meter=%+v, want withdrawn", resource)
	}
	if info, ok := f.hub.Source(oraSiteSource); !ok || info.Live {
		t.Fatalf("the withdrawal revived the stopped driver: %+v", info)
	}
	cut := f.cut(t)
	oraRequireCoherent(t, cut, oraSiteAsset, oraMeasures, oraConnection, oraMeterA)
	if _, ok := cut.Relation(semreg.RelationKey{Definition: oraMeasures, From: oraConnection, To: oraSubMeter}); ok {
		t.Fatal("the cut still holds the withdrawn relation")
	}
	if got := oraSameUnit(cut); len(got) != 1 {
		t.Fatalf("same-unit findings after the withdrawal=%+v, want the declared pair only", got)
	}
}

// oraExecutor accepts every dispatch without terminal evidence and records
// the routes it received.
type oraExecutor struct {
	mu     sync.Mutex
	routes []operation.Route
}

func (e *oraExecutor) Execute(_ context.Context, dispatch semanticoperations.Dispatch) (semanticoperations.Completion, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.routes = append(e.routes, dispatch.Route)
	return semanticoperations.Completion{}, nil
}

func (e *oraExecutor) received() []operation.Route {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]operation.Route(nil), e.routes...)
}

var oraEVSEPack = evse.New().Pack()

func oraEVSEDefinition(id semreg.DefinitionID) semreg.DefinitionRef {
	return semreg.DefinitionRef{Pack: oraEVSEPack, ID: id, Version: oraEVSEPack.Version}
}

// oraAmperes is an allocated-current value.
func oraAmperes(coefficient string) semreg.Value {
	return semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient}, Unit: "unit.ampere"}}
}

// oraOwner is a fake runtime intent owner for the EVSE allocated-current
// operation: it completes only the members a request does not carry.
type oraOwner struct{ calls atomic.Int32 }

func (o *oraOwner) CompleteIntent(_ context.Context, draft operation.Intent, budget time.Time) (operation.Intent, operation.AuthorityResolver, error) {
	n := strconv.Itoa(int(o.calls.Add(1)))
	intent := draft
	intent.IntentID = semreg.IntentID("intent:ora-" + n)
	intent.IdempotencyKey = semreg.IdempotencyKey("idempotency:ora-" + n)
	intent.RequiredCapability = operation.CapabilityRequirement{Pack: oraEVSEPack, DefinitionID: "evse.capability.set_allocated_current", Versions: semreg.VersionRange{Minimum: "1.0.0", MaximumExclusive: "2.0.0"}, InstanceID: draft.RequiredCapability.InstanceID}
	expected := semreg.Value{}
	if len(draft.Arguments) == 1 {
		expected = draft.Arguments[0].Value
	}
	connector := "connector-1"
	fact := semreg.FactKey{PackID: oraEVSEPack.ID, PackVersion: oraEVSEPack.Version, FactID: "evse.limit.allocated_current", Dimensions: []semreg.Dimension{{ID: "evse.dimension.connector", Value: semreg.Value{Kind: semreg.ValueText, Text: &connector}}}}
	intent.ExpectedEffect = operation.ExpectedEffect{Rule: oraEVSEDefinition("evse.effect.set_allocated_current"), Fact: fact, Operator: semreg.PredicateEqual, Expected: expected}
	intent.Authority = oraEvidence("oratest.authority", "operator")
	now := time.Now()
	intent.Causal = semreg.CausalContext{Origin: semreg.OriginRef{OriginID: "origin:ora-operator", Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{oraEvidence("oratest.operator", "operator")}},
		CorrelationID: semreg.CorrelationID("correlation:ora-" + n), MaxHops: 8, FirstSeenAt: semoptest.TimePoint(now), ExpiresAt: semoptest.TimePoint(now.Add(2 * time.Minute)), Path: []semreg.TargetID{}}
	intent.Deadline = semoptest.TimePoint(budget)
	return intent, operation.AuthorityResolverFunc(func(operation.Intent, operation.Route, semreg.EvaluationContext) error { return nil }), nil
}

// oraChargingPath is the charging driver: its binding observes the EVSE,
// within the charging station, and the EVSE's connector; it publishes a
// qualified read capability of the EVSE and no control capability.
func oraChargingPath() *oraPath {
	return &oraPath{asset: oraChargingAsset, source: "source:ora-charging", driver: "driver-ora-charging", kind: "resource.evse", class: semreg.AnchorComponent, resource: oraEVSE, parent: oraStation, slot: "evse-1",
		first: func(t *testing.T, p *oraPath, batch *semreg.PublicationBatch) {
			resource := oraEVSE
			batch.ResourceUpserts = []semreg.Resource{
				p.nativeResource(t, oraConnector, "resource.connector", semreg.AnchorComponent, oraEVSE, "connector-1"),
				batch.ResourceUpserts[0],
				p.nativeResource(t, oraStation, "resource.charging_station", semreg.AnchorEquipment, "", "station-1"),
			}
			batch.ServiceUpserts = []semreg.ServiceInstance{{InstanceID: oraReadService, AssetID: p.asset, Definition: oraEVSEDefinition("evse.service.evse"), BindingID: p.binding(t), SourceEpochID: p.handle.Epoch(), DriverGeneration: p.generation(),
				Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Resource: &resource, Revision: "1"}}
			batch.CapabilityUpserts = []semreg.CapabilityInstance{{InstanceID: oraReadCapability, AssetID: p.asset, ServiceInstance: oraReadService, Definition: oraEVSEDefinition("evse.capability.read.evse"), BindingID: p.binding(t),
				SourceEpochID: p.handle.Epoch(), DriverGeneration: p.generation(), Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable,
				Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{oraEvidence("oratest.activation", "read")}, Revision: "1"}}
		}}
}

func (p *oraPath) controlRoute(t *testing.T) operation.Route {
	return operation.Route{CapabilityInstance: oraControlCapability, ServiceInstance: oraControlService, BindingID: p.binding(t), SourceID: p.source, SourceEpochID: p.handle.Epoch(), DriverGeneration: p.generation()}
}

// oraRequest asks for the allocated current through the driver's control
// capability against the EVSE asset's current publication.
func oraRequest(t *testing.T, f *oraFixture, path *oraPath) semanticoperations.Request {
	t.Helper()
	snapshot := f.inspect(t, path.asset).Snapshot
	revision := semreg.Uint64("1")
	for _, capability := range snapshot.Capabilities {
		if capability.InstanceID == oraControlCapability {
			revision = capability.Revision
		}
	}
	return semanticoperations.Request{
		AssetID: path.asset, BindingID: path.binding(t), CapabilityInstanceID: oraControlCapability, Operation: oraEVSEDefinition("evse.operation.set_allocated_current"),
		Arguments:                          []semreg.TypedField{{ID: "evse.limit.allocated_current", Value: oraAmperes("16")}},
		ExpectedSemanticRevision:           snapshot.Revisions.Semantic,
		ExpectedCapabilityRevision:         snapshot.Revisions.Capabilities,
		ExpectedCapabilityInstanceRevision: revision,
		ExpectedSourceEpochID:              path.handle.Epoch(),
		ExpectedDriverGeneration:           path.generation(),
		Preconditions:                      []operation.Precondition{},
		Timeout:                            2 * time.Second,
	}
}

func oraReason(result semanticoperations.Result) string {
	if result.Reason == nil {
		return "<none>"
	}
	return *result.Reason
}

// A configured measures relation grants no control. While the relation is a
// qualified, coherent edge naming the driver's EVSE, an intent on that
// measured EVSE is refused without a send as long as the driver publishes no
// qualified control capability of its own; the same intent is admitted and
// routed to the driver's own binding once the driver publishes one. The
// configured relation is the same in both publications.
func TestOperatorRecordAcceptanceConfiguredMeasuresGrantsNoControl(t *testing.T) {
	charging := oraChargingPath()
	f := newOraFixture(t, evse.New, []*oraPath{charging}, oraChargingMeterHome)
	f.replace(t, oraDecode(t, oraChargingDocument), "published:", "published:")
	relation := operatorRelation{Definition: oraChargingMeasures, From: oraEVSE, To: oraChargingMeter}
	if held, ok := oprRelationIn(f.inspect(t, oraChargingAsset).Snapshot, relation); !ok || held.State != semreg.LinkQualified {
		t.Fatalf("relation=%+v present=%v, want the configured measures instance qualified", held, ok)
	}
	if held, ok := f.cut(t).Relation(semreg.RelationKey{Definition: oraChargingMeasures, From: oraEVSE, To: oraChargingMeter}); !ok || held.Coherence != graph.CoherenceCoherent {
		t.Fatalf("cut relation=%+v present=%v, want coherent", held, ok)
	}

	kernel, err := semanticoperations.NewBuiltinKernel()
	if err != nil {
		t.Fatal(err)
	}
	executor := &oraExecutor{}
	lookup := &semoptest.Lookup{}
	lookup.Register(charging.controlRoute(t), oraEVSEDefinition("evse.operation.set_allocated_current"), executor)
	router := semanticoperations.NewRouter(semanticoperations.Config{Snapshots: &hubSemanticSnapshotProvider{hub: f.hub, now: time.Now}, Kernel: kernel, Intents: &oraOwner{}, Executors: lookup})

	result, err := router.Route(context.Background(), oraRequest(t, f, charging))
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if result.Status != semanticoperations.StatusFailed || oraReason(result) != string(semreg.AmbiguousRoute) {
		t.Fatalf("intent without the driver's control capability=%+v (reason %s), want refused for want of an eligible route", result, oraReason(result))
	}
	if routes := executor.received(); len(routes) != 0 {
		t.Fatalf("a refused intent reached the executor: %+v", routes)
	}

	connector := oraConnector
	if err := f.publish(t, charging, func(_ *semhub.Tx, batch *semreg.PublicationBatch) {
		batch.ServiceUpserts = []semreg.ServiceInstance{{InstanceID: oraControlService, AssetID: charging.asset, Definition: oraEVSEDefinition("evse.service.connector"), BindingID: charging.binding(t), SourceEpochID: charging.handle.Epoch(), DriverGeneration: charging.generation(),
			Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Resource: &connector, Revision: "1"}}
		batch.CapabilityUpserts = []semreg.CapabilityInstance{{InstanceID: oraControlCapability, AssetID: charging.asset, ServiceInstance: oraControlService, Definition: oraEVSEDefinition("evse.capability.set_allocated_current"), BindingID: charging.binding(t),
			SourceEpochID: charging.handle.Epoch(), DriverGeneration: charging.generation(), Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable,
			Constraints: []semreg.TypedField{{ID: "evse.limit.allocated_current", Value: oraAmperes("16")}}, ActivationEvidence: []semreg.EvidenceRef{oraEvidence("oratest.activation", "control")}, Revision: "1"}}
	}); err != nil {
		t.Fatalf("publish the driver's control capability: %v", err)
	}
	if held, ok := oprRelationIn(f.inspect(t, oraChargingAsset).Snapshot, relation); !ok || held.Revision != "1" {
		t.Fatalf("relation=%+v present=%v, want the configured instance unchanged", held, ok)
	}
	result, err = router.Route(context.Background(), oraRequest(t, f, charging))
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if result.Status != semanticoperations.StatusAccepted {
		t.Fatalf("intent with the driver's qualified control capability=%+v (reason %s), want accepted", result, oraReason(result))
	}
	if routes := executor.received(); len(routes) != 1 || routes[0] != charging.controlRoute(t) {
		t.Fatalf("executor routes=%+v, want the driver's own route once", routes)
	}
}

// A pairing that joins two native observations whose anchor keys differ is
// refused: it cannot override native evidence. Each view's home refuses the
// set with the kernel's stable error, both snapshots stay byte-identical,
// both resources keep their native slots, and the cut reports no same unit.
func TestOperatorRecordAcceptanceRefusesAJoinOfDifferentNativeAnchors(t *testing.T) {
	direct := &oraPath{asset: "asset:ora-direct", source: "source:ora-direct", driver: "driver-ora-direct", kind: "resource.meter", class: semreg.AnchorEquipment, resource: "resource:ora:direct-meter", slot: "meter-1"}
	relayed := &oraPath{asset: "asset:ora-relayed", source: "source:ora-relayed", driver: "driver-ora-relayed", kind: "resource.meter", class: semreg.AnchorEquipment, resource: "resource:ora:relayed-meter", slot: "port-3"}
	f := newOraFixture(t, infrastructure.New, []*oraPath{direct, relayed})
	before := map[semreg.AssetID][]byte{}
	for _, path := range []*oraPath{direct, relayed} {
		before[path.asset] = f.inspect(t, path.asset).Canonical
	}
	cursor := f.hub.Cursor()

	join := oraDecode(t, `{"format": "helianthus.portal-config.operator-records/v1", "revision": "0", "records": [
    {"form": "pairing", "kind": "resource.meter", "slot": "meter-main", "views": [
      {"home": "asset:ora-direct", "resource_id": "resource:ora:direct-meter"},
      {"home": "asset:ora-relayed", "resource_id": "resource:ora:relayed-meter"}
    ]}]}`)
	f.replace(t, join, "refused:"+string(semreg.IdentityDrift))

	for _, path := range []*oraPath{direct, relayed} {
		view := f.inspect(t, path.asset)
		if string(view.Canonical) != string(before[path.asset]) {
			t.Fatalf("the refused join changed %s", path.asset)
		}
		resource, ok := oprResourceIn(view.Snapshot, path.resource)
		if !ok || resource.Anchor.Basis != semreg.AnchorNativeSlot || resource.Anchor.Slot == nil || *resource.Anchor.Slot != path.slot || resource.Origin.Kind != semreg.OriginNativeObservation {
			t.Fatalf("resource %s=%+v present=%v, want its native slot unchanged", path.resource, resource, ok)
		}
	}
	events, err := f.hub.Since(cursor)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Source == operatorRecordSource && (event.Asset == direct.asset || event.Asset == relayed.asset) {
			t.Fatalf("the refused join emitted an event on %s: %+v", event.Asset, event)
		}
	}
	if got := oraSameUnit(f.cut(t)); len(got) != 0 {
		t.Fatalf("same-unit findings=%+v, want none for two different native anchors", got)
	}
}

// The one remaining limit of the operator-record path: an operator identity
// declaration in the form of an identity link names the driver's binding, so
// only the driver's own batch can carry it. Declaring it publishes nothing; a
// driver batch that does not carry it leaves it pending; the driver's next
// batch that carries the pending links publishes it.
func TestOperatorRecordAcceptanceIdentityLinkWaitsForTheDriversNextBatch(t *testing.T) {
	site := oraSitePath()
	site.unlinked = true
	f := newOraFixture(t, infrastructure.New, []*oraPath{site})
	audit := portalOperatorAudit(oraSiteAsset, nil)
	if err := f.hub.SetOperatorLinks(oraSiteAsset, oraSiteSource, []semhub.OperatorLink{{Native: site.native(), Resource: oraConnection, Basis: []semreg.EvidenceRef{audit.evidence()}}}); err != nil {
		t.Fatalf("declare the operator identity link: %v", err)
	}
	linked := func() (semreg.IdentityLink, bool) {
		for _, link := range f.inspect(t, oraSiteAsset).Snapshot.IdentityLinks {
			if link.BindingID == site.binding(t) && link.Resource != nil {
				return link, true
			}
		}
		return semreg.IdentityLink{}, false
	}
	if link, ok := linked(); ok {
		t.Fatalf("the declaration was published before the driver's batch: %+v", link)
	}
	if err := f.publish(t, site, nil); err != nil {
		t.Fatalf("driver batch without the pending link: %v", err)
	}
	if link, ok := linked(); ok {
		t.Fatalf("a batch that did not carry the declaration published it: %+v", link)
	}
	if err := f.publish(t, site, func(tx *semhub.Tx, batch *semreg.PublicationBatch) {
		batch.IdentityLinkUpserts = tx.OperatorLinks(batch.BindingUpserts)
	}); err != nil {
		t.Fatalf("driver batch with the pending link: %v", err)
	}
	link, ok := linked()
	if !ok || *link.Resource != oraConnection || link.State != semreg.LinkQualified || !reflect.DeepEqual(link.Basis, []semreg.EvidenceRef{audit.evidence()}) {
		t.Fatalf("link=%+v present=%v, want the declaration carried by the driver's batch", link, ok)
	}
}
