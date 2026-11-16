package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
)

// The operator-record fixture is one synthetic PV installation. The driver
// asset is owned by a synthetic PV driver that publishes one inverter
// resource; the configuration provider is a declared contributor of that
// asset and owns a second, configuration-only asset that homes the PV array
// the operator attached to the inverter.
const (
	oprDriverAsset  semreg.AssetID    = "asset:opr-inverter"
	oprDriverSource semreg.SourceID   = "source:opr-inverter"
	oprDriver                         = "driver-opr-inverter"
	oprHomeAsset    semreg.AssetID    = "asset:opr-configuration"
	oprInverter     semreg.ResourceID = "resource:opr:inverter-1"
	oprMissing      semreg.ResourceID = "resource:opr:inverter-9"
	oprArray        semreg.ResourceID = "resource:opr:array-1"
	oprArraySecond  semreg.ResourceID = "resource:opr:array-2"
)

var oprPVPack = semreg.PackRef{ID: "helianthus.pack.pv", Version: "1.0.0"}

func oprHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func oprEvidence(kind semreg.DefinitionID) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: "helianthus.gateway.oprtest", Kind: kind, Digest: semreg.Digest("sha256:" + oprHash("oprtest", string(kind))), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

// oprAudit is the audit of one synthetic operator declaration.
func oprAudit(entry string) operatorAudit {
	return operatorAudit{Declaration: semreg.Digest("sha256:" + oprHash("oprtest.declaration", entry))}
}

func oprWall(seconds int64) semreg.TimePoint {
	return semreg.TimePoint{UnixNanoseconds: semreg.Int64(strconv.FormatInt(1_800_000_000_000_000_000+seconds*1_000_000_000, 10)), ClockID: "wall.utc", UncertaintyNS: "0"}
}

func oprMono(seconds int64) semreg.MonotonicPoint {
	return semreg.MonotonicPoint{ClockEpochID: "clock:opr-test", Nanoseconds: semreg.Uint64(strconv.FormatInt(seconds*1_000_000_000, 10))}
}

func oprInverterArray(to semreg.ResourceID) operatorRelation {
	return operatorRelation{Definition: semreg.DefinitionRef{Pack: oprPVPack, ID: "pv.relation.inverter_array", Version: "1.0.0"}, From: oprInverter, To: to}
}

func oprArrayResource(id semreg.ResourceID, slot string) operatorResource {
	return operatorResource{ID: id, Kind: "resource.pv_array", Class: semreg.AnchorFunction, Slot: slot}
}

type oprFixture struct {
	hub       *semhub.Hub
	publisher *operatorRecordPublisher
	driver    *semhub.Handle
	clock     int64
}

// newOprFixture declares the driver asset with the configuration provider as
// a contributor and the provider's configuration-only home, admits the
// driver and publishes its inverter resource.
func newOprFixture(t *testing.T) *oprFixture {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f := &oprFixture{hub: hub, clock: 100}
	f.publisher, err = newOperatorRecordPublisher(hub, operatorRecordOptions{Now: func() (semreg.TimePoint, semreg.MonotonicPoint) {
		f.clock++
		return oprWall(f.clock), oprMono(f.clock)
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: oprDriverAsset, SourceID: oprDriverSource, Driver: oprDriver, Validators: []semreg.PackValidator{pv.New()}, Graph: true,
		Contributors: []semhub.ContributorSpec{f.publisher.Contributor(pv.New())}}); err != nil {
		t.Fatal(err)
	}
	if err := f.publisher.DeclareHome(oprHomeAsset, pv.New()); err != nil {
		t.Fatal(err)
	}
	f.driver, err = hub.AdmitSource(semhub.Admission{Source: oprDriverSource, Driver: oprDriver})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.publishDriver(t, nil); err != nil {
		t.Fatalf("driver publication: %v", err)
	}
	return f
}

// driverBatch builds the driver's next batch: on the first batch its source,
// binding, the identity link that names the inverter and the natively
// anchored inverter resource; edit then changes it.
func (f *oprFixture) driverBatch(t *testing.T, tx *semhub.Tx, edit func(*semreg.PublicationBatch, semreg.Snapshot)) semreg.PublicationBatch {
	t.Helper()
	inc := tx.Incarnation()
	current, exists := tx.Current()
	generation := semreg.Uint64(strconv.FormatUint(inc.Generation, 10))
	binding, err := semhub.NativeBindingID("opr_driver", inc.Asset, inc.Source, "oprtest.profile.v1", inc.Epoch, inc.Generation)
	if err != nil {
		t.Fatal(err)
	}
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	f.clock++
	at := f.clock
	epoch, source := inc.Epoch, inc.Source
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:opr-driver:" + oprHash(string(inc.Asset), string(epoch), string(generation), string(tx.NextSequence()))), AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, Sequence: tx.NextSequence(), ExpectedSemanticRevision: expected, ObservedAt: oprWall(at),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !exists {
		evidence := oprEvidence("oprtest.observation")
		slot := "inverter-1"
		resource := oprInverter
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: source, SourceEpochID: epoch, ProtocolID: "oprtest_protocol", ProfileID: "oprtest.profile.v1", ProfileVersion: "1", RegistryEvidence: oprEvidence("oprtest.registry"), StartedAt: oprWall(0), State: semreg.SourceCurrent, Revision: "1"}}
		batch.BindingUpserts = []semreg.NativeBinding{{BindingID: binding, AssetID: inc.Asset, SourceID: source, SourceEpochID: epoch, DriverGeneration: generation, NativeResource: oprEvidence("oprtest.native"), State: semreg.BindingCurrent, Revision: "1"}}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{{AssetID: inc.Asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{evidence}, Resource: &resource, Revision: "1"}}
		batch.ResourceUpserts = []semreg.Resource{{ResourceID: oprInverter, Kind: "resource.inverter", Anchor: semreg.ResourceAnchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorNativeSlot, Slot: &slot, Evidence: []semreg.EvidenceRef{evidence}}, State: semreg.ResourceCurrent,
			Origin: semreg.OriginRef{OriginID: "origin:opr-driver:inverter-1", Kind: semreg.OriginNativeObservation, SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}}, Revision: "1"}}
	}
	if edit != nil {
		edit(&batch, current)
	}
	batch.BatchDigest = ""
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
	return batch
}

// publishDriver runs one driver transaction on the driver asset and returns
// the first refusal (Begin, Apply or Commit).
func (f *oprFixture) publishDriver(t *testing.T, edit func(*semreg.PublicationBatch, semreg.Snapshot)) error {
	t.Helper()
	return f.publishDriverOn(t, oprDriverAsset, edit)
}

func (f *oprFixture) publishDriverOn(t *testing.T, id semreg.AssetID, edit func(*semreg.PublicationBatch, semreg.Snapshot)) error {
	t.Helper()
	tx, err := f.hub.Begin(f.driver, id)
	if err != nil {
		return err
	}
	defer tx.Abort()
	if _, _, err := tx.Apply(f.driverBatch(t, tx, edit), oprMono(f.clock)); err != nil {
		return err
	}
	outcome, err := tx.Commit(nil)
	if err == nil && outcome != semhub.Accepted {
		err = errors.New("driver batch not accepted: " + string(outcome))
	}
	return err
}

func (f *oprFixture) fenceDriver(t *testing.T) {
	t.Helper()
	f.clock++
	if _, err := f.hub.Fence(semhub.FenceRequest{Source: oprDriverSource, Epoch: f.driver.Epoch(), Generation: f.driver.Generation(), Reason: "lifecycle.generation_replaced", Evidence: []semreg.EvidenceRef{oprEvidence("oprtest.fence")}, ObservedAt: oprWall(f.clock), Monotonic: oprMono(f.clock)}); err != nil {
		t.Fatalf("fence driver: %v", err)
	}
	info, ok := f.hub.Source(oprDriverSource)
	if !ok || info.Live {
		t.Fatalf("driver still live after the fence: %+v", info)
	}
}

func (f *oprFixture) snapshot(t *testing.T, id semreg.AssetID) semreg.Snapshot {
	t.Helper()
	view, err := f.hub.Inspect(id)
	if err != nil || !view.Present {
		t.Fatalf("inspect %s: present=%v err=%v", id, view.Present, err)
	}
	return view.Snapshot
}

func oprRelationIn(snapshot semreg.Snapshot, relation operatorRelation) (semreg.RelationInstance, bool) {
	for _, held := range snapshot.Relations {
		if reflect.DeepEqual(held.Definition, relation.Definition) && held.From == relation.From && held.To == relation.To {
			return held, true
		}
	}
	return semreg.RelationInstance{}, false
}

func oprResourceIn(snapshot semreg.Snapshot, id semreg.ResourceID) (semreg.Resource, bool) {
	for _, held := range snapshot.Resources {
		if held.ResourceID == id {
			return held, true
		}
	}
	return semreg.Resource{}, false
}

// oprRequireOperatorOrigin asserts the operator origin and the audit
// evidence of one record.
func oprRequireOperatorOrigin(t *testing.T, origin semreg.OriginRef, audit operatorAudit) {
	t.Helper()
	if origin.Kind != semreg.OriginOperator || origin.SourceID != nil || origin.SourceEpochID != nil || origin.BindingID != nil {
		t.Fatalf("origin=%+v, want an operator origin without a native path", origin)
	}
	if len(origin.Evidence) != 1 || origin.Evidence[0] != audit.evidence() {
		t.Fatalf("origin evidence=%+v, want the declaration's audit evidence %+v", origin.Evidence, audit.evidence())
	}
}

// A configured relation whose starting end is the driver's inverter is
// published by the configuration source into the driver's asset, as one
// batch of the provider's own key, with an operator origin and the audit
// evidence of its declaration.
func TestOperatorRecordPublisherPublishesAConfiguredRelationIntoTheDriversAsset(t *testing.T) {
	f := newOprFixture(t)
	audit := oprAudit("meter-role-1")
	relation := oprInverterArray(oprArray)
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}}, audit); err != nil {
		t.Fatal(err)
	}
	snapshot := f.snapshot(t, oprDriverAsset)
	held, ok := oprRelationIn(snapshot, relation)
	if !ok {
		t.Fatalf("relation not published: %+v", snapshot.Relations)
	}
	if held.BasisClass != semreg.RelationConfiguration || held.State != semreg.LinkQualified || held.Revision != "1" {
		t.Fatalf("relation=%+v, want a qualified configuration instance at revision 1", held)
	}
	if !reflect.DeepEqual(held.Basis, []semreg.EvidenceRef{audit.evidence()}) {
		t.Fatalf("basis=%+v, want the declaration's audit evidence", held.Basis)
	}
	oprRequireOperatorOrigin(t, held.Origin, audit)
	info, ok := f.hub.Source(operatorRecordSource)
	if !ok || !info.Live || info.Driver != operatorRecordDriver || info.Legacy != "" {
		t.Fatalf("configuration source=%+v, want a live hub-native source of the configuration provider", info)
	}
	sources := map[semreg.SourceID]bool{}
	for _, descriptor := range snapshot.Sources {
		sources[descriptor.SourceID] = true
		if descriptor.SourceID == operatorRecordSource && descriptor.SourceEpochID != info.Epoch {
			t.Fatalf("configuration descriptor epoch=%s, want %s", descriptor.SourceEpochID, info.Epoch)
		}
	}
	if !sources[oprDriverSource] || !sources[operatorRecordSource] || len(sources) != 2 {
		t.Fatalf("driver asset sources=%v, want the driver and the configuration provider", sources)
	}
	// The provider's records have their own revisions: an unchanged set
	// publishes nothing, a changed audit revises the record.
	cursor := f.hub.Cursor()
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}}, audit); err != nil {
		t.Fatal(err)
	}
	if events, err := f.hub.Since(cursor); err != nil || len(events) != 0 {
		t.Fatalf("an unchanged record set emitted %d events (%v)", len(events), err)
	}
	revised := oprAudit("meter-role-1-revised")
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}}, revised); err != nil {
		t.Fatal(err)
	}
	held, _ = oprRelationIn(f.snapshot(t, oprDriverAsset), relation)
	if held.Revision != "2" {
		t.Fatalf("revised relation revision=%s, want 2", held.Revision)
	}
	oprRequireOperatorOrigin(t, held.Origin, revised)
	desired := f.publisher.Desired(oprDriverAsset)
	if len(desired.Relations) != 1 || desired.Relations[0].Revision != 2 || desired.Relations[0].Record != relation {
		t.Fatalf("desired records=%+v, want the relation at revision 2", desired)
	}
}

// The provider publishes while the driver's generation is fenced, and its
// configuration records survive the fence; the driver's native resource
// stays as an identity record.
func TestOperatorRecordPublisherPublishesWhileTheDriverIsFencedAndSurvivesTheFence(t *testing.T) {
	f := newOprFixture(t)
	audit := oprAudit("battery-attachment-1")
	first := oprInverterArray(oprArray)
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{first}}, audit); err != nil {
		t.Fatal(err)
	}
	f.fenceDriver(t)
	snapshot := f.snapshot(t, oprDriverAsset)
	if _, ok := oprRelationIn(snapshot, first); !ok {
		t.Fatal("the configuration relation did not survive the driver's fence")
	}
	for _, cursor := range snapshot.Cursors {
		if cursor.SourceID == oprDriverSource && !cursor.Fenced {
			t.Fatalf("driver cursor not fenced: %+v", cursor)
		}
	}
	second := oprInverterArray(oprArraySecond)
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{first, second}}, audit); err != nil {
		t.Fatalf("publication while the driver is fenced: %v", err)
	}
	snapshot = f.snapshot(t, oprDriverAsset)
	for _, relation := range []operatorRelation{first, second} {
		if held, ok := oprRelationIn(snapshot, relation); !ok || held.Revision != "1" {
			t.Fatalf("relation %s present=%v revision=%s, want present at revision 1", relation.To, ok, held.Revision)
		}
	}
	if resource, ok := oprResourceIn(snapshot, oprInverter); !ok || resource.State != semreg.ResourceCurrent {
		t.Fatalf("driver resource=%+v present=%v, want current after the fence", resource, ok)
	}
}

// A configured relation is withdrawn while the driver is stopped: the
// withdrawal does not depend on the driver.
func TestOperatorRecordPublisherWithdrawsWhileTheDriverIsStopped(t *testing.T) {
	f := newOprFixture(t)
	audit := oprAudit("meter-role-2")
	relation := oprInverterArray(oprArray)
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}}, audit); err != nil {
		t.Fatal(err)
	}
	f.fenceDriver(t)
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{}, audit); err != nil {
		t.Fatalf("withdrawal while the driver is stopped: %v", err)
	}
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), relation); ok {
		t.Fatal("the withdrawn relation is still published")
	}
	if desired := f.publisher.Desired(oprDriverAsset); len(desired.Relations) != 0 {
		t.Fatalf("desired records after the withdrawal=%+v", desired)
	}
}

// A record whose starting end does not resolve in the asset is refused with
// a reason before anything is submitted: the snapshot, the stream, the
// provider's admission and its desired records stay as they were.
func TestOperatorRecordPublisherRefusesAnUnresolvedStartingEnd(t *testing.T) {
	f := newOprFixture(t)
	before, err := f.hub.Inspect(oprDriverAsset)
	if err != nil {
		t.Fatal(err)
	}
	cursor := f.hub.Cursor()
	dangling := operatorRelation{Definition: oprInverterArray(oprArray).Definition, From: oprMissing, To: oprArray}
	err = f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{oprInverterArray(oprArraySecond), dangling}}, oprAudit("meter-role-3"))
	var refusal *operatorRecordRefusal
	if !errors.As(err, &refusal) || refusal.Reason != operatorReasonStartingEndUnresolved || refusal.Asset != oprDriverAsset || refusal.Resource != oprMissing {
		t.Fatalf("refusal=%v, want %s for %s", err, operatorReasonStartingEndUnresolved, oprMissing)
	}
	after, err := f.hub.Inspect(oprDriverAsset)
	if err != nil || string(after.Canonical) != string(before.Canonical) {
		t.Fatalf("a refused record set changed the snapshot (err=%v)", err)
	}
	if events, err := f.hub.Since(cursor); err != nil || len(events) != 0 {
		t.Fatalf("a refused record set emitted %d events (%v)", len(events), err)
	}
	if info, ok := f.hub.Source(operatorRecordSource); !ok || info.Live || info.Epoch != "" {
		t.Fatalf("a refused record set admitted the configuration source: %+v (declared=%v)", info, ok)
	}
	if desired := f.publisher.Desired(oprDriverAsset); len(desired.Relations) != 0 || len(desired.Resources) != 0 {
		t.Fatalf("a refused record set changed the desired records: %+v", desired)
	}
	// A record set without audit evidence is refused the same way.
	err = f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{oprInverterArray(oprArray)}}, operatorAudit{})
	if !errors.As(err, &refusal) || refusal.Reason != operatorReasonAuditMissing {
		t.Fatalf("refusal=%v, want %s", err, operatorReasonAuditMissing)
	}
}

// A resource that only configuration knows is homed in an asset whose single
// source is the configuration provider, under an operator slot anchor.
func TestOperatorRecordPublisherHomesAConfigurationOnlyResourceInItsOwnAsset(t *testing.T) {
	f := newOprFixture(t)
	audit := oprAudit("array-1")
	if err := f.publisher.Set(oprHomeAsset, operatorRecords{Resources: []operatorResource{oprArrayResource(oprArray, "array-1")}}, audit); err != nil {
		t.Fatal(err)
	}
	snapshot := f.snapshot(t, oprHomeAsset)
	if len(snapshot.Sources) != 1 || snapshot.Sources[0].SourceID != operatorRecordSource {
		t.Fatalf("home asset sources=%+v, want the configuration provider only", snapshot.Sources)
	}
	if len(snapshot.Bindings) != 0 || len(snapshot.Facts) != 0 {
		t.Fatalf("home asset holds bindings or facts: %+v", snapshot)
	}
	resource, ok := oprResourceIn(snapshot, oprArray)
	if !ok || resource.State != semreg.ResourceCurrent || resource.Kind != "resource.pv_array" || resource.Revision != "1" {
		t.Fatalf("resource=%+v present=%v", resource, ok)
	}
	if resource.Anchor.Basis != semreg.AnchorOperatorSlot || resource.Anchor.Class != semreg.AnchorFunction || resource.Anchor.Slot == nil || *resource.Anchor.Slot != "array-1" || !reflect.DeepEqual(resource.Anchor.Evidence, []semreg.EvidenceRef{audit.evidence()}) {
		t.Fatalf("anchor=%+v, want an operator slot with the audit evidence", resource.Anchor)
	}
	oprRequireOperatorOrigin(t, resource.Origin, audit)
	// Removing the resource from the configuration withdraws it with a new
	// revision; the identity record stays in the snapshot.
	if err := f.publisher.Set(oprHomeAsset, operatorRecords{}, audit); err != nil {
		t.Fatal(err)
	}
	resource, ok = oprResourceIn(f.snapshot(t, oprHomeAsset), oprArray)
	if !ok || resource.State != semreg.ResourceWithdrawn || resource.Revision != "2" {
		t.Fatalf("withdrawn resource=%+v present=%v, want withdrawn at revision 2", resource, ok)
	}
}

// A driver cannot revise or withdraw the provider's records: it is not a
// source of the provider's configuration-only home at all.
func TestOperatorRecordPublisherKeepsTheProvidersRecordsFromADriver(t *testing.T) {
	f := newOprFixture(t)
	if err := f.publisher.Set(oprHomeAsset, operatorRecords{Resources: []operatorResource{oprArrayResource(oprArray, "array-1")}}, oprAudit("array-1")); err != nil {
		t.Fatal(err)
	}
	before, err := f.hub.Inspect(oprHomeAsset)
	if err != nil {
		t.Fatal(err)
	}
	err = f.publishDriverOn(t, oprHomeAsset, nil)
	if !errors.Is(err, semhub.ErrTransaction) {
		t.Fatalf("driver publication on the provider's home=%v, want ErrTransaction", err)
	}
	after, err := f.hub.Inspect(oprHomeAsset)
	if err != nil || string(after.Canonical) != string(before.Canonical) {
		t.Fatalf("the driver changed the provider's home (err=%v)", err)
	}
}

// The composition seam wires the provider with an empty record set: it
// admits no source, declares no asset and emits no event.
func TestOperatorRecordPublisherComposesAnEmptyRecordSet(t *testing.T) {
	hub := newTestSemanticHub(t)
	cursor := hub.Cursor()
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	if err := composeOperatorRecords(hub, ebusgateway.Config{}); err != nil {
		t.Fatal(err)
	}
	if records := configuredOperatorRecords(ebusgateway.Config{}); len(records) != 0 {
		t.Fatalf("configured operator records=%+v, want none", records)
	}
	if _, ok := hub.Source(operatorRecordSource); ok {
		t.Fatal("an empty record set admitted the configuration source")
	}
	if events, err := hub.Since(cursor); err != nil || len(events) != 0 {
		t.Fatalf("an empty record set emitted %d events (%v)", len(events), err)
	}
	after, err := hub.GraphState()
	if err != nil || !reflect.DeepEqual(after, state) {
		t.Fatalf("an empty record set changed the declared assets: %+v (err=%v)", after, err)
	}
}

// A configured relation whose starting end belongs to a driver that has not
// published it yet is neither fatal nor lost: the composition keeps it pending
// without touching the hub and publishes it after the driver's publication.
func TestOperatorRecordPublisherDefersARecordUntilItsOwnerResourceExists(t *testing.T) {
	f := newOprFixture(t)
	cursor := f.hub.Cursor()
	late := operatorRelation{Definition: oprInverterArray(oprArray).Definition, From: oprMissing, To: oprArray}
	audit := oprAudit("meter-role-late")
	if err := f.publisher.Compose([]operatorAssetRecords{{Asset: oprDriverAsset, Records: operatorRecords{Relations: []operatorRelation{late}}, Audit: audit}}); err != nil {
		t.Fatalf("compose refused a record that waits for its owner: %v", err)
	}
	if pending := f.publisher.Pending(); len(pending) != 1 || pending[0] != oprDriverAsset {
		t.Fatalf("pending=%v, want the driver asset", pending)
	}
	if events, err := f.hub.Since(cursor); err != nil || len(events) != 0 {
		t.Fatalf("a pending record emitted %d events (%v)", len(events), err)
	}
	if info, ok := f.hub.Source(operatorRecordSource); !ok || info.Live {
		t.Fatalf("a pending record admitted the configuration source: %+v", info)
	}
	err := f.publishDriver(t, func(batch *semreg.PublicationBatch, current semreg.Snapshot) {
		resource := current.Resources[0]
		resource.ResourceID = oprMissing
		slot := "inverter-9"
		resource.Anchor.Slot = &slot
		resource.Origin.OriginID = "origin:opr-driver:inverter-9"
		resource.Revision = "1"
		batch.ResourceUpserts = []semreg.Resource{resource}
	})
	if err != nil {
		t.Fatalf("driver publication of the late resource: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(f.publisher.Pending()) != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if pending := f.publisher.Pending(); len(pending) != 0 {
		t.Fatalf("pending=%v after the owner published", pending)
	}
	held, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), late)
	if !ok {
		t.Fatal("the deferred relation was not published after its owner resource appeared")
	}
	oprRequireOperatorOrigin(t, held.Origin, audit)
}

// A deferred set that later fails for another reason is reported, not lost.
func TestOperatorRecordPublisherReportsADeferredSetThatFailsLater(t *testing.T) {
	f := newOprFixture(t)
	bad := operatorRelation{Definition: semreg.DefinitionRef{Pack: semreg.PackRef{ID: "pack.unknown.opr", Version: "1.0.0"}, ID: "unknown.relation", Version: "1.0.0"}, From: oprMissing, To: oprArray}
	if err := f.publisher.Compose([]operatorAssetRecords{{Asset: oprDriverAsset, Records: operatorRecords{Relations: []operatorRelation{bad}}, Audit: oprAudit("meter-role-bad")}}); err != nil {
		t.Fatalf("compose: %v", err)
	}
	err := f.publishDriver(t, func(batch *semreg.PublicationBatch, current semreg.Snapshot) {
		resource := current.Resources[0]
		resource.ResourceID = oprMissing
		slot := "inverter-9"
		resource.Anchor.Slot = &slot
		resource.Origin.OriginID = "origin:opr-driver:inverter-9"
		resource.Revision = "1"
		batch.ResourceUpserts = []semreg.Resource{resource}
	})
	if err != nil {
		t.Fatalf("driver publication: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(f.publisher.Failures()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if failures := f.publisher.Failures(); failures[oprDriverAsset] == nil || len(f.publisher.Pending()) != 0 {
		t.Fatalf("failures=%v pending=%v, want the failed set reported", failures, f.publisher.Pending())
	}
}

// When one retry refuses several deferred sets, every refusal is returned and
// recorded, none only through Failures.
func TestOperatorRecordPublisherReportsEveryDeferredRefusalOfOneRetry(t *testing.T) {
	f := newOprFixture(t)
	first, second := semreg.AssetID("asset:opr-late-a"), semreg.AssetID("asset:opr-late-b")
	relation := operatorRelation{Definition: oprInverterArray(oprArray).Definition, From: oprMissing, To: oprArray}
	f.publisher.mu.Lock()
	f.publisher.pending = map[semreg.AssetID]operatorAssetRecords{
		first:  {Asset: first, Records: operatorRecords{Relations: []operatorRelation{relation}}},
		second: {Asset: second, Records: operatorRecords{Relations: []operatorRelation{relation}}},
	}
	f.publisher.mu.Unlock()
	refusals := operatorRecordRefusals(f.publisher.Retry())
	if len(refusals) != 2 {
		t.Fatalf("retry returned %d refusals (%v), want both deferred sets", len(refusals), refusals)
	}
	for i, want := range []semreg.AssetID{first, second} {
		var refusal *operatorRecordRefusal
		if !errors.As(refusals[i], &refusal) || refusal.Asset != want || refusal.Reason != operatorReasonAuditMissing {
			t.Fatalf("refusal %d=%v, want %s refused for %s", i, refusals[i], want, operatorReasonAuditMissing)
		}
	}
	if failures := f.publisher.Failures(); failures[first] == nil || failures[second] == nil || len(f.publisher.Pending()) != 0 {
		t.Fatalf("failures=%v pending=%v, want both sets recorded and none pending", failures, f.publisher.Pending())
	}
	if refusals := operatorRecordRefusals(f.publisher.Retry()); len(refusals) != 0 {
		t.Fatalf("a retry with nothing pending returned %v", refusals)
	}
}

// Any other refusal stays fatal at composition.
func TestOperatorRecordPublisherComposeStopsOnAnotherRefusal(t *testing.T) {
	f := newOprFixture(t)
	err := f.publisher.Compose([]operatorAssetRecords{{Asset: oprDriverAsset, Records: operatorRecords{Relations: []operatorRelation{oprInverterArray(oprArray)}}}})
	var refusal *operatorRecordRefusal
	if !errors.As(err, &refusal) || refusal.Reason != operatorReasonAuditMissing || len(f.publisher.Pending()) != 0 {
		t.Fatalf("compose=%v pending=%v, want a fatal %s", err, f.publisher.Pending(), operatorReasonAuditMissing)
	}
}
