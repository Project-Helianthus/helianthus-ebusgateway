package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
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
	return newOprFixtureWith(t, []semreg.PackValidator{pv.New()}, []semreg.PackValidator{pv.New()})
}

// newOprFixtureWith is newOprFixture with the driver asset's packs and the
// packs the asset's owner declares for the configuration provider.
func newOprFixtureWith(t *testing.T, assetPacks, contributorPacks []semreg.PackValidator) *oprFixture {
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
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: oprDriverAsset, SourceID: oprDriverSource, Driver: oprDriver, Validators: assetPacks, Graph: true,
		Contributors: []semhub.ContributorSpec{f.publisher.Contributor(contributorPacks...)}}); err != nil {
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

// The provider publishes relations only of the packs the asset's owner
// declared for it: on an asset that holds the PV and storage packs, a
// provider declared with the storage pack alone cannot publish a PV relation,
// and the refused set changes neither the snapshot nor the desired records.
func TestOperatorRecordPublisherPublishesOnlyTheDeclaredPacksRelations(t *testing.T) {
	f := newOprFixtureWith(t, []semreg.PackValidator{pv.New(), storage.New()}, []semreg.PackValidator{storage.New()})
	before, err := f.hub.Inspect(oprDriverAsset)
	if err != nil {
		t.Fatal(err)
	}
	relation := oprInverterArray(oprArray)
	err = f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}}, oprAudit("meter-role-undeclared-pack"))
	if !errors.Is(err, semhub.ErrTransaction) {
		t.Fatalf("err=%v, want the hub's refusal of a relation of an undeclared pack", err)
	}
	after, err := f.hub.Inspect(oprDriverAsset)
	if err != nil || string(after.Canonical) != string(before.Canonical) {
		t.Fatalf("a refused record set changed the snapshot (err=%v)", err)
	}
	if _, ok := oprRelationIn(after.Snapshot, relation); ok {
		t.Fatal("a relation of an undeclared pack was published")
	}
	if desired := f.publisher.Desired(oprDriverAsset); len(desired.Relations) != 0 {
		t.Fatalf("a refused record set changed the desired records: %+v", desired)
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

// The provider is a contributor without a reprojection callback: it keeps no
// cache, so a control operation of the driver settles the shared asset at
// once and a publication of the driver owes the provider nothing. Reads of
// the shared asset are never withheld on the provider's account.
func TestOperatorRecordPublisherNeedsNoReprojectionOnASharedAsset(t *testing.T) {
	f := newOprFixture(t)
	relation := oprInverterArray(oprArray)
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}}, oprAudit("meter-role-3")); err != nil {
		t.Fatal(err)
	}
	if spec := f.publisher.Contributor(pv.New()); spec.OnControl != nil {
		t.Fatal("the provider declares a reprojection callback")
	}
	if !f.hub.Settled(oprDriverAsset) || !f.hub.Readable(oprDriverAsset) {
		t.Fatal("the provider's publication left the shared asset unsettled or withheld")
	}
	if err := f.publishDriver(t, nil); err != nil {
		t.Fatalf("driver publication: %v", err)
	}
	if !f.hub.Settled(oprDriverAsset) || !f.hub.Readable(oprDriverAsset) {
		t.Fatal("a driver publication owed the provider a peer reprojection")
	}
	f.fenceDriver(t)
	if !f.hub.Settled(oprDriverAsset) || !f.hub.Readable(oprDriverAsset) {
		t.Fatal("the driver's fence left the shared asset unsettled on the provider's account")
	}
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), relation); !ok {
		t.Fatal("the configuration relation did not survive the driver's fence")
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
	if records, err := configuredOperatorRecords(ebusgateway.Config{}); err != nil || len(records) != 0 {
		t.Fatalf("configured operator records=%+v %v, want none", records, err)
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

func oprWriteSection(t *testing.T, root string, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, "portal-config-operator-records.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func oprCompositionDigest(label string) string {
	sum := sha256.Sum256([]byte("opr-composition-identity\x00" + label))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// A configuration that holds one operator record yields that record. An empty
// section yields an empty set.
func TestOperatorRecordCompositionLoadsAConfiguredRecord(t *testing.T) {
	root := t.TempDir()
	records, err := configuredOperatorRecords(ebusgateway.Config{PortalConfigRoot: root})
	if err != nil || len(records) != 0 {
		t.Fatalf("missing section=%+v %v, want none", records, err)
	}
	oprWriteSection(t, root, `{"format":"helianthus.portal-config.operator-records/v1","revision":"0","records":[]}`)
	records, err = configuredOperatorRecords(ebusgateway.Config{PortalConfigRoot: root})
	if err != nil || len(records) != 0 {
		t.Fatalf("empty section=%+v %v, want none", records, err)
	}
	oprWriteSection(t, root, `{"format":"helianthus.portal-config.operator-records/v1","revision":"1","records":[{"form":"anchor","home":"asset:opr-configuration","resource_id":"resource:opr:array-1","kind":"resource.pv_array","slot":"array-south"}]}`)
	records, err = configuredOperatorRecords(ebusgateway.Config{PortalConfigRoot: root})
	if err != nil || len(records) != 1 || records[0].Asset != oprHomeAsset || len(records[0].Records.Resources) != 1 || !records[0].ConfigurationOnly || len(records[0].Records.Links) != 0 {
		t.Fatalf("configured operator records=%+v %v, want the one configured anchor", records, err)
	}
}

// An identity link is returned with the configured set and is not a
// configuration-only home. Composition does not declare that home and does
// not admit the provider, because the provider does not publish the link.
func TestOperatorRecordCompositionLoadsAnIdentityLink(t *testing.T) {
	root := t.TempDir()
	body, err := json.Marshal(map[string]any{
		"format":   "helianthus.portal-config.operator-records/v1",
		"revision": "1",
		"records": []any{map[string]any{
			"form": "identity_link",
			"home": string(oprDriverAsset),
			"native": map[string]any{
				"owner":     "helianthus.gateway.oprtest",
				"kind":      "oprtest.native",
				"digest":    oprCompositionDigest("native"),
				"contract":  string(semreg.ContractKernelV1),
				"access":    string(semreg.EvidenceAccessAuthorized),
				"redaction": string(semreg.RedactionMetadataOnly),
			},
			"resource_id": string(oprInverter),
			"basis_class": string(semreg.RelationConfiguration),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	oprWriteSection(t, root, string(body))
	records, err := configuredOperatorRecords(ebusgateway.Config{PortalConfigRoot: root})
	if err != nil || len(records) != 1 || records[0].ConfigurationOnly || len(records[0].Records.Links) != 1 || len(records[0].Records.Resources) != 0 {
		t.Fatalf("configured identity link=%+v %v", records, err)
	}
	link := records[0].Records.Links[0]
	if records[0].Asset != oprDriverAsset || link.Resource != oprInverter || link.BasisClass != string(semreg.RelationConfiguration) || link.Native.Owner != "helianthus.gateway.oprtest" || string(link.Native.Digest) != oprCompositionDigest("native") {
		t.Fatalf("identity link=%+v, want the configured native resource", link)
	}
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := composeOperatorRecords(hub, ebusgateway.Config{PortalConfigRoot: root}); err != nil {
		t.Fatal(err)
	}
	state, err := hub.GraphState()
	if err != nil || len(state.Assets) != 0 {
		t.Fatalf("an identity link declared %+v (%v)", state.Assets, err)
	}
	if _, ok := hub.Source(operatorRecordSource); ok {
		t.Fatal("an identity link admitted the configuration source")
	}
}

// A section that does not decode fails closed and admits nothing.
func TestOperatorRecordCompositionRejectsASectionThatDoesNotDecode(t *testing.T) {
	root := t.TempDir()
	oprWriteSection(t, root, `{`)
	records, err := configuredOperatorRecords(ebusgateway.Config{PortalConfigRoot: root})
	if err == nil || len(records) != 0 {
		t.Fatalf("configured=%+v err=%v, want a refusal and no records", records, err)
	}
	hub, hubErr := semhub.New(semhub.Options{})
	if hubErr != nil {
		t.Fatal(hubErr)
	}
	before, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	if err := composeOperatorRecords(hub, ebusgateway.Config{PortalConfigRoot: root}); err == nil {
		t.Fatal("composition accepted a section that does not decode")
	}
	after, err := hub.GraphState()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("a refused section changed the declared assets: %+v %v", after, err)
	}
	if _, ok := hub.Source(operatorRecordSource); ok {
		t.Fatal("a refused section admitted the configuration source")
	}
}

// A home a driver already owns stays that driver's asset. When the owner has
// not admitted the provider, the configured set is reported and left
// unpublished. Absent homes, including pairing views, are not claimed.
func TestOperatorRecordCompositionReportsADriverOwnedSetItCannotPublish(t *testing.T) {
	root := t.TempDir()
	oprWriteSection(t, root, `{"format":"helianthus.portal-config.operator-records/v1","revision":"1","records":[{"form":"anchor","home":"asset:opr-configuration","resource_id":"resource:opr:array-1","kind":"resource.pv_array","slot":"array-south"},{"form":"anchor","home":"asset:opr-inverter","resource_id":"resource:opr:array-2","kind":"resource.pv_array","slot":"array-north"},{"form":"pairing","kind":"resource.pv_system","slot":"system-main","views":[{"home":"asset:opr-pair-a","resource_id":"resource:opr:system-a"},{"home":"asset:opr-pair-b","resource_id":"resource:opr:system-b"}]}]}`)
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: oprDriverAsset, SourceID: oprDriverSource, Driver: oprDriver, Validators: []semreg.PackValidator{pv.New()}, Graph: true}); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	if err := composeOperatorRecords(hub, ebusgateway.Config{PortalConfigRoot: root}); err != nil {
		t.Fatal(err)
	}
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	found := map[semreg.AssetID]string{}
	for _, asset := range state.Assets {
		found[asset.Asset] = asset.Driver
	}
	if found[oprDriverAsset] != oprDriver {
		t.Fatalf("driver home driver=%q, want %s", found[oprDriverAsset], oprDriver)
	}
	for _, id := range []semreg.AssetID{oprHomeAsset, "asset:opr-pair-a", "asset:opr-pair-b"} {
		if _, claimed := found[id]; claimed {
			t.Fatalf("startup claimed absent home %s for %s", id, found[id])
		}
	}
	if !strings.Contains(logged.String(), string(oprDriverAsset)) || !strings.Contains(logged.String(), "not published") {
		t.Fatalf("log=%q, want the driver-owned set reported as not published", logged.String())
	}
	view, err := hub.Inspect(oprDriverAsset)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := oprResourceIn(view.Snapshot, oprArraySecond); ok {
		t.Fatal("a set the owner has not admitted was published")
	}
	publisher, err := operatorPublisherFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	pending := map[semreg.AssetID]bool{}
	for _, id := range publisher.Pending() {
		pending[id] = true
	}
	for _, id := range []semreg.AssetID{oprHomeAsset, "asset:opr-pair-a", "asset:opr-pair-b"} {
		if !pending[id] {
			t.Fatalf("pending=%v, want the absent home %s kept", publisher.Pending(), id)
		}
	}
	if pending[oprDriverAsset] {
		t.Fatal("a reported skip stayed pending")
	}
}

// A configured set on a home whose owner has admitted the provider is
// published at startup. The home stays the driver's.
func TestOperatorRecordCompositionPublishesADriverOwnedSet(t *testing.T) {
	f := newOprFixture(t)
	root := t.TempDir()
	body, err := json.Marshal(oprRelationSection("1", oprArray))
	if err != nil {
		t.Fatal(err)
	}
	oprWriteSection(t, root, string(body))
	if err := composeOperatorRecords(f.hub, ebusgateway.Config{PortalConfigRoot: root}); err != nil {
		t.Fatal(err)
	}
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), oprInverterArray(oprArray)); !ok {
		t.Fatal("a configured relation on a driver-owned home was not published")
	}
	state, err := f.hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range state.Assets {
		if asset.Asset == oprDriverAsset && asset.Driver != oprDriver {
			t.Fatalf("driver home driver=%q, want %s", asset.Driver, oprDriver)
		}
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

// An absent home is not claimed, whatever its identifier looks like. A later
// declaration by the driver that owns it succeeds, including a home whose
// identifier is not the discovered-asset grammar.
func TestOperatorRecordCompositionDoesNotClaimAnAbsentHome(t *testing.T) {
	const (
		discovered semreg.AssetID = "pv-asset-5c57adba3ad8a529617c1f8627ae32f1"
		paired     semreg.AssetID = "pv-asset-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		lookalike  semreg.AssetID = "asset:pv-asset-5c57adba3ad8a529617c1f8627ae32f1"
		boiler     semreg.AssetID = "asset:synthetic-boiler"
	)
	root := t.TempDir()
	oprWriteSection(t, root, `{"format":"helianthus.portal-config.operator-records/v1","revision":"1","records":[{"form":"anchor","home":"pv-asset-5c57adba3ad8a529617c1f8627ae32f1","resource_id":"resource:opr:array-discovered","kind":"resource.pv_array","slot":"array-discovered"},{"form":"anchor","home":"asset:opr-configuration","resource_id":"resource:opr:array-1","kind":"resource.pv_array","slot":"array-south"},{"form":"anchor","home":"asset:pv-asset-5c57adba3ad8a529617c1f8627ae32f1","resource_id":"resource:opr:array-lookalike","kind":"resource.pv_array","slot":"array-lookalike"},{"form":"anchor","home":"asset:synthetic-boiler","resource_id":"resource:opr:boiler","kind":"resource.pv_array","slot":"boiler-flow"},{"form":"pairing","kind":"resource.pv_system","slot":"system-main","views":[{"home":"pv-asset-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","resource_id":"resource:opr:system-discovered"},{"home":"asset:opr-pair-b","resource_id":"resource:opr:system-b"}]}]}`)
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := composeOperatorRecords(hub, ebusgateway.Config{PortalConfigRoot: root}); err != nil {
		t.Fatal(err)
	}
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Assets) != 0 {
		t.Fatalf("startup claimed absent homes: %+v", state.Assets)
	}
	class, err := hub.DeclareClass(semhub.ClassSpec{
		Name: "opr_discovered", Driver: "driver-opr-discovered", Graph: true,
		Validators: func() []semreg.PackValidator { return []semreg.PackValidator{pv.New()} },
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []semreg.AssetID{discovered, paired, lookalike} {
		source := semreg.SourceID("source:opr-discovered-" + strconv.Itoa(i))
		if _, err := class.Declare(id, source); err != nil {
			t.Fatalf("dynamic driver could not claim %s: %v", id, err)
		}
	}
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: boiler, SourceID: "source:synthetic-boiler", Driver: string(primaryEBusDriverID), Validators: []semreg.PackValidator{pv.New()}, Graph: true}); err != nil {
		t.Fatalf("a lazily declaring driver could not claim its home: %v", err)
	}
	found := map[semreg.AssetID]string{}
	state, err = hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range state.Assets {
		found[asset.Asset] = asset.Driver
	}
	if found[discovered] != "driver-opr-discovered" || found[paired] != "driver-opr-discovered" || found[lookalike] != "driver-opr-discovered" {
		t.Fatalf("discovered homes=%v", found)
	}
	if found[boiler] != string(primaryEBusDriverID) {
		t.Fatalf("boiler home driver=%q, want %s", found[boiler], primaryEBusDriverID)
	}
	if _, claimed := found[oprHomeAsset]; claimed {
		t.Fatalf("startup claimed %s for %s", oprHomeAsset, found[oprHomeAsset])
	}
}

// Startup reports a configuration-only home that no declaration names, and
// only that home: a relation home and a driver-owned home are not reported.
// The configuration-only mark is what selects the report.
func TestOperatorRecordCompositionReportsAnUndeclaredConfigurationOnlyHome(t *testing.T) {
	root := t.TempDir()
	oprWriteSection(t, root, `{"format":"helianthus.portal-config.operator-records/v1","revision":"1","records":[{"form":"anchor","home":"asset:opr-configuration","resource_id":"resource:opr:array-1","kind":"resource.pv_array","slot":"array-south"},{"form":"anchor","home":"asset:opr-inverter","resource_id":"resource:opr:array-2","kind":"resource.pv_array","slot":"array-north"},{"form":"relation","home":"asset:opr-relation-only","definition":{"pack":{"id":"helianthus.pack.pv","version":"1.0.0"},"id":"pv.relation.inverter_array","version":"1.0.0"},"from":"resource:opr:inverter-r","to":"resource:opr:array-r","basis_class":"configuration"}]}`)
	records, err := configuredOperatorRecords(ebusgateway.Config{PortalConfigRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	marked := map[semreg.AssetID]bool{}
	for _, set := range records {
		marked[set.Asset] = set.ConfigurationOnly
	}
	if !marked[oprHomeAsset] || marked["asset:opr-relation-only"] {
		t.Fatalf("configuration-only marks=%v", marked)
	}
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: oprDriverAsset, SourceID: oprDriverSource, Driver: oprDriver, Validators: []semreg.PackValidator{pv.New()}, Graph: true}); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	if err := composeOperatorRecords(hub, ebusgateway.Config{PortalConfigRoot: root}); err != nil {
		t.Fatal(err)
	}
	report := func(home semreg.AssetID) bool {
		for _, line := range strings.Split(logged.String(), "\n") {
			if strings.Contains(line, string(home)+" wait until an owner declares that home") {
				return true
			}
		}
		return false
	}
	if !report(oprHomeAsset) {
		t.Fatalf("log=%q, want the undeclared configuration-only home reported", logged.String())
	}
	if report("asset:opr-relation-only") || report(oprDriverAsset) {
		t.Fatalf("log=%q, want no report for a relation home or a driver-owned home", logged.String())
	}
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range state.Assets {
		if asset.Asset == oprHomeAsset {
			t.Fatalf("startup declared %s for %s", oprHomeAsset, asset.Driver)
		}
	}
}

// oprLinkFixture declares the driver asset without any contributor, admits
// the driver and publishes its binding of oprEvidence("oprtest.native"). The
// configuration provider is not declared anywhere, so an identity link
// cannot be published by it.
func oprLinkFixture(t *testing.T) *oprFixture {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := operatorPublisherFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Declare(semhub.AssetSpec{AssetID: oprDriverAsset, SourceID: oprDriverSource, Driver: oprDriver, Validators: []semreg.PackValidator{pv.New()}, Graph: true}); err != nil {
		t.Fatal(err)
	}
	handle, err := hub.AdmitSource(semhub.Admission{Source: oprDriverSource, Driver: oprDriver})
	if err != nil {
		t.Fatal(err)
	}
	f := &oprFixture{hub: hub, publisher: publisher, driver: handle, clock: 100}
	if err := f.publishDriver(t, nil); err != nil {
		t.Fatalf("driver publication: %v", err)
	}
	return f
}

// oprPendingLinks returns the operator identity links the driver's next
// transaction must carry.
func oprPendingLinks(t *testing.T, f *oprFixture) []semreg.IdentityLink {
	t.Helper()
	tx, err := f.hub.Begin(f.driver, oprDriverAsset)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	return tx.OperatorLinks(nil)
}

// An identity link is declared to the hub and not published. Its state says
// so, rather than waiting as if the declaration had not been applied.
func TestOperatorRecordIdentityLinkStateIsAppliedAndUnpublished(t *testing.T) {
	f := oprLinkFixture(t)
	hub, publisher := f.hub, f.publisher
	native := oprEvidence("oprtest.native")
	set := portalconfig.OperatorRecordSet{
		Format: portalconfig.OperatorRecordsFormat, Revision: "1",
		Records: []portalconfig.OperatorRecord{{
			Form: portalconfig.OperatorFormIdentityLink, Home: string(oprDriverAsset), ResourceID: string(oprInverter), BasisClass: string(semreg.RelationConfiguration), Native: &native,
		}},
	}
	operators := &portalOperatorRecords{publisher: publisher, desired: map[semreg.AssetID]operatorAssetRecords{}, outcomes: map[semreg.AssetID]portalOperatorOutcome{}}
	operators.apply(set)
	runtime := &gatewayPortalConfigRuntime{operators: operators}
	states := runtime.operatorRecordStates(set)
	if len(states) != 1 || states[0].Status != portalconfig.OperatorRecordWaiting || states[0].Reason != portalOperatorIdentityLinkPending {
		t.Fatalf("identity link state=%+v, want waiting %s", states, portalOperatorIdentityLinkPending)
	}
	operators.retry()
	states = runtime.operatorRecordStates(set)
	if len(states) != 1 || states[0].Reason != portalOperatorIdentityLinkPending {
		t.Fatalf("identity link state after retry=%+v", states)
	}
	if _, ok := hub.Source(operatorRecordSource); ok {
		t.Fatal("an identity link admitted the configuration source")
	}
	if links := oprPendingLinks(t, f); len(links) != 1 {
		t.Fatalf("pending links=%+v, want the configured link declared for the driver's source", links)
	}
}

// A configured identity link is declared to the hub for the source whose
// binding observes its native resource, so that source's next transaction
// carries it. A link whose native resource no binding observes waits, and a
// link the configuration no longer holds is withdrawn from the hub.
func TestOperatorRecordIdentityLinkIsDeclaredForTheBindingSource(t *testing.T) {
	f := oprLinkFixture(t)
	audit := oprAudit("identity-link")
	link := operatorIdentityLink{Native: oprEvidence("oprtest.native"), Resource: oprInverter, BasisClass: string(semreg.RelationConfiguration)}
	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Links: []operatorIdentityLink{link}}, audit); err != nil {
		t.Fatalf("set identity link: %v", err)
	}
	links := oprPendingLinks(t, f)
	if len(links) != 1 || links[0].Resource == nil || *links[0].Resource != oprInverter || links[0].State != semreg.LinkQualified {
		t.Fatalf("pending links=%+v, want the configured link of the inverter", links)
	}
	if len(links[0].Basis) != 1 || links[0].Basis[0] != audit.evidence() || links[0].Basis[0].Owner != semhub.OperatorEvidenceOwner {
		t.Fatalf("link basis=%+v, want the declaration's audit evidence", links[0].Basis)
	}
	if _, ok := f.hub.Source(operatorRecordSource); ok {
		t.Fatal("declaring an identity link admitted the configuration source")
	}

	unobserved := operatorIdentityLink{Native: oprEvidence("oprtest.native-unobserved"), Resource: oprInverter, BasisClass: string(semreg.RelationConfiguration)}
	err := f.publisher.Set(oprDriverAsset, operatorRecords{Links: []operatorIdentityLink{unobserved}}, audit)
	var refusal *operatorRecordRefusal
	if !errors.As(err, &refusal) || refusal.Reason != operatorReasonNativeUnobserved || !operatorRecordDeferred(err) {
		t.Fatalf("set of an unobserved native=%v, want a deferred %s", err, operatorReasonNativeUnobserved)
	}
	if outcome := portalOperatorClassify(err); outcome.status != portalconfig.OperatorRecordWaiting || outcome.reason != portalOperatorNativeUnobserved || !outcome.retry {
		t.Fatalf("outcome=%+v, want a retried wait %s", outcome, portalOperatorNativeUnobserved)
	}
	if links := oprPendingLinks(t, f); len(links) != 1 {
		t.Fatalf("pending links after a deferred set=%+v, want the declared link kept", links)
	}

	if err := f.publisher.Set(oprDriverAsset, operatorRecords{}, audit); err != nil {
		t.Fatalf("withdraw identity link: %v", err)
	}
	if links := oprPendingLinks(t, f); len(links) != 0 {
		t.Fatalf("pending links after the withdrawal=%+v, want none", links)
	}
}

// An identity link that shares its home with a published relation still
// waits: the relation's publication does not publish the link.
func TestOperatorRecordIdentityLinkInAMixedHomeStaysPending(t *testing.T) {
	f := newOprFixture(t)
	native := oprEvidence("oprtest.native")
	set := portalconfig.OperatorRecordSet{
		Format: portalconfig.OperatorRecordsFormat, Revision: "1",
		Records: []portalconfig.OperatorRecord{
			opr27Relation(oprDriverAsset, "pv.relation.inverter_array", oprInverter, oprArray),
			{Form: portalconfig.OperatorFormIdentityLink, Home: string(oprDriverAsset), ResourceID: string(oprInverter), BasisClass: string(semreg.RelationConfiguration), Native: &native},
		},
	}
	operators := &portalOperatorRecords{publisher: f.publisher, desired: map[semreg.AssetID]operatorAssetRecords{}, outcomes: map[semreg.AssetID]portalOperatorOutcome{}}
	operators.apply(set)
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), oprInverterArray(oprArray)); !ok {
		t.Fatal("the configured relation was not published")
	}
	runtime := &gatewayPortalConfigRuntime{operators: operators}
	states := runtime.operatorRecordStates(set)
	if len(states) != 2 || states[0].Status != portalconfig.OperatorRecordPublished {
		t.Fatalf("states=%+v, want the relation published", states)
	}
	if states[1].Status != portalconfig.OperatorRecordWaiting || states[1].Reason != portalOperatorIdentityLinkPending {
		t.Fatalf("identity link state=%+v, want waiting %s", states[1], portalOperatorIdentityLinkPending)
	}
	if links := oprPendingLinks(t, f); len(links) != 1 {
		t.Fatalf("pending links=%+v, want the configured link declared", links)
	}
}

// A home that combines a resource or a relation with an identity link whose
// native binding is not yet observed refuses the whole set before anything is
// submitted. The new records do not reach the graph, and a replacement does
// not withdraw records that were already published. The home waits.
func TestOperatorRecordUnobservedLinkRefusesTheWholeSetBeforePublish(t *testing.T) {
	f := newOprFixture(t)
	audit := oprAudit("mixed-unobserved")
	relation := oprInverterArray(oprArray)
	unobserved := operatorIdentityLink{Native: oprEvidence("oprtest.native-unobserved"), Resource: oprInverter, BasisClass: string(semreg.RelationConfiguration)}
	err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}, Links: []operatorIdentityLink{unobserved}}, audit)
	var refusal *operatorRecordRefusal
	if !errors.As(err, &refusal) || refusal.Reason != operatorReasonNativeUnobserved || !operatorRecordDeferred(err) {
		t.Fatalf("mixed set=%v, want a deferred %s", err, operatorReasonNativeUnobserved)
	}
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), relation); ok {
		t.Fatal("an unobserved identity link published the relation of the same set")
	}
	if desired := f.publisher.Desired(oprDriverAsset); len(desired.Relations) != 0 || len(desired.Resources) != 0 {
		t.Fatalf("desired=%+v, want the refused set left unset", desired)
	}
	if links := oprPendingLinks(t, f); len(links) != 0 {
		t.Fatalf("pending links=%+v, want none declared", links)
	}

	if err := f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{relation}}, audit); err != nil {
		t.Fatalf("publish the prior relation: %v", err)
	}
	replacement := oprInverterArray(oprArraySecond)
	err = f.publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{replacement}, Links: []operatorIdentityLink{unobserved}}, audit)
	if !errors.As(err, &refusal) || refusal.Reason != operatorReasonNativeUnobserved {
		t.Fatalf("replacement=%v, want a deferred %s", err, operatorReasonNativeUnobserved)
	}
	snapshot := f.snapshot(t, oprDriverAsset)
	if _, ok := oprRelationIn(snapshot, relation); !ok {
		t.Fatal("the unobserved link withdrew the prior relation")
	}
	if _, ok := oprRelationIn(snapshot, replacement); ok {
		t.Fatal("the unobserved link published the replacement relation")
	}
	if desired := f.publisher.Desired(oprDriverAsset); len(desired.Relations) != 1 || desired.Relations[0].Record.To != oprArray {
		t.Fatalf("desired=%+v, want the prior relation kept", desired.Relations)
	}
	if links := oprPendingLinks(t, f); len(links) != 0 {
		t.Fatalf("pending links after the refused replacement=%+v, want none", links)
	}

	resource := oprArrayResource(oprArray, "array-1")
	if err := f.publisher.Set(oprHomeAsset, operatorRecords{Resources: []operatorResource{resource}}, audit); err != nil {
		t.Fatalf("publish the prior resource: %v", err)
	}
	err = f.publisher.Set(oprHomeAsset, operatorRecords{Links: []operatorIdentityLink{unobserved}}, audit)
	if !errors.As(err, &refusal) || refusal.Reason != operatorReasonNativeUnobserved {
		t.Fatalf("resource withdrawal=%v, want a deferred %s", err, operatorReasonNativeUnobserved)
	}
	held, ok := oprResourceIn(f.snapshot(t, oprHomeAsset), oprArray)
	if !ok || held.State != semreg.ResourceCurrent {
		t.Fatalf("resource=%+v present=%v, want the prior resource still current", held, ok)
	}
	if desired := f.publisher.Desired(oprHomeAsset); len(desired.Resources) != 1 || desired.Resources[0].Record.ID != oprArray {
		t.Fatalf("desired resources=%+v, want the prior resource kept", desired.Resources)
	}
}

// The portal reports every record of a mixed home as waiting when its
// identity link is unobserved, and it does not publish the home's relation
// while that wait stands.
func TestOperatorRecordMixedHomeWaitsWithoutPublishingAnUnobservedLink(t *testing.T) {
	f := newOprFixture(t)
	native := oprEvidence("oprtest.native-unobserved")
	set := portalconfig.OperatorRecordSet{
		Format: portalconfig.OperatorRecordsFormat, Revision: "1",
		Records: []portalconfig.OperatorRecord{
			opr27Relation(oprDriverAsset, "pv.relation.inverter_array", oprInverter, oprArray),
			{Form: portalconfig.OperatorFormIdentityLink, Home: string(oprDriverAsset), ResourceID: string(oprInverter), BasisClass: string(semreg.RelationConfiguration), Native: &native},
		},
	}
	operators := &portalOperatorRecords{publisher: f.publisher, desired: map[semreg.AssetID]operatorAssetRecords{}, outcomes: map[semreg.AssetID]portalOperatorOutcome{}}
	operators.apply(set)
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), oprInverterArray(oprArray)); ok {
		t.Fatal("a waiting home published its relation")
	}
	runtime := &gatewayPortalConfigRuntime{operators: operators}
	states := runtime.operatorRecordStates(set)
	if len(states) != 2 {
		t.Fatalf("states=%+v, want both records of the home", states)
	}
	for _, state := range states {
		if state.Status != portalconfig.OperatorRecordWaiting || state.Reason != portalOperatorNativeUnobserved {
			t.Fatalf("state=%+v, want waiting %s", state, portalOperatorNativeUnobserved)
		}
	}
}

func oprRelationSection(revision string, to semreg.ResourceID) portalconfig.OperatorRecordSet {
	return portalconfig.OperatorRecordSet{
		Format:   portalconfig.OperatorRecordsFormat,
		Revision: revision,
		Records:  []portalconfig.OperatorRecord{opr27Relation(oprDriverAsset, "pv.relation.inverter_array", oprInverter, to)},
	}
}

func oprComposeConfiguredRelation(t *testing.T, to semreg.ResourceID) (*semhub.Hub, *operatorRecordPublisher) {
	t.Helper()
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	body, err := json.Marshal(oprRelationSection("1", to))
	if err != nil {
		t.Fatal(err)
	}
	oprWriteSection(t, root, string(body))
	if err := composeOperatorRecords(hub, ebusgateway.Config{PortalConfigRoot: root}); err != nil {
		t.Fatal(err)
	}
	publisher, err := operatorPublisherFor(hub)
	if err != nil {
		t.Fatal(err)
	}
	if pending := publisher.Pending(); len(pending) != 1 || pending[0] != oprDriverAsset {
		t.Fatalf("pending=%v, want the undeclared home", pending)
	}
	return hub, publisher
}

func oprDeclareDriverHome(t *testing.T, hub *semhub.Hub, publisher *operatorRecordPublisher) *oprFixture {
	t.Helper()
	if _, err := hub.Declare(semhub.AssetSpec{
		AssetID: oprDriverAsset, SourceID: oprDriverSource, Driver: oprDriver, Validators: []semreg.PackValidator{pv.New()}, Graph: true,
		Contributors: []semhub.ContributorSpec{publisher.Contributor(pv.New())},
	}); err != nil {
		t.Fatal(err)
	}
	handle, err := hub.AdmitSource(semhub.Admission{Source: oprDriverSource, Driver: oprDriver})
	if err != nil {
		t.Fatal(err)
	}
	f := &oprFixture{hub: hub, publisher: publisher, driver: handle, clock: 100}
	if err := f.publishDriver(t, nil); err != nil {
		t.Fatalf("driver publication: %v", err)
	}
	return f
}

// Set replaces a startup-pending record of the same asset and drops one whose
// replacement is empty. It does not create a pending entry for an asset that
// was not already pending.
func TestOperatorRecordPublisherSetSupersedesAPendingRecord(t *testing.T) {
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := newOperatorRecordPublisher(hub, operatorRecordOptions{})
	if err != nil {
		t.Fatal(err)
	}
	audit := oprAudit("pending-replacement")
	old := oprInverterArray(oprArray)
	replacement := oprInverterArray(oprArraySecond)
	publisher.mu.Lock()
	publisher.pending = map[semreg.AssetID]operatorAssetRecords{
		oprDriverAsset: {Asset: oprDriverAsset, Records: operatorRecords{Relations: []operatorRelation{old}}, Audit: audit},
	}
	publisher.mu.Unlock()
	if err := publisher.Set(oprDriverAsset, operatorRecords{Relations: []operatorRelation{replacement}}, audit); !errors.Is(err, semhub.ErrUnknownAsset) {
		t.Fatalf("set=%v, want the home still unknown", err)
	}
	publisher.mu.Lock()
	got := publisher.pending[oprDriverAsset]
	publisher.mu.Unlock()
	if len(got.Records.Relations) != 1 || got.Records.Relations[0].To != oprArraySecond {
		t.Fatalf("pending=%+v, want the replacement relation", got.Records.Relations)
	}
	if err := publisher.Set(oprHomeAsset, operatorRecords{Relations: []operatorRelation{replacement}}, audit); !errors.Is(err, semhub.ErrUnknownAsset) {
		t.Fatalf("set of a home that was not pending=%v", err)
	}
	publisher.mu.Lock()
	_, resurrected := publisher.pending[oprHomeAsset]
	publisher.mu.Unlock()
	if resurrected {
		t.Fatal("Set created a pending entry for a home that was not pending")
	}
	if err := publisher.Set(oprDriverAsset, operatorRecords{}, audit); err != nil {
		t.Fatalf("empty set=%v", err)
	}
	if pending := publisher.Pending(); len(pending) != 0 {
		t.Fatalf("pending=%v, want the empty set to drop it", pending)
	}
}

// A Portal section that does not hold the startup relation supersedes the
// startup pending set, including a home the Portal runtime never published.
// The startup watcher does not publish that relation when the driver asset
// appears later.
func TestOperatorRecordPortalReplacementDropsAStartupPendingRelation(t *testing.T) {
	hub, publisher := oprComposeConfiguredRelation(t, oprArray)
	runtime := opr27Runtime(t)
	if runtime.callbacks(ebusgateway.Config{}, &gatewayRuntimeControl{hub: hub}).OperatorRecords == nil {
		t.Fatal("the runtime serves no operator-record section")
	}
	if pending := publisher.Pending(); len(pending) != 0 {
		t.Fatalf("pending=%v after the Portal section omitted the relation", pending)
	}
	f := oprDeclareDriverHome(t, hub, publisher)
	if err := publisher.Retry(); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), oprInverterArray(oprArray)); ok {
		t.Fatal("a relation the Portal section omitted was published when its home appeared")
	}
	if pending := publisher.Pending(); len(pending) != 0 {
		t.Fatalf("pending=%v after the home appeared", pending)
	}
}

// A Portal replacement of a relation that is still waiting replaces the
// startup pending set. The relation published when the home appears is the
// replacement, not the one startup loaded.
func TestOperatorRecordPortalReplacementSupersedesAStartupPendingRelation(t *testing.T) {
	hub, publisher := oprComposeConfiguredRelation(t, oprArray)
	runtime := opr27Runtime(t)
	seeded, err := runtime.operatorRecordCallbacks().Replace(context.Background(), oprRelationSection("0", oprArray))
	if err != nil {
		t.Fatal(err)
	}
	callbacks := runtime.callbacks(ebusgateway.Config{}, &gatewayRuntimeControl{hub: hub}).OperatorRecords
	if callbacks == nil {
		t.Fatal("the runtime serves no operator-record section")
	}
	replaced, err := callbacks.Replace(context.Background(), oprRelationSection(seeded.Revision, oprArraySecond))
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Revision == seeded.Revision {
		t.Fatal("the replacement did not advance the section")
	}
	publisher.mu.Lock()
	got := publisher.pending[oprDriverAsset]
	publisher.mu.Unlock()
	if len(got.Records.Relations) != 1 || got.Records.Relations[0].To != oprArraySecond {
		t.Fatalf("pending=%+v, want the replacement relation", got.Records.Relations)
	}
	f := oprDeclareDriverHome(t, hub, publisher)
	if err := publisher.Retry(); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), oprInverterArray(oprArraySecond)); !ok {
		t.Fatal("the replacement relation was not published after its home appeared")
	}
	if _, ok := oprRelationIn(f.snapshot(t, oprDriverAsset), oprInverterArray(oprArray)); ok {
		t.Fatal("the startup relation was published after the operator replaced it")
	}
}
