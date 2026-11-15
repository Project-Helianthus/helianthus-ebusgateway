package semhub

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
)

// The selected SemReg module carries the graph contract: the element catalog
// read through the root package's own interfaces, the graph-enforced kernel
// constructor and the graph record types. Each assertion fails to compile
// against a module without them.
var (
	_ semreg.GraphCatalog         = (*graph.Catalog)(nil)
	_ semreg.GraphResourceCatalog = (*graph.Catalog)(nil)
	_                             = semreg.NewGraphPublicationKernel
	_                             = semreg.NewGraphRegistry
	_                             = semreg.PublicationBatch{ResourceUpserts: []semreg.Resource{}, RelationUpserts: []semreg.RelationInstance{}, RelationWithdrawals: []semreg.RelationKey{}, AbsenceUpserts: []semreg.RelationAbsence{}, AbsenceWithdrawals: []semreg.RelationAbsenceKey{}}
	_                             = semreg.Snapshot{Resources: []semreg.Resource{}, Relations: []semreg.RelationInstance{}, RelationAbsences: []semreg.RelationAbsence{}}
)

const graphTestResource semreg.ResourceID = "resource:semhubtest:a"

func graphTestOrigin() semreg.OriginRef {
	return semreg.OriginRef{OriginID: "origin:semhubtest:operator", Kind: semreg.OriginOperator, Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.operator")}}
}

func graphTestSlotResource() semreg.Resource {
	slot := "slot-1"
	return semreg.Resource{ResourceID: graphTestResource, Kind: "resource.equipment", Anchor: semreg.ResourceAnchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Slot: &slot, Evidence: []semreg.EvidenceRef{testEvidence("semhubtest.anchor")}}, State: semreg.ResourceCurrent, Origin: graphTestOrigin(), Revision: "1"}
}

// graphTestIdentityKey digests a synthetic identity object, so the test holds
// no literal digest.
func graphTestIdentityKey() semreg.Digest {
	sum := sha256.Sum256([]byte(`{"issuer":"","scheme":"scheme.semhubtest","token":"token-1"}`))
	return semreg.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// graphMemberVariants adds exactly one graph member to an otherwise
// acceptable batch of the synthetic family. A changed link or service keeps
// the revision the batch already carries, so only the graph member differs.
func graphMemberVariants() map[string]func(*testing.T, *semreg.PublicationBatch) {
	relation := testRef("semhubtest.relation.contains")
	absence := testRef("semhubtest.relation.supplies")
	return map[string]func(*testing.T, *semreg.PublicationBatch){
		"resource upsert": func(_ *testing.T, b *semreg.PublicationBatch) {
			b.ResourceUpserts = []semreg.Resource{graphTestSlotResource()}
		},
		"relation upsert": func(_ *testing.T, b *semreg.PublicationBatch) {
			b.RelationUpserts = []semreg.RelationInstance{{Definition: relation, From: graphTestResource, To: "resource:semhubtest:b", State: semreg.LinkQualified, BasisClass: semreg.RelationConfiguration, Basis: []semreg.EvidenceRef{testEvidence("semhubtest.relation")}, Origin: graphTestOrigin(), Revision: "1"}}
		},
		"relation withdrawal": func(_ *testing.T, b *semreg.PublicationBatch) {
			b.RelationWithdrawals = []semreg.RelationKey{{Definition: relation, From: graphTestResource, To: "resource:semhubtest:b"}}
		},
		"absence upsert": func(_ *testing.T, b *semreg.PublicationBatch) {
			b.AbsenceUpserts = []semreg.RelationAbsence{{Definition: absence, Resource: graphTestResource, Side: semreg.AbsenceTo, State: semreg.LinkCandidate, BasisClass: semreg.RelationConfiguration, Basis: []semreg.EvidenceRef{testEvidence("semhubtest.absence")}, Origin: graphTestOrigin(), Revision: "1"}}
		},
		"absence withdrawal": func(_ *testing.T, b *semreg.PublicationBatch) {
			b.AbsenceWithdrawals = []semreg.RelationAbsenceKey{{Definition: absence, Resource: graphTestResource, Side: semreg.AbsenceTo}}
		},
		"identity link resource": func(t *testing.T, b *semreg.PublicationBatch) {
			if len(b.IdentityLinkUpserts) != 1 {
				t.Fatalf("identity links=%d", len(b.IdentityLinkUpserts))
			}
			resource := graphTestResource
			b.IdentityLinkUpserts[0].Resource = &resource
		},
		"identity link key": func(t *testing.T, b *semreg.PublicationBatch) {
			if len(b.IdentityLinkUpserts) != 1 {
				t.Fatalf("identity links=%d", len(b.IdentityLinkUpserts))
			}
			key := graphTestIdentityKey()
			b.IdentityLinkUpserts[0].Key = &key
		},
		"service resource": func(t *testing.T, b *semreg.PublicationBatch) {
			if len(b.ServiceUpserts) != 1 {
				t.Fatalf("services=%d", len(b.ServiceUpserts))
			}
			resource := graphTestResource
			b.ServiceUpserts[0].Resource = &resource
		},
	}
}

func sealGraphTestBatch(t *testing.T, batch *semreg.PublicationBatch) {
	t.Helper()
	batch.BatchDigest = ""
	digest, err := batch.ComputedDigest()
	if err != nil {
		t.Fatal(err)
	}
	batch.BatchDigest = digest
	if err := batch.Validate(); err != nil {
		t.Fatalf("graph member is not a valid record: %v", err)
	}
}

// applyGraphMember stages the next batch of the synthetic family with one
// graph member and returns the error of the hub transaction's Apply. The
// transaction is aborted; a refused batch must leave no trace.
func applyGraphMember(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, change func(*testing.T, *semreg.PublicationBatch), value, at int64) error {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	batch := testBatch(t, tx, "fam", value, at)
	change(t, &batch)
	sealGraphTestBatch(t, &batch)
	_, _, err = tx.Apply(batch, testMono(at))
	if err == nil {
		if _, commitErr := tx.Commit(nil); commitErr != nil {
			t.Fatalf("graph member applied but commit failed: %v", commitErr)
		}
	}
	return err
}

// The element catalog compiled into the selected module loads, and a registry
// built with it is graph-enforced and reports that catalog's digest. No hub
// kernel is built this way yet.
func TestSemRegGraphDependencyExposesTheElementCatalog(t *testing.T) {
	catalog, err := graph.Embedded()
	if err != nil {
		t.Fatalf("embedded element catalog: %v", err)
	}
	registry, err := graph.Enforced()
	if err != nil {
		t.Fatalf("graph-enforced registry: %v", err)
	}
	digest, ok := registry.CatalogDigest()
	if !registry.GraphEnforced() || !ok || digest != catalog.Digest() {
		t.Fatalf("graph-enforced registry=%v digest=%q/%v want %q", registry.GraphEnforced(), digest, ok, catalog.Digest())
	}
	plain, err := semreg.NewRegistry(newTestValidator())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.CatalogDigest(); plain.GraphEnforced() || ok {
		t.Fatal("a registry built without the element catalog reports graph enforcement")
	}
	// The synthetic hub pack has no catalog entry, so a graph-enforced kernel
	// for it cannot be built: graph enforcement is switched on per family.
	if _, err := semreg.NewGraphPublicationKernel("asset:a", catalog, newTestValidator()); errorID(err) != semreg.MembershipMissing {
		t.Fatalf("graph kernel for an uncovered pack=%v", err)
	}
}

// A hub-declared kernel is built without the element catalog. It refuses a
// batch carrying any graph member with membership_missing, on the first batch
// of an incarnation and on a later one, and changes no snapshot, cursor or
// event; the same batch without the graph member is published as before.
func TestSemRegGraphDependencyHubKernelRefusesGraphMembers(t *testing.T) {
	for name, change := range graphMemberVariants() {
		t.Run(name, func(t *testing.T) {
			hub := newTestHub(t)
			mustDeclare(t, hub, "asset:a", "source:a", "driver-a", nil)
			handle := admitLegacy(t, hub, "source:a", "driver-a", "epoch:a", 1)
			cursor := hub.Cursor()
			if err := applyGraphMember(t, hub, handle, "asset:a", change, 1, 1); errorID(err) != semreg.MembershipMissing {
				t.Fatalf("first batch with %s=%v", name, err)
			}
			if view, err := hub.Inspect("asset:a"); err != nil || view.Present || hub.Cursor() != cursor {
				t.Fatalf("refused first batch left state: present=%v err=%v", view.Present, err)
			}
			if events, err := hub.Since(cursor); err != nil || len(events) != 0 {
				t.Fatalf("refused first batch emitted %d events (%v)", len(events), err)
			}

			// The refused batch consumed no sequence.
			if next, err := hub.NextSequence("asset:a", "source:a", "epoch:a", 1); err != nil || next != "1" {
				t.Fatalf("next sequence after refusal=%q/%v", next, err)
			}
			mustPublish(t, hub, handle, "asset:a", "fam", 1, 1)
			before, err := hub.Inspect("asset:a")
			if err != nil {
				t.Fatal(err)
			}
			cursor = hub.Cursor()
			// A later batch carries no link or service, so it re-publishes the
			// accepted one with the graph member and the next revision.
			later := change
			switch name {
			case "identity link resource", "identity link key":
				later = func(t *testing.T, b *semreg.PublicationBatch) {
					link := before.Snapshot.IdentityLinks[0]
					link.Revision = "2"
					b.IdentityLinkUpserts = []semreg.IdentityLink{link}
					change(t, b)
				}
			case "service resource":
				later = func(t *testing.T, b *semreg.PublicationBatch) {
					service := before.Snapshot.Services[0]
					service.Revision = "2"
					b.ServiceUpserts = []semreg.ServiceInstance{service}
					change(t, b)
				}
			}
			if err := applyGraphMember(t, hub, handle, "asset:a", later, 2, 2); errorID(err) != semreg.MembershipMissing {
				t.Fatalf("later batch with %s=%v", name, err)
			}
			after, err := hub.Inspect("asset:a")
			if err != nil || after.Snapshot.SnapshotID != before.Snapshot.SnapshotID || string(after.Canonical) != string(before.Canonical) || hub.Cursor() != cursor {
				t.Fatalf("refused later batch changed the snapshot, cursor or events (err=%v)", err)
			}
			if len(after.Snapshot.Resources) != 0 || len(after.Snapshot.Relations) != 0 || len(after.Snapshot.RelationAbsences) != 0 {
				t.Fatal("a graph member entered the snapshot")
			}
			mustPublish(t, hub, handle, "asset:a", "fam", 2, 2)
		})
	}
}
