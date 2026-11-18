package semhub

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The operator-link fixture is one graph-enforced PV asset of one driver. The
// driver's binding observes the native resource opNative; the asset holds two
// operator-slot inverter resources, so a declaration can name either.
const (
	opAsset     semreg.AssetID    = "asset:operator-links"
	opSource    semreg.SourceID   = "source:operator-links"
	opDriver                      = "driver-operator-links"
	opInverter2 semreg.ResourceID = "resource:semhubtest:inverter-2"
)

// opDigest digests a synthetic label, so the test holds no literal digest.
func opDigest(label string) semreg.Digest {
	sum := sha256.Sum256([]byte("semhubtest.operator-links\x00" + label))
	return semreg.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// opNative is the native resource evidence of the driver's binding; label
// varies it so two bindings of one batch observe different native resources.
func opNative(label string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: testPackRef.ID, Kind: "semhubtest.native", Digest: opDigest("native/" + label), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

// opAudit is the audit evidence of one operator declaration.
func opAudit(label string) semreg.EvidenceRef {
	return semreg.EvidenceRef{Owner: OperatorEvidenceOwner, Kind: "configuration.operator_declaration", Digest: opDigest("audit/" + label), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

func opInverter2Resource() semreg.Resource {
	resource := graphInverterResource()
	slot := "inverter-2"
	resource.ResourceID, resource.Anchor.Slot = opInverter2, &slot
	return resource
}

// opDeclaration declares that the binding observing native observes resource.
func opDeclaration(native string, resource semreg.ResourceID, audit string) OperatorLink {
	return OperatorLink{Native: opNative(native), Resource: resource, Basis: []semreg.EvidenceRef{opAudit(audit)}}
}

func opBindingID(t *testing.T, tx *Tx, label string) semreg.NativeBindingID {
	t.Helper()
	inc := tx.Incarnation()
	id, err := NativeBindingID("operator_links", inc.Asset, inc.Source, "semhubtest.profile."+label, inc.Epoch, inc.Generation)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// opBatch builds one driver publication from the staged transaction state.
// The first batch of an incarnation carries the source descriptor and one
// binding per native label, each with the driver's own native identity link
// (no resource, no operator basis). resources are upserted as given. The
// batch carries no fact, so it is about identity only.
func opBatch(t *testing.T, tx *Tx, at int64, natives []string, resources ...semreg.Resource) semreg.PublicationBatch {
	t.Helper()
	inc := tx.Incarnation()
	current, exists := tx.Current()
	generation := semreg.Uint64(strconv.FormatUint(inc.Generation, 10))
	expected := semreg.Uint64("0")
	if exists {
		expected = current.Revisions.Semantic
	}
	hasSource := false
	for _, existing := range current.Sources {
		hasSource = hasSource || existing.SourceID == inc.Source && existing.SourceEpochID == inc.Epoch
	}
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: semreg.BatchID("batch:operator_links:" + idHash("batch", string(inc.Asset), string(inc.Source), string(inc.Epoch), string(generation), string(tx.NextSequence()))), AssetID: inc.Asset, SourceID: inc.Source, SourceEpochID: inc.Epoch, DriverGeneration: generation, Sequence: tx.NextSequence(), ExpectedSemanticRevision: expected, ObservedAt: testWall(at),
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: tx.SourceRetirements(), BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, ResourceUpserts: resources, FactUpserts: []semreg.FactCandidate{}, FactWithdrawals: []semreg.CandidateID{},
		ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	if !hasSource {
		batch.SourceUpserts = []semreg.SourceDescriptor{{SourceID: inc.Source, SourceEpochID: inc.Epoch, ProtocolID: "semhubtest_protocol", ProfileID: "semhubtest.profile.v1", ProfileVersion: "1", RegistryEvidence: testEvidence("semhubtest.registry"), StartedAt: testWall(0), State: semreg.SourceCurrent, Revision: "1"}}
	}
	for _, label := range natives {
		binding := opBindingID(t, tx, label)
		known := false
		for _, existing := range current.Bindings {
			known = known || existing.BindingID == binding
		}
		if known {
			continue
		}
		batch.BindingUpserts = append(batch.BindingUpserts, semreg.NativeBinding{BindingID: binding, AssetID: inc.Asset, SourceID: inc.Source, SourceEpochID: inc.Epoch, DriverGeneration: generation, NativeResource: opNative(label), State: semreg.BindingCurrent, Revision: "1"})
		batch.IdentityLinkUpserts = append(batch.IdentityLinkUpserts, semreg.IdentityLink{AssetID: inc.Asset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{testEvidence("semhubtest.observation")}, Revision: "1"})
	}
	sortBindings(batch.BindingUpserts)
	sortLinks(batch.IdentityLinkUpserts)
	if len(batch.ResourceUpserts) == 0 {
		batch.ResourceUpserts = nil
	}
	return batch
}

func sortBindings(bindings []semreg.NativeBinding) {
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].BindingID < bindings[j].BindingID })
}

func sortLinks(links []semreg.IdentityLink) {
	sort.Slice(links, func(i, j int) bool { return links[i].BindingID < links[j].BindingID })
}

// carry is what an adopting publisher does: every operator link the Tx
// exposes replaces the publisher's own link of that binding.
func carry(tx *Tx, batch *semreg.PublicationBatch) []semreg.IdentityLink {
	pending := tx.OperatorLinks(batch.BindingUpserts)
	replaced := make(map[semreg.NativeBindingID]bool, len(pending))
	for _, link := range pending {
		replaced[link.BindingID] = true
	}
	links := append([]semreg.IdentityLink{}, pending...)
	for _, link := range batch.IdentityLinkUpserts {
		if !replaced[link.BindingID] {
			links = append(links, link)
		}
	}
	sortLinks(links)
	batch.IdentityLinkUpserts = links
	return pending
}

// commitOp applies and commits batch. An Apply refusal is returned as is; a
// Commit refusal is returned after the transaction has finished.
func commitOp(t *testing.T, tx *Tx, batch semreg.PublicationBatch, at int64) (Outcome, error) {
	t.Helper()
	sealGraphTestBatch(t, &batch)
	if _, _, err := tx.Apply(batch, testMono(at)); err != nil {
		tx.Abort()
		return "", err
	}
	return tx.Commit(nil)
}

func newOperatorLinkHub(t *testing.T, contributors ...ContributorSpec) (*Hub, *Handle) {
	t.Helper()
	hub := newTestHub(t)
	if _, err := hub.Declare(graphPVSpec(opAsset, opSource, opDriver, true, contributors...)); err != nil {
		t.Fatal(err)
	}
	return hub, admitLegacy(t, hub, opSource, opDriver, "epoch:operator-links", 1)
}

// publishOp runs one driver transaction that carries every pending operator
// link and requires it to be accepted.
func publishOp(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, at int64, natives []string, resources ...semreg.Resource) []semreg.IdentityLink {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	batch := opBatch(t, tx, at, natives, resources...)
	pending := carry(tx, &batch)
	if outcome, err := commitOp(t, tx, batch, at); err != nil || outcome != Accepted {
		t.Fatalf("publication=%v/%v", outcome, err)
	}
	return pending
}

func mustSetOperatorLinks(t *testing.T, hub *Hub, id semreg.AssetID, src semreg.SourceID, links ...OperatorLink) {
	t.Helper()
	if err := hub.SetOperatorLinks(id, src, links); err != nil {
		t.Fatalf("set operator links: %v", err)
	}
}

// linkOf returns the snapshot's identity link of binding.
func linkOf(t *testing.T, hub *Hub, id semreg.AssetID, binding semreg.NativeBindingID) semreg.IdentityLink {
	t.Helper()
	view, err := hub.Inspect(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, link := range view.Snapshot.IdentityLinks {
		if link.BindingID == binding {
			return link
		}
	}
	t.Fatalf("no identity link of %s", binding)
	return semreg.IdentityLink{}
}

// pendingOf opens a transaction of handle, reads the operator links pending
// for natives and aborts it.
func pendingOf(t *testing.T, hub *Hub, handle *Handle, id semreg.AssetID, natives ...string) []semreg.IdentityLink {
	t.Helper()
	tx, err := hub.Begin(handle, id)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Abort()
	batch := opBatch(t, tx, 0, natives)
	bindings := batch.BindingUpserts
	return tx.OperatorLinks(bindings)
}

func wantOperatorLink(t *testing.T, got semreg.IdentityLink, binding semreg.NativeBindingID, resource semreg.ResourceID, audit string, revision semreg.Uint64) {
	t.Helper()
	want := semreg.IdentityLink{AssetID: opAsset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{opAudit(audit)}, Revision: revision, Resource: &resource}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("operator link=%+v want %+v", got, want)
	}
}

// A declared operator link is exposed on the driver's Tx, the driver's next
// batch carries it, and once published nothing stays pending.
func TestOperatorLinksPendingLinkIsCarriedByTheDriversNextBatch(t *testing.T) {
	hub, handle := newOperatorLinkHub(t)
	publishOp(t, hub, handle, opAsset, 1, []string{"a"}, graphInverterResource(), opInverter2Resource())
	tx, err := hub.Begin(handle, opAsset)
	if err != nil {
		t.Fatal(err)
	}
	binding := opBindingID(t, tx, "a")
	tx.Abort()
	if got := pendingOf(t, hub, handle, opAsset, "a"); len(got) != 0 {
		t.Fatalf("pending without a declaration=%+v", got)
	}

	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"))
	pending := pendingOf(t, hub, handle, opAsset, "a")
	if len(pending) != 1 {
		t.Fatalf("pending=%+v", pending)
	}
	// The driver's own link of the binding had revision 1; the operator link
	// replaces it with the next revision.
	wantOperatorLink(t, pending[0], binding, graphInverter, "one", "2")

	if carried := publishOp(t, hub, handle, opAsset, 2, []string{"a"}); !reflect.DeepEqual(carried, pending) {
		t.Fatalf("carried=%+v want %+v", carried, pending)
	}
	wantOperatorLink(t, linkOf(t, hub, opAsset, binding), binding, graphInverter, "one", "2")
	if got := pendingOf(t, hub, handle, opAsset, "a"); len(got) != 0 {
		t.Fatalf("pending after publication=%+v", got)
	}

	// A revised declaration is pending again with the next revision.
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", opInverter2, "two"))
	publishOp(t, hub, handle, opAsset, 3, []string{"a"})
	wantOperatorLink(t, linkOf(t, hub, opAsset, binding), binding, opInverter2, "two", "3")

	// A withdrawn declaration is pending as the withdrawal of the published
	// link; once carried, nothing is pending.
	mustSetOperatorLinks(t, hub, opAsset, opSource)
	pending = pendingOf(t, hub, handle, opAsset, "a")
	if len(pending) != 1 || pending[0].State != semreg.LinkWithdrawn || pending[0].Revision != "4" || pending[0].BindingID != binding {
		t.Fatalf("pending withdrawal=%+v", pending)
	}
	publishOp(t, hub, handle, opAsset, 4, []string{"a"})
	if link := linkOf(t, hub, opAsset, binding); link.State != semreg.LinkWithdrawn || link.Revision != "4" {
		t.Fatalf("withdrawn link=%+v", link)
	}
	if got := pendingOf(t, hub, handle, opAsset, "a"); len(got) != 0 {
		t.Fatalf("pending after withdrawal=%+v", got)
	}
}

// A declaration for a binding the batch creates is carried by the batch that
// creates it, as the binding's first link.
func TestOperatorLinksFirstBatchCarriesTheLinkOfANewBinding(t *testing.T) {
	hub, handle := newOperatorLinkHub(t)
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"))
	carried := publishOp(t, hub, handle, opAsset, 1, []string{"a", "b"}, graphInverterResource())
	if len(carried) != 1 {
		t.Fatalf("carried=%+v", carried)
	}
	wantOperatorLink(t, linkOf(t, hub, opAsset, carried[0].BindingID), carried[0].BindingID, graphInverter, "one", "1")
}

// A batch that does not carry a pending link is accepted and leaves the link
// pending: a publisher that has not adopted the accessor keeps publishing.
func TestOperatorLinksBatchWithoutThePendingLinkLeavesItPending(t *testing.T) {
	hub, handle := newOperatorLinkHub(t)
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"))
	tx, err := hub.Begin(handle, opAsset)
	if err != nil {
		t.Fatal(err)
	}
	batch := opBatch(t, tx, 1, []string{"a"}, graphInverterResource())
	if outcome, err := commitOp(t, tx, batch, 1); err != nil || outcome != Accepted {
		t.Fatalf("batch without the operator link=%v/%v", outcome, err)
	}
	pending := pendingOf(t, hub, handle, opAsset, "a")
	if len(pending) != 1 || pending[0].Revision != "2" {
		t.Fatalf("pending=%+v", pending)
	}
}

// The forgery refusal: a batch whose operator-basis links differ from the
// pending set is refused at Commit, after the kernel accepted it, and leaves
// the snapshot, the cursor and the event stream unchanged. Every member of
// the link is varied; each case names the production mutation it catches.
func TestOperatorLinksForgedOrAlteredLinkIsRefusedAtCommit(t *testing.T) {
	other := semreg.ResourceID(opInverter2)
	key := opDigest("key")
	cases := []struct {
		name    string
		declare bool
		change  func(t *testing.T, tx *Tx, links []semreg.IdentityLink) []semreg.IdentityLink
	}{
		// Mutation: the refusal is skipped when nothing is declared.
		{"undeclared operator link", false, func(t *testing.T, tx *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			binding := opBindingID(t, tx, "a")
			return []semreg.IdentityLink{{AssetID: opAsset, BindingID: binding, State: semreg.LinkQualified, Basis: []semreg.EvidenceRef{opAudit("forged")}, Revision: "1", Resource: &other}}
		}},
		// Mutation: the comparison ignores the binding (an operator link on a
		// binding no declaration names).
		{"binding", true, func(t *testing.T, tx *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			forged := links[0]
			forged.BindingID = opBindingID(t, tx, "b")
			return []semreg.IdentityLink{forged}
		}},
		// Mutation: the comparison ignores Resource.
		{"resource", true, func(_ *testing.T, _ *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			links[0].Resource = &other
			return links
		}},
		// Mutation: the comparison ignores Key.
		{"key", true, func(_ *testing.T, _ *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			links[0].Key = &key
			return links
		}},
		// Mutation: the comparison only checks that an operator reference is
		// present in the basis.
		{"basis", true, func(_ *testing.T, _ *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			links[0].Basis = []semreg.EvidenceRef{opAudit("forged")}
			return links
		}},
		// Mutation: the comparison ignores State.
		{"state", true, func(_ *testing.T, _ *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			links[0].State = semreg.LinkCandidate
			return links
		}},
		// Mutation: the comparison ignores Revision (the kernel accepts any
		// revision for a binding's first link).
		{"revision", true, func(_ *testing.T, _ *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			links[0].Revision = "7"
			return links
		}},
		// Mutation: a subset of the pending set is accepted.
		{"partial carry", true, func(_ *testing.T, _ *Tx, links []semreg.IdentityLink) []semreg.IdentityLink {
			return links[:1]
		}},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			hub, handle := newOperatorLinkHub(t)
			if item.declare {
				mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"), opDeclaration("c", graphInverter, "one"))
			}
			before, cursor := InspectView{}, hub.Cursor()
			tx, err := hub.Begin(handle, opAsset)
			if err != nil {
				t.Fatal(err)
			}
			batch := opBatch(t, tx, 1, []string{"a", "b", "c"}, graphInverterResource(), opInverter2Resource())
			pending := tx.OperatorLinks(batch.BindingUpserts)
			if item.declare && len(pending) != 2 {
				t.Fatalf("pending=%+v", pending)
			}
			forged := item.change(t, tx, append([]semreg.IdentityLink{}, pending...))
			replaced := map[semreg.NativeBindingID]bool{}
			for _, link := range forged {
				replaced[link.BindingID] = true
			}
			links := append([]semreg.IdentityLink{}, forged...)
			for _, link := range batch.IdentityLinkUpserts {
				if !replaced[link.BindingID] {
					links = append(links, link)
				}
			}
			sortLinks(links)
			batch.IdentityLinkUpserts = links
			sealGraphTestBatch(t, &batch)
			if _, _, err := tx.Apply(batch, testMono(1)); err != nil {
				tx.Abort()
				t.Fatalf("the kernel refused the forged batch, so the hub check is not exercised: %v", err)
			}
			if outcome, err := tx.Commit(nil); !errors.Is(err, ErrOperatorLink) || outcome != "" {
				t.Fatalf("forged batch commit=%v/%v", outcome, err)
			}
			requireUnchanged(t, hub, opAsset, before, cursor)
			// The refused transaction released the asset: the honest batch commits.
			publishOp(t, hub, handle, opAsset, 2, []string{"a", "b", "c"}, graphInverterResource(), opInverter2Resource())
		})
	}
}

// Once an operator link is published, the driver cannot replace it with a
// link of its own basis or withdraw it while the declaration stands.
func TestOperatorLinksDriverCannotOverwritePublishedOperatorLink(t *testing.T) {
	for name, change := range map[string]func(link *semreg.IdentityLink){
		// Mutation: only links that carry an operator basis are compared.
		"native basis": func(link *semreg.IdentityLink) {
			link.Basis = []semreg.EvidenceRef{testEvidence("semhubtest.observation")}
		},
		// Mutation: a withdrawal is not compared.
		"withdrawal": func(link *semreg.IdentityLink) { link.State = semreg.LinkWithdrawn },
	} {
		t.Run(name, func(t *testing.T) {
			hub, handle := newOperatorLinkHub(t)
			mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"))
			carried := publishOp(t, hub, handle, opAsset, 1, []string{"a"}, graphInverterResource())
			view, err := hub.Inspect(opAsset)
			if err != nil {
				t.Fatal(err)
			}
			cursor := hub.Cursor()
			tx, err := hub.Begin(handle, opAsset)
			if err != nil {
				t.Fatal(err)
			}
			batch := opBatch(t, tx, 2, []string{"a"})
			link := carried[0]
			link.Revision = "2"
			change(&link)
			batch.IdentityLinkUpserts = []semreg.IdentityLink{link}
			sealGraphTestBatch(t, &batch)
			if _, _, err := tx.Apply(batch, testMono(2)); err != nil {
				tx.Abort()
				t.Fatalf("the kernel refused the batch, so the hub check is not exercised: %v", err)
			}
			if _, err := tx.Commit(nil); !errors.Is(err, ErrOperatorLink) {
				t.Fatalf("driver overwrite commit=%v", err)
			}
			requireUnchanged(t, hub, opAsset, view, cursor)
		})
	}
}

// The pending set is kept per asset and per source, and a declaration
// matches a binding by every member of its native resource.
func TestOperatorLinksPendingSetIsKeyedByAssetSourceAndNativeResource(t *testing.T) {
	const (
		peerSource semreg.SourceID = "source:operator-links-peer"
		peerDriver                 = "driver-operator-links-peer"
		otherAsset semreg.AssetID  = "asset:operator-links-other"
	)
	hub, handle := newOperatorLinkHub(t, ContributorSpec{SourceID: peerSource, Driver: peerDriver, Validators: graphPVSpec(opAsset, opSource, opDriver, true).Validators})
	if _, err := hub.Declare(graphPVSpec(otherAsset, opSource, opDriver, true)); err != nil {
		t.Fatal(err)
	}
	peer := admitLegacy(t, hub, peerSource, peerDriver, "epoch:operator-links-peer", 1)
	publishOp(t, hub, handle, opAsset, 1, []string{"a"}, graphInverterResource())
	publishOp(t, hub, handle, otherAsset, 1, []string{"a"}, graphInverterResource())
	publishOp(t, hub, peer, opAsset, 1, []string{"a"})

	// Field asset. Mutation: the pending set is keyed by source only.
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"))
	if got := pendingOf(t, hub, handle, otherAsset, "a"); len(got) != 0 {
		t.Fatalf("another asset sees the declaration: %+v", got)
	}
	// Field source. Mutation: the pending set is keyed by asset only.
	if got := pendingOf(t, hub, peer, opAsset, "a"); len(got) != 0 {
		t.Fatalf("another source of the asset sees the declaration: %+v", got)
	}
	if got := pendingOf(t, hub, handle, opAsset, "a"); len(got) != 1 {
		t.Fatalf("the declared asset and source see %+v", got)
	}
	mustSetOperatorLinks(t, hub, opAsset, peerSource, opDeclaration("a", graphInverter, "peer"))
	if got := pendingOf(t, hub, peer, opAsset, "a"); len(got) != 1 || got[0].Basis[0] != opAudit("peer") {
		t.Fatalf("the peer's own declaration=%+v", got)
	}
	if got := pendingOf(t, hub, handle, opAsset, "a"); len(got) != 1 || got[0].Basis[0] != opAudit("one") {
		t.Fatalf("the peer's declaration replaced the owner's: %+v", got)
	}

	// Each member of the native resource. Mutation: the match compares a
	// subset of the members.
	for name, change := range map[string]func(ref *semreg.EvidenceRef){
		"owner":     func(ref *semreg.EvidenceRef) { ref.Owner = "helianthus.pack.semhubtest.other" },
		"kind":      func(ref *semreg.EvidenceRef) { ref.Kind = "semhubtest.native.other" },
		"digest":    func(ref *semreg.EvidenceRef) { ref.Digest = opDigest("native/other") },
		"contract":  func(ref *semreg.EvidenceRef) { ref.Contract = semreg.ContractEvaluationV1 },
		"access":    func(ref *semreg.EvidenceRef) { ref.Access = semreg.EvidenceAccessPublic },
		"redaction": func(ref *semreg.EvidenceRef) { ref.Redaction = semreg.RedactionNone },
	} {
		declaration := opDeclaration("a", graphInverter, "one")
		change(&declaration.Native)
		mustSetOperatorLinks(t, hub, opAsset, opSource, declaration)
		if got := pendingOf(t, hub, handle, opAsset, "a"); len(got) != 0 {
			t.Fatalf("native %s differs but the declaration matched: %+v", name, got)
		}
	}
}

// While the driver's generation is fenced the declaration stays pending; the
// successor's first batch carries it on the successor's binding.
func TestOperatorLinksStayPendingWhileFencedAndTheSuccessorCarriesThem(t *testing.T) {
	hub, handle := newOperatorLinkHub(t)
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"))
	first := publishOp(t, hub, handle, opAsset, 1, []string{"a"}, graphInverterResource())
	if len(first) != 1 {
		t.Fatalf("first carried=%+v", first)
	}
	if _, err := hub.Fence(testFence(opSource, "epoch:operator-links", 1, 2)); err != nil {
		t.Fatal(err)
	}
	if link := linkOf(t, hub, opAsset, first[0].BindingID); link.State != semreg.LinkWithdrawn {
		t.Fatalf("the fence left the link %+v", link)
	}
	if _, err := hub.Begin(handle, opAsset); err == nil {
		t.Fatal("the fenced generation's handle still begins")
	}

	// A declaration revised while fenced replaces the pending one.
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "fenced"))
	hub.mu.RLock()
	held := append([]OperatorLink(nil), hub.assets[opAsset].operatorLinks[opSource]...)
	hub.mu.RUnlock()
	if len(held) != 1 || !reflect.DeepEqual(held[0], opDeclaration("a", graphInverter, "fenced")) {
		t.Fatalf("pending declarations while fenced=%+v", held)
	}

	successor, err := hub.AdmitSource(Admission{Source: opSource, Driver: opDriver, Legacy: LegacyGrowattStorage, Epoch: "epoch:operator-links-2", Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	carried := publishOp(t, hub, successor, opAsset, 3, []string{"a"})
	if len(carried) != 1 || carried[0].BindingID == first[0].BindingID {
		t.Fatalf("successor carried=%+v", carried)
	}
	wantOperatorLink(t, linkOf(t, hub, opAsset, carried[0].BindingID), carried[0].BindingID, graphInverter, "fenced", "1")
	if got := pendingOf(t, hub, successor, opAsset, "a"); len(got) != 0 {
		t.Fatalf("pending after the successor's first batch=%+v", got)
	}
}

// carryOperatorWithdrawals carries every pending withdrawal and leaves every
// qualified declaration out of the batch.
func carryOperatorWithdrawals(tx *Tx, batch *semreg.PublicationBatch) []semreg.IdentityLink {
	pending := tx.OperatorLinks(batch.BindingUpserts)
	replaced := map[semreg.NativeBindingID]bool{}
	links := []semreg.IdentityLink{}
	for _, link := range pending {
		if link.State != semreg.LinkWithdrawn {
			continue
		}
		replaced[link.BindingID] = true
		links = append(links, link)
	}
	for _, link := range batch.IdentityLinkUpserts {
		if !replaced[link.BindingID] {
			links = append(links, link)
		}
	}
	sortLinks(links)
	batch.IdentityLinkUpserts = links
	return pending
}

// A batch may commit every pending withdrawal and leave a qualified
// declaration pending. The published link that declaration would replace
// stays as it was.
func TestOperatorLinksWithdrawalCarryLeavesQualifiedDeclarationPending(t *testing.T) {
	hub, handle := newOperatorLinkHub(t)
	publishOp(t, hub, handle, opAsset, 1, []string{"a", "c"}, graphInverterResource(), opInverter2Resource())
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"), opDeclaration("c", opInverter2, "two"))
	published := publishOp(t, hub, handle, opAsset, 2, []string{"a", "c"})
	if len(published) != 2 {
		t.Fatalf("published=%+v", published)
	}
	var bindingA, bindingC semreg.NativeBindingID
	for _, link := range published {
		if link.Resource != nil && *link.Resource == graphInverter {
			bindingA = link.BindingID
		}
		if link.Resource != nil && *link.Resource == opInverter2 {
			bindingC = link.BindingID
		}
	}
	if bindingA == "" || bindingC == "" || bindingA == bindingC {
		t.Fatalf("published bindings a=%s c=%s", bindingA, bindingC)
	}
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("c", graphInverter, "revised"))
	tx, err := hub.Begin(handle, opAsset)
	if err != nil {
		t.Fatal(err)
	}
	batch := opBatch(t, tx, 3, []string{"a", "c"})
	pending := carryOperatorWithdrawals(tx, &batch)
	if len(pending) != 2 {
		t.Fatalf("pending=%+v", pending)
	}
	var withdrawal semreg.IdentityLink
	qualified := 0
	for _, link := range pending {
		switch link.State {
		case semreg.LinkWithdrawn:
			withdrawal = link
		case semreg.LinkQualified:
			qualified++
			wantOperatorLink(t, link, bindingC, graphInverter, "revised", "3")
		default:
			t.Fatalf("pending link=%+v", link)
		}
	}
	if withdrawal.BindingID != bindingA || withdrawal.Revision != "3" || qualified != 1 {
		t.Fatalf("pending withdrawal=%+v qualified=%d", withdrawal, qualified)
	}
	if outcome, err := commitOp(t, tx, batch, 3); err != nil || outcome != Accepted {
		t.Fatalf("withdrawal beside a qualified declaration=%v/%v", outcome, err)
	}
	if link := linkOf(t, hub, opAsset, bindingA); link.State != semreg.LinkWithdrawn || link.Revision != "3" || link.Resource == nil || *link.Resource != graphInverter || !operatorBasis(link.Basis) {
		t.Fatalf("withdrawn link=%+v", link)
	}
	wantOperatorLink(t, linkOf(t, hub, opAsset, bindingC), bindingC, opInverter2, "two", "2")
	remaining := pendingOf(t, hub, handle, opAsset, "a", "c")
	if len(remaining) != 1 {
		t.Fatalf("pending after the withdrawal=%+v", remaining)
	}
	wantOperatorLink(t, remaining[0], bindingC, graphInverter, "revised", "3")
}

// Carrying fewer withdrawals than are pending, or a qualified declaration
// without the withdrawal pending beside it, is still refused.
func TestOperatorLinksPartialOperatorCarryIsRefused(t *testing.T) {
	t.Run("one of two withdrawals", func(t *testing.T) {
		hub, handle := newOperatorLinkHub(t)
		publishOp(t, hub, handle, opAsset, 1, []string{"a", "c"}, graphInverterResource(), opInverter2Resource())
		mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"), opDeclaration("c", opInverter2, "two"))
		publishOp(t, hub, handle, opAsset, 2, []string{"a", "c"})
		mustSetOperatorLinks(t, hub, opAsset, opSource)
		before, err := hub.Inspect(opAsset)
		if err != nil {
			t.Fatal(err)
		}
		cursor := hub.Cursor()
		tx, err := hub.Begin(handle, opAsset)
		if err != nil {
			t.Fatal(err)
		}
		batch := opBatch(t, tx, 3, []string{"a", "c"})
		pending := tx.OperatorLinks(batch.BindingUpserts)
		if len(pending) != 2 || pending[0].State != semreg.LinkWithdrawn || pending[1].State != semreg.LinkWithdrawn {
			t.Fatalf("pending withdrawals=%+v", pending)
		}
		batch.IdentityLinkUpserts = []semreg.IdentityLink{pending[0]}
		sealGraphTestBatch(t, &batch)
		if _, _, err := tx.Apply(batch, testMono(3)); err != nil {
			tx.Abort()
			t.Fatalf("the kernel refused the batch, so the hub check is not exercised: %v", err)
		}
		if outcome, err := tx.Commit(nil); !errors.Is(err, ErrOperatorLink) || outcome != "" {
			t.Fatalf("partial withdrawal commit=%v/%v", outcome, err)
		}
		requireUnchanged(t, hub, opAsset, before, cursor)
	})
	t.Run("qualified link without its withdrawal", func(t *testing.T) {
		hub, handle := newOperatorLinkHub(t)
		publishOp(t, hub, handle, opAsset, 1, []string{"a", "c"}, graphInverterResource(), opInverter2Resource())
		mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"), opDeclaration("c", opInverter2, "two"))
		publishOp(t, hub, handle, opAsset, 2, []string{"a", "c"})
		mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("c", graphInverter, "revised"))
		before, err := hub.Inspect(opAsset)
		if err != nil {
			t.Fatal(err)
		}
		cursor := hub.Cursor()
		tx, err := hub.Begin(handle, opAsset)
		if err != nil {
			t.Fatal(err)
		}
		batch := opBatch(t, tx, 3, []string{"a", "c"})
		pending := tx.OperatorLinks(batch.BindingUpserts)
		var qualified []semreg.IdentityLink
		for _, link := range pending {
			if link.State == semreg.LinkQualified {
				qualified = append(qualified, link)
			}
		}
		if len(pending) != 2 || len(qualified) != 1 {
			t.Fatalf("pending=%+v", pending)
		}
		batch.IdentityLinkUpserts = qualified
		sealGraphTestBatch(t, &batch)
		if _, _, err := tx.Apply(batch, testMono(3)); err != nil {
			tx.Abort()
			t.Fatalf("the kernel refused the batch, so the hub check is not exercised: %v", err)
		}
		if outcome, err := tx.Commit(nil); !errors.Is(err, ErrOperatorLink) || outcome != "" {
			t.Fatalf("qualified carry without the withdrawal=%v/%v", outcome, err)
		}
		requireUnchanged(t, hub, opAsset, before, cursor)
	})
}

// A declaration is refused before it is held when it names an unknown asset,
// a source that does not publish on the asset, or carries no operator basis.
func TestOperatorLinksDeclarationRules(t *testing.T) {
	hub, _ := newOperatorLinkHub(t)
	if err := hub.SetOperatorLinks("asset:operator-links-unknown", opSource, nil); !errors.Is(err, ErrUnknownAsset) {
		t.Fatalf("unknown asset=%v", err)
	}
	if err := hub.SetOperatorLinks(opAsset, "source:operator-links-stranger", nil); !errors.Is(err, ErrUnknownSource) {
		t.Fatalf("stranger source=%v", err)
	}
	native := opDeclaration("a", graphInverter, "one")
	native.Basis = []semreg.EvidenceRef{testEvidence("semhubtest.observation")}
	invalidResource := opDeclaration("a", "", "one")
	invalidNative := opDeclaration("a", graphInverter, "one")
	invalidNative.Native.Digest = "sha256:short"
	invalidKey := opDeclaration("a", graphInverter, "one")
	short := semreg.Digest("sha256:short")
	invalidKey.Key = &short
	for name, links := range map[string][]OperatorLink{
		"no operator basis": {native},
		"empty basis":       {{Native: opNative("a"), Resource: graphInverter, Basis: []semreg.EvidenceRef{}}},
		"invalid resource":  {invalidResource},
		"invalid native":    {invalidNative},
		"invalid key":       {invalidKey},
		"repeated native":   {opDeclaration("a", graphInverter, "one"), opDeclaration("a", opInverter2, "two")},
	} {
		if err := hub.SetOperatorLinks(opAsset, opSource, links); !errors.Is(err, ErrInvalidIdentity) {
			t.Fatalf("%s=%v", name, err)
		}
	}
	hub.mu.RLock()
	held := len(hub.assets[opAsset].operatorLinks)
	hub.mu.RUnlock()
	if held != 0 {
		t.Fatalf("a refused declaration was held: %d", held)
	}
}

// A declaration is refused while Restore installs state and is accepted again
// once the restore ends.
func TestOperatorLinksDeclarationIsRefusedWhileRestoring(t *testing.T) {
	hub, _ := newOperatorLinkHub(t)
	hub.mu.Lock()
	hub.restoring = true
	hub.mu.Unlock()
	err := hub.SetOperatorLinks(opAsset, opSource, []OperatorLink{opDeclaration("a", graphInverter, "one")})
	if !errors.Is(err, ErrRestoreInProgress) {
		t.Fatalf("restoring=%v", err)
	}
	hub.mu.Lock()
	hub.restoring = false
	held := len(hub.assets[opAsset].operatorLinks)
	hub.mu.Unlock()
	if held != 0 {
		t.Fatalf("a declaration was held while restoring: %d", held)
	}
	mustSetOperatorLinks(t, hub, opAsset, opSource, opDeclaration("a", graphInverter, "one"))
}
