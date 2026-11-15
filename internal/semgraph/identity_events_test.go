package semgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// Synthetic kinds, schemes and issuer codes; no family, model or unit is named.
const (
	kindEquipment semreg.DefinitionID = "resource.equipment"
	kindInput     semreg.DefinitionID = "semgraphtest.input"
	kindZone      semreg.DefinitionID = "semgraphtest.zone"
	testScheme    semreg.DefinitionID = "scheme.semgraphtest"
	testIssuer                        = "issuer-a"
)

// evidence returns one synthetic evidence reference whose digest is computed
// from its label, so no test holds a literal digest.
func evidence(label string) semreg.EvidenceRef {
	sum := sha256.Sum256([]byte("semgraphtest evidence\x00" + label))
	return semreg.EvidenceRef{Owner: "semgraphtest.owner", Kind: "semgraphtest.anchor", Digest: semreg.Digest("sha256:" + hex.EncodeToString(sum[:])), Contract: semreg.ContractKernelV1, Access: semreg.EvidenceAccessAuthorized, Redaction: semreg.RedactionMetadataOnly}
}

func tokenAnchor(token, label string) Anchor {
	return Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorNativeIdentity, Source: SourceNativeToken, Scheme: testScheme, Issuer: testIssuer, Token: token, Evidence: []semreg.EvidenceRef{evidence(label)}}
}

func operatorEquipment(slot, label string) Anchor {
	return Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: SourceOperatorLabel, Slot: slot, Evidence: []semreg.EvidenceRef{evidence(label)}}
}

func inputAnchor(parent semreg.ResourceID, slot, label string) Anchor {
	return Anchor{Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeSlot, Source: SourceNativePosition, Parent: parent, Slot: slot, Evidence: []semreg.EvidenceRef{evidence(label)}}
}

func zoneAnchor(basis semreg.AnchorBasis, slot, label string) Anchor {
	source := SourceOperatorLabel
	if basis == semreg.AnchorNativeSlot {
		source = SourceNativePosition
	}
	return Anchor{Class: semreg.AnchorFunction, Basis: basis, Source: source, Slot: slot, Evidence: []semreg.EvidenceRef{evidence(label)}}
}

func continuity(basis ContinuityBasis, label string) Continuity {
	return Continuity{Basis: basis, Evidence: []semreg.EvidenceRef{evidence(label)}}
}

func newTestAllocator(t *testing.T, store Store) *Allocator {
	t.Helper()
	allocator, err := NewAllocator(store)
	if err != nil {
		t.Fatalf("NewAllocator: %v", err)
	}
	return allocator
}

func mustResolve(t *testing.T, allocator *Allocator, kind semreg.DefinitionID, anchor Anchor) semreg.ResourceID {
	t.Helper()
	id, record, err := allocator.Resolve(kind, anchor)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", kind, err)
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("Resolve(%s) anchor record: %v", kind, err)
	}
	if err := id.Validate(); err != nil {
		t.Fatalf("Resolve(%s) resource id: %v", kind, err)
	}
	return id
}

// GW-ID-1: a driver restart, a new source epoch or a new generation keeps
// every resource identifier. A fresh allocator over an empty store and one
// over the retained supersession table both return the identifiers the first
// incarnation returned, and evidence of the new epoch changes none of them.
func TestGWID1RestartAndNewEpochKeepIdentifiers(t *testing.T) {
	store := NewMemoryStore()
	first := newTestAllocator(t, store)
	equipment := mustResolve(t, first, kindEquipment, tokenAnchor("token-a", "epoch-1"))
	input := mustResolve(t, first, kindInput, inputAnchor(equipment, "input-1", "epoch-1"))
	zone := mustResolve(t, first, kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "zone-a", "epoch-1"))
	labelled := mustResolve(t, first, kindEquipment, operatorEquipment("unit-b", "epoch-1"))
	upgraded, _, err := first.Supersede(Claim{kindEquipment, operatorEquipment("unit-b", "epoch-1")}, Claim{kindEquipment, tokenAnchor("token-b", "epoch-1")}, continuity(ContinuityBinding, "both-anchors"))
	if err != nil || upgraded != labelled {
		t.Fatalf("Supersede = %q, %v; want %q", upgraded, err, labelled)
	}
	for name, store := range map[string]Store{"empty store": NewMemoryStore(), "retained store": store} {
		t.Run(name, func(t *testing.T) {
			restarted := newTestAllocator(t, store)
			if got := mustResolve(t, restarted, kindEquipment, tokenAnchor("token-a", "epoch-2")); got != equipment {
				t.Fatalf("equipment after restart = %q, want %q", got, equipment)
			}
			if got := mustResolve(t, restarted, kindInput, inputAnchor(equipment, "input-1", "generation-2")); got != input {
				t.Fatalf("component after restart = %q, want %q", got, input)
			}
			if got := mustResolve(t, restarted, kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "zone-a", "generation-2")); got != zone {
				t.Fatalf("function after restart = %q, want %q", got, zone)
			}
		})
	}
	restarted := newTestAllocator(t, store)
	if got := mustResolve(t, restarted, kindEquipment, tokenAnchor("token-b", "epoch-2")); got != labelled {
		t.Fatalf("superseded anchor after restart = %q, want the retained %q", got, labelled)
	}
}

// GW-ID-2: re-addressing on the bus or the network keeps the identifier. An
// address is refused as an anchor source; an address that only appears in
// evidence changes nothing.
func TestGWID2ReaddressingKeepsIdentifier(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	before := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "address-before"))
	after := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "address-after"))
	if before != after {
		t.Fatalf("identifier changed with the address: %q -> %q", before, after)
	}
	for _, source := range []AnchorSource{SourceBusAddress, SourceNetworkAddress, SourceRoutedUnit, SourceEntityAddressPath} {
		t.Run(string(source), func(t *testing.T) {
			anchor := operatorEquipment("address-text", "address")
			anchor.Source = source
			if _, _, err := allocator.Resolve(kindEquipment, anchor); !errors.Is(err, ErrRefusedAnchorSource) {
				t.Fatalf("Resolve with source %q: err=%v, want ErrRefusedAnchorSource", source, err)
			}
		})
	}
}

// GW-ID-3: a credential or certificate rotation keeps the identifier. A
// credential is refused as an anchor source.
func TestGWID3CredentialRotationKeepsIdentifier(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	before := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "credential-before"))
	after := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "credential-after"))
	if before != after {
		t.Fatalf("identifier changed with the credential: %q -> %q", before, after)
	}
	for _, source := range []AnchorSource{SourceCredential, SourceCertificateFingerprint} {
		t.Run(string(source), func(t *testing.T) {
			anchor := tokenAnchor("credential-text", "credential")
			anchor.Source = source
			if _, _, err := allocator.Resolve(kindEquipment, anchor); !errors.Is(err, ErrRefusedAnchorSource) {
				t.Fatalf("Resolve with source %q: err=%v, want ErrRefusedAnchorSource", source, err)
			}
		})
	}
}

// GW-ID-4: equipment replaced by another unit gets a new identifier, and so
// do its components; a function keeps its identifier.
func TestGWID4EquipmentReplacedNewForEquipmentAndComponentsSameForFunctions(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	oldUnit := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "old-unit"))
	oldInput := mustResolve(t, allocator, kindInput, inputAnchor(oldUnit, "input-1", "old-unit"))
	zone := mustResolve(t, allocator, kindZone, zoneAnchor(semreg.AnchorNativeSlot, "zone-a", "old-unit"))

	newUnit := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-c", "new-unit"))
	newInput := mustResolve(t, allocator, kindInput, inputAnchor(newUnit, "input-1", "new-unit"))
	sameZone := mustResolve(t, allocator, kindZone, zoneAnchor(semreg.AnchorNativeSlot, "zone-a", "new-unit"))
	if newUnit == oldUnit {
		t.Fatalf("replacement equipment reused %q", oldUnit)
	}
	if newInput == oldInput {
		t.Fatalf("component of the replacement reused %q", oldInput)
	}
	if sameZone != zone {
		t.Fatalf("function changed with the equipment: %q -> %q", zone, sameZone)
	}
	// A replacement is not an upgrade: the new unit's key is already held.
	if _, _, err := allocator.Supersede(Claim{kindEquipment, tokenAnchor("token-a", "old-unit")}, Claim{kindEquipment, tokenAnchor("token-c", "new-unit")}, continuity(ContinuityNativeMapping, "mapping")); !errors.Is(err, ErrKeyHeld) {
		t.Fatalf("Supersede onto the replacement's held key: err=%v, want ErrKeyHeld", err)
	}
}

// GW-ID-5: an operator renaming a configuration slot gets a new identifier
// for the function, unless the operator maps the old slot by supersession.
// An operator slot maps only onto a native anchor; an operator slot to
// another operator slot is not an allowed upgrade.
func TestGWID5OperatorRenameNewUnlessMappedBySupersession(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	original := mustResolve(t, allocator, kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "upstairs", "declared"))
	renamed := mustResolve(t, allocator, kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "first-floor", "renamed"))
	if renamed == original {
		t.Fatalf("renamed slot reused %q without a supersession", original)
	}

	mapped := newTestAllocator(t, NewMemoryStore())
	original = mustResolve(t, mapped, kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "upstairs", "declared"))
	if _, _, err := mapped.Supersede(Claim{kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "upstairs", "declared")}, Claim{kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "first-floor", "renamed")}, continuity(ContinuityOperatorDeclaration, "audit")); !errors.Is(err, ErrNotUpgrade) {
		t.Fatalf("operator slot to operator slot: err=%v, want ErrNotUpgrade", err)
	}
	kept, record, err := mapped.Supersede(Claim{kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "upstairs", "declared")}, Claim{kindZone, zoneAnchor(semreg.AnchorNativeSlot, "circuit-2", "native")}, continuity(ContinuityOperatorDeclaration, "audit"))
	if err != nil || kept != original {
		t.Fatalf("mapped rename = %q, %v; want %q", kept, err, original)
	}
	if got := mustResolve(t, mapped, kindZone, zoneAnchor(semreg.AnchorNativeSlot, "circuit-2", "native")); got != original {
		t.Fatalf("mapped slot resolves to %q, want %q", got, original)
	}
	want, _ := zoneAnchor(semreg.AnchorOperatorSlot, "upstairs", "declared").Record()
	if !reflect.DeepEqual(record.Previous, want) {
		t.Fatalf("supersedes.previous = %+v, want %+v", record.Previous, want)
	}
	if _, _, err := mapped.Resolve(kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "upstairs", "reused")); !errors.Is(err, ErrAnchorSuperseded) {
		t.Fatalf("old slot after the mapping: err=%v, want ErrAnchorSuperseded", err)
	}
}

// GW-ID-7 (upgrade): an operator label or a native slot replaced by a
// qualified native token keeps the identifier and returns the supersedes
// record naming the exact previous anchor.
func TestGWID7AnchorUpgradeKeepsIdentifierWithSupersedes(t *testing.T) {
	cases := []struct {
		name     string
		previous Anchor
		basis    ContinuityBasis
	}{
		{"operator label", operatorEquipment("unit-b", "label"), ContinuityOperatorDeclaration},
		{"native slot", Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorNativeSlot, Source: SourceNativePosition, Slot: "position-3", Evidence: []semreg.EvidenceRef{evidence("position")}}, ContinuityBinding},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			allocator := newTestAllocator(t, NewMemoryStore())
			before := mustResolve(t, allocator, kindEquipment, tc.previous)
			next := tokenAnchor("token-b", "token")
			after, record, err := allocator.Supersede(Claim{kindEquipment, tc.previous}, Claim{kindEquipment, next}, continuity(tc.basis, "continuity"))
			if err != nil || after != before {
				t.Fatalf("Supersede = %q, %v; want %q", after, err, before)
			}
			wantPrevious, _ := tc.previous.Record()
			if !reflect.DeepEqual(record.Previous, wantPrevious) || !reflect.DeepEqual(record.Evidence, []semreg.EvidenceRef{evidence("continuity")}) {
				t.Fatalf("supersedes = %+v", record)
			}
			if err := record.Validate(); err != nil {
				t.Fatalf("supersedes record: %v", err)
			}
			if got := mustResolve(t, allocator, kindEquipment, next); got != before {
				t.Fatalf("native token resolves to %q, want %q", got, before)
			}
			if _, _, err := allocator.Supersede(Claim{kindEquipment, next}, Claim{kindEquipment, tc.previous}, continuity(tc.basis, "continuity")); !errors.Is(err, ErrDowngrade) {
				t.Fatalf("downgrade: err=%v, want ErrDowngrade", err)
			}
		})
	}
}

// GW-ID-7 (revised token form): the native owner revises the canonical token
// form; the identifier stays and the supersedes record names the old form.
// Only a revision within one scheme and issuer is a supersession.
func TestGWID7RevisedCanonicalTokenFormKeepsIdentifierWithSupersedes(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	oldForm := tokenAnchor("TOKEN-A", "old-form")
	newForm := tokenAnchor("token-a", "new-form")
	before := mustResolve(t, allocator, kindEquipment, oldForm)
	after, record, err := allocator.Supersede(Claim{kindEquipment, oldForm}, Claim{kindEquipment, newForm}, continuity(ContinuityNativeMapping, "documented-mapping"))
	if err != nil || after != before {
		t.Fatalf("Supersede = %q, %v; want %q", after, err, before)
	}
	wantPrevious, _ := oldForm.Record()
	if !reflect.DeepEqual(record.Previous, wantPrevious) {
		t.Fatalf("supersedes.previous = %+v, want %+v", record.Previous, wantPrevious)
	}
	if got := mustResolve(t, allocator, kindEquipment, newForm); got != before {
		t.Fatalf("new token form resolves to %q, want %q", got, before)
	}
	if _, _, err := allocator.Resolve(kindEquipment, oldForm); !errors.Is(err, ErrAnchorSuperseded) {
		t.Fatalf("old token form after the revision: err=%v, want ErrAnchorSuperseded", err)
	}

	otherIssuer := tokenAnchor("token-a", "other-issuer")
	otherIssuer.Issuer = "issuer-b"
	otherScheme := tokenAnchor("token-a", "other-scheme")
	otherScheme.Scheme = "scheme.semgraphtest-other"
	for name, next := range map[string]Anchor{"issuer": otherIssuer, "scheme": otherScheme} {
		t.Run(name, func(t *testing.T) {
			fresh := newTestAllocator(t, NewMemoryStore())
			if _, _, err := fresh.Supersede(Claim{kindEquipment, oldForm}, Claim{kindEquipment, next}, continuity(ContinuityNativeMapping, "documented-mapping")); !errors.Is(err, ErrNotUpgrade) {
				t.Fatalf("token revision across %s: err=%v, want ErrNotUpgrade", name, err)
			}
		})
	}
}
