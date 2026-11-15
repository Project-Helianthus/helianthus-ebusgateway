package semgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// expectedID recomputes the identifier derivation independently, so a change
// of the domain, the member order or the separators fails here.
func expectedID(fields ...string) semreg.ResourceID {
	digest := sha256.New()
	digest.Write([]byte("helianthus.semgraph.resource/v1"))
	for _, field := range fields {
		digest.Write([]byte{0})
		digest.Write([]byte(field))
	}
	return semreg.ResourceID("resource:" + hex.EncodeToString(digest.Sum(nil))[:32])
}

func digestOf(raw string) semreg.Digest {
	sum := sha256.Sum256([]byte(raw))
	return semreg.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// The native key is sha256 over the JCS bytes of {scheme, issuer, token}:
// members in code-unit order and characters JCS leaves unescaped, which a
// plain JSON encoder would escape.
func TestIdentityKeyDigestsTheJCSIdentityObject(t *testing.T) {
	for _, tc := range []struct{ issuer, token, jcs string }{
		{"", "token-a", `{"issuer":"","scheme":"scheme.semgraphtest","token":"token-a"}`},
		{"issuer-a", "a<b&c>", `{"issuer":"issuer-a","scheme":"scheme.semgraphtest","token":"a<b&c>"}`},
		{"issuer-a", "é\"x", `{"issuer":"issuer-a","scheme":"scheme.semgraphtest","token":"é\"x"}`},
	} {
		got, err := IdentityKey(testScheme, tc.issuer, tc.token)
		if err != nil {
			t.Fatalf("IdentityKey(%q, %q): %v", tc.issuer, tc.token, err)
		}
		if want := digestOf(tc.jcs); got != want {
			t.Fatalf("IdentityKey(%q, %q) = %s, want the digest of %s", tc.issuer, tc.token, got, tc.jcs)
		}
	}
}

// The SemReg record carries the key and the scheme in clear; the issuer and
// token stay inside the digest.
func TestRecordCarriesKeyNotToken(t *testing.T) {
	record, err := tokenAnchor("token-a", "record").Record()
	if err != nil {
		t.Fatal(err)
	}
	key, _ := IdentityKey(testScheme, testIssuer, "token-a")
	if record.Key == nil || *record.Key != key || record.Scheme == nil || *record.Scheme != string(testScheme) || record.Parent != nil || record.Slot != nil {
		t.Fatalf("record = %+v", record)
	}
	if record.Class != semreg.AnchorEquipment || record.Basis != semreg.AnchorNativeIdentity || len(record.Evidence) != 1 || record.Evidence[0] != evidence("record") {
		t.Fatalf("record = %+v", record)
	}
}

// Every member of the anchor key changes the identifier; nothing else does.
//
//	member  variation                         production mutation caught
//	kind    resource.equipment -> other kind   kind left out of the hash
//	scheme  scheme.semgraphtest -> other       scheme left out of the identity object
//	issuer  issuer-a -> issuer-b               issuer left out of the identity object
//	token   token-a -> token-b                 token left out of the identity object
//	kind    (slot form)                         kind left out of the slot hash
//	parent  parent A -> parent B               parent left out of the slot hash
//	slot    input-1 -> input-2                 slot left out of the slot hash
func TestResourceIDVariesWithEveryAnchorKeyMember(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	resolve := func(kind semreg.DefinitionID, anchor Anchor) semreg.ResourceID {
		return mustResolve(t, allocator, kind, anchor)
	}
	base := tokenAnchor("token-a", "base")
	baseID := resolve(kindEquipment, base)
	key, _ := IdentityKey(testScheme, testIssuer, "token-a")
	if want := expectedID(string(kindEquipment), "key", string(key)); baseID != want {
		t.Fatalf("native identifier = %q, want %q", baseID, want)
	}
	otherScheme, otherIssuer, otherToken := base, base, base
	otherScheme.Scheme = "scheme.semgraphtest-other"
	otherIssuer.Issuer = "issuer-b"
	otherToken.Token = "token-b"
	native := map[string]semreg.ResourceID{
		"kind":   resolve("semgraphtest.meter", base),
		"scheme": resolve(kindEquipment, otherScheme),
		"issuer": resolve(kindEquipment, otherIssuer),
		"token":  resolve(kindEquipment, otherToken),
	}
	assertDistinct(t, baseID, native)

	parentA := baseID
	parentB := native["token"]
	slotBase := inputAnchor(parentA, "input-1", "base")
	slotID := resolve(kindInput, slotBase)
	if want := expectedID(string(kindInput), "slot", string(parentA), "input-1"); slotID != want {
		t.Fatalf("slot identifier = %q, want %q", slotID, want)
	}
	slotKind := slotBase
	otherParent := inputAnchor(parentB, "input-1", "base")
	otherSlot := inputAnchor(parentA, "input-2", "base")
	slotKind.Evidence = []semreg.EvidenceRef{evidence("kind")}
	slot := map[string]semreg.ResourceID{
		"kind":   resolve("semgraphtest.string", slotKind),
		"parent": resolve(kindInput, otherParent),
		"slot":   resolve(kindInput, otherSlot),
	}
	assertDistinct(t, slotID, slot)

	// Evidence, the source and the basis are not key members: an operator
	// slot and a native slot at the same parent and slot share the key.
	if got := resolve(kindEquipment, tokenAnchor("token-a", "other-evidence")); got != baseID {
		t.Fatalf("evidence changed the identifier: %q", got)
	}
	label := zoneAnchor(semreg.AnchorOperatorSlot, "zone-a", "label")
	position := zoneAnchor(semreg.AnchorNativeSlot, "zone-a", "position")
	if a, b := resolve(kindZone, label), resolve(kindZone, position); a != b {
		t.Fatalf("operator and native slot of one key differ: %q, %q", a, b)
	}
}

func assertDistinct(t *testing.T, base semreg.ResourceID, variants map[string]semreg.ResourceID) {
	t.Helper()
	seen := map[semreg.ResourceID]string{base: "base"}
	for member, id := range variants {
		if previous, dup := seen[id]; dup {
			t.Fatalf("varying %s gives the identifier of %s: %q", member, previous, id)
		}
		seen[id] = member
	}
}

func TestAnchorValidationRefusesNonAnchors(t *testing.T) {
	refused := []AnchorSource{SourceBusAddress, SourceNetworkAddress, SourceRoutedUnit, SourceEntityAddressPath, SourceCredential, SourceCertificateFingerprint, SourcePackClass, SourceProfileSelector, SourceDisplayName, SourceNonLocalIndex}
	for _, source := range refused {
		for _, anchor := range []Anchor{tokenAnchor("token-a", "x"), operatorEquipment("text", "x")} {
			anchor.Source = source
			if err := anchor.Validate(); !errors.Is(err, ErrRefusedAnchorSource) {
				t.Fatalf("source %q: err=%v, want ErrRefusedAnchorSource", source, err)
			}
		}
	}
	mutate := func(base Anchor, change func(*Anchor)) Anchor {
		change(&base)
		return base
	}
	token := tokenAnchor("token-a", "x")
	label := operatorEquipment("unit-a", "x")
	input := inputAnchor("resource:parent", "input-1", "x")
	invalid := map[string]Anchor{
		"unknown source":             mutate(label, func(a *Anchor) { a.Source = "serial_text" }),
		"empty source":               mutate(label, func(a *Anchor) { a.Source = "" }),
		"token source, slot basis":   mutate(token, func(a *Anchor) { a.Basis = semreg.AnchorNativeSlot }),
		"label source, native basis": mutate(label, func(a *Anchor) { a.Basis = semreg.AnchorNativeSlot }),
		"position source, op basis":  mutate(input, func(a *Anchor) { a.Basis = semreg.AnchorOperatorSlot }),
		"native without token":       mutate(token, func(a *Anchor) { a.Token = "" }),
		"native without scheme":      mutate(token, func(a *Anchor) { a.Scheme = "" }),
		"issuer is a name":           mutate(token, func(a *Anchor) { a.Issuer = "Issuer Name" }),
		"token with control":         mutate(token, func(a *Anchor) { a.Token = "token\x00a" }),
		"token with white space":     mutate(token, func(a *Anchor) { a.Token = " token-a" }),
		"token too long":             mutate(token, func(a *Anchor) { a.Token = string(make([]byte, maxTokenBytes+1)) }),
		"label with token":           mutate(label, func(a *Anchor) { a.Token = "token-a" }),
		"label with issuer":          mutate(label, func(a *Anchor) { a.Issuer = "issuer-a" }),
		"label with scheme":          mutate(label, func(a *Anchor) { a.Scheme = testScheme }),
		"equipment label no slot":    mutate(label, func(a *Anchor) { a.Slot = "" }),
		"component without parent":   mutate(input, func(a *Anchor) { a.Parent = "" }),
		"component without slot":     mutate(input, func(a *Anchor) { a.Slot = "" }),
		"episode without parent":     mutate(input, func(a *Anchor) { a.Class = semreg.AnchorEpisode; a.Parent = "" }),
		"function without slot":      mutate(zoneAnchor(semreg.AnchorOperatorSlot, "zone-a", "x"), func(a *Anchor) { a.Slot = "" }),
		"unknown class":              mutate(label, func(a *Anchor) { a.Class = "device" }),
		"slot with control":          mutate(label, func(a *Anchor) { a.Slot = "unit\ta" }),
		"slot too long":              mutate(label, func(a *Anchor) { a.Slot = strings.Repeat("a", 129) }),
		"invalid parent":             mutate(input, func(a *Anchor) { a.Parent = " parent" }),
		"no evidence":                mutate(label, func(a *Anchor) { a.Evidence = nil }),
		"empty evidence":             mutate(label, func(a *Anchor) { a.Evidence = []semreg.EvidenceRef{} }),
		"unsorted evidence":          mutate(label, func(a *Anchor) { a.Evidence = sortedPair(false) }),
	}
	for name, anchor := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := anchor.Validate(); !errors.Is(err, ErrInvalidAnchor) {
				t.Fatalf("err=%v, want ErrInvalidAnchor", err)
			}
			if _, err := anchor.Record(); err == nil {
				t.Fatal("Record accepted an invalid anchor")
			}
		})
	}
	if err := mutate(label, func(a *Anchor) { a.Evidence = sortedPair(true) }).Validate(); err != nil {
		t.Fatalf("sorted evidence: %v", err)
	}
	if _, err := token.Key("Invalid Kind"); !errors.Is(err, ErrInvalidAnchor) {
		t.Fatalf("invalid kind: err=%v", err)
	}
	if _, err := IdentityKey(testScheme, "Issuer Name", "token-a"); !errors.Is(err, ErrInvalidAnchor) {
		t.Fatalf("IdentityKey with a name: err=%v", err)
	}
}

func sortedPair(sorted bool) []semreg.EvidenceRef {
	a, b := evidence("a"), evidence("b")
	if a.Digest > b.Digest {
		a, b = b, a
	}
	if sorted {
		return []semreg.EvidenceRef{a, b}
	}
	return []semreg.EvidenceRef{b, a}
}

func TestNewAllocatorRefusesNilStore(t *testing.T) {
	if _, err := NewAllocator(nil); !errors.Is(err, ErrNilStore) {
		t.Fatalf("err=%v, want ErrNilStore", err)
	}
}

func TestResolveRefusesParentNotAllocated(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	if _, _, err := allocator.Resolve(kindInput, inputAnchor("resource:foreign", "input-1", "x")); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("err=%v, want ErrUnknownResource", err)
	}
	if _, _, err := allocator.Resolve("Invalid Kind", tokenAnchor("token-a", "x")); !errors.Is(err, ErrInvalidAnchor) {
		t.Fatalf("invalid kind: err=%v", err)
	}
}

// A basis-only upgrade at one parent and slot keeps the identifier and the
// key; afterwards the lower basis is refused for that key.
func TestBasisOnlyUpgradeRefusesLaterDowngrade(t *testing.T) {
	store := NewMemoryStore()
	allocator := newTestAllocator(t, store)
	label := zoneAnchor(semreg.AnchorOperatorSlot, "zone-a", "label")
	position := zoneAnchor(semreg.AnchorNativeSlot, "zone-a", "position")
	id := mustResolve(t, allocator, kindZone, label)
	kept, _, err := allocator.Supersede(Claim{kindZone, label}, Claim{kindZone, position}, continuity(ContinuityBinding, "both"))
	if err != nil || kept != id {
		t.Fatalf("Supersede = %q, %v; want %q", kept, err, id)
	}
	if got := mustResolve(t, allocator, kindZone, position); got != id {
		t.Fatalf("native slot = %q, want %q", got, id)
	}
	for _, a := range []*Allocator{allocator, newTestAllocator(t, store)} {
		if _, _, err := a.Resolve(kindZone, label); !errors.Is(err, ErrDowngrade) {
			t.Fatalf("operator slot after the upgrade: err=%v, want ErrDowngrade", err)
		}
	}
	if _, _, err := allocator.Supersede(Claim{kindZone, label}, Claim{kindZone, zoneAnchor(semreg.AnchorNativeSlot, "zone-b", "x")}, continuity(ContinuityBinding, "both")); !errors.Is(err, ErrPreviousAnchor) {
		t.Fatalf("previous anchor below the recorded basis: err=%v, want ErrPreviousAnchor", err)
	}
}

func TestSupersedeRefusals(t *testing.T) {
	label := operatorEquipment("unit-a", "label")
	token := tokenAnchor("token-a", "token")
	binding := continuity(ContinuityBinding, "binding")
	cases := map[string]struct {
		previous, next Claim
		continuity     Continuity
		want           error
	}{
		"kind change":                {Claim{kindEquipment, label}, Claim{"semgraphtest.meter", token}, binding, ErrKindChange},
		"class change":               {Claim{kindZone, zoneAnchor(semreg.AnchorOperatorSlot, "zone-a", "x")}, Claim{kindZone, Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorNativeSlot, Source: SourceNativePosition, Slot: "zone-a", Evidence: []semreg.EvidenceRef{evidence("x")}}}, binding, ErrNotUpgrade},
		"native slot to native slot": {Claim{kindZone, zoneAnchor(semreg.AnchorNativeSlot, "zone-a", "x")}, Claim{kindZone, zoneAnchor(semreg.AnchorNativeSlot, "zone-b", "x")}, binding, ErrNotUpgrade},
		"same token":                 {Claim{kindEquipment, token}, Claim{kindEquipment, tokenAnchor("token-a", "again")}, binding, ErrNotUpgrade},
		"native to slot":             {Claim{kindEquipment, token}, Claim{kindEquipment, Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorNativeSlot, Source: SourceNativePosition, Slot: "position-1", Evidence: []semreg.EvidenceRef{evidence("x")}}}, binding, ErrDowngrade},
		"nil evidence":               {Claim{kindEquipment, label}, Claim{kindEquipment, token}, Continuity{Basis: ContinuityBinding}, ErrContinuity},
		"empty evidence":             {Claim{kindEquipment, label}, Claim{kindEquipment, token}, Continuity{Basis: ContinuityBinding, Evidence: []semreg.EvidenceRef{}}, ErrContinuity},
		"unsorted evidence":          {Claim{kindEquipment, label}, Claim{kindEquipment, token}, Continuity{Basis: ContinuityBinding, Evidence: sortedPair(false)}, ErrContinuity},
		"unknown continuity":         {Claim{kindEquipment, label}, Claim{kindEquipment, token}, continuity("vendor_note", "x"), ErrContinuity},
		"operator declaration after a native slot": {Claim{kindZone, zoneAnchor(semreg.AnchorNativeSlot, "zone-a", "x")}, Claim{kindZone, Anchor{Class: semreg.AnchorFunction, Basis: semreg.AnchorNativeIdentity, Source: SourceNativeToken, Scheme: testScheme, Token: "token-z", Slot: "zone-a", Evidence: []semreg.EvidenceRef{evidence("x")}}}, continuity(ContinuityOperatorDeclaration, "audit"), ErrContinuity},
		"refused source": {Claim{kindEquipment, label}, Claim{kindEquipment, mutateSource(token, SourceDisplayName)}, binding, ErrRefusedAnchorSource},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			allocator := newTestAllocator(t, NewMemoryStore())
			if _, _, err := allocator.Supersede(tc.previous, tc.next, tc.continuity); !errors.Is(err, tc.want) {
				t.Fatalf("err=%v, want %v", err, tc.want)
			}
		})
	}
}

func mutateSource(anchor Anchor, source AnchorSource) Anchor {
	anchor.Source = source
	return anchor
}

func TestSupersedeRefusesHeldAndSupersededKeys(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	binding := continuity(ContinuityBinding, "binding")
	labelA, labelB := operatorEquipment("unit-a", "a"), operatorEquipment("unit-b", "b")
	tokenA, tokenB := tokenAnchor("token-a", "a"), tokenAnchor("token-b", "b")
	idA := mustResolve(t, allocator, kindEquipment, labelA)
	mustResolve(t, allocator, kindEquipment, labelB)
	if _, _, err := allocator.Supersede(Claim{kindEquipment, labelA}, Claim{kindEquipment, tokenA}, binding); err != nil {
		t.Fatal(err)
	}
	// The next key is current for another resource in the table.
	if _, _, err := allocator.Supersede(Claim{kindEquipment, labelB}, Claim{kindEquipment, tokenA}, binding); !errors.Is(err, ErrKeyHeld) {
		t.Fatalf("next key recorded current: err=%v, want ErrKeyHeld", err)
	}
	// The previous key is superseded.
	if _, _, err := allocator.Supersede(Claim{kindEquipment, labelA}, Claim{kindEquipment, tokenB}, binding); !errors.Is(err, ErrAnchorSuperseded) {
		t.Fatalf("previous key superseded: err=%v, want ErrAnchorSuperseded", err)
	}
	// The next key is superseded, so it is held as history.
	revised := tokenAnchor("token-a2", "a2")
	if _, _, err := allocator.Supersede(Claim{kindEquipment, tokenA}, Claim{kindEquipment, revised}, continuity(ContinuityNativeMapping, "map")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := allocator.Supersede(Claim{kindEquipment, labelB}, Claim{kindEquipment, tokenA}, binding); !errors.Is(err, ErrKeyHeld) {
		t.Fatalf("next key superseded: err=%v, want ErrKeyHeld", err)
	}
	if got := mustResolve(t, allocator, kindEquipment, revised); got != idA {
		t.Fatalf("two revisions = %q, want %q", got, idA)
	}
}

type failingStore struct {
	lookup, commit error
}

func (s failingStore) Lookup(AnchorKey) (Entry, bool, error) { return Entry{}, false, s.lookup }
func (s failingStore) Commit(Supersession) error             { return s.commit }

func TestStoreFailuresFailClosed(t *testing.T) {
	failure := errors.New("unavailable")
	broken := newTestAllocator(t, failingStore{lookup: failure})
	if _, _, err := broken.Resolve(kindEquipment, tokenAnchor("token-a", "x")); !errors.Is(err, ErrStore) || !errors.Is(err, failure) {
		t.Fatalf("Resolve: err=%v", err)
	}
	refusing := newTestAllocator(t, failingStore{commit: failure})
	if _, _, err := refusing.Supersede(Claim{kindEquipment, operatorEquipment("unit-a", "x")}, Claim{kindEquipment, tokenAnchor("token-a", "x")}, continuity(ContinuityBinding, "x")); !errors.Is(err, ErrStore) || !errors.Is(err, failure) {
		t.Fatalf("Supersede: err=%v", err)
	}
	// A refused commit hands out no new key.
	if got := mustResolve(t, refusing, kindEquipment, tokenAnchor("token-a", "x")); got == mustResolve(t, refusing, kindEquipment, operatorEquipment("unit-a", "x")) {
		t.Fatal("a refused commit still mapped the next key")
	}
}

func TestMemoryStoreRefusesContradictions(t *testing.T) {
	store := NewMemoryStore()
	keyA := AnchorKey{Kind: kindEquipment, Slot: "a"}
	keyB := AnchorKey{Kind: kindEquipment, Slot: "b"}
	keyC := AnchorKey{Kind: kindEquipment, Slot: "c"}
	if err := store.Commit(Supersession{Resource: "resource:x", Previous: keyA, PreviousBasis: semreg.AnchorOperatorSlot, Next: keyB, NextBasis: semreg.AnchorNativeSlot}); err != nil {
		t.Fatal(err)
	}
	for name, record := range map[string]Supersession{
		"previous superseded": {Resource: "resource:x", Previous: keyA, Next: keyC},
		"previous of another": {Resource: "resource:y", Previous: keyB, Next: keyC},
		"next held":           {Resource: "resource:z", Previous: keyC, Next: keyB},
	} {
		if err := store.Commit(record); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("%s: err=%v, want ErrStoreConflict", name, err)
		}
	}
	if entry, ok, _ := store.Lookup(keyA); !ok || !entry.Superseded || entry.Resource != "resource:x" {
		t.Fatalf("previous entry = %+v, %v", entry, ok)
	}
	if entry, ok, _ := store.Lookup(keyB); !ok || entry.Superseded || entry.Basis != semreg.AnchorNativeSlot {
		t.Fatalf("next entry = %+v, %v", entry, ok)
	}
	if _, ok, _ := store.Lookup(keyC); ok {
		t.Fatal("a refused commit left an entry")
	}
}

func TestHomeRecordsOneHomeAsset(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	if _, err := allocator.Home("resource:foreign", "asset:a"); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("unknown resource: err=%v", err)
	}
	if _, err := allocator.Home(" resource", "asset:a"); !errors.Is(err, ErrUnknownResource) {
		t.Fatalf("invalid resource: err=%v", err)
	}
	id := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "x"))
	if _, err := allocator.Home(id, " asset"); err == nil {
		t.Fatal("invalid asset accepted")
	}
	if home, err := allocator.Home(id, "asset:a"); err != nil || home != "asset:a" {
		t.Fatalf("first home = %q, %v", home, err)
	}
	if home, err := allocator.Home(id, "asset:a"); err != nil || home != "asset:a" {
		t.Fatalf("same home again = %q, %v", home, err)
	}
	if home, err := allocator.Home(id, "asset:b"); !errors.Is(err, ErrSecondHome) || home != "asset:a" {
		t.Fatalf("second home = %q, %v; want the recorded home and ErrSecondHome", home, err)
	}
}

func TestConcurrentResolveIsDeterministic(t *testing.T) {
	allocator := newTestAllocator(t, NewMemoryStore())
	want := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "x"))
	var wg sync.WaitGroup
	results := make([]semreg.ResourceID, 32)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, _, err := allocator.Resolve(kindEquipment, tokenAnchor("token-a", "x"))
			if err == nil {
				results[i] = id
			}
		}(i)
	}
	wg.Wait()
	for i, got := range results {
		if got != want {
			t.Fatalf("goroutine %d = %q, want %q", i, got, want)
		}
	}
}
