package semgraph

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

func openFileStoreForTest(t *testing.T, dir string) *FileStore {
	t.Helper()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	file, ok := store.(*FileStore)
	if !ok {
		t.Fatalf("OpenStore(dir) = %T, want *FileStore", store)
	}
	return file
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

// An identifier allocated under an operator label and superseded by a native
// token is the same after the allocator is rebuilt from the file; the label
// stays superseded and the home asset stays recorded. Over an empty table the
// token derives another identifier, which is the identifier a restart without
// the file would have allocated.
func TestStoreKeepsASupersededIdentifierAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	label, token := operatorEquipment("unit-a", "label"), tokenAnchor("token-a", "token")
	first := newTestAllocator(t, openFileStoreForTest(t, dir))
	id := mustResolve(t, first, kindEquipment, label)
	kept, _, err := first.Supersede(Claim{kindEquipment, label}, Claim{kindEquipment, token}, continuity(ContinuityOperatorDeclaration, "audit"))
	if err != nil || kept != id {
		t.Fatalf("Supersede = %q, %v; want %q", kept, err, id)
	}
	if home, err := first.Home(id, "asset:a"); err != nil || home != "asset:a" {
		t.Fatalf("Home = %q, %v", home, err)
	}

	if derived := mustResolve(t, newTestAllocator(t, NewMemoryStore()), kindEquipment, token); derived == id {
		t.Fatal("the token derives the superseded identifier; the test proves nothing")
	}
	rebuilt := newTestAllocator(t, openFileStoreForTest(t, dir))
	if got := mustResolve(t, rebuilt, kindEquipment, token); got != id {
		t.Fatalf("native token after a restart = %q, want %q", got, id)
	}
	if _, _, err := rebuilt.Resolve(kindEquipment, label); !errors.Is(err, ErrAnchorSuperseded) {
		t.Fatalf("operator label after a restart: err=%v, want ErrAnchorSuperseded", err)
	}
	if home, err := rebuilt.Home(id, "asset:b"); !errors.Is(err, ErrSecondHome) || home != "asset:a" {
		t.Fatalf("second home after a restart = %q, %v; want asset:a and ErrSecondHome", home, err)
	}
	// A revision of the token after the restart continues the same history.
	revised := tokenAnchor("token-a2", "revised")
	if kept, _, err := rebuilt.Supersede(Claim{kindEquipment, token}, Claim{kindEquipment, revised}, continuity(ContinuityNativeMapping, "map")); err != nil || kept != id {
		t.Fatalf("revision after a restart = %q, %v; want %q", kept, err, id)
	}
	if got := mustResolve(t, newTestAllocator(t, openFileStoreForTest(t, dir)), kindEquipment, revised); got != id {
		t.Fatalf("revised token after a second restart = %q, want %q", got, id)
	}
}

// Every stored member survives a restart, and a key that differs in any one
// member is not found:
//
//	member              variation                        production mutation caught
//	kind                semgraphtest.input -> zone kind  kind not written or not read
//	key (native)        token-a -> token-b digest        native key not written or not read
//	parent (key)        parent A -> parent B             key parent not written or not read
//	slot (key)          input-1 -> input-2               key slot not written or not read
//	resource            per entry                        resource not written or not read
//	class               component, equipment, function   class not written or not read
//	basis               native_slot, operator_slot, ...  basis not written or not read
//	superseded          true and false                   superseded flag not written or not read
//	parent (non-key)    parent A on a native component   anchor parent dropped from a native entry
//	slot (non-key)      module-1 on a native component   anchor slot dropped from a native entry
//	home resource/asset two homes                        a home not written or not read
func TestStoreRoundTripsEveryEntryMember(t *testing.T) {
	dir := t.TempDir()
	store := openFileStoreForTest(t, dir)
	keyA, _ := IdentityKey(testScheme, testIssuer, "token-a")
	keyB, _ := IdentityKey(testScheme, testIssuer, "token-b")
	parentA, parentB := expectedID("parent-a"), expectedID("parent-b")
	idInput, idEquipment, idModule := expectedID("input"), expectedID("equipment"), expectedID("module")
	slotKey := AnchorKey{Kind: kindInput, Parent: parentA, Slot: "input-1"}
	labelKey := AnchorKey{Kind: kindZone, Slot: "zone-a"}
	nativeKey := AnchorKey{Kind: kindInput, Key: keyA}
	revisedKey := AnchorKey{Kind: kindInput, Key: keyB}
	records := []Supersession{
		{Resource: idInput, Class: semreg.AnchorComponent, Previous: slotKey, PreviousBasis: semreg.AnchorNativeSlot, PreviousParent: parentA, PreviousSlot: "input-1", Next: slotKey, NextBasis: semreg.AnchorNativeSlot, NextParent: parentA, NextSlot: "input-1"},
		{Resource: idEquipment, Class: semreg.AnchorFunction, Previous: labelKey, PreviousBasis: semreg.AnchorOperatorSlot, PreviousSlot: "zone-a", Next: labelKey, NextBasis: semreg.AnchorOperatorSlot, NextSlot: "zone-a"},
		{Resource: idModule, Class: semreg.AnchorComponent, Previous: nativeKey, PreviousBasis: semreg.AnchorNativeIdentity, PreviousParent: parentA, PreviousSlot: "module-1", Next: nativeKey, NextBasis: semreg.AnchorNativeIdentity, NextParent: parentA, NextSlot: "module-1"},
		{Resource: idModule, Class: semreg.AnchorComponent, Previous: nativeKey, PreviousBasis: semreg.AnchorNativeIdentity, PreviousParent: parentA, PreviousSlot: "module-1", Next: revisedKey, NextBasis: semreg.AnchorNativeIdentity, NextParent: parentA, NextSlot: "module-1"},
	}
	for _, record := range records {
		if err := store.Commit(record); err != nil {
			t.Fatalf("Commit(%+v): %v", record, err)
		}
	}
	for resource, asset := range map[semreg.ResourceID]semreg.AssetID{idInput: "asset:a", idModule: "asset:b"} {
		if home, err := store.RecordHome(resource, asset); err != nil || home != asset {
			t.Fatalf("RecordHome = %q, %v", home, err)
		}
	}
	want := map[AnchorKey]Entry{
		slotKey:    {Resource: idInput, Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeSlot, Parent: parentA, Slot: "input-1"},
		labelKey:   {Resource: idEquipment, Class: semreg.AnchorFunction, Basis: semreg.AnchorOperatorSlot, Slot: "zone-a"},
		nativeKey:  {Resource: idModule, Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeIdentity, Parent: parentA, Slot: "module-1", Superseded: true},
		revisedKey: {Resource: idModule, Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeIdentity, Parent: parentA, Slot: "module-1"},
	}
	reopened := openFileStoreForTest(t, dir)
	for key, entry := range want {
		got, ok, err := reopened.Lookup(key)
		if err != nil || !ok || got != entry {
			t.Fatalf("Lookup(%+v) after reopen = %+v, %v, %v; want %+v", key, got, ok, err, entry)
		}
	}
	for name, key := range map[string]AnchorKey{
		"kind":   {Kind: kindZone, Parent: parentA, Slot: "input-1"},
		"key":    {Kind: kindInput, Key: digestOf("other")},
		"parent": {Kind: kindInput, Parent: parentB, Slot: "input-1"},
		"slot":   {Kind: kindInput, Parent: parentA, Slot: "input-2"},
	} {
		if got, ok, err := reopened.Lookup(key); ok || err != nil {
			t.Fatalf("varying %s found %+v, %v", name, got, err)
		}
	}
	for resource, asset := range map[semreg.ResourceID]semreg.AssetID{idInput: "asset:a", idModule: "asset:b"} {
		if home, err := reopened.RecordHome(resource, "asset:other"); err != nil || home != asset {
			t.Fatalf("home of %s after reopen = %q, %v; want %q", resource, home, err, asset)
		}
	}
	if home, err := reopened.RecordHome(idEquipment, "asset:c"); err != nil || home != "asset:c" {
		t.Fatalf("a resource without a home = %q, %v", home, err)
	}
}

// An absent file is an empty table: opening writes nothing, and the first
// recorded key creates the file.
func TestStoreAbsentFileIsAnEmptyTable(t *testing.T) {
	dir := t.TempDir()
	store := openFileStoreForTest(t, dir)
	if names := dirNames(t, dir); len(names) != 0 {
		t.Fatalf("opening wrote %v", names)
	}
	key, err := tokenAnchor("token-a", "x").Key(kindEquipment)
	if err != nil {
		t.Fatal(err)
	}
	if entry, ok, err := store.Lookup(key); ok || err != nil {
		t.Fatalf("Lookup over an absent file = %+v, %v, %v", entry, ok, err)
	}
	allocator := newTestAllocator(t, store)
	id := mustResolve(t, allocator, kindEquipment, tokenAnchor("token-a", "x"))
	if id != resourceID(key) {
		t.Fatalf("identifier over an empty table = %q, want the derived %q", id, resourceID(key))
	}
	if names := dirNames(t, dir); len(names) != 1 || names[0] != storeFileName {
		t.Fatalf("directory after the first key = %v, want only %s", names, storeFileName)
	}
}

// Without a directory the store is the in-memory table; a directory that is
// missing or not a directory is refused.
func TestOpenStoreWithoutDirectoryIsInMemory(t *testing.T) {
	store, err := OpenStore("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*MemoryStore); !ok {
		t.Fatalf("OpenStore(\"\") = %T, want *MemoryStore", store)
	}
	dir := t.TempDir()
	if store, err := OpenStore(filepath.Join(dir, "missing")); err == nil || store != nil {
		t.Fatalf("missing directory = %T, %v", store, err)
	}
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if store, err := OpenStore(file); err == nil || store != nil {
		t.Fatalf("a file as directory = %T, %v", store, err)
	}
}

// validStoreDocument is a valid stored document with one current native
// entry, one superseded slot entry and one home, built as a map so that each
// case can damage one member.
func validStoreDocument() map[string]any {
	key, _ := IdentityKey(testScheme, testIssuer, "token-a")
	id := string(expectedID("stored"))
	return map[string]any{
		"format":  storeFormat,
		"version": storeVersion,
		"anchors": []any{
			map[string]any{"kind": string(kindEquipment), "key": string(key), "resource": id, "class": "equipment", "basis": "native_identity", "superseded": false},
			map[string]any{"kind": string(kindEquipment), "slot": "unit-a", "resource": id, "class": "equipment", "basis": "operator_slot", "superseded": true},
		},
		"homes": []any{map[string]any{"resource": id, "asset": "asset:a"}},
	}
}

func encodeDocument(t *testing.T, document map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func withAnchor(member string, value any) func(map[string]any) {
	return func(document map[string]any) {
		anchor := document["anchors"].([]any)[0].(map[string]any)
		if value == nil {
			delete(anchor, member)
			return
		}
		anchor[member] = value
	}
}

// A damaged or foreign file fails closed with a named error: OpenStore
// returns no store, so no allocator can be built over it, and the file and the
// directory are left as they were.
func TestStoreDamagedOrForeignFileFailsClosed(t *testing.T) {
	valid := encodeDocument(t, validStoreDocument())
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, storeFileName), valid, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(dir); err != nil {
		t.Fatalf("the valid fixture is refused: %v", err)
	}
	mutated := func(change func(map[string]any)) []byte {
		document := validStoreDocument()
		change(document)
		return encodeDocument(t, document)
	}
	cases := map[string]struct {
		data []byte
		want error
	}{
		"empty":                  {[]byte{}, ErrStoreDamaged},
		"not json":               {[]byte("not json"), ErrStoreDamaged},
		"truncated":              {valid[:len(valid)/2], ErrStoreDamaged},
		"trailing data":          {append(append([]byte{}, valid...), []byte(` {}`)...), ErrStoreDamaged},
		"array":                  {[]byte(`[]`), ErrStoreDamaged},
		"unknown member":         {mutated(func(d map[string]any) { d["extra"] = true }), ErrStoreDamaged},
		"unknown anchor member":  {mutated(withAnchor("extra", "x")), ErrStoreDamaged},
		"invalid kind":           {mutated(withAnchor("kind", "Invalid Kind")), ErrStoreDamaged},
		"invalid key":            {mutated(withAnchor("key", "sha256:zz")), ErrStoreDamaged},
		"invalid resource":       {mutated(withAnchor("resource", " resource")), ErrStoreDamaged},
		"missing resource":       {mutated(withAnchor("resource", nil)), ErrStoreDamaged},
		"unknown class":          {mutated(withAnchor("class", "device")), ErrStoreDamaged},
		"unknown basis":          {mutated(withAnchor("basis", "guess")), ErrStoreDamaged},
		"native key, slot basis": {mutated(withAnchor("basis", "native_slot")), ErrStoreDamaged},
		"slot key, native basis": {mutated(func(d map[string]any) { d["anchors"].([]any)[1].(map[string]any)["basis"] = "native_identity" }), ErrStoreDamaged},
		"slot key without slot":  {mutated(func(d map[string]any) { delete(d["anchors"].([]any)[1].(map[string]any), "slot") }), ErrStoreDamaged},
		"slot with control":      {mutated(withAnchor("slot", "unit\ta")), ErrStoreDamaged},
		"invalid parent":         {mutated(withAnchor("parent", " parent")), ErrStoreDamaged},
		"duplicate key":          {mutated(func(d map[string]any) { d["anchors"] = append(d["anchors"].([]any), d["anchors"].([]any)[0]) }), ErrStoreDamaged},
		"invalid home asset":     {mutated(func(d map[string]any) { d["homes"].([]any)[0].(map[string]any)["asset"] = " asset" }), ErrStoreDamaged},
		"duplicate home": {mutated(func(d map[string]any) {
			d["homes"] = append(d["homes"].([]any), map[string]any{"resource": string(expectedID("stored")), "asset": "asset:b"})
		}), ErrStoreDamaged},
		"string version":        {mutated(func(d map[string]any) { d["version"] = "1" }), ErrStoreDamaged},
		"other format":          {mutated(func(d map[string]any) { d["format"] = "semgraphtest.other" }), ErrStoreForeign},
		"no format":             {mutated(func(d map[string]any) { delete(d, "format") }), ErrStoreForeign},
		"other version":         {mutated(func(d map[string]any) { d["version"] = storeVersion + 1 }), ErrStoreForeign},
		"foreign json document": {[]byte(`{"slots":[]}`), ErrStoreForeign},
		"version zero":          {mutated(func(d map[string]any) { d["version"] = 0 }), ErrStoreForeign},
		"too large":             {append([]byte(`{"format":"`), bytes.Repeat([]byte("a"), maxStoreFileBytes)...), ErrStoreDamaged},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, storeFileName)
			if err := os.WriteFile(path, tc.data, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := OpenStore(dir)
			if !errors.Is(err, tc.want) || store != nil {
				t.Fatalf("OpenStore = %T, %v; want no store and %v", store, err, tc.want)
			}
			if _, err := NewAllocator(store); !errors.Is(err, ErrNilStore) {
				t.Fatalf("an allocator was built over a refused file: %v", err)
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, tc.data) {
				t.Fatalf("the refused file changed: %v", err)
			}
			if names := dirNames(t, dir); len(names) != 1 || names[0] != storeFileName {
				t.Fatalf("directory after a refused open = %v", names)
			}
		})
	}
	t.Run("not a regular file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, storeFileName), 0o700); err != nil {
			t.Fatal(err)
		}
		if store, err := OpenStore(dir); !errors.Is(err, ErrStoreDamaged) || store != nil {
			t.Fatalf("OpenStore = %T, %v; want ErrStoreDamaged", store, err)
		}
	})
}

// A document larger than the bound a reopen accepts is refused before the file
// is replaced, so the store that was valid stays valid across a restart.
func TestStoreRefusesOversizedDocument(t *testing.T) {
	dir := t.TempDir()
	store := openFileStoreForTest(t, dir)
	allocator := newTestAllocator(t, store)
	mustResolve(t, allocator, kindEquipment, operatorEquipment("unit-a", "label"))
	before, err := os.ReadFile(filepath.Join(dir, storeFileName))
	if err != nil {
		t.Fatal(err)
	}
	homes := make(map[semreg.ResourceID]semreg.AssetID)
	for i := 0; len(homes) < 120000; i++ {
		homes[expectedID(fmt.Sprintf("bulk-%d", i))] = "asset:a"
	}
	if err := store.write(store.entries, homes); !errors.Is(err, ErrStore) {
		t.Fatalf("oversized write: err=%v", err)
	}
	if after, err := os.ReadFile(filepath.Join(dir, storeFileName)); err != nil || !bytes.Equal(after, before) {
		t.Fatalf("an oversized write changed the file: %v", err)
	}
	if names := dirNames(t, dir); len(names) != 1 {
		t.Fatalf("an oversized write left %v", names)
	}
	if _, err := OpenStore(dir); err != nil {
		t.Fatalf("reopen: %v", err)
	}
}

// A write that fails before the replacement leaves the table and the file as
// they were and the store usable; a failure after the replacement leaves the
// store uncertain, so every later operation is refused until it is reopened,
// and the reopened store holds what was written.
func TestStoreWriteFailuresFailClosed(t *testing.T) {
	failure := errors.New("injected")
	for _, step := range []string{"create", "write", "sync", "close", "rename"} {
		t.Run(step, func(t *testing.T) {
			dir := t.TempDir()
			store := openFileStoreForTest(t, dir)
			allocator := newTestAllocator(t, store)
			label := operatorEquipment("unit-a", "label")
			id := mustResolve(t, allocator, kindEquipment, label)
			before, err := os.ReadFile(filepath.Join(dir, storeFileName))
			if err != nil {
				t.Fatal(err)
			}
			store.fault = func(at string) error {
				if at == step {
					return failure
				}
				return nil
			}
			token := tokenAnchor("token-a", "token")
			if _, _, err := allocator.Supersede(Claim{kindEquipment, label}, Claim{kindEquipment, token}, continuity(ContinuityBinding, "both")); !errors.Is(err, ErrStore) || !errors.Is(err, failure) {
				t.Fatalf("Supersede with a failing %s: err=%v", step, err)
			}
			if after, err := os.ReadFile(filepath.Join(dir, storeFileName)); err != nil || !bytes.Equal(after, before) {
				t.Fatalf("a failed %s changed the file: %v", step, err)
			}
			if names := dirNames(t, dir); len(names) != 1 {
				t.Fatalf("a failed %s left %v", step, names)
			}
			store.fault = nil
			if got := mustResolve(t, allocator, kindEquipment, label); got != id {
				t.Fatalf("label after a failed %s = %q, want %q", step, got, id)
			}
			if got := mustResolve(t, newTestAllocator(t, openFileStoreForTest(t, dir)), kindEquipment, label); got != id {
				t.Fatalf("label after a failed %s and a restart = %q, want %q", step, got, id)
			}
		})
	}
	t.Run("sync directory", func(t *testing.T) {
		dir := t.TempDir()
		store := openFileStoreForTest(t, dir)
		allocator := newTestAllocator(t, store)
		label, token := operatorEquipment("unit-a", "label"), tokenAnchor("token-a", "token")
		id := mustResolve(t, allocator, kindEquipment, label)
		store.fault = func(at string) error {
			if at == "sync directory" {
				return failure
			}
			return nil
		}
		if _, _, err := allocator.Supersede(Claim{kindEquipment, label}, Claim{kindEquipment, token}, continuity(ContinuityBinding, "both")); !errors.Is(err, ErrStoreUncertain) || !errors.Is(err, failure) {
			t.Fatalf("Supersede with a failing directory sync: err=%v", err)
		}
		store.fault = nil
		key, _ := token.Key(kindEquipment)
		if _, _, err := store.Lookup(key); !errors.Is(err, ErrStoreUncertain) {
			t.Fatalf("Lookup on an uncertain store: err=%v", err)
		}
		if err := store.Commit(Supersession{Resource: id, Class: semreg.AnchorEquipment, Previous: key, PreviousBasis: semreg.AnchorNativeIdentity, Next: key, NextBasis: semreg.AnchorNativeIdentity}); !errors.Is(err, ErrStoreUncertain) {
			t.Fatalf("Commit on an uncertain store: err=%v", err)
		}
		if _, err := store.RecordHome(id, "asset:a"); !errors.Is(err, ErrStoreUncertain) {
			t.Fatalf("RecordHome on an uncertain store: err=%v", err)
		}
		if _, _, err := allocator.Resolve(kindEquipment, token); !errors.Is(err, ErrStore) || !errors.Is(err, ErrStoreUncertain) {
			t.Fatalf("Resolve on an uncertain store: err=%v", err)
		}
		if got := mustResolve(t, newTestAllocator(t, openFileStoreForTest(t, dir)), kindEquipment, token); got != id {
			t.Fatalf("token after reopening = %q, want %q", got, id)
		}
	})
}

// A record the file could not load again is refused before anything is
// written: a slot key whose recorded parent or slot differs from the key's,
// and an invalid home asset.
func TestStoreRefusesRecordsItCouldNotLoad(t *testing.T) {
	dir := t.TempDir()
	store := openFileStoreForTest(t, dir)
	key := AnchorKey{Kind: kindZone, Slot: "zone-a"}
	for name, record := range map[string]Supersession{
		"slot differs from the key":   {Resource: "resource:x", Class: semreg.AnchorFunction, Previous: key, PreviousBasis: semreg.AnchorOperatorSlot, PreviousSlot: "zone-b", Next: key, NextBasis: semreg.AnchorOperatorSlot, NextSlot: "zone-b"},
		"parent differs from the key": {Resource: "resource:x", Class: semreg.AnchorFunction, Previous: key, PreviousBasis: semreg.AnchorOperatorSlot, PreviousParent: "resource:p", PreviousSlot: "zone-a", Next: key, NextBasis: semreg.AnchorOperatorSlot, NextParent: "resource:p", NextSlot: "zone-a"},
		"invalid resource":            {Resource: " x", Class: semreg.AnchorFunction, Previous: key, PreviousBasis: semreg.AnchorOperatorSlot, PreviousSlot: "zone-a", Next: key, NextBasis: semreg.AnchorOperatorSlot, NextSlot: "zone-a"},
		"unknown class":               {Resource: "resource:x", Class: "device", Previous: key, PreviousBasis: semreg.AnchorOperatorSlot, PreviousSlot: "zone-a", Next: key, NextBasis: semreg.AnchorOperatorSlot, NextSlot: "zone-a"},
	} {
		if err := store.Commit(record); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("%s: err=%v, want ErrStoreConflict", name, err)
		}
	}
	if _, err := store.RecordHome("resource:x", " asset"); err == nil {
		t.Fatal("an invalid home asset was recorded")
	}
	if names := dirNames(t, dir); len(names) != 0 {
		t.Fatalf("refused records wrote %v", names)
	}
}

// The parent and slot of a native anchor are not key members, but they are
// part of the current state of its key: a supersession whose previous anchor
// names the current token at another parent or another slot is refused, in
// the memory store and in the file store, by the allocator and by the table.
//
//	member  variation                 production mutation caught
//	parent  parent A -> parent B      parent not stored, or not compared at supersession
//	slot    module-1 -> module-2      slot not stored, or not compared at supersession
func TestSupersedeComparesTheNativeAnchorParentAndSlotInEveryStore(t *testing.T) {
	stores := map[string]func(t *testing.T) Store{
		"memory": func(*testing.T) Store { return NewMemoryStore() },
		"file":   func(t *testing.T) Store { return openFileStoreForTest(t, t.TempDir()) },
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			allocator := newTestAllocator(t, store)
			parentA := mustResolve(t, allocator, kindEquipment, tokenAnchor("parent-a", "a"))
			parentB := mustResolve(t, allocator, kindEquipment, tokenAnchor("parent-b", "b"))
			module := func(parent semreg.ResourceID, slot, token string) Anchor {
				return Anchor{Class: semreg.AnchorComponent, Basis: semreg.AnchorNativeIdentity, Source: SourceNativeToken, Scheme: testScheme, Issuer: testIssuer, Token: token, Parent: parent, Slot: slot, Evidence: []semreg.EvidenceRef{evidence(token)}}
			}
			current := module(parentA, "module-1", "module-a")
			id := mustResolve(t, allocator, kindInput, current)
			revised := module(parentA, "module-1", "module-a2")
			mapping := continuity(ContinuityNativeMapping, "map")
			for member, previous := range map[string]Anchor{
				"parent": module(parentB, "module-1", "module-a"),
				"slot":   module(parentA, "module-2", "module-a"),
			} {
				if _, _, err := allocator.Supersede(Claim{kindInput, previous}, Claim{kindInput, revised}, mapping); !errors.Is(err, ErrPreviousAnchor) {
					t.Fatalf("previous anchor at another %s: err=%v, want ErrPreviousAnchor", member, err)
				}
				key, _ := previous.Key(kindInput)
				record := Supersession{Resource: id, Class: semreg.AnchorComponent, Previous: key, PreviousBasis: semreg.AnchorNativeIdentity, PreviousParent: previous.Parent, PreviousSlot: previous.Slot, Next: key, NextBasis: semreg.AnchorNativeIdentity, NextParent: previous.Parent, NextSlot: previous.Slot}
				if err := store.Commit(record); !errors.Is(err, ErrStoreConflict) {
					t.Fatalf("table record at another %s: err=%v, want ErrStoreConflict", member, err)
				}
			}
			revisedKey, _ := revised.Key(kindInput)
			if entry, ok, err := store.Lookup(revisedKey); ok || err != nil {
				t.Fatalf("a refused supersession recorded %+v, %v", entry, err)
			}
			kept, record, err := allocator.Supersede(Claim{kindInput, current}, Claim{kindInput, revised}, mapping)
			if err != nil || kept != id || record.Previous.Parent == nil || *record.Previous.Parent != parentA || record.Previous.Slot == nil || *record.Previous.Slot != "module-1" {
				t.Fatalf("Supersede from the current anchor = %q, %+v, %v; want %q at parent A, module-1", kept, record, err, id)
			}
			if entry, ok, err := store.Lookup(revisedKey); !ok || err != nil || entry.Parent != parentA || entry.Slot != "module-1" || entry.Resource != id {
				t.Fatalf("revised entry = %+v, %v, %v", entry, ok, err)
			}
		})
	}
}
