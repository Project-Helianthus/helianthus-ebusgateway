package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semgraph"
	"github.com/Project-Helianthus/helianthus-modbusreg/huawei"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// The composition keeps the anchor table beside the runtime state file, or in
// memory when no runtime state path is configured; a hub is bound once, and a
// publisher of a hub the composition did not bind gets an in-memory table of
// its own.
func TestSemanticResourceAllocatorComposition(t *testing.T) {
	if _, err := composeSemanticResourceAllocator(nil, ""); err == nil {
		t.Fatal("a nil hub was bound")
	}
	if _, err := semanticResourceAllocatorFor(nil); err == nil {
		t.Fatal("a nil hub got an allocator")
	}

	memoryHub := newTestSemanticHub(t)
	memory, err := composeSemanticResourceAllocator(memoryHub, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := memory.store.(*semgraph.MemoryStore); !ok {
		t.Fatalf("no runtime state path composed %T, want the in-memory table", memory.store)
	}

	dir := t.TempDir()
	fileHub := newTestSemanticHub(t)
	file, err := composeSemanticResourceAllocator(fileHub, filepath.Join(dir, "runtime_state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := file.store.(*semgraph.FileStore); !ok {
		t.Fatalf("a runtime state path composed %T, want the file table", file.store)
	}
	anchor := semgraph.Anchor{Class: semreg.AnchorFunction, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: "alpha/1", Evidence: []semreg.EvidenceRef{semanticAnchorProbeEvidence}}
	if _, _, err := file.resolve(huaweiKindSystem, anchor, "asset:synthetic"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "semgraph-anchors.json")); err != nil {
		t.Fatalf("the anchor table is not kept beside the runtime state: %v", err)
	}
	if _, err := composeSemanticResourceAllocator(fileHub, filepath.Join(dir, "runtime_state.json")); !errors.Is(err, errSemanticResourceAllocatorBound) {
		t.Fatalf("second composition err=%v, want %v", err, errSemanticResourceAllocatorBound)
	}
	if got, err := semanticResourceAllocatorFor(fileHub); err != nil || got != file {
		t.Fatalf("publisher did not get the composed allocator: %v", err)
	}

	unbound := newTestSemanticHub(t)
	first, err := semanticResourceAllocatorFor(unbound)
	if err != nil {
		t.Fatal(err)
	}
	if second, _ := semanticResourceAllocatorFor(unbound); second != first || first == file || first == memory {
		t.Fatal("an unbound hub must keep one in-memory allocator of its own")
	}

	missing := newTestSemanticHub(t)
	if _, err := composeSemanticResourceAllocator(missing, filepath.Join(dir, "absent", "runtime_state.json")); err == nil {
		t.Fatal("an absent runtime state directory was accepted")
	}
}

// The ordinal of a slot is recovered from the anchor table. Every member of
// the slot key is varied: the kind (a slot of another kind is independent),
// the slot base (another base is independent) and the ordinal (an advanced
// slot reopens at the advanced ordinal).
func TestSemanticResourceAllocatorSlotOrdinalRecoveredFromTable(t *testing.T) {
	dir := t.TempDir()
	open := func() *semanticResourceAllocator {
		t.Helper()
		resources, err := openSemanticResourceAllocator(dir)
		if err != nil {
			t.Fatal(err)
		}
		return resources
	}
	resolve := func(resources *semanticResourceAllocator, kind semreg.DefinitionID, base string, ordinal uint64) {
		t.Helper()
		anchor := semgraph.Anchor{Class: semreg.AnchorEquipment, Basis: semreg.AnchorOperatorSlot, Source: semgraph.SourceOperatorLabel, Slot: semanticOrdinalSlot(base, ordinal), Evidence: []semreg.EvidenceRef{semanticAnchorProbeEvidence}}
		if _, _, err := resources.resolve(kind, anchor, "asset:synthetic"); err != nil {
			t.Fatal(err)
		}
	}
	ordinal := func(resources *semanticResourceAllocator, kind semreg.DefinitionID, base string, advance bool) uint64 {
		t.Helper()
		got, err := resources.slotOrdinal(kind, semreg.AnchorEquipment, base, advance)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	first := open()
	if got := ordinal(first, huaweiKindInverter, "alpha/1", false); got != 1 {
		t.Fatalf("empty table ordinal=%d, want 1", got)
	}
	resolve(first, huaweiKindInverter, "alpha/1", 1)
	next := ordinal(first, huaweiKindInverter, "alpha/1", true)
	if next != 2 {
		t.Fatalf("advanced ordinal=%d, want 2", next)
	}
	resolve(first, huaweiKindInverter, "alpha/1", next)

	reopened := open()
	for _, tc := range []struct {
		name string
		kind semreg.DefinitionID
		base string
		want uint64
	}{
		{"advanced slot", huaweiKindInverter, "alpha/1", 2},
		{"other kind", "resource.equipment", "alpha/1", 1},
		{"other base", huaweiKindInverter, "alpha/2", 1},
	} {
		if got := ordinal(reopened, tc.kind, tc.base, false); got != tc.want {
			t.Fatalf("%s: ordinal=%d, want %d", tc.name, got, tc.want)
		}
	}
	memory, err := openSemanticResourceAllocator("")
	if err != nil {
		t.Fatal(err)
	}
	if got := ordinal(memory, huaweiKindInverter, "alpha/1", false); got != 1 {
		t.Fatalf("an in-memory table recovered ordinal %d", got)
	}
}

// A process restart over the composed anchor table keeps the replaced
// child's inverter: the new unit's identifier, never the identifier of the
// unit it replaced. The PV system keeps its one identifier throughout.
func TestHuaweiPVGraphReplacementSurvivesProcessRestart(t *testing.T) {
	dir := t.TempDir()
	fixture := h10Load(t, "sdongle")
	provider, _, _, clock := h10Native(t, fixture)
	cycle := h10Refresh(t, provider, clock)
	hub := newTestSemanticHub(t)
	if _, err := composeSemanticResourceAllocator(hub, filepath.Join(dir, "runtime_state.json")); err != nil {
		t.Fatal(err)
	}
	publication, err := newHuaweiPVPublication(hub, "graph-restart", "source:graph-restart", "alpha", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if err := publication.Publish(cycle); err != nil {
		t.Fatal(err)
	}
	var unit byte
	for candidate := range publication.published {
		unit = candidate
	}
	asset := huaweiAssetID("alpha", "pv", unit)
	original := huaweiGraphOf(t, hub, asset).check(t, asset, "source:graph-restart")
	withheld := cycle
	withheld.Cycle++
	withheld.CompletedAt = withheld.CompletedAt.Add(time.Second)
	withheld.Children = append([]huawei.ProviderChildObservation(nil), cycle.Children...)
	for index := range withheld.Children {
		if withheld.Children[index].UnitID == unit {
			withheld.Children[index].Evidence = huawei.ProviderEvidenceWithheld
			withheld.Children[index].Reason = huawei.ProviderReasonIdentityMismatch
		}
	}
	if err := publication.Publish(withheld); err != nil {
		t.Fatal(err)
	}
	readmitted := cycle
	readmitted.Cycle = withheld.Cycle + 1
	readmitted.CompletedAt = withheld.CompletedAt.Add(time.Second)
	if err := publication.Publish(readmitted); err != nil {
		t.Fatal(err)
	}
	replaced := huaweiGraphOf(t, hub, asset).check(t, asset, "source:graph-restart")
	if replaced.inverter == original.inverter || replaced.system != original.system {
		t.Fatal("replacement did not move the inverter alone")
	}

	restartedHub := newTestSemanticHub(t)
	if _, err := composeSemanticResourceAllocator(restartedHub, filepath.Join(dir, "runtime_state.json")); err != nil {
		t.Fatal(err)
	}
	restarted, err := newHuaweiPVPublication(restartedHub, "graph-restart", "source:graph-restart", "alpha", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	freshProvider, _, _, freshClock := h10Native(t, fixture)
	if !freshClock.Now().After(readmitted.CompletedAt) {
		_ = freshClock.Wait(context.Background(), readmitted.CompletedAt.Sub(freshClock.Now())+time.Second)
	}
	if err := restarted.Publish(h10Refresh(t, freshProvider, freshClock)); err != nil {
		t.Fatal(err)
	}
	after := huaweiGraphOf(t, restartedHub, asset).check(t, asset, "source:graph-restart")
	if after.inverter != replaced.inverter || after.system != original.system {
		t.Fatalf("process restart changed identifiers: inverter %s want %s", after.inverter, replaced.inverter)
	}
}

// Every member of a child's anchors is varied: the configured instance label
// and the configured child entry name the system and the inverter, and the
// native string index names an input under its inverter. Identifiers never
// collide across any of them, and no identifier equals an asset identifier.
func TestHuaweiPVGraphIdentifiersFollowEveryAnchorMember(t *testing.T) {
	provider, _, _, clock := h10Native(t, h10Load(t, "smartlogger"))
	cycle := h10Refresh(t, provider, clock)
	ids := map[semreg.ResourceID]string{}
	record := func(id semreg.ResourceID, role string) {
		t.Helper()
		if other, seen := ids[id]; seen {
			t.Fatalf("%s and %s share identifier %s", role, other, id)
		}
		ids[id] = role
	}
	for _, label := range []string{"alpha", "beta"} {
		hub := newTestSemanticHub(t)
		publication, err := newHuaweiPVPublication(hub, "graph-members", "source:graph-members", label, nil, func() bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		labelled := cycle
		labelled.Source = label
		if err := publication.Publish(labelled); err != nil {
			t.Fatal(err)
		}
		if len(publication.published) < 2 {
			t.Fatal("fixture needs two admitted children")
		}
		for unit, asset := range publication.published {
			identity := huaweiGraphOf(t, hub, asset).check(t, asset, "source:graph-members")
			role := label + "/" + string(rune('0'+unit))
			record(identity.system, role+" system")
			record(identity.inverter, role+" inverter")
			for slot, input := range identity.inputs {
				record(input, role+" input "+slot)
			}
			if _, clash := ids[semreg.ResourceID(asset)]; clash {
				t.Fatal("a resource identifier equals an asset identifier")
			}
		}
	}
}
