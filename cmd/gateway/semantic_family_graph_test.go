package main

import (
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
)

// The table names every composed family once, in bytewise order, and every
// entry is off in this slice. The accessor returns each entry's value and
// answers off for a family the table does not list.
func TestSemanticFamilyGraphTableNamesEveryComposedFamilyOff(t *testing.T) {
	want := []semanticFamily{semanticFamilyEVSE, semanticFamilyPV, semanticFamilyStorage, semanticFamilyThermal}
	if len(semanticFamilyGraphTable) != len(want) {
		t.Fatalf("table lists %d families, want %d", len(semanticFamilyGraphTable), len(want))
	}
	for index, entry := range semanticFamilyGraphTable {
		if entry.family != want[index] {
			t.Fatalf("entry %d=%q want %q", index, entry.family, want[index])
		}
		if entry.graph {
			t.Fatalf("family %q is switched on before its publishers migrate", entry.family)
		}
		if semanticFamilyGraph(entry.family) != entry.graph {
			t.Fatalf("accessor for %q differs from the table", entry.family)
		}
	}
	if semanticFamilyGraph("unlisted") {
		t.Fatal("an unlisted family is graph-enforced")
	}
}

// semanticFamilyEnforcement is the hub's enforcement state for the table
// value of family.
func semanticFamilyEnforcement(family semanticFamily) semhub.GraphEnforcement {
	if semanticFamilyGraph(family) {
		return semhub.GraphEnforced
	}
	return semhub.GraphNotEnforced
}

// requireFamilyGraph asserts that the hub reports every asset of driver with
// the enforcement the table names for family, and that at least one exists.
func requireFamilyGraph(t *testing.T, hub *semhub.Hub, family semanticFamily, driver string) {
	t.Helper()
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	if state.CatalogDigest != graph.PinnedDigest {
		t.Fatalf("catalog digest=%q want %q", state.CatalogDigest, graph.PinnedDigest)
	}
	found := 0
	for _, asset := range state.Assets {
		if asset.Driver != driver {
			continue
		}
		found++
		if asset.Enforcement != semanticFamilyEnforcement(family) {
			t.Fatalf("family %q asset %q is %q, the table says %q", family, asset.Asset, asset.Enforcement, semanticFamilyEnforcement(family))
		}
	}
	if found == 0 {
		t.Fatalf("family %q declared no asset for driver %q (state %+v)", family, driver, state)
	}
}

// Every composed family's declaration path builds the kernel the table names:
// storage and EVSE through the preflight, both PV classes through their class
// declaration and thermal through its publisher's declaration.
func TestSemanticFamilyGraphMatchesEveryComposedDeclaration(t *testing.T) {
	t.Run("storage and evse", func(t *testing.T) {
		hub, err := preflightSemanticHub(ebusgateway.Config{ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: teslaRetainedConfig()}})
		if err != nil {
			t.Fatal(err)
		}
		requireFamilyGraph(t, hub, semanticFamilyStorage, growattStorageDriver)
		requireFamilyGraph(t, hub, semanticFamilyEVSE, teslaEVSEDriver)
	})
	t.Run("pv sunspec class", func(t *testing.T) {
		hub := newTestSemanticHub(t)
		class, err := modbusadapter.NewSemanticPVClass(hub, false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := class.Declare("asset:synthetic-pv", "source:synthetic-pv"); err != nil {
			t.Fatal(err)
		}
		requireFamilyGraph(t, hub, semanticFamilyPV, "pv_sunspec")
	})
	t.Run("pv logger class", func(t *testing.T) {
		hub := newTestSemanticHub(t)
		publication, err := newHuaweiPVPublication(hub, "pv.synthetic", "source:synthetic-logger", "alpha", nil, func() bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		if _, err := publication.class.Declare("asset:synthetic-logger-child", publication.handle.Source()); err != nil {
			t.Fatal(err)
		}
		requireFamilyGraph(t, hub, semanticFamilyPV, "pv.synthetic")
	})
	t.Run("thermal", func(t *testing.T) {
		publisher, _, _ := m15Fixture(t)
		if publisher.scalar(publisher.binding.Identity.Target) == nil {
			t.Fatalf("thermal fixture did not publish: %v", publisher.lastError)
		}
		requireFamilyGraph(t, publisher.hub, semanticFamilyThermal, string(primaryEBusDriverID))
	})
}

// setSemanticFamilyGraph switches one table entry for the test and restores it.
func setSemanticFamilyGraph(t *testing.T, family semanticFamily, value bool) {
	t.Helper()
	for index := range semanticFamilyGraphTable {
		if semanticFamilyGraphTable[index].family == family {
			previous := semanticFamilyGraphTable[index].graph
			semanticFamilyGraphTable[index].graph = value
			t.Cleanup(func() { semanticFamilyGraphTable[index].graph = previous })
			return
		}
	}
	t.Fatalf("family %q is not in the table", family)
}

// A table entry reaches its family's declarations: switching it on makes the
// storage and EVSE specs ask for enforcement and makes the logger-attached PV
// class declare enforced assets; the other families are unaffected.
func TestSemanticFamilyGraphEntryReachesDeclarations(t *testing.T) {
	setSemanticFamilyGraph(t, semanticFamilyStorage, true)
	setSemanticFamilyGraph(t, semanticFamilyEVSE, true)
	setSemanticFamilyGraph(t, semanticFamilyPV, true)
	if !growattStorageAssetSpec("asset:synthetic-storage", "source:synthetic-storage").Graph {
		t.Fatal("storage spec ignores the table")
	}
	if !teslaEVSEAssetSpec(teslaRetainedConfig()).Graph {
		t.Fatal("EVSE spec ignores the table")
	}
	hub := newTestSemanticHub(t)
	publication, err := newHuaweiPVPublication(hub, "pv.synthetic", "source:synthetic-logger", "alpha", nil, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publication.class.Declare("asset:synthetic-logger-child", publication.handle.Source()); err != nil {
		t.Fatal(err)
	}
	requireFamilyGraph(t, hub, semanticFamilyPV, "pv.synthetic")
	state, _ := hub.GraphState()
	for _, asset := range state.Assets {
		if asset.Driver == "pv.synthetic" && asset.Enforcement != semhub.GraphEnforced {
			t.Fatalf("asset %q not enforced", asset.Asset)
		}
	}
	pvHub := newTestSemanticHub(t)
	class, err := modbusadapter.NewSemanticPVClass(pvHub, semanticFamilyGraph(semanticFamilyPV))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := class.Declare("asset:synthetic-pv", "source:synthetic-pv"); err != nil {
		t.Fatal(err)
	}
	requireFamilyGraph(t, pvHub, semanticFamilyPV, "pv_sunspec")
}
