package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/releasecheck"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/graph"
)

func newTestSemanticHub(t *testing.T) *semhub.Hub {
	t.Helper()
	hub, err := preflightSemanticHub(ebusgateway.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func newTestSemanticPVClass(t *testing.T) *semhub.Class {
	t.Helper()
	class, err := modbusadapter.NewSemanticPVClass(newTestSemanticHub(t), false)
	if err != nil {
		t.Fatal(err)
	}
	return class
}

func TestPreflightSemanticHubDeclaresStaticIdentitiesAndThePVClass(t *testing.T) {
	hub, err := preflightSemanticHub(ebusgateway.Config{})
	if err != nil || hub == nil || len(hub.Sources()) != 0 {
		t.Fatalf("disabled families declared identities: hub=%v err=%v", hub, err)
	}
	cfg := ebusgateway.Config{ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: teslaRetainedConfig()}}
	hub, err = preflightSemanticHub(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sources := hub.Sources()
	if len(sources) != 2 || sources[0].Driver != "growatt_storage" || sources[1].Driver != "tesla_evse" || sources[0].Live || sources[1].Live {
		t.Fatalf("declared sources=%+v", sources)
	}
}

func TestPreflightSemanticHubRejectsCrossFamilyCollisionsNamingBothOwners(t *testing.T) {
	for name, mutate := range map[string]func(*ebusgateway.TeslaGen3HSCRetainedConfig){
		"same asset":            func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.AssetID = growattProductionConfig().AssetID },
		"same source":           func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.SourceID = growattProductionConfig().SourceID },
		"asset is other source": func(c *ebusgateway.TeslaGen3HSCRetainedConfig) { c.AssetID = growattProductionConfig().SourceID },
	} {
		t.Run(name, func(t *testing.T) {
			tesla := teslaRetainedConfig()
			mutate(&tesla)
			_, err := preflightSemanticHub(ebusgateway.Config{ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: tesla}})
			var collision *semhub.CollisionError
			if !errors.As(err, &collision) || !strings.Contains(err.Error(), "growatt_storage") || !strings.Contains(err.Error(), "tesla_evse") {
				t.Fatalf("collision error=%v", err)
			}
			if strings.Contains(err.Error(), "asset:") || strings.Contains(err.Error(), "growatt-bms-a") {
				t.Fatalf("collision error carries identity material: %v", err)
			}
		})
	}
}

func TestRunGatewayLifecycleFailsOnPreflightBeforeReleaseCheckerAndEvidence(t *testing.T) {
	started := 0
	original := startGatewayReleaseChecker
	startGatewayReleaseChecker = func(*releasecheck.Checker, context.Context) { started++ }
	t.Cleanup(func() { startGatewayReleaseChecker = original })
	tesla := teslaRetainedConfig()
	tesla.AssetID = growattProductionConfig().AssetID
	evidence := enabledSynchronizedEvidenceConfig(t)
	cfg := ebusgateway.Config{HTTPAddr: "127.0.0.1:0", EvidenceRecorderConfig: evidence, ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: tesla}}
	err := runGatewayLifecycle(context.Background(), cfg)
	var collision *semhub.CollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("lifecycle error=%v", err)
	}
	if started != 0 {
		t.Fatal("release checker started before the semantic preflight")
	}
	if _, statErr := os.Stat(evidence.StateRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("synchronized evidence runtime opened before the semantic preflight: %v", statErr)
	}
}

func TestPreflightSemanticHubSkipsAnInvalidGrowattConfiguration(t *testing.T) {
	for name, mutate := range map[string]func(*ebusgateway.GrowattBMSRS485Config){
		"empty serial path": func(c *ebusgateway.GrowattBMSRS485Config) { c.SerialPath = "" },
		"invalid epoch":     func(c *ebusgateway.GrowattBMSRS485Config) { c.SourceEpoch = " epoch" },
		"zero generation":   func(c *ebusgateway.GrowattBMSRS485Config) { c.DriverGeneration = 0 },
		"zero unit":         func(c *ebusgateway.GrowattBMSRS485Config) { c.UnitID = 0 },
		"invalid timing":    func(c *ebusgateway.GrowattBMSRS485Config) { c.MaxQuiescence = c.MaxResponseDelay },
	} {
		t.Run(name, func(t *testing.T) {
			growatt := growattProductionConfig()
			mutate(&growatt)
			tesla := teslaRetainedConfig()
			tesla.AssetID = growatt.AssetID
			hub, err := preflightSemanticHub(ebusgateway.Config{ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growatt, TeslaGen3HSC: tesla}})
			if err != nil {
				t.Fatalf("an invalid Growatt configuration took part in the collision preflight: %v", err)
			}
			if sources := hub.Sources(); len(sources) != 1 || sources[0].Driver != "tesla_evse" {
				t.Fatalf("declared sources=%+v", sources)
			}
			if _, err := startGrowattBMSRS485Runtime(growatt, hub); err == nil {
				t.Fatal("the invalid Growatt configuration started")
			}
		})
	}
}

// The first-incarnation identifiers of the test configurations, as the
// characterization goldens record them.
const (
	goldenGrowattBinding    = "binding:semantic-storage:04a8bb2b4c91c682072f865033c62669"
	goldenGrowattService    = "service:semantic-storage:a26997ed2bbdde6a4285189518c4440d"
	goldenTeslaBinding      = "binding:tesla-wc3:04acb6030d7537c1cb7885976be38c95"
	goldenTeslaService      = "service:tesla-wc3:03ae485218190fab9d8ce3446238532a"
	goldenTeslaCapability   = "capability:tesla-wc3:d6814720e2e57f7af65acf5b9c411c38"
	goldenTeslaCapabilityB  = "capability:tesla-wc3:272930c2222f87a9659d70da89c63223"
	goldenTeslaServiceOther = "service:tesla-wc3:6edd3058a1b32972e7b33fe9e4d0822f"
)

func TestPreflightRejectsIdentitiesEqualToDerivableIdentifiersOfAnotherFamily(t *testing.T) {
	cases := map[string]func(*ebusgateway.GrowattBMSRS485Config, *ebusgateway.TeslaGen3HSCRetainedConfig){
		"tesla asset is the growatt binding": func(_ *ebusgateway.GrowattBMSRS485Config, c *ebusgateway.TeslaGen3HSCRetainedConfig) {
			c.AssetID = goldenGrowattBinding
		},
		"tesla source is the growatt service": func(_ *ebusgateway.GrowattBMSRS485Config, c *ebusgateway.TeslaGen3HSCRetainedConfig) {
			c.SourceID = goldenGrowattService
		},
		"growatt asset is the tesla binding": func(c *ebusgateway.GrowattBMSRS485Config, _ *ebusgateway.TeslaGen3HSCRetainedConfig) {
			c.AssetID = goldenTeslaBinding
		},
		"growatt source is the tesla capability": func(c *ebusgateway.GrowattBMSRS485Config, _ *ebusgateway.TeslaGen3HSCRetainedConfig) {
			c.SourceID = goldenTeslaCapability
		},
		"growatt asset is the tesla service": func(c *ebusgateway.GrowattBMSRS485Config, _ *ebusgateway.TeslaGen3HSCRetainedConfig) {
			c.AssetID = goldenTeslaService
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			growatt, tesla := growattProductionConfig(), teslaRetainedConfig()
			mutate(&growatt, &tesla)
			_, err := preflightSemanticHub(ebusgateway.Config{ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growatt, TeslaGen3HSC: tesla}})
			var collision *semhub.CollisionError
			if !errors.As(err, &collision) || !strings.Contains(err.Error(), "growatt_storage") || !strings.Contains(err.Error(), "tesla_evse") {
				t.Fatalf("preflight error=%v, want a cross-family collision", err)
			}
		})
	}
}

func TestPreflightReservesExactlyTheFirstIncarnationIdentifiers(t *testing.T) {
	binding, service := growattStorageFirstIdentifiers(growattProductionConfig().AssetID, growattProductionConfig().SourceID)
	if binding != goldenGrowattBinding || service != goldenGrowattService {
		t.Fatalf("growatt derived binding=%s service=%s", binding, service)
	}
	tesla := mcp.TeslaGen3EVSEFirstIdentifiers(teslaRetainedConfig().AssetID, teslaRetainedConfig().SourceID)
	got := map[string]bool{string(tesla.Binding): true}
	for _, id := range tesla.Services {
		got[string(id)] = true
	}
	for _, id := range tesla.Capabilities {
		got[string(id)] = true
	}
	for _, want := range []string{goldenTeslaBinding, goldenTeslaService, goldenTeslaServiceOther, goldenTeslaCapability, goldenTeslaCapabilityB} {
		if !got[want] {
			t.Errorf("tesla derived identifiers miss %s: %+v", want, tesla)
		}
	}
	if len(got) != 5 {
		t.Errorf("tesla derived %d identifiers, want 5", len(got))
	}
	// The valid configurations preflight unchanged, and the runtimes later
	// publish under the reserved identifiers without a collision.
	hub, err := preflightSemanticHub(ebusgateway.Config{ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: teslaRetainedConfig()}})
	if err != nil {
		t.Fatal(err)
	}
	if sources := hub.Sources(); len(sources) != 2 {
		t.Fatalf("declared sources=%+v", sources)
	}
}

func TestRunGatewayLifecycleFailsOnADerivedIdentifierCollisionBeforeRuntimes(t *testing.T) {
	started := 0
	original := startGatewayReleaseChecker
	startGatewayReleaseChecker = func(*releasecheck.Checker, context.Context) { started++ }
	t.Cleanup(func() { startGatewayReleaseChecker = original })
	tesla := teslaRetainedConfig()
	tesla.AssetID = goldenGrowattBinding
	evidence := enabledSynchronizedEvidenceConfig(t)
	cfg := ebusgateway.Config{HTTPAddr: "127.0.0.1:0", EvidenceRecorderConfig: evidence, ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: tesla}}
	err := runGatewayLifecycle(context.Background(), cfg)
	var collision *semhub.CollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("lifecycle error=%v", err)
	}
	if started != 0 {
		t.Fatal("release checker started before the semantic preflight")
	}
	if _, statErr := os.Stat(evidence.StateRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("synchronized evidence runtime opened before the semantic preflight: %v", statErr)
	}
}

// preflightCalls lists, in source order, the calls to the named functions in
// every non-test file of this package, with the enclosing function of each.
func preflightCalls(t *testing.T, names ...string) []string {
	t.Helper()
	want := map[string]bool{}
	for _, name := range names {
		want[name] = true
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	calls := []string{}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := ""
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					name = callee.Name
				case *ast.SelectorExpr:
					name = callee.Sel.Name
				}
				if want[name] {
					calls = append(calls, fn.Name.Name+":"+name)
				}
				return true
			})
		}
	}
	return calls
}

// The preflight composes the pairings and then the operator records exactly
// once each, after the PV class is declared; nothing else calls either seam.
func TestSemanticHubPreflightCallsEachCompositionSeamOnce(t *testing.T) {
	calls := []string{}
	for _, call := range preflightCalls(t, "NewSemanticPVClass", "composeSemanticPairings", "composeOperatorRecords") {
		// The PV class is also declared by the instance runtime; only the
		// preflight's own order matters for it.
		if strings.HasPrefix(call, "preflightSemanticHub:") || !strings.HasSuffix(call, ":NewSemanticPVClass") {
			calls = append(calls, call)
		}
	}
	got := strings.Join(calls, ",")
	want := "preflightSemanticHub:NewSemanticPVClass,preflightSemanticHub:composeSemanticPairings,preflightSemanticHub:composeOperatorRecords"
	if got != want {
		t.Fatalf("composition calls=%s want %s", got, want)
	}
}

// The empty seams compose nothing: the preflight declares the same sources as
// before, every declared asset is not graph-enforced and the hub reports the
// pinned catalog digest.
func TestSemanticHubPreflightComposesNothingAndEnforcesNoAsset(t *testing.T) {
	for name, cfg := range map[string]ebusgateway.Config{
		"disabled": {},
		"enabled":  {ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: teslaRetainedConfig()}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := composeSemanticPairings(newTestSemanticHub(t), cfg); err != nil {
				t.Fatalf("pairings=%v", err)
			}
			if err := composeOperatorRecords(newTestSemanticHub(t), cfg); err != nil {
				t.Fatalf("operator records=%v", err)
			}
			hub, err := preflightSemanticHub(cfg)
			if err != nil {
				t.Fatal(err)
			}
			state, err := hub.GraphState()
			if err != nil {
				t.Fatal(err)
			}
			if state.CatalogDigest != graph.PinnedDigest || len(state.Assets) != len(hub.Sources()) {
				t.Fatalf("graph state=%+v sources=%+v", state, hub.Sources())
			}
			for _, asset := range state.Assets {
				if asset.Enforcement != semhub.GraphNotEnforced {
					t.Fatalf("asset %q is %q", asset.Asset, asset.Enforcement)
				}
			}
		})
	}
}
