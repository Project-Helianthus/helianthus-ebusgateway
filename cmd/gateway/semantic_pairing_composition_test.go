package main

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
)

// No configuration declares a pairing yet: every configuration composes none,
// and the preflight declares exactly the sources it declared before.
func TestSemanticPairingCompositionReadsNoPairingFromAnyConfiguration(t *testing.T) {
	for name, cfg := range map[string]ebusgateway.Config{
		"disabled": {},
		"enabled":  {ModbusTCPConfig: ebusgateway.ModbusTCPConfig{GrowattBMSRS485: growattProductionConfig(), TeslaGen3HSC: teslaRetainedConfig()}},
	} {
		t.Run(name, func(t *testing.T) {
			if pairings := semanticPairings(cfg); len(pairings) != 0 {
				t.Fatalf("configured pairings=%d", len(pairings))
			}
			hub, err := preflightSemanticHub(cfg)
			if err != nil {
				t.Fatal(err)
			}
			before := hub.Sources()
			if err := composeSemanticPairings(hub, cfg); err != nil {
				t.Fatalf("pairings=%v", err)
			}
			if after := hub.Sources(); !reflect.DeepEqual(before, after) {
				t.Fatalf("composition declared sources: %+v -> %+v", before, after)
			}
		})
	}
	if err := composeDeclaredPairings(nil, nil); err != nil {
		t.Fatalf("an empty pairing list needs no hub: %v", err)
	}
	if err := composeDeclaredPairings(nil, []semanticPairing{pairingDeclaration()}); !errors.Is(err, errSemanticPairing) {
		t.Fatalf("pairing without a hub=%v", err)
	}
}

// The shared bounds are the pinned kernel's per-asset bounds. The Huawei
// publication's bounds are the same kernel bounds, and its replay tests fill
// an asset to each of them.
func TestSemanticPairingCompositionBoundsAreTheKernelBounds(t *testing.T) {
	want := semanticCapacityBudget{SourceDescriptors: huaweiMaxAssetSources, Bindings: huaweiMaxAssetBindings, IdentityLinks: huaweiMaxAssetIdentityLinks}
	if semanticAssetBounds != want {
		t.Fatalf("pairing bounds=%+v, kernel bounds=%+v", semanticAssetBounds, want)
	}
}

// pairingHubState is what a refused pairing must leave unchanged.
type pairingHubState struct {
	Sources []semhub.SourceInfo
	Assets  []semhub.AssetGraph
}

func pairingStateOf(t *testing.T, hub *semhub.Hub) pairingHubState {
	t.Helper()
	state, err := hub.GraphState()
	if err != nil {
		t.Fatal(err)
	}
	return pairingHubState{Sources: hub.Sources(), Assets: state.Assets}
}

// Each field of a pairing declaration is varied on its own: the owner asset,
// the contributor source, the contributor driver, the contributor packs and
// every dimension of every budget. Each refusal leaves the hub as it was.
func TestSemanticPairingCompositionRefusesEachFieldBeforeDeclaring(t *testing.T) {
	type refusal struct {
		setup  func(*testing.T, *semhub.Hub)
		mutate func(*[]semanticPairing)
		want   func(error) bool
	}
	is := func(target error) func(error) bool { return func(err error) bool { return errors.Is(err, target) } }
	collision := func(err error) bool {
		var c *semhub.CollisionError
		return errors.As(err, &c)
	}
	first := func(mutate func(*semanticPairing)) func(*[]semanticPairing) {
		return func(pairings *[]semanticPairing) { mutate(&(*pairings)[0]) }
	}
	peer := func(mutate func(*semanticPairingContributor)) func(*[]semanticPairing) {
		return first(func(p *semanticPairing) { mutate(&p.Contributors[0]) })
	}
	cases := map[string]refusal{
		// Owner asset.
		"owner asset invalid": {mutate: first(func(p *semanticPairing) { p.Owner.AssetID = "" }), want: is(semhub.ErrInvalidIdentity)},
		"owner asset named by two pairings": {mutate: func(pairings *[]semanticPairing) {
			*pairings = append(*pairings, pairingDeclaration())
		}, want: is(errSemanticPairing)},
		"owner asset held by another driver": {setup: func(t *testing.T, hub *semhub.Hub) {
			if _, err := hub.Declare(semhub.AssetSpec{AssetID: pairingAsset, SourceID: "source:pairtest:third", Driver: pairingThirdDriver, Validators: []semreg.PackValidator{pv.New()}}); err != nil {
				t.Fatal(err)
			}
		}, want: collision},
		"owner declares its own contributors": {mutate: first(func(p *semanticPairing) {
			p.Owner.Contributors = []semhub.ContributorSpec{{SourceID: "source:pairtest:fourth", Driver: pairingThirdDriver, Validators: []semreg.PackValidator{pv.New()}}}
		}), want: is(errSemanticPairing)},
		"no contributor": {mutate: first(func(p *semanticPairing) { p.Contributors = nil }), want: is(errSemanticPairing)},
		// Contributor source.
		"contributor source invalid":        {mutate: peer(func(c *semanticPairingContributor) { c.Source = "" }), want: is(semhub.ErrInvalidIdentity)},
		"contributor source is the owner's": {mutate: peer(func(c *semanticPairingContributor) { c.Source = pairingOwnerSource }), want: is(semhub.ErrDeclarationConflict)},
		"contributor source is the asset":   {mutate: peer(func(c *semanticPairingContributor) { c.Source = semreg.SourceID(pairingAsset) }), want: is(semhub.ErrInvalidIdentity)},
		"contributor source held by a driver": {setup: func(t *testing.T, hub *semhub.Hub) {
			if _, err := hub.AdmitSource(semhub.Admission{Source: pairingPeerSource, Driver: pairingThirdDriver}); err != nil {
				t.Fatal(err)
			}
		}, want: collision},
		// Contributor driver.
		"contributor driver empty": {mutate: peer(func(c *semanticPairingContributor) { c.Driver = "" }), want: is(semhub.ErrInvalidIdentity)},
		// Contributor packs.
		"contributor packs empty":        {mutate: peer(func(c *semanticPairingContributor) { c.Validators = nil }), want: is(semhub.ErrInvalidIdentity)},
		"contributor packs not a subset": {mutate: peer(func(c *semanticPairingContributor) { c.Validators = []semreg.PackValidator{storage.New()} }), want: is(semhub.ErrDeclarationConflict)},
		"contributor packs hold a nil":   {mutate: peer(func(c *semanticPairingContributor) { c.Validators = []semreg.PackValidator{nil} }), want: is(semhub.ErrInvalidIdentity)},
		"second pairing refused, first kept undeclared": {mutate: func(pairings *[]semanticPairing) {
			second := pairingDeclaration()
			second.Owner.AssetID, second.Owner.SourceID, second.Contributors[0].Source = "asset:pairtest:other", "source:pairtest:other-owner", "source:pairtest:other-peer"
			second.OwnerBudget.Bindings = 0
			*pairings = append(*pairings, second)
		}, want: is(errSemanticPairing)},
	}
	// Every budget dimension of the owner and of the contributor: a zero
	// share, a share above the bound whose sum overflows, and a sum one above
	// the bound.
	dimensions := map[string]func(*semanticCapacityBudget) *int{
		"source descriptors": func(b *semanticCapacityBudget) *int { return &b.SourceDescriptors },
		"bindings":           func(b *semanticCapacityBudget) *int { return &b.Bindings },
		"identity links":     func(b *semanticCapacityBudget) *int { return &b.IdentityLinks },
	}
	holders := map[string]func(*semanticPairing) (own, other *semanticCapacityBudget){
		"owner": func(p *semanticPairing) (*semanticCapacityBudget, *semanticCapacityBudget) {
			return &p.OwnerBudget, &p.Contributors[0].Budget
		},
		"contributor": func(p *semanticPairing) (*semanticCapacityBudget, *semanticCapacityBudget) {
			return &p.Contributors[0].Budget, &p.OwnerBudget
		},
	}
	for holder, budgets := range holders {
		for dimension, field := range dimensions {
			budgets, field := budgets, field
			cases[holder+" "+dimension+" zero"] = refusal{mutate: first(func(p *semanticPairing) {
				own, _ := budgets(p)
				*field(own) = 0
			}), want: is(errSemanticPairing)}
			cases[holder+" "+dimension+" overflowing share"] = refusal{mutate: first(func(p *semanticPairing) {
				own, other := budgets(p)
				*field(own), *field(other) = math.MaxInt, 1
			}), want: is(errSemanticPairing)}
			cases[holder+" "+dimension+" sum above the bound"] = refusal{mutate: first(func(p *semanticPairing) {
				own, other := budgets(p)
				bound := *field(&semanticAssetBounds)
				*field(own), *field(other) = bound, 1
			}), want: is(errSemanticPairing)}
		}
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			hub, err := semhub.New(semhub.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if c.setup != nil {
				c.setup(t, hub)
			}
			before := pairingStateOf(t, hub)
			pairings := []semanticPairing{pairingDeclaration()}
			if c.mutate != nil {
				c.mutate(&pairings)
			}
			err = composeDeclaredPairings(hub, pairings)
			if err == nil || !c.want(err) {
				t.Fatalf("composition error=%v", err)
			}
			if after := pairingStateOf(t, hub); !reflect.DeepEqual(before, after) {
				t.Fatalf("refused pairing changed the hub: %+v -> %+v", before, after)
			}
			if errors.Is(err, errSemanticPairing) && (strings.Contains(err.Error(), "asset:") || strings.Contains(err.Error(), "source:") || strings.Contains(err.Error(), "pairtest_")) {
				t.Fatalf("pairing refusal carries identity material: %v", err)
			}
		})
	}
}

// Budgets whose sums reach every bound exactly are accepted, for two
// contributors as for one, and a repeated identical composition is accepted
// as the same declaration.
func TestSemanticPairingCompositionAcceptsBudgetsUpToTheBound(t *testing.T) {
	bounds := semanticAssetBounds
	pairing := pairingDeclaration()
	pairing.OwnerBudget = semanticCapacityBudget{SourceDescriptors: bounds.SourceDescriptors - 2, Bindings: bounds.Bindings - 2, IdentityLinks: bounds.IdentityLinks - 2}
	pairing.Contributors[0].Budget = semanticCapacityBudget{SourceDescriptors: 1, Bindings: 1, IdentityLinks: 1}
	pairing.Contributors = append(pairing.Contributors, semanticPairingContributor{Source: "source:pairtest:second-peer", Driver: pairingThirdDriver, Validators: []semreg.PackValidator{pv.New()}, Budget: semanticCapacityBudget{SourceDescriptors: 1, Bindings: 1, IdentityLinks: 1}})
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := composeDeclaredPairings(hub, []semanticPairing{pairing}); err != nil {
			t.Fatalf("composition %d at the bound: %v", attempt, err)
		}
	}
	sources := hub.Sources()
	if len(sources) != 3 || sources[0].Source != pairingOwnerSource || sources[1].Source != pairingPeerSource || sources[2].Source != "source:pairtest:second-peer" {
		t.Fatalf("declared sources=%+v", sources)
	}
	// One more of any dimension is refused.
	for name, grow := range map[string]func(*semanticCapacityBudget){
		"source descriptors": func(b *semanticCapacityBudget) { b.SourceDescriptors++ },
		"bindings":           func(b *semanticCapacityBudget) { b.Bindings++ },
		"identity links":     func(b *semanticCapacityBudget) { b.IdentityLinks++ },
	} {
		t.Run(name, func(t *testing.T) {
			over := pairing
			over.Contributors = append([]semanticPairingContributor(nil), pairing.Contributors...)
			grow(&over.Contributors[1].Budget)
			if err := checkSemanticPairing(over); !errors.Is(err, errSemanticPairing) {
				t.Fatalf("one above the bound=%v", err)
			}
		})
	}
}
