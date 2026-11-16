package main

import (
	"errors"
	"fmt"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// errSemanticPairing is the refusal of a pairing declaration before any of
// its identities is declared. It names no asset, source or driver.
var errSemanticPairing = errors.New("semantic pairing refused")

// semanticCapacityBudget is one publishing source's share of the kernel's
// per-asset bounds on a paired asset. Every publishing source of an asset
// shares those bounds, so the owner and its contributors each declare the
// share they may hold.
type semanticCapacityBudget struct {
	// SourceDescriptors is the number of source descriptors (one per source
	// epoch that published on the asset; a retired epoch keeps its own).
	SourceDescriptors int
	// Bindings is the number of native bindings (one per binding
	// incarnation; none is removed within its epoch).
	Bindings int
	// IdentityLinks is the number of identity links, counted against the
	// kernel's bound on links that name no resource, the lower of its two
	// link bounds.
	IdentityLinks int
}

// semanticAssetBounds are the pinned kernel's per-asset bounds that the
// budgets of one paired asset share.
var semanticAssetBounds = semanticCapacityBudget{SourceDescriptors: 32, Bindings: 128, IdentityLinks: 128}

// semanticPairingContributor is one contributor of a paired asset: another
// family's source and driver, the packs it publishes (a subset of the owner's)
// and its share of the asset's bounds.
type semanticPairingContributor struct {
	Source     semreg.SourceID
	Driver     string
	Validators []semreg.PackValidator
	// OnControl reprojects the contributor family's cache, as
	// semhub.ContributorSpec.OnControl does.
	OnControl func(semhub.ControlEvent) error
	Budget    semanticCapacityBudget
}

// semanticPairing declares one unit that two composed families observe
// through their own paths. Owner is the owning family's declaration of the
// asset, without contributors; the pairing names them. The pairing is static:
// it is composed once at startup and the hub never re-homes a resource at
// run time.
type semanticPairing struct {
	Owner        semhub.AssetSpec
	OwnerBudget  semanticCapacityBudget
	Contributors []semanticPairingContributor
}

// semanticPairings reads the pairing declarations from the composed
// configuration. No configuration declares a pairing yet, so every
// configuration composes none.
func semanticPairings(ebusgateway.Config) []semanticPairing {
	return nil
}

// composeSemanticPairings is the startup seam for the pairings between
// composed families. The preflight calls it once, after every static
// declaration and before any runtime starts; a refusal is fatal like a
// declaration collision.
func composeSemanticPairings(hub *semhub.Hub, cfg ebusgateway.Config) error {
	return composeDeclaredPairings(hub, semanticPairings(cfg))
}

// composeDeclaredPairings checks every pairing and then declares each paired
// asset with its contributors. A pairing with an empty contributor list, an
// asset named by two pairings, a budget share below one or budgets whose sum
// exceeds a bound of the asset is refused before anything is declared. The
// hub keeps its own refusals: an asset already held by another driver or a
// contributor source held by another driver is a collision, and an asset its
// owner already declared without these contributors, or a contributor whose
// packs are not a subset of the asset's, is a declaration conflict.
func composeDeclaredPairings(hub *semhub.Hub, pairings []semanticPairing) error {
	if len(pairings) == 0 {
		return nil
	}
	if hub == nil {
		return fmt.Errorf("%w: no publication hub", errSemanticPairing)
	}
	seen := make(map[semreg.AssetID]bool, len(pairings))
	for _, pairing := range pairings {
		if err := checkSemanticPairing(pairing); err != nil {
			return err
		}
		if seen[pairing.Owner.AssetID] {
			return fmt.Errorf("%w: two pairings name one asset", errSemanticPairing)
		}
		seen[pairing.Owner.AssetID] = true
	}
	for _, pairing := range pairings {
		spec := pairing.Owner
		spec.Contributors = make([]semhub.ContributorSpec, 0, len(pairing.Contributors))
		for _, item := range pairing.Contributors {
			spec.Contributors = append(spec.Contributors, semhub.ContributorSpec{SourceID: item.Source, Driver: item.Driver, Validators: item.Validators, OnControl: item.OnControl})
		}
		if _, err := hub.Declare(spec); err != nil {
			return err
		}
	}
	return nil
}

// checkSemanticPairing refuses a pairing without contributors, an owner
// declaration that names its own contributors, and budgets that do not fit
// the asset's shared bounds.
func checkSemanticPairing(pairing semanticPairing) error {
	if len(pairing.Contributors) == 0 {
		return fmt.Errorf("%w: a pairing names no contributor", errSemanticPairing)
	}
	if len(pairing.Owner.Contributors) != 0 {
		return fmt.Errorf("%w: the owner declaration names contributors outside the pairing", errSemanticPairing)
	}
	budgets := make([]semanticCapacityBudget, 0, 1+len(pairing.Contributors))
	budgets = append(budgets, pairing.OwnerBudget)
	for _, item := range pairing.Contributors {
		budgets = append(budgets, item.Budget)
	}
	return checkSemanticCapacityBudgets(budgets)
}

// checkSemanticCapacityBudgets refuses a share below one, since a source that
// publishes holds at least one descriptor, binding and identity link, a share
// above the asset's bound, and a sum of shares above the asset's bound. Each
// share is bounded before it is added, so the sum cannot overflow.
func checkSemanticCapacityBudgets(budgets []semanticCapacityBudget) error {
	var total semanticCapacityBudget
	bounds := semanticAssetBounds
	for _, budget := range budgets {
		if budget.SourceDescriptors < 1 || budget.Bindings < 1 || budget.IdentityLinks < 1 {
			return fmt.Errorf("%w: a capacity share is below one", errSemanticPairing)
		}
		if budget.SourceDescriptors > bounds.SourceDescriptors || budget.Bindings > bounds.Bindings || budget.IdentityLinks > bounds.IdentityLinks {
			return fmt.Errorf("%w: a capacity share is above the asset bound", errSemanticPairing)
		}
		total.SourceDescriptors += budget.SourceDescriptors
		total.Bindings += budget.Bindings
		total.IdentityLinks += budget.IdentityLinks
	}
	for _, dimension := range []struct {
		name         string
		total, bound int
	}{
		{"source descriptor", total.SourceDescriptors, bounds.SourceDescriptors},
		{"binding", total.Bindings, bounds.Bindings},
		{"identity link", total.IdentityLinks, bounds.IdentityLinks},
	} {
		if dimension.total > dimension.bound {
			return fmt.Errorf("%w: the %s budgets sum to %d, above the asset bound %d", errSemanticPairing, dimension.name, dimension.total, dimension.bound)
		}
	}
	return nil
}
