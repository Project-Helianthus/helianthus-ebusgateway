package main

import (
	"strings"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

// composeOperatorRecords is the startup seam for the operator-configured
// records of the composed families. The preflight calls it once, after the
// pairings and before any runtime starts; a refusal is fatal like a
// declaration collision. It wires the gateway's configuration provider with
// the configured record set. A configuration-only home is declared in the
// provider's name when its identifier is not one a dynamic driver allocates.
// A home a driver already owns, and a home a dynamic driver will discover,
// are left to that driver. A set that waits for its owner's resources stays
// pending and is applied after the owner publishes. An empty section admits
// no source and publishes nothing.
func composeOperatorRecords(hub *semhub.Hub, cfg ebusgateway.Config) error {
	records, err := configuredOperatorRecords(cfg)
	if err != nil {
		return err
	}
	publisher, err := operatorPublisherFor(hub)
	if err != nil {
		return err
	}
	state, err := hub.GraphState()
	if err != nil {
		return err
	}
	owned := make(map[semreg.AssetID]string, len(state.Assets))
	for _, asset := range state.Assets {
		owned[asset.Asset] = asset.Driver
	}
	publish := make([]operatorAssetRecords, 0, len(records))
	for _, set := range records {
		if driver, declared := owned[set.Asset]; declared && driver != operatorRecordDriver {
			continue
		}
		// Absence from this snapshot is not ownership. A dynamic driver allocates
		// its asset only when it discovers the unit, after preflight, and a
		// declaration here would collide with that later claim.
		if set.ConfigurationOnly && !operatorRecordDiscoveredHome(set.Asset) {
			if err := publisher.DeclareHome(set.Asset, operatorRecordPacks()...); err != nil {
				return err
			}
		}
		publish = append(publish, set)
	}
	return publisher.Compose(publish)
}

// configuredOperatorRecords returns the operator record sets the
// configuration already holds, one per asset, including an identity link.
// An empty or absent section returns an empty set.
func configuredOperatorRecords(cfg ebusgateway.Config) ([]operatorAssetRecords, error) {
	set, err := portalconfig.LoadOperatorRecords(cfg.PortalConfigRoot)
	if err != nil {
		return nil, err
	}
	records := portalOperatorAssetRecords(set)
	if len(records) == 0 {
		return nil, nil
	}
	return records, nil
}

// operatorRecordDiscoveredHome reports an asset identifier allocated by a
// dynamic driver class. The grammar is a family token, "-asset-" and 32 hex
// digits. Those assets are absent from the preflight snapshot until the class
// discovers them.
func operatorRecordDiscoveredHome(id semreg.AssetID) bool {
	text := string(id)
	marker := strings.Index(text, "-asset-")
	if marker <= 0 || strings.Contains(text[:marker], ":") {
		return false
	}
	for _, c := range text[:marker] {
		if c < 'a' || c > 'z' {
			return false
		}
	}
	tail := text[marker+len("-asset-"):]
	if len(tail) != 32 {
		return false
	}
	for _, c := range tail {
		if c < '0' || (c > '9' && c < 'a') || c > 'f' {
			return false
		}
	}
	return true
}

// operatorRecordPacks is the stable pack list of a configuration-only home.
// The catalog covers each of them, and declaration equality compares this order.
func operatorRecordPacks() []semreg.PackValidator {
	return []semreg.PackValidator{evse.New(), infrastructure.New(), pv.New(), storage.New(), thermal.New()}
}
