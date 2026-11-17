package main

import (
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
// provider's name. A home a driver already owns is left to that driver. A
// set that waits for its owner's resources stays pending and is applied after
// the owner publishes. An empty section admits no source and publishes nothing.
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
		if set.ConfigurationOnly {
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

// operatorRecordPacks is the stable pack list of a configuration-only home.
// The catalog covers each of them, and declaration equality compares this order.
func operatorRecordPacks() []semreg.PackValidator {
	return []semreg.PackValidator{evse.New(), infrastructure.New(), pv.New(), storage.New(), thermal.New()}
}
