package main

import (
	"errors"
	"log"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// composeOperatorRecords is the startup seam for the operator-configured
// records of the composed families. The preflight calls it once, after the
// pairings and before any runtime starts; a refusal is fatal like a
// declaration collision. It wires the gateway's configuration provider with
// the configured record set. It does not declare a home the preflight
// snapshot does not already name: an absent home may belong to a driver that
// declares it when the unit is discovered, and the identifier's shape does
// not decide that. A home a driver already owns is not declared again. Its
// set is published when that owner has admitted the provider, and it is
// reported and left unpublished when the owner has not. A set that waits for
// its home or for its owner's resources stays pending and is applied after
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
		driver, declared := owned[set.Asset]
		if !declared || driver == operatorRecordDriver {
			// Absence is not ownership. Declaring the home here would claim
			// it for the configuration provider, and a driver that declares
			// the same home later would collide for good.
			if !declared && set.ConfigurationOnly {
				reportUndeclaredConfigurationOnlyHome(set.Asset)
			}
			publish = append(publish, set)
			continue
		}
		kept, err := composeDriverOwnedOperatorSet(publisher, set)
		if err != nil {
			return err
		}
		if kept {
			publish = append(publish, set)
		}
	}
	return publisher.Compose(publish)
}

// reportUndeclaredConfigurationOnlyHome reports a home that holds a resource
// only configuration knows and that no declaration names yet. Startup does
// not declare it, so its records stay pending until an owner declares it.
func reportUndeclaredConfigurationOnlyHome(home semreg.AssetID) {
	log.Printf("operator records: configured records of %s wait until an owner declares that home; startup does not declare a configuration-only home", home)
}

// composeDriverOwnedOperatorSet publishes one configured set on a home a
// driver already owns. It does not declare that home. A set that still waits
// for the owner's resources is kept pending. A home the owner has not opened
// to the provider is reported and left unpublished: declarations are static,
// so a later hub event cannot admit the provider.
func composeDriverOwnedOperatorSet(publisher *operatorRecordPublisher, set operatorAssetRecords) (bool, error) {
	err := publisher.Set(set.Asset, set.Records, set.Audit)
	if err == nil {
		return false, nil
	}
	if operatorRecordDeferred(err) {
		return true, nil
	}
	if errors.Is(err, semhub.ErrTransaction) {
		log.Printf("operator records: configured records of %s were not published because the owner has not admitted the configuration provider: %v", set.Asset, err)
		return false, nil
	}
	return false, err
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
