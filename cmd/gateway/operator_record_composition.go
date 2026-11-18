package main

import (
	"errors"
	"log"

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

// operatorHomeOwner is who owns one configured home at startup. The
// distinction is the record set together with the preflight snapshot. The
// identifier's shape is not ownership.
type operatorHomeOwner int

const (
	// operatorHomeDriverOwned is a home a driver has already declared.
	operatorHomeDriverOwned operatorHomeOwner = iota
	// operatorHomeConfigurationOwned is a home the record set marks
	// configuration-only and that no driver has declared. Startup declares
	// it for the configuration provider.
	operatorHomeConfigurationOwned
	// operatorHomeUndeclared is a home no declaration names and that the
	// record set does not mark configuration-only. A driver may claim it.
	operatorHomeUndeclared
)

// composeOperatorRecords is the startup seam for the operator-configured
// records of the composed families. The preflight calls it once, after the
// pairings and before any runtime starts; a refusal is fatal like a
// declaration collision. A section that does not load is unavailable: the
// refusal is logged and semantic preflight continues, publishing nothing.
// It wires the gateway's configuration provider with the configured record
// set. A configuration-only home that no driver has declared is declared for
// the provider. A home a driver already owns is not declared again. Its set
// is published when that owner has admitted the provider, and it is reported
// and left unpublished when the owner has not. A set that waits for its home
// or for its owner's resources stays pending and is applied after the owner
// publishes. An empty section admits no source and publishes nothing.
func composeOperatorRecords(hub *semhub.Hub, cfg ebusgateway.Config) error {
	records, err := configuredOperatorRecords(cfg)
	if err != nil {
		log.Printf("operator records: operator-record section did not load (%s); nothing published", portalconfig.SafeOperatorRecordError(err).Code)
		return nil
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
		switch classifyOperatorHome(driver, declared, set.ConfigurationOnly) {
		case operatorHomeDriverOwned:
			kept, err := composeDriverOwnedOperatorSet(publisher, set)
			if err != nil {
				return err
			}
			if kept {
				publish = append(publish, set)
			}
		case operatorHomeConfigurationOwned:
			if err := publisher.DeclareHome(set.Asset, operatorRecordPacks()...); err != nil {
				return err
			}
			publish = append(publish, set)
		default:
			publish = append(publish, set)
		}
	}
	return publisher.Compose(publish)
}

// classifyOperatorHome reports the owner of one configured home. A driver
// that has already declared the home keeps it, even when the set also holds
// a resource that only configuration knows. Otherwise a configuration-only
// set is the provider's home. Any other absent home stays undeclared.
func classifyOperatorHome(driver string, declared, configurationOnly bool) operatorHomeOwner {
	if declared && driver != operatorRecordDriver {
		return operatorHomeDriverOwned
	}
	if configurationOnly {
		return operatorHomeConfigurationOwned
	}
	return operatorHomeUndeclared
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
// An empty or absent section returns an empty set. A section that does not
// decode or validate, or a file that cannot be read, fails closed.
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

// operatorRecordPacks is the stable pack list of a configuration-owned home.
// The catalog covers each of them, and declaration equality compares this order.
func operatorRecordPacks() []semreg.PackValidator {
	return []semreg.PackValidator{evse.New(), infrastructure.New(), pv.New(), storage.New(), thermal.New()}
}
