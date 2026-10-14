package main

import (
	"fmt"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// preflightSemanticHub starts the process's semantic publication hub and
// declares every statically configured semantic identity before any runtime,
// release check or evidence sidecar starts. A cross-family collision of
// static Growatt Storage and Tesla EVSE identities is fatal and names both
// driver labels only. Each family's statically derivable first-incarnation
// binding, service and capability identifiers are reserved in the same
// identifier space, so a configured asset or source identifier equal to
// another family's derived identifier is the same fatal collision; the
// commit-time identifier check stays the runtime guard. PV identifiers derive
// from the SunSpec Common identity read at runtime, so PV reserves nothing
// here. A disabled family declares nothing. An identity that
// the family itself would refuse is not declared here, so that family keeps
// its own existing failure path. Dynamically discovered PV assets are
// declared later, per asset, through the PV class.
func preflightSemanticHub(cfg ebusgateway.Config) (*semhub.Hub, error) {
	hub, err := semhub.New(semhub.Options{})
	if err != nil {
		return nil, fmt.Errorf("semantic publication hub: %w", err)
	}
	if growatt := cfg.ModbusTCPConfig.GrowattBMSRS485; growatt.Enabled {
		// The complete runtime validity predicate applies: a configuration the
		// Growatt runtime would refuse keeps its non-fatal "continuing without"
		// path and takes no part in the collision preflight.
		if _, err := growattBMSRS485Timing(growatt); err == nil {
			spec := growattStorageAssetSpec(growatt.AssetID, growatt.SourceID)
			if _, err := hub.Declare(spec); err != nil {
				return nil, fmt.Errorf("semantic publication preflight: %w", err)
			}
			binding, service := growattStorageFirstIdentifiers(growatt.AssetID, growatt.SourceID)
			if err := reserveIdentifiers(hub, spec.Driver, []semhub.Kind{semhub.KindBinding, semhub.KindService}, []string{string(binding), string(service)}); err != nil {
				return nil, err
			}
		}
	}
	if tesla := cfg.ModbusTCPConfig.TeslaGen3HSC; tesla.Enabled && validTeslaRetainedConfig(tesla) {
		spec := teslaEVSEAssetSpec(tesla)
		if _, err := hub.Declare(spec); err != nil {
			return nil, fmt.Errorf("semantic publication preflight: %w", err)
		}
		ids := mcp.TeslaGen3EVSEFirstIdentifiers(tesla.AssetID, tesla.SourceID)
		kinds, values := []semhub.Kind{semhub.KindBinding}, []string{string(ids.Binding)}
		for _, id := range ids.Services {
			kinds, values = append(kinds, semhub.KindService), append(values, string(id))
		}
		for _, id := range ids.Capabilities {
			kinds, values = append(kinds, semhub.KindCapability), append(values, string(id))
		}
		if err := reserveIdentifiers(hub, spec.Driver, kinds, values); err != nil {
			return nil, err
		}
	}
	if _, err := modbusadapter.NewSemanticPVClass(hub); err != nil {
		return nil, fmt.Errorf("semantic publication preflight: %w", err)
	}
	return hub, nil
}

func reserveIdentifiers(hub *semhub.Hub, driver string, kinds []semhub.Kind, values []string) error {
	for index, value := range values {
		if err := hub.Reserve(driver, kinds[index], value); err != nil {
			return fmt.Errorf("semantic publication preflight: %w", err)
		}
	}
	return nil
}
