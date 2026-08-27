package main

import (
	"fmt"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
)

// prepareGatewayRunConfig preserves run's pre-lifecycle configuration order.
// It intentionally owns no resource, goroutine, timer, or cleanup.
func prepareGatewayRunConfig(cfg *ebusgateway.Config) (gatewayBuildInfo, error) {
	resolvedBuildInfo, err := resolveGatewayBuildInfo(buildVersion, buildID)
	if err != nil {
		return gatewayBuildInfo{}, fmt.Errorf("gateway build identity: %w", err)
	}

	applyTransportSourcePolicy(cfg)
	if err := cfg.ValidatePortalPV(); err != nil {
		return gatewayBuildInfo{}, fmt.Errorf("validate Portal PV configuration: %w", err)
	}
	if err := ebusgateway.ValidateSynchronizedEvidenceConfig(*cfg); err != nil {
		return gatewayBuildInfo{}, fmt.Errorf("validate synchronized evidence config: %w", err)
	}
	if len(cfg.Providers) == 0 {
		cfg.Providers = vaillantproviders.Default()
	}
	return resolvedBuildInfo, nil
}
