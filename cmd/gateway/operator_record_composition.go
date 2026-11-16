package main

import (
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
)

// composeOperatorRecords is the startup seam for the operator-configured
// records of the composed families. The preflight calls it once, after the
// pairings and before any runtime starts; a refusal is fatal like a
// declaration collision. It wires the gateway's configuration provider with
// the configured record set; a set that waits for its owner's resources stays
// pending and is applied after the owner publishes. No operator record is
// configured yet, so the provider admits no source and publishes nothing.
func composeOperatorRecords(hub *semhub.Hub, cfg ebusgateway.Config) error {
	publisher, err := newOperatorRecordPublisher(hub, operatorRecordOptions{})
	if err != nil {
		return err
	}
	return publisher.Compose(configuredOperatorRecords(cfg))
}

// configuredOperatorRecords returns the operator record sets the
// configuration declares, one per asset. The configuration has no operator
// record yet.
func configuredOperatorRecords(ebusgateway.Config) []operatorAssetRecords {
	return nil
}
