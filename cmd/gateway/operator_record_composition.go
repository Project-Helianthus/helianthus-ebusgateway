package main

import (
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
)

// composeOperatorRecords is the startup seam for the operator-configured
// records of the composed families. The preflight calls it once, after the
// pairings and before any runtime starts; a refusal is fatal like a
// declaration collision. No operator record is composed yet.
func composeOperatorRecords(hub *semhub.Hub, cfg ebusgateway.Config) error {
	return nil
}
