package main

import (
	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
)

// composeSemanticPairings is the startup seam for the pairings between
// composed families. The preflight calls it once, after every static
// declaration and before any runtime starts; a refusal is fatal like a
// declaration collision. No pairing is composed yet.
func composeSemanticPairings(hub *semhub.Hub, cfg ebusgateway.Config) error {
	return nil
}
