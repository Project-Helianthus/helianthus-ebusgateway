package ebusgateway

import "github.com/Project-Helianthus/helianthus-ebusgateway/internal/ebus/evidence/discovery"

// EvidenceStrength classifies an observation's contribution to discovery candidacy.
type EvidenceStrength = discovery.EvidenceStrength

const (
	EvidencePresenceOnly = discovery.EvidencePresenceOnly
	EvidenceWeak         = discovery.EvidenceWeak
	EvidenceStrong       = discovery.EvidenceStrong
)

// EvidenceRecord is a single observation for a bus address.
type EvidenceRecord = discovery.EvidenceRecord

// EvidenceBuffer delegates bounded discovery evidence storage to its eBUS owner.
type EvidenceBuffer = discovery.EvidenceBuffer

// BaselineTopologySeed is the set of addresses protected from LRU eviction.
// Operators MAY override via config; validation
// enforces responder-range [0x03, 0xFE] excluding 0xAA (SYN) and 0xFE (broadcast).
var VaillantBaselineTopologySeed = []byte{0x08, 0x15, 0x26, 0x04, 0xF6, 0xEC}

// VaillantStructuralStartupProbeTargets is the bounded set of Vaillant
// device targets used as the structural startup-probe fallback when the
// source-address selector's passive warmup observed no probable targets.
//
// Why this is narrower than VaillantBaselineTopologySeed: stealth slaves
// in the default seed are passive-only and do not respond to
// directed identity probes. The structural set is the active-probable
// subset {boiler, regulator, primary controller}.
//
// These are TARGETS for active probes from the admitted source —
// never source addresses themselves.
var VaillantStructuralStartupProbeTargets = []byte{0x08, 0x15, 0x26}

// ValidateBaselineTopologySeed validates the configured responder addresses.
func ValidateBaselineTopologySeed(seed []byte) error {
	return discovery.ValidateBaselineTopologySeed(seed)
}

func NewEvidenceBuffer(maxEntries int, baseline []byte) (*EvidenceBuffer, error) {
	return discovery.NewEvidenceBuffer(maxEntries, baseline)
}
