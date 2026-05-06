package ebusgateway

import (
	"fmt"
	"log"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// FormatStartupSourceSelectionExplicitLog returns the low-confidence
// explicit-validate-only log line emitted before the first active frame.
func FormatStartupSourceSelectionExplicitLog(source byte) string {
	return fmt.Sprintf("startup source selection explicit_validate_only source=0x%02X confidence=low", source)
}

// CheckExplicitSourceCompanionConflict compares the configured explicit source
// against the selector's selected source and emits advisory conflict
// observability when they disagree.
func CheckExplicitSourceCompanionConflict(explicitSource byte, selection *protocol.SourceAddressSelection, metrics *StartupSourceSelectionMetrics) bool {
	if selection == nil {
		return false
	}
	if selection.Source == explicitSource {
		return false
	}
	log.Printf("WARN: startup source selection explicit_source_conflict_detected=1 explicit_source=0x%02X selector_preferred=0x%02X", explicitSource, selection.Source)
	if metrics != nil {
		metrics.SetExplicitSourceConflictDetected()
	}
	return true
}
