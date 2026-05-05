package ebusgateway

import (
	"fmt"
	"log"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// FormatStartupAdmissionOverrideLog returns the low-confidence override
// admission log line emitted before the first active frame.
func FormatStartupAdmissionOverrideLog(source byte) string {
	return fmt.Sprintf("startup admission override source=0x%02X confidence=low", source)
}

// CheckOverrideCompanionConflict compares the configured override source
// against the selector's selected source and emits advisory conflict
// observability when they disagree.
func CheckOverrideCompanionConflict(overrideSource byte, selection *protocol.SourceAddressSelection, metrics *StartupAdmissionMetrics) bool {
	if selection == nil {
		return false
	}
	if selection.Source == overrideSource {
		return false
	}
	log.Printf("WARN: startup admission override conflict_detected=1 override_source=0x%02X selector_preferred=0x%02X", overrideSource, selection.Source)
	if metrics != nil {
		metrics.SetOverrideConflictDetected()
	}
	return true
}
