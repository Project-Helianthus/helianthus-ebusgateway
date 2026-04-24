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
// against the Joiner's selected initiator and emits advisory conflict
// observability when they disagree.
func CheckOverrideCompanionConflict(overrideSource byte, joinResult protocol.JoinResult, metrics *StartupAdmissionMetrics) bool {
	if joinResult.Initiator == 0 {
		return false
	}
	if joinResult.Initiator == overrideSource {
		return false
	}
	log.Printf("WARN: startup admission override conflict_detected=1 override_source=0x%02X joiner_preferred=0x%02X", overrideSource, joinResult.Initiator)
	if metrics != nil {
		metrics.SetOverrideConflictDetected()
	}
	return true
}
