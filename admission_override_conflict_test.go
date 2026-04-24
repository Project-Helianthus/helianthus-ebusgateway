package ebusgateway

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestCheckOverrideCompanionConflict_DetectsDisagreement(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	jr := protocol.JoinResult{Initiator: 0xF1}
	conflict := CheckOverrideCompanionConflict(0xF0, jr, m)
	if !conflict {
		t.Error("expected conflict when override=0xF0 but Joiner preferred 0xF1")
	}
	if m.OverrideConflictDetected.Value() != 1 {
		t.Error("expected expvar to be set to 1")
	}
}

func TestCheckOverrideCompanionConflict_NoConflictWhenEqual(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	jr := protocol.JoinResult{Initiator: 0xF0}
	conflict := CheckOverrideCompanionConflict(0xF0, jr, m)
	if conflict {
		t.Error("expected no conflict when override matches Joiner pick")
	}
}

func TestCheckOverrideCompanionConflict_NoOpWhenJoinerUnset(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	jr := protocol.JoinResult{}
	conflict := CheckOverrideCompanionConflict(0xF0, jr, m)
	if conflict {
		t.Error("expected no conflict when Joiner did not run")
	}
	if m.OverrideConflictDetected.Value() != 0 {
		t.Error("expected expvar to remain 0")
	}
}
