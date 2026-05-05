package ebusgateway

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestCheckOverrideCompanionConflict_DetectsDisagreement(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	selection := &protocol.SourceAddressSelection{Source: 0xF1}
	conflict := CheckOverrideCompanionConflict(0xF0, selection, m)
	if !conflict {
		t.Error("expected conflict when override=0xF0 but selector preferred 0xF1")
	}
	if m.OverrideConflictDetected.Value() != 1 {
		t.Error("expected expvar to be set to 1")
	}
}

func TestCheckOverrideCompanionConflict_NoConflictWhenEqual(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	selection := &protocol.SourceAddressSelection{Source: 0xF0}
	conflict := CheckOverrideCompanionConflict(0xF0, selection, m)
	if conflict {
		t.Error("expected no conflict when override matches selector pick")
	}
}

func TestCheckOverrideCompanionConflict_NoOpWhenSelectionUnset(t *testing.T) {
	m := NewStartupAdmissionMetrics()
	conflict := CheckOverrideCompanionConflict(0xF0, nil, m)
	if conflict {
		t.Error("expected no conflict when selector did not run")
	}
	if m.OverrideConflictDetected.Value() != 0 {
		t.Error("expected expvar to remain 0")
	}
}
