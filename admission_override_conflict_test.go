package ebusgateway

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestCheckExplicitSourceCompanionConflict_DetectsDisagreement(t *testing.T) {
	m := NewStartupSourceSelectionMetrics()
	selection := &protocol.SourceAddressSelection{Source: 0xF1}
	conflict := CheckExplicitSourceCompanionConflict(0xF0, selection, m)
	if !conflict {
		t.Error("expected conflict when explicit_source=0xF0 but selector preferred 0xF1")
	}
	if m.ExplicitSourceConflictDetected.Value() != 1 {
		t.Error("expected expvar to be set to 1")
	}
}

func TestCheckExplicitSourceCompanionConflict_NoConflictWhenEqual(t *testing.T) {
	m := NewStartupSourceSelectionMetrics()
	selection := &protocol.SourceAddressSelection{Source: 0xF0}
	conflict := CheckExplicitSourceCompanionConflict(0xF0, selection, m)
	if conflict {
		t.Error("expected no conflict when explicit source matches selector pick")
	}
}

func TestCheckExplicitSourceCompanionConflict_NoOpWhenSelectionUnset(t *testing.T) {
	m := NewStartupSourceSelectionMetrics()
	conflict := CheckExplicitSourceCompanionConflict(0xF0, nil, m)
	if conflict {
		t.Error("expected no conflict when selector did not run")
	}
	if m.ExplicitSourceConflictDetected.Value() != 0 {
		t.Error("expected expvar to remain 0")
	}
}
