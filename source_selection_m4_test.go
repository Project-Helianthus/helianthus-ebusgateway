package ebusgateway

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSourceSelectionM4RejectsLegacyAdmissionFields(t *testing.T) {
	legacy := []byte(`{"admission":{"state":"active","source":113,"companion_target":118,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","admission_path_selected":"join"},"discovery":{"wire_bytes":128,"window_s":15,"startup_burst_pct":10,"post_startup_sustained_rate_probes_per_15s":1,"probe_count":1,"promoted_suspects_without_identity":0,"per_baseline_address_evidence_counts":{"08":1}}}`)
	if err := sourceSelectionArtifactSchemaError(legacy); err == nil {
		t.Fatal("legacy admission_path_selected artifact unexpectedly passed")
	}
	explicit := []byte(`{"admission":{"state":"active","source":113,"companion_target":118,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","source_selection":{"mode":"explicit_validate_only"}},"discovery":{"wire_bytes":128,"window_s":15,"startup_burst_pct":10,"post_startup_sustained_rate_probes_per_15s":1,"probe_count":1,"promoted_suspects_without_identity":0,"per_baseline_address_evidence_counts":{"08":1}}}`)
	if err := sourceSelectionArtifactSchemaError(explicit); err != nil {
		t.Fatalf("explicit_validate_only artifact failed: %v", err)
	}
	var decoded SourceSelectionArtifact
	if err := json.Unmarshal(explicit, &decoded); err != nil {
		t.Fatalf("decode explicit artifact: %v", err)
	}
	if decoded.Admission.SourceSelection.Mode != "explicit_validate_only" {
		t.Fatalf("mode=%q", decoded.Admission.SourceSelection.Mode)
	}
}

func TestStartupSourceSelectionExpvarSnapshot(t *testing.T) {
	names := StartupSourceSelectionExpvarNames()
	joined := "\n" + strings.Join(names, "\n") + "\n"
	for _, old := range []string{
		"startup_admission_degraded_total",
		"startup_admission_state",
		"startup_admission_override_active",
		"startup_admission_warmup_events_seen",
		"startup_admission_warmup_cycles_total",
		"startup_admission_override_bypass_total",
		"startup_admission_override_conflict_detected",
		"startup_admission_degraded_escalated",
		"startup_admission_degraded_since_ms",
		"startup_admission_consecutive_rejoin_failures",
		"startup_admission_degraded_cumulative_ms",
	} {
		if strings.Contains(joined, "\n"+old+"\n") {
			t.Fatalf("legacy expvar %q still present in snapshot", old)
		}
	}
	for _, want := range []string{
		"startup_source_selection_degraded_total",
		"startup_source_selection_state",
		"startup_source_selection_explicit_source_active",
		"startup_source_selection_warmup_events_seen",
		"startup_source_selection_warmup_cycles_total",
		"startup_source_selection_explicit_validate_only_total",
		"startup_source_selection_explicit_source_conflict_detected",
		"startup_source_selection_degraded_escalated",
		"startup_source_selection_degraded_since_ms",
		"startup_source_selection_consecutive_failures",
		"startup_source_selection_degraded_cumulative_ms",
	} {
		if !strings.Contains(joined, "\n"+want+"\n") {
			t.Fatalf("expected expvar %q missing from snapshot", want)
		}
	}
}
