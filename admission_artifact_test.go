package ebusgateway

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSourceSelectionArtifact_EmitValidatesAgainstSchema(t *testing.T) {
	builder := NewSourceSelectionArtifactBuilder("ens")
	builder.startedAt = time.Now().Add(-60 * time.Second)
	if err := builder.SetSourceSelectionMode("source_selection"); err != nil {
		t.Fatal(err)
	}
	builder.SetSourceSelection(0x71, 0x08, 5*time.Second)
	builder.SetSourceSelectionActive()
	builder.RecordProbe(120)
	builder.RecordProbe(180)
	builder.SetPostStartupSustainedRateProbesPer15s(1.5)
	builder.SetPromotedSuspects(2)
	builder.RecordBaselineEvidence(0x08, 3)
	builder.RecordBaselineEvidence(0x15, 1)

	artifact, data, err := builder.Emit()
	if err != nil {
		t.Fatal(err)
	}
	validateSourceSelectionArtifactAgainstSchema(t, data)

	var reparsed SourceSelectionArtifact
	if err := json.Unmarshal(data, &reparsed); err != nil {
		t.Fatalf("reparse artifact: %v", err)
	}
	if reparsed.Admission.SourceSelection.Mode != "source_selection" {
		t.Fatalf("source_selection.mode=%q", reparsed.Admission.SourceSelection.Mode)
	}
	if reparsed.Admission.State != "active" {
		t.Fatalf("state=%q", reparsed.Admission.State)
	}
	if artifact.Discovery.ProbeCount != 2 {
		t.Fatalf("probe_count=%d", artifact.Discovery.ProbeCount)
	}
}

func TestSourceSelectionArtifact_BadModeRejected(t *testing.T) {
	builder := NewSourceSelectionArtifactBuilder("enh")
	err := builder.SetSourceSelectionMode("bogus")
	if err == nil {
		t.Fatal("expected invalid source_selection.mode to fail")
	}
	if !strings.HasPrefix(err.Error(), "FATAL:") {
		t.Fatalf("unexpected error %q", err)
	}
}

func TestSourceSelectionArtifact_WireLoadMath(t *testing.T) {
	builder := NewSourceSelectionArtifactBuilder("tcp-plain")
	builder.startedAt = time.Now().Add(-60 * time.Second)
	if err := builder.SetSourceSelectionMode("source_selection"); err != nil {
		t.Fatal(err)
	}
	builder.RecordProbe(100)
	builder.RecordProbe(200)

	artifact, _, err := builder.Emit()
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Discovery.StartupBurstPct < 2.07 || artifact.Discovery.StartupBurstPct > 2.10 {
		t.Fatalf("startup_burst_pct=%f; want approx 2.08", artifact.Discovery.StartupBurstPct)
	}
}
