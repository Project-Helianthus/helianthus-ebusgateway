package ebusgateway

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAdmissionArtifact_EmitValidatesAgainstSchema(t *testing.T) {
	builder := NewAdmissionArtifactBuilder("ens")
	builder.startedAt = time.Now().Add(-60 * time.Second)
	if err := builder.SetAdmissionPathSelected("join"); err != nil {
		t.Fatal(err)
	}
	builder.SetJoinerSelection(0x71, 0x08, 5*time.Second)
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
	validateAdmissionArtifactAgainstSchema(t, data)

	var reparsed AdmissionArtifact
	if err := json.Unmarshal(data, &reparsed); err != nil {
		t.Fatalf("reparse artifact: %v", err)
	}
	if reparsed.Admission.AdmissionPathSelected != "join" {
		t.Fatalf("admission_path_selected=%q", reparsed.Admission.AdmissionPathSelected)
	}
	if reparsed.Admission.State != "active" {
		t.Fatalf("state=%q", reparsed.Admission.State)
	}
	if artifact.Discovery.ProbeCount != 2 {
		t.Fatalf("probe_count=%d", artifact.Discovery.ProbeCount)
	}
}

func TestAdmissionArtifact_BadAdmissionPathRejected(t *testing.T) {
	builder := NewAdmissionArtifactBuilder("enh")
	err := builder.SetAdmissionPathSelected("bogus")
	if err == nil {
		t.Fatal("expected invalid admission_path_selected to fail")
	}
	if !strings.HasPrefix(err.Error(), "FATAL:") {
		t.Fatalf("unexpected error %q", err)
	}
}

func TestAdmissionArtifact_WireLoadMath(t *testing.T) {
	builder := NewAdmissionArtifactBuilder("tcp-plain")
	builder.startedAt = time.Now().Add(-60 * time.Second)
	if err := builder.SetAdmissionPathSelected("join"); err != nil {
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
