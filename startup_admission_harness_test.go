package ebusgateway

// This file is the offline harness for startup admission + discovery per M2a. Schema validation is the only concrete test that can run against M2-era code; skeleton tests with t.Skip markers become real as M3..M6 land.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func synthesizedTransactionEvent(src, dst byte, hasResponse bool) PassiveClassifiedEvent {
	event := PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventTransaction,
		Request: protocol.Frame{Source: src, Target: dst, Primary: 0x07, Secondary: 0x04, Data: []byte{0x01}},
	}
	if hasResponse {
		event.HasResponse = true
		event.Response = protocol.Frame{Source: dst, Target: src, Data: []byte{0x11, 0x55}}
	}
	return event
}

func validateSourceSelectionArtifactAgainstSchema(t *testing.T, artifactJSON []byte) {
	t.Helper()
	if err := sourceSelectionArtifactSchemaError(artifactJSON); err != nil {
		t.Fatalf("artifact does not validate against source-selection schema: %v", err)
	}
}

func TestM2aHarness_SourceSelectionSuccessPath(t *testing.T) {
	var forwarded []protocol.Frame
	forwardSourceSelectionBusEvent(synthesizedTransactionEvent(0x71, 0x08, true), func(frame protocol.Frame) {
		forwarded = append(forwarded, frame)
	})
	if len(forwarded) == 0 {
		t.Fatal("expected synthesized passive traffic to forward at least one frame")
	}

	builder := NewSourceSelectionArtifactBuilder("ens")
	builder.startedAt = time.Now().Add(-60 * time.Second)
	if err := builder.SetSourceSelectionMode("source_selection"); err != nil {
		t.Fatal(err)
	}
	builder.SetSourceSelection(0x71, 0x08, 5*time.Second)
	builder.SetSourceSelectionActive()
	builder.RecordProbe(120)

	artifact, data, err := builder.Emit()
	if err != nil {
		t.Fatal(err)
	}
	validateSourceSelectionArtifactAgainstSchema(t, data)
	if artifact.Admission.SourceSelection.Mode != "source_selection" {
		t.Fatalf("source_selection.mode=%q", artifact.Admission.SourceSelection.Mode)
	}
	if artifact.Admission.State != "active" {
		t.Fatalf("state=%q", artifact.Admission.State)
	}
}

func TestM2aHarness_SourceSelectionFailNoFreeInitiatorPath(t *testing.T) {
	t.Skip("pending M3: source-address selection error-path capture; ebusgo surfaces no-available-source errors but the gateway's degraded-state transition on that error lands in M3/M4")
}

func TestM2aHarness_TransportBlindPath(t *testing.T) {
	t.Skip("partial coverage in M2 warmup integration test; full end-to-end pending M7 when MarkDegraded is wired into the M3 startup flow")
}

func TestM2aHarness_ExplicitValidateFalsePath(t *testing.T) {
	m := NewStartupSourceSelectionMetrics()
	m.SetExplicitSourceActive(true)
	m.RecordExplicitValidateOnly()
	if got := FormatStartupSourceSelectionExplicitLog(0xF0); got != "startup source selection explicit_validate_only source=0xF0 confidence=low" {
		t.Fatalf("unexpected explicit-source log line %q", got)
	}
	if m.ExplicitSourceActive.Value() != 1 {
		t.Fatalf("expected explicit_source_active=1, got %d", m.ExplicitSourceActive.Value())
	}
	if m.ExplicitValidateOnlyTotal.Value() != 1 {
		t.Fatalf("expected explicit_validate_only_total=1, got %d", m.ExplicitValidateOnlyTotal.Value())
	}
	if m.ExplicitSourceConflictDetected.Value() != 0 {
		t.Fatalf("expected no advisory source-address selector conflict when validate=false, got %d", m.ExplicitSourceConflictDetected.Value())
	}
}

func TestM2aHarness_ExplicitValidateTruePath(t *testing.T) {
	t.Run("advisory source-address selector agrees", func(t *testing.T) {
		m := NewStartupSourceSelectionMetrics()
		if CheckExplicitSourceCompanionConflict(0xF0, &protocol.SourceAddressSelection{Source: 0xF0}, m) {
			t.Fatal("expected no conflict when advisory selector agrees")
		}
		if m.ExplicitSourceConflictDetected.Value() != 0 {
			t.Fatalf("expected explicit_source_conflict_detected=0, got %d", m.ExplicitSourceConflictDetected.Value())
		}
	})
	t.Run("advisory source-address selector disagrees", func(t *testing.T) {
		m := NewStartupSourceSelectionMetrics()
		if !CheckExplicitSourceCompanionConflict(0xF0, &protocol.SourceAddressSelection{Source: 0xF1}, m) {
			t.Fatal("expected conflict when advisory selector disagrees")
		}
		if m.ExplicitSourceConflictDetected.Value() != 1 {
			t.Fatalf("expected explicit_source_conflict_detected=1, got %d", m.ExplicitSourceConflictDetected.Value())
		}
	})
}

func TestM2aHarness_EvidenceBufferFloodBaselineProtection(t *testing.T) {
	b, err := NewEvidenceBuffer(128, VaillantBaselineTopologySeed)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, addr := range VaillantBaselineTopologySeed {
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now})
	}
	for i := 0; i < 1000; i++ {
		addr := byte(0x20 + (i % 0xD0))
		b.Record(EvidenceRecord{Address: addr, Strength: EvidenceWeak, Observed: now.Add(time.Duration(i) * time.Millisecond)})
	}
	for _, addr := range VaillantBaselineTopologySeed {
		b.mu.Lock()
		_, ok := b.entries[addr]
		b.mu.Unlock()
		if !ok {
			t.Fatalf("baseline address 0x%02X was evicted", addr)
		}
	}
}

func TestM2aHarness_SourceSelectionArtifactEmissionValidatesAgainstSchema(t *testing.T) {
	t.Run("valid artifact passes", func(t *testing.T) {
		valid := []byte(`{"admission":{"state":"joined","source":113,"companion_target":8,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","source_selection":{"mode":"source_selection"}},"discovery":{"wire_bytes":2048,"window_s":15,"startup_burst_pct":62.5,"post_startup_sustained_rate_probes_per_15s":1.5,"probe_count":12,"promoted_suspects_without_identity":1,"per_baseline_address_evidence_counts":{"08":3,"15":1}}}`)
		validateSourceSelectionArtifactAgainstSchema(t, valid)
	})
	t.Run("invalid enum fails", func(t *testing.T) {
		invalid := []byte(`{"admission":{"state":"joined","source":113,"companion_target":8,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","source_selection":{"mode":"bogus"}},"discovery":{"wire_bytes":2048,"window_s":15,"startup_burst_pct":62.5,"post_startup_sustained_rate_probes_per_15s":1.5,"probe_count":12,"promoted_suspects_without_identity":1,"per_baseline_address_evidence_counts":{"08":3}}}`)
		if err := sourceSelectionArtifactSchemaError(invalid); err == nil {
			t.Fatal("expected invalid enum to fail schema validation")
		}
	})
	t.Run("missing required field fails", func(t *testing.T) {
		invalid := []byte(`{"admission":{"state":"joined","source":113,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","source_selection":{"mode":"source_selection"}},"discovery":{"wire_bytes":2048,"window_s":15,"startup_burst_pct":62.5,"post_startup_sustained_rate_probes_per_15s":1.5,"probe_count":12,"promoted_suspects_without_identity":1,"per_baseline_address_evidence_counts":{"08":3}}}`)
		if err := sourceSelectionArtifactSchemaError(invalid); err == nil {
			t.Fatal("expected missing required field to fail schema validation")
		}
	})
}

func sourceSelectionArtifactSchemaError(artifactJSON []byte) error {
	schemaPath := filepath.Join("docs", "schemas", "source-selection-artifact.schema.json")
	schemaJSON, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	var schemaEnvelope map[string]any
	if err := json.Unmarshal(schemaJSON, &schemaEnvelope); err != nil {
		return fmt.Errorf("parse schema: %w", err)
	}
	if schemaEnvelope["$id"] == "" {
		return fmt.Errorf("schema missing $id")
	}

	dec := json.NewDecoder(bytes.NewReader(artifactJSON))
	dec.DisallowUnknownFields()
	var artifact struct {
		Admission *struct {
			State            *string  `json:"state"`
			Source           *int     `json:"source"`
			CompanionTarget  *int     `json:"companion_target"`
			WarmupDurationS  *float64 `json:"warmup_duration_s"`
			ReasonIfDegraded *string  `json:"reason_if_degraded"`
			TransportKind    *string  `json:"transport_kind"`
			SourceSelection  *struct {
				Mode *string `json:"mode"`
			} `json:"source_selection"`
		} `json:"admission"`
		Discovery *struct {
			WireBytes                         *int           `json:"wire_bytes"`
			WindowS                           *float64       `json:"window_s"`
			StartupBurstPct                   *float64       `json:"startup_burst_pct"`
			PostStartupSustainedRateProbes15S *float64       `json:"post_startup_sustained_rate_probes_per_15s"`
			ProbeCount                        *int           `json:"probe_count"`
			PromotedSuspectsWithoutIdentity   *int           `json:"promoted_suspects_without_identity"`
			PerBaselineAddressEvidenceCounts  map[string]int `json:"per_baseline_address_evidence_counts"`
		} `json:"discovery"`
	}
	if err := dec.Decode(&artifact); err != nil {
		return fmt.Errorf("decode artifact: %w", err)
	}
	if artifact.Admission == nil || artifact.Discovery == nil {
		return fmt.Errorf("artifact must contain admission and discovery objects")
	}
	if artifact.Admission.State == nil || artifact.Admission.Source == nil || artifact.Admission.CompanionTarget == nil || artifact.Admission.WarmupDurationS == nil || artifact.Admission.ReasonIfDegraded == nil || artifact.Admission.TransportKind == nil || artifact.Admission.SourceSelection == nil || artifact.Admission.SourceSelection.Mode == nil {
		return fmt.Errorf("admission missing required fields")
	}
	if artifact.Discovery.WireBytes == nil || artifact.Discovery.WindowS == nil || artifact.Discovery.StartupBurstPct == nil || artifact.Discovery.PostStartupSustainedRateProbes15S == nil || artifact.Discovery.ProbeCount == nil || artifact.Discovery.PromotedSuspectsWithoutIdentity == nil || artifact.Discovery.PerBaselineAddressEvidenceCounts == nil {
		return fmt.Errorf("discovery missing required fields")
	}
	if *artifact.Admission.Source < 0 || *artifact.Admission.Source > 255 || *artifact.Admission.CompanionTarget < 0 || *artifact.Admission.CompanionTarget > 255 || *artifact.Admission.WarmupDurationS < 0 {
		return fmt.Errorf("admission numeric bounds violated")
	}
	if !containsSourceSelectionEnum([]string{"enh", "ens", "ebusd-tcp", "udp-plain", "tcp-plain", "adapter-direct"}, *artifact.Admission.TransportKind) {
		return fmt.Errorf("invalid transport_kind %q", *artifact.Admission.TransportKind)
	}
	if !containsSourceSelectionEnum([]string{"source_selection", "explicit_validate_only", "degraded_transport_blind", "degraded_no_events"}, *artifact.Admission.SourceSelection.Mode) {
		return fmt.Errorf("invalid source_selection.mode %q", *artifact.Admission.SourceSelection.Mode)
	}
	if *artifact.Discovery.WireBytes < 0 || *artifact.Discovery.WindowS <= 0 || *artifact.Discovery.StartupBurstPct < 0 || *artifact.Discovery.StartupBurstPct > 100 || *artifact.Discovery.PostStartupSustainedRateProbes15S < 0 || *artifact.Discovery.ProbeCount < 0 || *artifact.Discovery.PromotedSuspectsWithoutIdentity < 0 {
		return fmt.Errorf("discovery numeric bounds violated")
	}
	hexKey := regexp.MustCompile(`^[0-9A-Fa-f]{2}$`)
	for key, count := range artifact.Discovery.PerBaselineAddressEvidenceCounts {
		if !hexKey.MatchString(key) {
			return fmt.Errorf("invalid baseline evidence key %q", key)
		}
		if count < 0 {
			return fmt.Errorf("negative evidence count for key %q", key)
		}
	}
	return nil
}

func containsSourceSelectionEnum(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
