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

func synthesizedBroadcastEvent(src byte) PassiveClassifiedEvent {
	return PassiveClassifiedEvent{
		Kind:    PassiveClassifiedEventBroadcastFrame,
		Request: protocol.Frame{Source: src, Target: protocol.AddressBroadcast, Primary: 0xB5, Secondary: 0x16, Data: []byte{0x01}},
	}
}

func feedSynthesizedPassiveStream(t *testing.T, reconstructor *PassiveTransactionReconstructor, events []PassiveClassifiedEvent) func() {
	t.Helper()
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		base := time.Unix(0, 0)
		for i, event := range events {
			select {
			case <-stopCh:
				return
			default:
			}
			feedPassiveSymbols(reconstructor, base.Add(time.Duration(i)*time.Second), synthesizedEventSymbols(t, event))
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return func() {
		close(stopCh)
		<-done
	}
}

func validateAdmissionArtifactAgainstSchema(t *testing.T, artifactJSON []byte) {
	t.Helper()
	if err := admissionArtifactSchemaError(artifactJSON); err != nil {
		t.Fatalf("artifact does not validate against admission schema: %v", err)
	}
}

func TestM2aHarness_JoinerSuccessPath(t *testing.T) {
	t.Skip("pending M3: admission FSM + JoinResult capture")
}

func TestM2aHarness_JoinerFailNoFreeInitiatorPath(t *testing.T) {
	t.Skip("pending M3: JoinResult error-path capture; ebusgo surfaces ErrNoFreeInitiatorAddress but the gateway's degraded-state transition on that error lands in M3/M4")
}

func TestM2aHarness_TransportBlindPath(t *testing.T) {
	t.Skip("partial coverage in M2 warmup integration test; full end-to-end pending M7 when MarkDegraded is wired into the M3 startup flow")
}

func TestM2aHarness_OverrideValidateFalsePath(t *testing.T) {
	t.Skip("pending M6: StartupSource.Override wiring; override_semantics_source_of_truth: M6")
}

func TestM2aHarness_OverrideValidateTruePath(t *testing.T) {
	t.Skip("pending M6: StartupSource.Override.Validate wiring + retrospective conflict detection")
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

func TestM2aHarness_AdmissionArtifactEmissionValidatesAgainstSchema(t *testing.T) {
	t.Run("valid artifact passes", func(t *testing.T) {
		valid := []byte(`{"admission":{"state":"joined","source":113,"companion_target":8,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","admission_path_selected":"join"},"discovery":{"wire_bytes":2048,"window_s":15,"startup_burst_pct":62.5,"post_startup_sustained_rate_probes_per_15s":1.5,"probe_count":12,"promoted_suspects_without_identity":1,"per_baseline_address_evidence_counts":{"08":3,"15":1}}}`)
		validateAdmissionArtifactAgainstSchema(t, valid)
	})
	t.Run("invalid enum fails", func(t *testing.T) {
		invalid := []byte(`{"admission":{"state":"joined","source":113,"companion_target":8,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","admission_path_selected":"bogus"},"discovery":{"wire_bytes":2048,"window_s":15,"startup_burst_pct":62.5,"post_startup_sustained_rate_probes_per_15s":1.5,"probe_count":12,"promoted_suspects_without_identity":1,"per_baseline_address_evidence_counts":{"08":3}}}`)
		if err := admissionArtifactSchemaError(invalid); err == nil {
			t.Fatal("expected invalid enum to fail schema validation")
		}
	})
	t.Run("missing required field fails", func(t *testing.T) {
		invalid := []byte(`{"admission":{"state":"joined","source":113,"warmup_duration_s":5,"reason_if_degraded":"","transport_kind":"enh","admission_path_selected":"join"},"discovery":{"wire_bytes":2048,"window_s":15,"startup_burst_pct":62.5,"post_startup_sustained_rate_probes_per_15s":1.5,"probe_count":12,"promoted_suspects_without_identity":1,"per_baseline_address_evidence_counts":{"08":3}}}`)
		if err := admissionArtifactSchemaError(invalid); err == nil {
			t.Fatal("expected missing required field to fail schema validation")
		}
	})
}

func synthesizedEventSymbols(t *testing.T, event PassiveClassifiedEvent) []byte {
	t.Helper()
	switch event.Kind {
	case PassiveClassifiedEventBroadcastFrame:
		return frameBytes(event.Request)
	case PassiveClassifiedEventTransaction:
		payload := append([]byte{}, frameBytes(event.Request)...)
		payload = append(payload, protocol.SymbolAck)
		if event.HasResponse {
			payload = append(payload, responseSegmentBytes(event.Response.Data)...)
			payload = append(payload, protocol.SymbolAck, protocol.SymbolSyn)
			return payload
		}
		return append(payload, protocol.SymbolSyn)
	default:
		t.Fatalf("unsupported synthesized event kind %d", event.Kind)
		return nil
	}
}

func admissionArtifactSchemaError(artifactJSON []byte) error {
	schemaPath := filepath.Join("docs", "schemas", "admission-artifact.schema.json")
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
			State                 *string  `json:"state"`
			Source                *int     `json:"source"`
			CompanionTarget       *int     `json:"companion_target"`
			WarmupDurationS       *float64 `json:"warmup_duration_s"`
			ReasonIfDegraded      *string  `json:"reason_if_degraded"`
			TransportKind         *string  `json:"transport_kind"`
			AdmissionPathSelected *string  `json:"admission_path_selected"`
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
	if artifact.Admission.State == nil || artifact.Admission.Source == nil || artifact.Admission.CompanionTarget == nil || artifact.Admission.WarmupDurationS == nil || artifact.Admission.ReasonIfDegraded == nil || artifact.Admission.TransportKind == nil || artifact.Admission.AdmissionPathSelected == nil {
		return fmt.Errorf("admission missing required fields")
	}
	if artifact.Discovery.WireBytes == nil || artifact.Discovery.WindowS == nil || artifact.Discovery.StartupBurstPct == nil || artifact.Discovery.PostStartupSustainedRateProbes15S == nil || artifact.Discovery.ProbeCount == nil || artifact.Discovery.PromotedSuspectsWithoutIdentity == nil || artifact.Discovery.PerBaselineAddressEvidenceCounts == nil {
		return fmt.Errorf("discovery missing required fields")
	}
	if *artifact.Admission.Source < 0 || *artifact.Admission.Source > 255 || *artifact.Admission.CompanionTarget < 0 || *artifact.Admission.CompanionTarget > 255 || *artifact.Admission.WarmupDurationS < 0 {
		return fmt.Errorf("admission numeric bounds violated")
	}
	if !containsAdmissionEnum([]string{"enh", "ens", "ebusd-tcp", "udp-plain", "tcp-plain"}, *artifact.Admission.TransportKind) {
		return fmt.Errorf("invalid transport_kind %q", *artifact.Admission.TransportKind)
	}
	if !containsAdmissionEnum([]string{"join", "override", "degraded_transport_blind", "degraded_no_events"}, *artifact.Admission.AdmissionPathSelected) {
		return fmt.Errorf("invalid admission_path_selected %q", *artifact.Admission.AdmissionPathSelected)
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

func containsAdmissionEnum(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
