package ebusgateway

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// AdmissionArtifact is the machine-readable summary emitted at the end
// of the startup admission + discovery window per plan §M7.
type AdmissionArtifact struct {
	Admission AdmissionArtifactAdmission `json:"admission"`
	Discovery AdmissionArtifactDiscovery `json:"discovery"`
}

type AdmissionArtifactAdmission struct {
	State                 string  `json:"state"`
	Source                uint8   `json:"source"`
	CompanionTarget       uint8   `json:"companion_target"`
	WarmupDurationS       float64 `json:"warmup_duration_s"`
	ReasonIfDegraded      string  `json:"reason_if_degraded"`
	TransportKind         string  `json:"transport_kind"`
	AdmissionPathSelected string  `json:"admission_path_selected"`
}

type AdmissionArtifactDiscovery struct {
	WireBytes                            int            `json:"wire_bytes"`
	WindowS                              float64        `json:"window_s"`
	StartupBurstPct                      float64        `json:"startup_burst_pct"`
	PostStartupSustainedRateProbesPer15s float64        `json:"post_startup_sustained_rate_probes_per_15s"`
	ProbeCount                           int            `json:"probe_count"`
	PromotedSuspectsWithoutIdentity      int            `json:"promoted_suspects_without_identity"`
	PerBaselineAddressEvidenceCounts     map[string]int `json:"per_baseline_address_evidence_counts"`
}

// AdmissionArtifactBuilder aggregates runtime events over the startup
// window and produces the final AdmissionArtifact on Emit.
type AdmissionArtifactBuilder struct {
	mu        sync.Mutex
	artifact  AdmissionArtifact
	startedAt time.Time
}

func NewAdmissionArtifactBuilder(transportKind string) *AdmissionArtifactBuilder {
	return &AdmissionArtifactBuilder{
		startedAt: time.Now(),
		artifact: AdmissionArtifact{
			Admission: AdmissionArtifactAdmission{
				TransportKind:         transportKind,
				State:                 "pending",
				AdmissionPathSelected: "degraded_no_events",
			},
			Discovery: AdmissionArtifactDiscovery{
				PerBaselineAddressEvidenceCounts: make(map[string]int),
			},
		},
	}
}

func (b *AdmissionArtifactBuilder) SetAdmissionPathSelected(v string) error {
	if err := ValidateAdmissionPathSelected(v); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.AdmissionPathSelected = v
	return nil
}

func (b *AdmissionArtifactBuilder) SetJoinerSelection(source, companionTarget uint8, warmupDuration time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.Source = source
	b.artifact.Admission.CompanionTarget = companionTarget
	b.artifact.Admission.WarmupDurationS = warmupDuration.Seconds()
	b.artifact.Admission.State = "active"
}

func (b *AdmissionArtifactBuilder) SetOverrideSource(source uint8) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.Source = source
}

func (b *AdmissionArtifactBuilder) SetDegraded(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.State = "degraded"
	b.artifact.Admission.ReasonIfDegraded = reason
}

func (b *AdmissionArtifactBuilder) RecordProbe(wireBytes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Discovery.WireBytes += wireBytes
	b.artifact.Discovery.ProbeCount++
}

func (b *AdmissionArtifactBuilder) SetPromotedSuspects(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Discovery.PromotedSuspectsWithoutIdentity = n
}

func (b *AdmissionArtifactBuilder) SetPostStartupSustainedRateProbesPer15s(rate float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Discovery.PostStartupSustainedRateProbesPer15s = rate
}

func (b *AdmissionArtifactBuilder) RecordBaselineEvidence(address uint8, count int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := fmt.Sprintf("%02X", address)
	b.artifact.Discovery.PerBaselineAddressEvidenceCounts[key] = count
}

func (b *AdmissionArtifactBuilder) Emit() (AdmissionArtifact, []byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	windowS := time.Since(b.startedAt).Seconds()
	if windowS > 0 {
		b.artifact.Discovery.WindowS = windowS
		b.artifact.Discovery.StartupBurstPct = float64(b.artifact.Discovery.WireBytes) / (windowS * 240) * 100
	}
	data, err := json.MarshalIndent(b.artifact, "", "  ")
	if err != nil {
		return AdmissionArtifact{}, nil, err
	}
	return b.artifact, data, nil
}

func (b *AdmissionArtifactBuilder) EmitToFile(path string) error {
	_, data, err := b.Emit()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
