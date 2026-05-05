package ebusgateway

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	emitOnce  uint32 // CAS guard for EmitToFile
	// baselineEvidenceProvider is called immediately before Emit/EmitToFile
	// to populate per_baseline_address_evidence_counts from a runtime
	// observability source (typically the bus_observability store). Set
	// by main.go after busObservability is wired. nil during early
	// startup; emit before the provider is set produces an empty map.
	baselineEvidenceProvider func() map[string]int
}

// SetBaselineEvidenceProvider installs a callback that returns the
// per-baseline-address evidence counts at emit time. Called by Emit and
// EmitToFile under the builder's mutex; the provider must be safe for
// concurrent invocation. Resolves cruise-run #20 validation finding that
// per_baseline_address_evidence_counts was unconditionally empty in the
// emitted artifact even when the registry observed traffic to baseline
// addresses.
func (b *AdmissionArtifactBuilder) SetBaselineEvidenceProvider(provider func() map[string]int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baselineEvidenceProvider = provider
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

func (b *AdmissionArtifactBuilder) SetSourceSelection(source, companionTarget uint8, warmupDuration time.Duration) {
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

// SetActiveOverride flips Admission.State to "active" and records the
// override Source. Used by the override path (AD09 (c2) soft short-
// circuit) where the override source is in use from the first active
// frame regardless of source-address selector outcome — so the artifact must reflect
// state="active", not the default "pending". Found by AD20 second-
// reviewer M7 pass: the prior code path emitted state=pending on
// override which was misleading to operators.
func (b *AdmissionArtifactBuilder) SetActiveOverride(source uint8) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.State = "active"
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
	if b.baselineEvidenceProvider != nil {
		if counts := b.baselineEvidenceProvider(); counts != nil {
			b.artifact.Discovery.PerBaselineAddressEvidenceCounts = counts
		}
	}
	data, err := json.MarshalIndent(b.artifact, "", "  ")
	if err != nil {
		return AdmissionArtifact{}, nil, err
	}
	return b.artifact, data, nil
}

// EmitToFile writes the artifact JSON to the given path. Once a snapshot
// is emitted (e.g. by the 60s startup-window goroutine), subsequent calls
// are no-ops via the emitOnce guard — this prevents the AD20-reviewer-
// flagged race where a defer-on-shutdown emit could overwrite the
// canonical 60s window snapshot with later state.
//
// To re-arm the builder for a new window, call ResetEmitOnce.
func (b *AdmissionArtifactBuilder) EmitToFile(path string) error {
	if !atomic.CompareAndSwapUint32(&b.emitOnce, 0, 1) {
		return nil
	}
	_, data, err := b.Emit()
	if err != nil {
		// Re-arm so a follow-up call can retry; the file was not written.
		atomic.StoreUint32(&b.emitOnce, 0)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		atomic.StoreUint32(&b.emitOnce, 0)
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ResetEmitOnce re-arms EmitToFile so the next call will write again.
// Used by tests; production callers should not need this.
func (b *AdmissionArtifactBuilder) ResetEmitOnce() {
	atomic.StoreUint32(&b.emitOnce, 0)
}
