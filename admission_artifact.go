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

// SourceSelectionArtifact is the machine-readable summary emitted at the end
// of the startup source-selection + discovery window per plan §M7.
type SourceSelectionArtifact struct {
	Admission SourceSelectionArtifactAdmission `json:"admission"`
	Discovery SourceSelectionArtifactDiscovery `json:"discovery"`
}

type SourceSelectionArtifactAdmission struct {
	State            string                                 `json:"state"`
	Source           uint8                                  `json:"source"`
	CompanionTarget  uint8                                  `json:"companion_target"`
	WarmupDurationS  float64                                `json:"warmup_duration_s"`
	ReasonIfDegraded string                                 `json:"reason_if_degraded"`
	TransportKind    string                                 `json:"transport_kind"`
	SourceSelection  SourceSelectionArtifactSourceSelection `json:"source_selection"`
}

type SourceSelectionArtifactSourceSelection struct {
	Mode string `json:"mode"`
}

type SourceSelectionArtifactDiscovery struct {
	WireBytes                            int            `json:"wire_bytes"`
	WindowS                              float64        `json:"window_s"`
	StartupBurstPct                      float64        `json:"startup_burst_pct"`
	PostStartupSustainedRateProbesPer15s float64        `json:"post_startup_sustained_rate_probes_per_15s"`
	ProbeCount                           int            `json:"probe_count"`
	PromotedSuspectsWithoutIdentity      int            `json:"promoted_suspects_without_identity"`
	PerBaselineAddressEvidenceCounts     map[string]int `json:"per_baseline_address_evidence_counts"`
}

// SourceSelectionArtifactBuilder aggregates runtime events over the startup
// window and produces the final SourceSelectionArtifact on Emit.
type SourceSelectionArtifactBuilder struct {
	mu        sync.Mutex
	artifact  SourceSelectionArtifact
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
func (b *SourceSelectionArtifactBuilder) SetBaselineEvidenceProvider(provider func() map[string]int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.baselineEvidenceProvider = provider
}

func NewSourceSelectionArtifactBuilder(transportKind string) *SourceSelectionArtifactBuilder {
	return &SourceSelectionArtifactBuilder{
		startedAt: time.Now(),
		artifact: SourceSelectionArtifact{
			Admission: SourceSelectionArtifactAdmission{
				TransportKind: transportKind,
				State:         "pending",
				SourceSelection: SourceSelectionArtifactSourceSelection{
					Mode: "degraded_no_events",
				},
			},
			Discovery: SourceSelectionArtifactDiscovery{
				PerBaselineAddressEvidenceCounts: make(map[string]int),
			},
		},
	}
}

func (b *SourceSelectionArtifactBuilder) SetSourceSelectionMode(v string) error {
	if err := ValidateSourceSelectionMode(v); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.SourceSelection.Mode = v
	return nil
}

func (b *SourceSelectionArtifactBuilder) SetSourceSelection(source, companionTarget uint8, warmupDuration time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.Source = source
	b.artifact.Admission.CompanionTarget = companionTarget
	b.artifact.Admission.WarmupDurationS = warmupDuration.Seconds()
}

func (b *SourceSelectionArtifactBuilder) SetSourceSelectionActive() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.State = "active"
}

func (b *SourceSelectionArtifactBuilder) SetExplicitSource(source uint8) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.Source = source
}

// SetActiveExplicitSource flips Admission.State to "active" and records the
// explicit Source used from the first active frame.
func (b *SourceSelectionArtifactBuilder) SetActiveExplicitSource(source uint8) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.State = "active"
	b.artifact.Admission.Source = source
}

func (b *SourceSelectionArtifactBuilder) SetDegraded(reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Admission.State = "degraded"
	b.artifact.Admission.ReasonIfDegraded = reason
}

func (b *SourceSelectionArtifactBuilder) RecordProbe(wireBytes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Discovery.WireBytes += wireBytes
	b.artifact.Discovery.ProbeCount++
}

func (b *SourceSelectionArtifactBuilder) SetPromotedSuspects(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Discovery.PromotedSuspectsWithoutIdentity = n
}

func (b *SourceSelectionArtifactBuilder) SetPostStartupSustainedRateProbesPer15s(rate float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.artifact.Discovery.PostStartupSustainedRateProbesPer15s = rate
}

func (b *SourceSelectionArtifactBuilder) RecordBaselineEvidence(address uint8, count int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := fmt.Sprintf("%02X", address)
	b.artifact.Discovery.PerBaselineAddressEvidenceCounts[key] = count
}

func (b *SourceSelectionArtifactBuilder) Emit() (SourceSelectionArtifact, []byte, error) {
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
		return SourceSelectionArtifact{}, nil, err
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
func (b *SourceSelectionArtifactBuilder) EmitToFile(path string) error {
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
func (b *SourceSelectionArtifactBuilder) ResetEmitOnce() {
	atomic.StoreUint32(&b.emitOnce, 0)
}
