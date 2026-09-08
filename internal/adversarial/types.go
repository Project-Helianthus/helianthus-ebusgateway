package adversarial

// The v1 types intentionally mirror the public JSON contract. They contain no
// executable, network, adapter, or live-control capability.
type Suite struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

type ReportV1 struct {
	Schema        string           `json:"$schema"`
	SchemaVersion int64            `json:"schema_version"`
	Suite         Suite            `json:"suite"`
	Execution     Execution        `json:"execution"`
	Provenance    Provenance       `json:"provenance"`
	Scenarios     []ScenarioResult `json:"scenarios"`
	Summary       Summary          `json:"summary"`
}

type Execution struct {
	Mode        string `json:"mode"`
	RunID       string `json:"run_id"`
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at"`
}

type Provenance struct {
	Subject          Subject  `json:"subject"`
	Producer         Producer `json:"producer"`
	FixtureSetSHA256 string   `json:"fixture_set_sha256"`
	FixtureCaseID    string   `json:"fixture_case_id"`
}

type Subject struct {
	Repository     string `json:"repository"`
	Commit         string `json:"commit"`
	SourceTree     string `json:"source_tree"`
	ArtifactKind   string `json:"artifact_kind"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}

type Producer struct {
	Repository               string  `json:"repository"`
	Commit                   string  `json:"commit"`
	Component                string  `json:"component"`
	BuildKind                string  `json:"build_kind"`
	BuildSHA256              string  `json:"build_sha256"`
	InputGatewayReportSHA256 *string `json:"input_gateway_report_sha256"`
}

type ProducerIdentity struct {
	Repository               string
	Commit                   string
	Component                string
	BuildKind                string
	BuildSHA256              string
	InputGatewayReportSHA256 *string
}

func (p ProducerIdentity) wire() Producer {
	return Producer{p.Repository, p.Commit, p.Component, p.BuildKind, p.BuildSHA256, cloneString(p.InputGatewayReportSHA256)}
}

type Definition struct {
	ScenarioID             string `json:"scenario_id"`
	Name                   string `json:"name"`
	DurationLimitMS        int64  `json:"duration_limit_ms"`
	TriggerKind            string `json:"trigger_kind"`
	RecoveryTarget         string `json:"recovery_target"`
	MinimumLiveEpochDelta  int64  `json:"minimum_live_epoch_delta"`
	ZonesRequired          bool   `json:"zones_required"`
	DHWRequired            bool   `json:"dhw_required"`
	MaximumCollisionsDelta int64  `json:"maximum_collisions_delta"`
	MaximumRecoveryMS      int64  `json:"maximum_recovery_ms"`

	RecoveryAnchor string   `json:"-"`
	RecoveryEvent  string   `json:"-"`
	ExpectedEvents []string `json:"-"`
	BaselinePhase  string   `json:"-"`
	EndPhase       string   `json:"-"`
}

type ScenarioResult struct {
	Definition           Definition      `json:"definition"`
	Action               Action          `json:"action"`
	Timing               Timing          `json:"timing"`
	Metrics              Metrics         `json:"metrics"`
	Evaluation           *Evaluation     `json:"evaluation"`
	Errors               []ScenarioError `json:"errors"`
	InfrastructureReason *string         `json:"infrastructure_reason"`
	ResultKind           string          `json:"result_kind"`
	Outcome              string          `json:"outcome"`
}

type Action struct {
	Events []ActionEvent `json:"events"`
}
type ActionEvent struct {
	Kind         string `json:"kind"`
	Source       string `json:"source"`
	At           string `json:"at"`
	OffsetMS     int64  `json:"offset_ms"`
	ErrorBoundMS int64  `json:"error_bound_ms"`
}
type Timing struct {
	ScenarioStartedAt string  `json:"scenario_started_at"`
	ScenarioEndedAt   string  `json:"scenario_ended_at"`
	ElapsedMS         int64   `json:"elapsed_ms"`
	RecoveryAnchor    *string `json:"recovery_anchor"`
	RecoveryObserved  *string `json:"recovery_observed"`
	RecoveryMS        *int64  `json:"recovery_ms"`
	ErrorBoundMS      int64   `json:"error_bound_ms"`
}
type Metrics struct {
	Baseline *Snapshot `json:"baseline"`
	End      *Snapshot `json:"end"`
	Delta    *Delta    `json:"delta"`
}
type Snapshot struct {
	CounterEpoch                string `json:"counter_epoch"`
	CapturedAt                  string `json:"captured_at"`
	OffsetMS                    int64  `json:"offset_ms"`
	SemanticStartupCurrentPhase string `json:"semantic_startup_current_phase"`
	SemanticLiveEpoch           int64  `json:"semantic_live_epoch"`
	SemanticBusCollisionsTotal  int64  `json:"semantic_bus_collisions_total"`
	SemanticZoneCount           int64  `json:"semantic_zone_count"`
	SemanticDHWPresent          bool   `json:"semantic_dhw_present"`
}
type Delta struct {
	SemanticLiveEpoch          int64 `json:"semantic_live_epoch"`
	SemanticBusCollisionsTotal int64 `json:"semantic_bus_collisions_total"`
}
type Evaluation struct {
	Duration   DurationDecision `json:"duration"`
	Action     ActionDecision   `json:"action"`
	Recovery   RecoveryDecision `json:"recovery"`
	LiveEpoch  MinimumDecision  `json:"live_epoch"`
	Zones      RequiredDecision `json:"zones"`
	DHW        RequiredDecision `json:"dhw"`
	Collisions MaximumDecision  `json:"collisions"`
}
type DurationDecision struct {
	ExpectedMS   int64 `json:"expected_ms"`
	ObservedMS   int64 `json:"observed_ms"`
	ErrorBoundMS int64 `json:"error_bound_ms"`
	Passed       bool  `json:"passed"`
}
type ActionDecision struct {
	ExpectedKind string `json:"expected_kind"`
	ObservedKind string `json:"observed_kind"`
	Passed       bool   `json:"passed"`
}
type RecoveryDecision struct {
	MaximumMS    int64 `json:"maximum_ms"`
	ObservedMS   int64 `json:"observed_ms"`
	ErrorBoundMS int64 `json:"error_bound_ms"`
	Passed       bool  `json:"passed"`
}
type MinimumDecision struct {
	Minimum  int64 `json:"minimum"`
	Observed int64 `json:"observed"`
	Passed   bool  `json:"passed"`
}
type RequiredDecision struct {
	Required bool `json:"required"`
	Observed bool `json:"observed"`
	Passed   bool `json:"passed"`
}
type MaximumDecision struct {
	Maximum  int64 `json:"maximum"`
	Observed int64 `json:"observed"`
	Passed   bool  `json:"passed"`
}

type ScenarioError struct {
	Phase string `json:"phase"`
	Code  string `json:"code"`
}
type Summary struct {
	Total   int64  `json:"total"`
	Passed  int64  `json:"passed"`
	Failed  int64  `json:"failed"`
	XFailed int64  `json:"xfailed"`
	Blocked int64  `json:"blocked"`
	Unknown int64  `json:"unknown"`
	Verdict string `json:"verdict"`
}

type FixtureV1 struct {
	Schema        string            `json:"$schema"`
	SchemaVersion int64             `json:"schema_version"`
	Suite         Suite             `json:"suite"`
	FixtureCaseID string            `json:"fixture_case_id"`
	RunID         string            `json:"run_id"`
	Scenarios     []FixtureScenario `json:"scenarios"`
}
type FixtureScenario struct {
	ScenarioID          string              `json:"scenario_id"`
	TriggerKind         string              `json:"trigger_kind"`
	Precondition        Precondition        `json:"precondition"`
	Events              []FixtureEvent      `json:"events"`
	Observations        FixtureObservations `json:"observations"`
	TerminalError       *TerminalError      `json:"terminal_error"`
	ResourceArtifactIDs []string            `json:"resource_artifact_ids"`
}
type Precondition struct {
	Available         bool    `json:"available"`
	UnavailableReason *string `json:"unavailable_reason"`
}
type FixtureEvent struct {
	Kind         string `json:"kind"`
	OffsetMS     int64  `json:"offset_ms"`
	ErrorBoundMS int64  `json:"error_bound_ms"`
}
type FixtureObservations struct {
	Baseline *FixtureSnapshot `json:"baseline"`
	End      *FixtureSnapshot `json:"end"`
}
type FixtureSnapshot struct {
	CounterEpoch                string `json:"counter_epoch"`
	OffsetMS                    int64  `json:"offset_ms"`
	SemanticStartupCurrentPhase string `json:"semantic_startup_current_phase"`
	SemanticLiveEpoch           int64  `json:"semantic_live_epoch"`
	SemanticBusCollisionsTotal  int64  `json:"semantic_bus_collisions_total"`
	SemanticZoneCount           int64  `json:"semantic_zone_count"`
	SemanticDHWPresent          bool   `json:"semantic_dhw_present"`
}
type TerminalError struct {
	Phase string `json:"phase"`
	Code  string `json:"code"`
}

func cloneString(v *string) *string {
	if v == nil {
		return nil
	}
	x := *v
	return &x
}
