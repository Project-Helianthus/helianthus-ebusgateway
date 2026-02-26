package adversarial

import "time"

// Scenario defines an adversarial runtime test scenario with explicit
// duration and pass/fail thresholds. Scenarios are executed against a
// live gateway+HA stack during smoke testing; the framework itself is
// standalone and testable without hardware.
type Scenario struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Duration    time.Duration      `json:"duration"`
	Thresholds  ScenarioThresholds `json:"thresholds"`
}

// ScenarioThresholds contains the pass/fail criteria that are evaluated
// after an adverse event has been injected and the gateway attempts
// recovery.
//
// All counter-based thresholds (MinLiveEpoch, MaxCollisions) are evaluated
// as deltas from a baseline snapshot taken at scenario start. The runner
// must capture expvar values before injecting the adverse event and compute
// the delta at evaluation time. This prevents monotonic counter accumulation
// across scenarios from producing false passes or fails.
type ScenarioThresholds struct {
	// MaxRecoveryTime is the maximum wall-clock time allowed to reach
	// LIVE_READY after the adverse event is injected.
	MaxRecoveryTime time.Duration `json:"max_recovery_time"`

	// MinLiveEpoch is the minimum delta in the semantic_live_epoch
	// expvar counter during this scenario. A value of 2 means the
	// gateway must have incremented live_epoch at least twice since
	// the baseline snapshot (i.e. recovered and resumed live data).
	MinLiveEpoch int `json:"min_live_epoch"`

	// RequireZones indicates whether heating zone data must be present
	// (non-empty) after recovery.
	RequireZones bool `json:"require_zones"`

	// RequireDHW indicates whether domestic hot water data must be
	// present after recovery. Some adverse events (bus outage) may
	// cause DHW to expire, so this is not always required.
	RequireDHW bool `json:"require_dhw"`

	// MaxCollisions is the maximum delta in the
	// semantic_bus_collisions_total expvar counter allowed during
	// this scenario's duration.
	MaxCollisions int `json:"max_collisions"`
}

// ScenarioVerdict is the machine-readable result of executing a single
// adversarial scenario against the live stack.
type ScenarioVerdict struct {
	ScenarioID  string         `json:"scenario_id"`
	Name        string         `json:"name"`
	Outcome     string         `json:"outcome"` // "pass", "fail", "xfail", "blocked-infra"
	InfraReason string         `json:"infra_reason,omitempty"`
	Duration    string         `json:"duration"`
	Error       string         `json:"error,omitempty"`
	Metrics     map[string]any `json:"metrics,omitempty"`
}

// Outcome constants for ScenarioVerdict.Outcome.
const (
	OutcomePass         = "pass"
	OutcomeFail         = "fail"
	OutcomeXFail        = "xfail"
	OutcomeBlockedInfra = "blocked-infra"
)
