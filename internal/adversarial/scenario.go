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
type ScenarioThresholds struct {
	// MaxRecoveryTime is the maximum wall-clock time allowed to reach
	// LIVE_READY after the adverse event is injected.
	MaxRecoveryTime time.Duration `json:"max_recovery_time"`

	// MinLiveEpoch is the minimum live_epoch expvar value that the
	// gateway must report after recovery. A value of 2 means the
	// gateway booted, reached LIVE_READY, experienced the adverse
	// event, and reached LIVE_READY again.
	MinLiveEpoch int `json:"min_live_epoch"`

	// RequireZones indicates whether heating zone data must be present
	// (non-empty) after recovery.
	RequireZones bool `json:"require_zones"`

	// RequireDHW indicates whether domestic hot water data must be
	// present after recovery. Some adverse events (bus outage) may
	// cause DHW to expire, so this is not always required.
	RequireDHW bool `json:"require_dhw"`

	// MaxCollisions is the maximum number of eBUS arbitration
	// collisions allowed during the entire scenario duration.
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
