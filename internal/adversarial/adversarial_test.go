package adversarial

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultScenarios_Count(t *testing.T) {
	t.Parallel()

	scenarios := DefaultScenarios()
	if len(scenarios) != 4 {
		t.Fatalf("len(scenarios) = %d; want 4", len(scenarios))
	}
}

func TestDefaultScenarios_UniqueIDs(t *testing.T) {
	t.Parallel()

	scenarios := DefaultScenarios()
	seen := make(map[string]struct{}, len(scenarios))
	for _, scenario := range scenarios {
		if _, duplicate := seen[scenario.ID]; duplicate {
			t.Fatalf("duplicate scenario id %q", scenario.ID)
		}
		seen[scenario.ID] = struct{}{}
	}
}

func TestDefaultScenarios_ValidThresholds(t *testing.T) {
	t.Parallel()

	scenarios := DefaultScenarios()
	for _, scenario := range scenarios {
		if scenario.Duration <= 0 {
			t.Fatalf("scenario %s: duration = %v; want > 0", scenario.ID, scenario.Duration)
		}
		if scenario.Thresholds.MaxRecoveryTime <= 0 {
			t.Fatalf("scenario %s: max_recovery_time = %v; want > 0", scenario.ID, scenario.Thresholds.MaxRecoveryTime)
		}
		if scenario.Thresholds.MinLiveEpoch <= 0 {
			t.Fatalf("scenario %s: min_live_epoch = %d; want > 0", scenario.ID, scenario.Thresholds.MinLiveEpoch)
		}
		if scenario.Thresholds.MaxCollisions < 0 {
			t.Fatalf("scenario %s: max_collisions = %d; want >= 0", scenario.ID, scenario.Thresholds.MaxCollisions)
		}
		if scenario.Name == "" {
			t.Fatalf("scenario %s: name is empty", scenario.ID)
		}
		if scenario.Description == "" {
			t.Fatalf("scenario %s: description is empty", scenario.ID)
		}
	}
}

func TestDefaultScenarios_RecoveryWithinDuration(t *testing.T) {
	t.Parallel()

	scenarios := DefaultScenarios()
	for _, scenario := range scenarios {
		if scenario.Thresholds.MaxRecoveryTime > scenario.Duration {
			t.Fatalf(
				"scenario %s: max_recovery_time %v exceeds duration %v",
				scenario.ID,
				scenario.Thresholds.MaxRecoveryTime,
				scenario.Duration,
			)
		}
	}
}

func TestGenerateReport_Summary(t *testing.T) {
	t.Parallel()

	verdicts := []ScenarioVerdict{
		{ScenarioID: "ADV-01", Name: "pass-scenario", Outcome: OutcomePass, Duration: "1m30s"},
		{ScenarioID: "ADV-02", Name: "fail-scenario", Outcome: OutcomeFail, Duration: "2m0s", Error: "recovery timeout"},
		{ScenarioID: "ADV-03", Name: "xfail-scenario", Outcome: OutcomeXFail, Duration: "3m0s"},
		{ScenarioID: "ADV-04", Name: "blocked-scenario", Outcome: OutcomeBlockedInfra, Duration: "0s", InfraReason: "adapter_no_signal"},
	}

	report := GenerateReport(verdicts)

	if report.Summary.Total != 4 {
		t.Fatalf("summary.total = %d; want 4", report.Summary.Total)
	}
	if report.Summary.Passed != 1 {
		t.Fatalf("summary.passed = %d; want 1", report.Summary.Passed)
	}
	if report.Summary.Failed != 1 {
		t.Fatalf("summary.failed = %d; want 1", report.Summary.Failed)
	}
	if report.Summary.XFailed != 1 {
		t.Fatalf("summary.xfailed = %d; want 1", report.Summary.XFailed)
	}
	if report.Summary.Blocked != 1 {
		t.Fatalf("summary.blocked = %d; want 1", report.Summary.Blocked)
	}
	if len(report.Scenarios) != 4 {
		t.Fatalf("len(report.scenarios) = %d; want 4", len(report.Scenarios))
	}
	if report.GeneratedAt == "" {
		t.Fatalf("generated_at is empty")
	}
}

func TestGenerateReport_EmptyVerdicts(t *testing.T) {
	t.Parallel()

	report := GenerateReport(nil)

	if report.Summary.Total != 0 {
		t.Fatalf("summary.total = %d; want 0", report.Summary.Total)
	}
	if report.Summary.Passed != 0 {
		t.Fatalf("summary.passed = %d; want 0", report.Summary.Passed)
	}
	if report.GeneratedAt == "" {
		t.Fatalf("generated_at is empty")
	}
}

func TestWriteReport_JSON(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	reportPath := filepath.Join(tempDir, "adversarial", "report.json")

	verdicts := []ScenarioVerdict{
		{
			ScenarioID: "ADV-01",
			Name:       "HA restart while gateway stable",
			Outcome:    OutcomePass,
			Duration:   "1m30s",
			Metrics:    map[string]any{"live_epoch": 3, "collisions": 1},
		},
		{
			ScenarioID: "ADV-02",
			Name:       "Bus reset mid-flight",
			Outcome:    OutcomeFail,
			Duration:   "2m5s",
			Error:      "recovery exceeded 120s threshold",
		},
	}

	report := GenerateReport(verdicts)

	if err := WriteReport(report, reportPath); err != nil {
		t.Fatalf("WriteReport error = %v", err)
	}

	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}

	var decoded AdversarialReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode report: %v", err)
	}

	if decoded.Summary.Total != 2 {
		t.Fatalf("decoded summary.total = %d; want 2", decoded.Summary.Total)
	}
	if decoded.Summary.Passed != 1 {
		t.Fatalf("decoded summary.passed = %d; want 1", decoded.Summary.Passed)
	}
	if decoded.Summary.Failed != 1 {
		t.Fatalf("decoded summary.failed = %d; want 1", decoded.Summary.Failed)
	}
	if len(decoded.Scenarios) != 2 {
		t.Fatalf("decoded len(scenarios) = %d; want 2", len(decoded.Scenarios))
	}
	if decoded.Scenarios[0].ScenarioID != "ADV-01" {
		t.Fatalf("decoded scenario[0].id = %q; want ADV-01", decoded.Scenarios[0].ScenarioID)
	}
	if decoded.Scenarios[1].Error != "recovery exceeded 120s threshold" {
		t.Fatalf("decoded scenario[1].error = %q", decoded.Scenarios[1].Error)
	}

	// Verify metrics round-trip.
	metricsRaw, ok := decoded.Scenarios[0].Metrics["live_epoch"]
	if !ok {
		t.Fatalf("metrics missing live_epoch key")
	}
	// JSON numbers decode as float64.
	if epoch, ok := metricsRaw.(float64); !ok || epoch != 3 {
		t.Fatalf("metrics live_epoch = %v; want 3", metricsRaw)
	}
}

func TestWriteReport_CreatesParentDir(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	reportPath := filepath.Join(tempDir, "deep", "nested", "report.json")

	report := GenerateReport(nil)
	if err := WriteReport(report, reportPath); err != nil {
		t.Fatalf("WriteReport error = %v", err)
	}

	if _, err := os.Stat(reportPath); err != nil {
		t.Fatalf("stat report: %v", err)
	}
}

func TestGenerateReport_UnknownOutcome(t *testing.T) {
	t.Parallel()

	verdicts := []ScenarioVerdict{
		{ScenarioID: "ADV-01", Name: "pass", Outcome: OutcomePass, Duration: "1m0s"},
		{ScenarioID: "ADV-02", Name: "bogus", Outcome: "bogus_outcome", Duration: "1m0s"},
	}

	report := GenerateReport(verdicts)

	if report.Summary.Total != 2 {
		t.Fatalf("summary.total = %d; want 2", report.Summary.Total)
	}
	if report.Summary.Passed != 1 {
		t.Fatalf("summary.passed = %d; want 1", report.Summary.Passed)
	}
	if report.Summary.Unknown != 1 {
		t.Fatalf("summary.unknown = %d; want 1", report.Summary.Unknown)
	}
	// Invariant: total == passed+failed+xfailed+blocked+unknown
	sum := report.Summary.Passed + report.Summary.Failed + report.Summary.XFailed + report.Summary.Blocked + report.Summary.Unknown
	if sum != report.Summary.Total {
		t.Fatalf("sum of buckets %d != total %d", sum, report.Summary.Total)
	}
}

func TestScenarioVerdict_JSONOmitsEmpty(t *testing.T) {
	t.Parallel()

	verdict := ScenarioVerdict{
		ScenarioID: "ADV-01",
		Name:       "test",
		Outcome:    OutcomePass,
		Duration:   "1m0s",
	}

	data, err := json.Marshal(verdict)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}

	if _, present := raw["infra_reason"]; present {
		t.Fatalf("infra_reason should be omitted when empty")
	}
	if _, present := raw["error"]; present {
		t.Fatalf("error should be omitted when empty")
	}
	if _, present := raw["metrics"]; present {
		t.Fatalf("metrics should be omitted when nil")
	}
}
