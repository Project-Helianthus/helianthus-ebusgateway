package adversarial

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// AdversarialReport is the top-level machine-readable artifact produced
// by an adversarial scenario run. It is written as JSON and consumed by
// the tester gate in CI.
type AdversarialReport struct {
	GeneratedAt string            `json:"generated_at"`
	Scenarios   []ScenarioVerdict `json:"scenarios"`
	Summary     ReportSummary     `json:"summary"`
}

// ReportSummary aggregates the outcome counts across all scenarios in a
// single adversarial run. The invariant Total == Passed+Failed+XFailed+Blocked+Unknown
// always holds.
type ReportSummary struct {
	Total   int `json:"total"`
	Passed  int `json:"passed"`
	Failed  int `json:"failed"`
	XFailed int `json:"xfailed"`
	Blocked int `json:"blocked"`
	Unknown int `json:"unknown"`
}

// GenerateReport builds an AdversarialReport from a slice of verdicts,
// computing the summary counts and stamping the generation time.
func GenerateReport(verdicts []ScenarioVerdict) AdversarialReport {
	summary := ReportSummary{Total: len(verdicts)}
	for _, verdict := range verdicts {
		switch verdict.Outcome {
		case OutcomePass:
			summary.Passed++
		case OutcomeFail:
			summary.Failed++
		case OutcomeXFail:
			summary.XFailed++
		case OutcomeBlockedInfra:
			summary.Blocked++
		default:
			summary.Unknown++
		}
	}

	return AdversarialReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Scenarios:   verdicts,
		Summary:     summary,
	}
}

// WriteReport serialises the report as indented JSON and writes it to
// the given path. Parent directories are created as needed.
func WriteReport(report AdversarialReport, path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	data = append(data, '\n')

	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}
