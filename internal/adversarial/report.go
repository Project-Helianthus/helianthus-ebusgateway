package adversarial

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const maximumReportBytes = 1024 * 1024

// ValidateReportBytes rejects oversized, malformed, duplicate-key-prone JSON
// before it reaches the report guard. The decoder uses json.Number so counters
// cannot silently pass through float conversion.
func ValidateReportBytes(data []byte) error {
	if len(data) > maximumReportBytes {
		return errors.New("report exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var report map[string]any
	if err := decoder.Decode(&report); err != nil {
		return fmt.Errorf("decode report: %w", err)
	}
	if decoder.More() {
		return errors.New("report has trailing value")
	}
	return ValidateReport(report)
}

// WriteReport validates a complete v1 offline report before atomically replacing
// path. Callers never observe a partially serialized report.
func WriteReport(report map[string]any, path string) error {
	if err := ValidateReport(report); err != nil {
		return fmt.Errorf("invalid adversarial report: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".adversarial-report-*")
	if err != nil {
		return fmt.Errorf("create report temp: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod report temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write report temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync report temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close report temp: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace report: %w", err)
	}
	return nil
}

// ValidateReport is a fail-closed in-process guard. The public JSON schema
// remains the cross-repository consumer contract.
func ValidateReport(report map[string]any) error {
	if report == nil {
		return errors.New("report is nil")
	}
	if report["$schema"] != ReportSchemaURL || integer(report["schema_version"]) != 1 {
		return errors.New("unsupported report schema")
	}
	suite, ok := report["suite"].(map[string]any)
	if !ok || suite["id"] != SuiteID || integer(suite["version"]) != 1 {
		return errors.New("unsupported suite")
	}
	scenarios, ok := report["scenarios"].([]any)
	if !ok || len(scenarios) != 4 {
		return errors.New("report must contain exactly four scenarios")
	}
	passed, failed, blocked := 0, 0, 0
	for index, raw := range scenarios {
		s, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("scenario %d is not an object", index)
		}
		definition, ok := s["definition"].(map[string]any)
		if !ok || definition["scenario_id"] != Catalog()[index].ScenarioID {
			return fmt.Errorf("scenario %d catalog mismatch", index)
		}
		switch s["outcome"] {
		case "pass":
			passed++
		case "fail":
			failed++
		case "blocked-infra":
			blocked++
		default:
			return fmt.Errorf("scenario %d has invalid outcome", index)
		}
	}
	summary, ok := report["summary"].(map[string]any)
	if !ok || integer(summary["total"]) != 4 || integer(summary["passed"]) != int64(passed) || integer(summary["failed"]) != int64(failed) || integer(summary["blocked"]) != int64(blocked) || integer(summary["xfailed"]) != 0 || integer(summary["unknown"]) != 0 {
		return errors.New("report summary mismatch")
	}
	wantVerdict := "pass"
	if failed > 0 {
		wantVerdict = "fail"
	} else if blocked > 0 {
		wantVerdict = "blocked-infra"
	}
	if summary["verdict"] != wantVerdict {
		return errors.New("report verdict mismatch")
	}
	return nil
}
