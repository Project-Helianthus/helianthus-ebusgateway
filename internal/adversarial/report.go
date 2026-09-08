package adversarial

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func ValidateReport(report ReportV1) error { return ValidateRuntimeReportV1(report) }

func ValidateReportBytes(raw []byte) error {
	_, err := ParseReportV1(raw)
	return err
}

// WriteReport validates before creating any destination and atomically replaces
// the file only after a complete write and fsync.
func WriteReport(report ReportV1, path string) error {
	if err := ValidateRuntimeReportV1(report); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	raw = append(raw, '\n')
	if err := ValidateReportBytes(raw); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}
	f, err := os.CreateTemp(dir, ".adversarial-report-*")
	if err != nil {
		return fmt.Errorf("create report temp: %w", err)
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if err := f.Chmod(0o644); err != nil {
		_ = f.Close()
		return fmt.Errorf("chmod report temp: %w", err)
	}
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		return fmt.Errorf("write report temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync report temp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close report temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace report: %w", err)
	}
	return nil
}
