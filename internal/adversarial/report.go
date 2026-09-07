package adversarial

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

const maximumReportBytes = 1024 * 1024

// ValidateReportBytes rejects oversized, malformed, duplicate-key-prone JSON
// before it reaches the report guard. The decoder uses json.Number so counters
// cannot silently pass through float conversion.
func ValidateReportBytes(data []byte) error {
	if len(data) > maximumReportBytes {
		return errors.New("report exceeds 1 MiB")
	}
	if err := uniqueJSONKeys(data); err != nil {
		return err
	}
	temp, err := os.CreateTemp("", "helianthus-adversarial-report-*.json")
	if err != nil {
		return errors.New("report validation unavailable")
	}
	path := temp.Name()
	defer os.Remove(path)
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return errors.New("report validation unavailable")
	}
	if err := temp.Close(); err != nil {
		return errors.New("report validation unavailable")
	}
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("report validation unavailable")
	}
	root := filepath.Join(filepath.Dir(source), "contract")
	command := exec.Command("python3", filepath.Join(root, "scripts", "validate_adversarial_runtime_report_v1.py"), path)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil || !bytes.Contains(output, []byte("adversarial_runtime_report_v1_ok")) {
		return errors.New("report violates adversarial runtime v1 contract")
	}
	return nil
}

func decodedObject(data []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value map[string]any
	err := decoder.Decode(&value)
	return value, err
}
func uniqueJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeJSONValue(decoder); err != nil {
		return fmt.Errorf("invalid report JSON: %w", err)
	}
	if decoder.More() {
		return errors.New("report has trailing value")
	}
	return nil
}
func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("object key invalid")
			}
			if seen[name] {
				return fmt.Errorf("duplicate key %q", name)
			}
			seen[name] = true
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	return nil
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
	if err := ValidateReportBytes(data); err != nil {
		return fmt.Errorf("invalid adversarial report: %w", err)
	}
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
	data, err := json.Marshal(report)
	if err != nil {
		return errors.New("report is invalid")
	}
	return ValidateReportBytes(data)
}
