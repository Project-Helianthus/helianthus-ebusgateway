package adversarial

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func fixture(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "fixtures", path))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func runner() Executor { return Executor{StartedAt: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)} }

func TestExecutor_OfflineAllPassMatchesPublishedFixture(t *testing.T) {
	actual, err := runner().Run(fixture(t, "inputs/offline-all-pass.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReport(actual); err != nil {
		t.Fatalf("ValidateReport: %v", err)
	}
	var expected map[string]any
	if err := json.Unmarshal(fixture(t, "positive/offline-all-pass.json"), &expected); err != nil {
		t.Fatal(err)
	}
	actualJSON, _ := json.Marshal(actual)
	var normalized map[string]any
	_ = json.Unmarshal(actualJSON, &normalized)
	if !reflect.DeepEqual(normalized, expected) {
		t.Fatalf("offline projection differs from published report fixture")
	}
}

func TestExecutor_EvaluatesThresholdFailure(t *testing.T) {
	report, err := runner().Run(fixture(t, "inputs/evaluated-fail.json"))
	if err != nil {
		t.Fatal(err)
	}
	summary := report["summary"].(map[string]any)
	if summary["verdict"] != "fail" || summary["failed"] != int64(1) {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestExecutor_ExecutionErrorPrecedesEvaluation(t *testing.T) {
	report, err := runner().Run(fixture(t, "inputs/execution-error.json"))
	if err != nil {
		t.Fatal(err)
	}
	first := report["scenarios"].([]any)[0].(map[string]any)
	if first["result_kind"] != "execution-error" || first["evaluation"] != nil || first["outcome"] != "fail" {
		t.Fatalf("first = %#v", first)
	}
}

func TestExecutor_InfrastructureBlockIsPreAction(t *testing.T) {
	report, err := runner().Run(fixture(t, "inputs/infrastructure-block.json"))
	if err != nil {
		t.Fatal(err)
	}
	first := report["scenarios"].([]any)[0].(map[string]any)
	if first["result_kind"] != "infrastructure-block" || len(first["action"].(map[string]any)["events"].([]any)) != 0 {
		t.Fatalf("first = %#v", first)
	}
}

func TestExecutor_RejectsWrongCatalogBeforeAction(t *testing.T) {
	var driver map[string]any
	if err := json.Unmarshal(fixture(t, "inputs/offline-all-pass.json"), &driver); err != nil {
		t.Fatal(err)
	}
	driver["scenarios"].([]any)[0].(map[string]any)["trigger_kind"] = "live-command"
	b, _ := json.Marshal(driver)
	if _, err := runner().Run(b); err == nil {
		t.Fatal("Run accepted a non-catalog trigger")
	}
}

func TestExecutor_ConcurrentRunsAreIndependent(t *testing.T) {
	input := fixture(t, "inputs/offline-all-pass.json")
	var wg sync.WaitGroup
	errors := make(chan error, 24)
	for i := 0; i < cap(errors); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report, err := runner().Run(input)
			if err == nil {
				err = ValidateReport(report)
			}
			errors <- err
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestWriteReport_IsAtomicAndRejectsInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "report.json")
	if err := WriteReport(map[string]any{}, path); err == nil {
		t.Fatal("WriteReport accepted invalid artifact")
	}
	report, err := runner().Run(fixture(t, "inputs/offline-all-pass.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteReport(report, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		t.Fatalf("report = %q, err=%v", data, err)
	}
	if err := ValidateReportBytes(data); err != nil {
		t.Fatalf("ValidateReportBytes: %v", err)
	}
}

func TestValidateReportBytes_FailsClosedOnForgedSummaryAndOversize(t *testing.T) {
	report, err := runner().Run(fixture(t, "inputs/offline-all-pass.json"))
	if err != nil {
		t.Fatal(err)
	}
	report["summary"].(map[string]any)["verdict"] = "fail"
	payload, _ := json.Marshal(report)
	if err := ValidateReportBytes(payload); err == nil {
		t.Fatal("forged summary accepted")
	}
	if err := ValidateReportBytes(make([]byte, maximumReportBytes+1)); err == nil {
		t.Fatal("oversized report accepted")
	}
}
