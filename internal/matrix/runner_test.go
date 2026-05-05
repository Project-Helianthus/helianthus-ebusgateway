package matrix

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunnerDryRunWritesMatrixArtifacts(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runner, err := NewRunner(RunnerOptions{
		OutputDir: filepath.Join(tempDir, "results"),
		Target:    "local",
		Execute:   false,
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}

	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(verdicts) != 88 {
		t.Fatalf("len(verdicts) = %d; want 88", len(verdicts))
	}
	var verdictT07 *CaseVerdict
	for index := range verdicts {
		if verdicts[index].CaseID == "T07" {
			verdictT07 = &verdicts[index]
			break
		}
	}
	if verdictT07 == nil {
		t.Fatalf("T07 verdict missing")
	}
	if verdictT07.Expected != "fail" {
		t.Fatalf("T07 expected = %q; want fail", verdictT07.Expected)
	}
	if verdictT07.Expectation == "" {
		t.Fatalf("T07 expectation should not be empty")
	}

	checkPath := filepath.Join(tempDir, "results", "T01", "configs", "helianthus.json")
	if _, err := os.Stat(checkPath); err != nil {
		t.Fatalf("stat %s: %v", checkPath, err)
	}
	checkPath = filepath.Join(tempDir, "results", "T09", "configs", "proxy.json")
	if _, err := os.Stat(checkPath); err != nil {
		t.Fatalf("stat %s: %v", checkPath, err)
	}
	checkPath = filepath.Join(tempDir, "results", "T05", "configs", "ebusd.json")
	if _, err := os.Stat(checkPath); err != nil {
		t.Fatalf("stat %s: %v", checkPath, err)
	}
	checkPath = filepath.Join(tempDir, "results", "T26", "verdict.json")
	data, err := os.ReadFile(checkPath)
	if err != nil {
		t.Fatalf("read verdict %s: %v", checkPath, err)
	}

	var verdict CaseVerdict
	if err := json.Unmarshal(data, &verdict); err != nil {
		t.Fatalf("decode verdict: %v", err)
	}
	if verdict.Status != "planned" {
		t.Fatalf("verdict status = %q; want planned", verdict.Status)
	}
	if verdict.Outcome != "planned" {
		t.Fatalf("verdict outcome = %q; want planned", verdict.Outcome)
	}
	if verdict.Expected != "pass" {
		t.Fatalf("verdict expected = %q; want pass", verdict.Expected)
	}
	if verdict.Suite != SuiteFullMatrix {
		t.Fatalf("verdict suite = %q; want %q", verdict.Suite, SuiteFullMatrix)
	}
}

func TestRunnerDryRunWritesPassiveSuiteArtifacts(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runner, err := NewRunner(RunnerOptions{
		OutputDir: filepath.Join(tempDir, "results"),
		Target:    "local",
		Suite:     SuitePassive,
		Execute:   false,
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}

	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(verdicts) != 6 {
		t.Fatalf("len(verdicts) = %d; want 6", len(verdicts))
	}
	if verdicts[0].CaseID != "P01" || verdicts[len(verdicts)-1].CaseID != "P06" {
		t.Fatalf("passive verdict ids = first %q last %q; want P01/P06", verdicts[0].CaseID, verdicts[len(verdicts)-1].CaseID)
	}
	if verdicts[len(verdicts)-1].PassiveMode != "unsupported_or_misconfigured" {
		t.Fatalf("P06 passive mode = %q; want unsupported_or_misconfigured", verdicts[len(verdicts)-1].PassiveMode)
	}

	checkPath := filepath.Join(tempDir, "results", "P01", "configs", "helianthus.json")
	data, err := os.ReadFile(checkPath)
	if err != nil {
		t.Fatalf("read %s: %v", checkPath, err)
	}

	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatalf("decode helianthus config: %v", err)
	}
	if got := config["suite"]; got != SuitePassive {
		t.Fatalf("config suite = %#v; want %q", got, SuitePassive)
	}
	if got := config["passive_mode"]; got != "unsupported_or_misconfigured" {
		t.Fatalf("config passive_mode = %#v; want unsupported_or_misconfigured", got)
	}
}

func TestRunnerExecuteCapturesFailure(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runner, err := NewRunner(RunnerOptions{
		OutputDir:    filepath.Join(tempDir, "results"),
		Target:       "local",
		IncludeIDs:   []string{"T01"},
		Execute:      true,
		StartGateway: "echo gateway-start",
		StopGateway:  "echo gateway-stop",
		SmokeCommand: "exit 7",
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}

	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(verdicts) != 1 {
		t.Fatalf("len(verdicts) = %d; want 1", len(verdicts))
	}

	verdict := verdicts[0]
	if verdict.Status != "failed" {
		t.Fatalf("verdict status = %q; want failed", verdict.Status)
	}
	if verdict.Outcome != "fail" {
		t.Fatalf("verdict outcome = %q; want fail", verdict.Outcome)
	}
	if verdict.InfraReason != "" {
		t.Fatalf("verdict infra_reason = %q; want empty", verdict.InfraReason)
	}
	if verdict.Error == "" {
		t.Fatalf("verdict error empty")
	}

	smokeLogged := false
	for _, command := range verdict.Commands {
		if command.Name == "smoke" {
			smokeLogged = true
			if command.Status != "failed" {
				t.Fatalf("smoke status = %q; want failed", command.Status)
			}
			if command.ExitCode != 7 {
				t.Fatalf("smoke exit code = %d; want 7", command.ExitCode)
			}
		}
	}
	if !smokeLogged {
		t.Fatalf("smoke command result missing")
	}

	logPath := filepath.Join(tempDir, "results", "T01", "logs", "runner.log")
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("expected runner.log, stat error: %v", err)
	}
}

func TestRunnerExecuteClassifiesBlockedInfra(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runner, err := NewRunner(RunnerOptions{
		OutputDir:    filepath.Join(tempDir, "results"),
		Target:       "local",
		IncludeIDs:   []string{"T01"},
		Execute:      true,
		StartGateway: "printf 'adapter preflight failed: eBUS initial signal is not acquired\\n'; exit 1",
		StopGateway:  "echo gateway-stop",
		SmokeCommand: "echo smoke-ok",
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}

	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(verdicts) != 1 {
		t.Fatalf("len(verdicts) = %d; want 1", len(verdicts))
	}

	verdict := verdicts[0]
	if verdict.Status != "failed" {
		t.Fatalf("verdict status = %q; want failed", verdict.Status)
	}
	if verdict.Outcome != "blocked-infra" {
		t.Fatalf("verdict outcome = %q; want blocked-infra", verdict.Outcome)
	}
	if verdict.InfraReason != "adapter_no_signal" {
		t.Fatalf("verdict infra_reason = %q; want adapter_no_signal", verdict.InfraReason)
	}
}

func TestRunnerExecuteClassifiesExpectedFailure(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runner, err := NewRunner(RunnerOptions{
		OutputDir:        filepath.Join(tempDir, "results"),
		Target:           "local",
		IncludeIDs:       []string{"T01"},
		ExpectedFailures: map[string]string{"t01": "source selection unavailable via ebusd-tcp"},
		Execute:          true,
		StartGateway:     "echo gateway-start",
		StopGateway:      "echo gateway-stop",
		SmokeCommand:     "exit 9",
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}

	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(verdicts) != 1 {
		t.Fatalf("len(verdicts) = %d; want 1", len(verdicts))
	}

	verdict := verdicts[0]
	if verdict.Status != "failed" {
		t.Fatalf("verdict status = %q; want failed", verdict.Status)
	}
	if verdict.Outcome != "xfail" {
		t.Fatalf("verdict outcome = %q; want xfail", verdict.Outcome)
	}
	if verdict.Expected != "fail" {
		t.Fatalf("verdict expected = %q; want fail", verdict.Expected)
	}
	if verdict.Expectation != "source selection unavailable via ebusd-tcp" {
		t.Fatalf("verdict expectation = %q", verdict.Expectation)
	}
}

func TestRunnerExecuteClassifiesUnexpectedPassForExpectedFailure(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runner, err := NewRunner(RunnerOptions{
		OutputDir:        filepath.Join(tempDir, "results"),
		Target:           "local",
		IncludeIDs:       []string{"T01"},
		ExpectedFailures: map[string]string{"T01": "known limitation"},
		Execute:          true,
		StartGateway:     "echo gateway-start",
		StopGateway:      "echo gateway-stop",
		SmokeCommand:     "echo smoke-ok",
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}

	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(verdicts) != 1 {
		t.Fatalf("len(verdicts) = %d; want 1", len(verdicts))
	}

	verdict := verdicts[0]
	if verdict.Status != "passed" {
		t.Fatalf("verdict status = %q; want passed", verdict.Status)
	}
	if verdict.Outcome != "xpass" {
		t.Fatalf("verdict outcome = %q; want xpass", verdict.Outcome)
	}
	if verdict.Expected != "fail" {
		t.Fatalf("verdict expected = %q; want fail", verdict.Expected)
	}
}

func TestRunnerRunsCleanupAfterCaseTimeout(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runner, err := NewRunner(RunnerOptions{
		OutputDir:    filepath.Join(tempDir, "results"),
		Target:       "local",
		IncludeIDs:   []string{"T01"},
		Execute:      true,
		CaseTimeout:  100 * time.Millisecond,
		StartGateway: "echo gateway-start",
		StopGateway:  "echo gateway-stop",
		SmokeCommand: "sleep 2",
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}

	verdicts, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if len(verdicts) != 1 {
		t.Fatalf("len(verdicts) = %d; want 1", len(verdicts))
	}

	verdict := verdicts[0]
	if verdict.Status != "failed" {
		t.Fatalf("verdict status = %q; want failed", verdict.Status)
	}
	foundStop := false
	for _, command := range verdict.Commands {
		if command.Name != "gateway-stop" {
			continue
		}
		foundStop = true
		if command.Status != "passed" {
			t.Fatalf("gateway-stop status = %q; want passed", command.Status)
		}
	}
	if !foundStop {
		t.Fatalf("gateway-stop command result missing")
	}
}

func TestDefaultExpectedFailureCoverage(t *testing.T) {
	t.Parallel()

	cases := GenerateTopologyCases()
	caseByID := make(map[string]TopologyCase, len(cases))
	for _, testCase := range cases {
		caseByID[testCase.ID] = testCase
	}

	checkReason := func(caseID, want string) {
		t.Helper()
		testCase, found := caseByID[caseID]
		if !found {
			t.Fatalf("missing case %s", caseID)
		}
		got := defaultExpectedFailure(testCase)
		if got != want {
			t.Fatalf("defaultExpectedFailure(%s) = %q; want %q", caseID, got, want)
		}
	}

	checkReason("T05", "")
	checkReason("T07", "ebusd direct udp/tcp to adapter reports no signal in matrix runs")
	checkReason("T27", "proxy dual-client with ebusd northbound udp reports no signal")
	checkReason("T33", "proxy dual-client with southbound udp reports no signal (ens/enh clients also show host comm framing)")
	checkReason("T37", "proxy dual-client with southbound tcp reports no signal (ens/enh clients also show host comm framing)")
	checkReason("T26", "")

	defaultFailCount := 0
	for _, testCase := range cases {
		if defaultExpectedFailure(testCase) != "" {
			defaultFailCount++
		}
	}
	if defaultFailCount != 42 {
		t.Fatalf("default expected-failure count = %d; want 42", defaultFailCount)
	}
}
