package matrix

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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
	if len(verdicts) != 42 {
		t.Fatalf("len(verdicts) = %d; want 42", len(verdicts))
	}

	checkPath := filepath.Join(tempDir, "results", "T01", "configs", "helianthus.json")
	if _, err := os.Stat(checkPath); err != nil {
		t.Fatalf("stat %s: %v", checkPath, err)
	}
	checkPath = filepath.Join(tempDir, "results", "T07", "configs", "proxy.json")
	if _, err := os.Stat(checkPath); err != nil {
		t.Fatalf("stat %s: %v", checkPath, err)
	}
	checkPath = filepath.Join(tempDir, "results", "T04", "configs", "ebusd.json")
	if _, err := os.Stat(checkPath); err != nil {
		t.Fatalf("stat %s: %v", checkPath, err)
	}
	checkPath = filepath.Join(tempDir, "results", "T42", "verdict.json")
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
