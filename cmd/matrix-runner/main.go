package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/matrix"
)

func main() {
	var (
		outputDir      = flag.String("output-dir", "results", "matrix output directory")
		target         = flag.String("target", "local", "execution target: local|ha-addon")
		includeIDs     = flag.String("cases", "", "comma-separated case IDs (default: all T01..T88)")
		execute        = flag.Bool("execute", false, "execute commands instead of dry-run planning")
		settleDelay    = flag.Duration("settle-delay", 3*time.Second, "delay after service startup")
		caseTimeout    = flag.Duration("case-timeout", 2*time.Minute, "per-case timeout (0 disables timeout)")
		startGateway   = flag.String("start-gateway", "", "shell command to start gateway service")
		stopGateway    = flag.String("stop-gateway", "", "shell command to stop gateway service")
		startProxy     = flag.String("start-proxy", "", "shell command to start proxy service")
		stopProxy      = flag.String("stop-proxy", "", "shell command to stop proxy service")
		startEbusd     = flag.String("start-ebusd", "", "shell command to start ebusd service")
		stopEbusd      = flag.String("stop-ebusd", "", "shell command to stop ebusd service")
		smokeCommand   = flag.String("smoke-command", "", "shell command that executes smoke validation")
		expectedFail   = flag.String("expected-failures", "", "comma-separated case IDs expected to fail")
		expectedFile   = flag.String("expected-failures-file", "", "path to JSON object map: {\"T07\":\"reason\"}")
		expectedReason = flag.String("expected-failure-reason", "known limitation", "fallback reason for --expected-failures entries")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	expectedFailures, err := collectExpectedFailures(*expectedFail, *expectedFile, *expectedReason)
	if err != nil {
		log.Fatalf("matrix expected failures: %v", err)
	}

	runner, err := matrix.NewRunner(matrix.RunnerOptions{
		OutputDir:        *outputDir,
		Target:           *target,
		IncludeIDs:       splitCSV(*includeIDs),
		ExpectedFailures: expectedFailures,
		Execute:          *execute,
		SettleDelay:      *settleDelay,
		CaseTimeout:      *caseTimeout,
		StartGateway:     *startGateway,
		StopGateway:      *stopGateway,
		StartProxy:       *startProxy,
		StopProxy:        *stopProxy,
		StartEbusd:       *startEbusd,
		StopEbusd:        *stopEbusd,
		SmokeCommand:     *smokeCommand,
	})
	if err != nil {
		log.Fatalf("matrix runner options: %v", err)
	}

	verdicts, err := runner.Run(ctx)
	if err != nil {
		log.Fatalf("matrix run failed after %d case(s): %v", len(verdicts), err)
	}

	passed, failed, planned, xfailed, xpassed, blocked := 0, 0, 0, 0, 0, 0
	for _, verdict := range verdicts {
		switch verdict.Outcome {
		case "pass":
			passed++
		case "xfail":
			xfailed++
		case "xpass":
			xpassed++
		case "blocked-infra":
			blocked++
		case "fail":
			failed++
		default:
			planned++
		}
	}

	fmt.Printf(
		"matrix complete: total=%d pass=%d fail=%d xfail=%d xpass=%d blocked=%d planned=%d output=%s\n",
		len(verdicts),
		passed,
		failed,
		xfailed,
		xpassed,
		blocked,
		planned,
		*outputDir,
	)
}

func splitCSV(value string) []string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	parts := strings.Split(trimmed, ",")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func collectExpectedFailures(idsCSV string, jsonPath string, fallbackReason string) (map[string]string, error) {
	entries := make(map[string]string)
	reason := strings.TrimSpace(fallbackReason)
	if reason == "" {
		reason = "known limitation"
	}

	for _, caseID := range splitCSV(idsCSV) {
		entries[strings.ToUpper(caseID)] = reason
	}

	path := strings.TrimSpace(jsonPath)
	if path == "" {
		if len(entries) == 0 {
			return nil, nil
		}
		return entries, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read expected-failures-file %q: %w", path, err)
	}
	var fromFile map[string]string
	if err := json.Unmarshal(data, &fromFile); err != nil {
		return nil, fmt.Errorf("decode expected-failures-file %q: %w", path, err)
	}
	for caseID, entryReason := range fromFile {
		trimmedID := strings.TrimSpace(strings.ToUpper(caseID))
		if trimmedID == "" {
			continue
		}
		trimmedReason := strings.TrimSpace(entryReason)
		if trimmedReason == "" {
			trimmedReason = reason
		}
		entries[trimmedID] = trimmedReason
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return entries, nil
}
