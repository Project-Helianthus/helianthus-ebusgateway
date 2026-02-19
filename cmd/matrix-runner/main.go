package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway/internal/matrix"
)

func main() {
	var (
		outputDir    = flag.String("output-dir", "results", "matrix output directory")
		target       = flag.String("target", "local", "execution target: local|ha-addon")
		includeIDs   = flag.String("cases", "", "comma-separated case IDs (default: all T01..T42)")
		execute      = flag.Bool("execute", false, "execute commands instead of dry-run planning")
		settleDelay  = flag.Duration("settle-delay", 3*time.Second, "delay after service startup")
		caseTimeout  = flag.Duration("case-timeout", 2*time.Minute, "per-case timeout (0 disables timeout)")
		startGateway = flag.String("start-gateway", "", "shell command to start gateway service")
		stopGateway  = flag.String("stop-gateway", "", "shell command to stop gateway service")
		startProxy   = flag.String("start-proxy", "", "shell command to start proxy service")
		stopProxy    = flag.String("stop-proxy", "", "shell command to stop proxy service")
		startEbusd   = flag.String("start-ebusd", "", "shell command to start ebusd service")
		stopEbusd    = flag.String("stop-ebusd", "", "shell command to stop ebusd service")
		smokeCommand = flag.String("smoke-command", "", "shell command that executes smoke validation")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runner, err := matrix.NewRunner(matrix.RunnerOptions{
		OutputDir:    *outputDir,
		Target:       *target,
		IncludeIDs:   splitCSV(*includeIDs),
		Execute:      *execute,
		SettleDelay:  *settleDelay,
		CaseTimeout:  *caseTimeout,
		StartGateway: *startGateway,
		StopGateway:  *stopGateway,
		StartProxy:   *startProxy,
		StopProxy:    *stopProxy,
		StartEbusd:   *startEbusd,
		StopEbusd:    *stopEbusd,
		SmokeCommand: *smokeCommand,
	})
	if err != nil {
		log.Fatalf("matrix runner options: %v", err)
	}

	verdicts, err := runner.Run(ctx)
	if err != nil {
		log.Fatalf("matrix run failed after %d case(s): %v", len(verdicts), err)
	}

	passed, failed, planned := 0, 0, 0
	for _, verdict := range verdicts {
		switch verdict.Status {
		case "passed":
			passed++
		case "failed":
			failed++
		default:
			planned++
		}
	}

	fmt.Printf(
		"matrix complete: total=%d passed=%d failed=%d planned=%d output=%s\n",
		len(verdicts),
		passed,
		failed,
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
