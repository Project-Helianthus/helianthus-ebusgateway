package main

import (
	"context"
	"flag"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

func TestIssue764SynchronizedEvidenceOneShotFlagIsExplicitOptIn(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("synchronized-evidence-one-shot-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-synchronized-evidence-one-shot-enabled=true"}); err != nil {
		t.Fatal(err)
	}
	if !cfg.EvidenceOneShotEnabled {
		t.Fatal("one-shot control was not enabled")
	}
	if cfg.EvidenceRecorderConfig != ebusgateway.DefaultEvidenceRecorderConfig() {
		t.Fatalf("generic recorder config changed: %#v", cfg.EvidenceRecorderConfig)
	}
	if err := ebusgateway.ValidateSynchronizedEvidenceConfig(cfg); err != nil {
		t.Fatalf("parsed one-shot config is invalid: %v", err)
	}
	adapter := &eebusRuntimeAdapter{runtime: &msp06GatewayRuntime{}}
	runtime, err := newSynchronizedEvidenceOneShotRuntime(
		cfg.EvidenceOneShotEnabled,
		eebusMCPProvider(adapter),
		eebusMCPCommandRouter(adapter),
		gatewayBuildInfo{ReleaseVersion: "test", BuildID: "test-build"},
	)
	if err != nil || runtime == nil {
		t.Fatalf("construct enabled one-shot runtime = %#v, %v", runtime, err)
	}
	runtime.execute = func(_ context.Context, options syncevidence.OneShotExecutionOptions) syncevidence.OneShotReceiptV1 {
		if options.Root != synchronizedEvidenceOneShotRoot {
			t.Fatalf("one-shot root = %q", options.Root)
		}
		return syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotExisting}
	}
	if receipt := runtime.CaptureSynchronizedEvidence(context.Background()); receipt.Category != syncevidence.OneShotExisting {
		t.Fatalf("capture receipt = %#v", receipt)
	}
}

func TestIssue764SynchronizedEvidenceOneShotDefaultIsInert(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("synchronized-evidence-one-shot-default-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if cfg.EvidenceOneShotEnabled {
		t.Fatal("one-shot control enabled by default")
	}
	if cfg.EvidenceRecorderConfig != ebusgateway.DefaultEvidenceRecorderConfig() {
		t.Fatalf("default generic recorder config changed: %#v", cfg.EvidenceRecorderConfig)
	}
	adapter := &eebusRuntimeAdapter{runtime: &msp06GatewayRuntime{}}
	runtime, err := newSynchronizedEvidenceOneShotRuntime(
		cfg.EvidenceOneShotEnabled,
		eebusMCPProvider(adapter),
		eebusMCPCommandRouter(adapter),
		gatewayBuildInfo{ReleaseVersion: "test", BuildID: "test-build"},
	)
	if err != nil || runtime != nil {
		t.Fatalf("default one-shot runtime = %#v, %v", runtime, err)
	}
}
