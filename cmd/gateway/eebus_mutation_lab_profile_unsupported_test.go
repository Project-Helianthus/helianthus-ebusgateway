//go:build !linux

package main

import (
	"context"
	"testing"

	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
)

func TestIssue755NonLinuxPresentProfileFailsClosedBeforeFactory(t *testing.T) {
	root := t.TempDir()
	issue755WriteProfile(t, root, issue755ValidMutationLabProfileJSON(t))
	runtime := &msp05bRuntime{}
	factoryCalls := 0

	adapter, err := startEEBusRuntime(
		context.Background(),
		issue755EnabledConfig(root),
		issue755Resolver,
		func(eebusruntime.Config) (eebusruntime.Runtime, error) {
			factoryCalls++
			return runtime, nil
		},
	)
	if adapter != nil || err == nil {
		t.Fatalf("non-Linux present profile startup = (%v, %v); want nil adapter and error", adapter, err)
	}
	if factoryCalls != 0 || runtime.startCalls != 0 {
		t.Fatalf("calls factory=%d start=%d; want zero", factoryCalls, runtime.startCalls)
	}
}
