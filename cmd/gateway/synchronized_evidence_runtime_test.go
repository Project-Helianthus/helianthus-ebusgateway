package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

const synchronizedEvidenceTestVersion = "1.0.0+git.61bc62fa1d08dcf7b677c3dc08855beb5c68ceb1"

func enabledSynchronizedEvidenceConfig(t *testing.T) ebusgateway.EvidenceRecorderConfig {
	t.Helper()
	config := ebusgateway.DefaultEvidenceRecorderConfig()
	physicalTemp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config.Enabled = true
	config.Scope = ebusgateway.EvidenceRecorderScopeV1
	config.StateRoot = filepath.Join(physicalTemp, "evidence")
	config.Retention = ebusgateway.DefaultEvidenceRecorderRetention
	config.QuotaBytes = ebusgateway.DefaultEvidenceRecorderQuotaBytes
	config.Limits = ebusgateway.DefaultEvidenceRecorderLimits()
	return config
}

func TestMSP065SynchronizedEvidenceRuntimeDisabledIsInert(t *testing.T) {
	runtime, err := openSynchronizedEvidenceRuntime(ebusgateway.DefaultEvidenceRecorderConfig())
	if err != nil {
		t.Fatalf("openSynchronizedEvidenceRuntime() error = %v", err)
	}
	if runtime != nil {
		t.Fatalf("disabled runtime = %#v, want nil", runtime)
	}
}

func TestMSP065SynchronizedEvidenceRuntimeReservesCapturesAndPersistsReplayableBundle(t *testing.T) {
	config := enabledSynchronizedEvidenceConfig(t)
	runtime, err := openSynchronizedEvidenceRuntime(config)
	if err != nil {
		t.Fatalf("openSynchronizedEvidenceRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.Configure(nil, synchronizedEvidenceTestVersion, newSynchronizedEvidenceClock(), bytes.NewReader(bytes.Repeat([]byte{0x42}, 1<<20))); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	id, bundle, err := runtime.Capture(context.Background(), syncevidence.ActionMarker{
		EvidenceRef: contentEvidenceRef(strings.Repeat("1", 64)),
	})
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if !strings.HasPrefix(id, "sebv1:sha256:") {
		t.Fatalf("bundle id = %q", id)
	}
	if _, err := syncevidence.Replay(bundle); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	var decoded struct {
		Artifacts []json.RawMessage `json:"artifacts"`
		Sources   []struct {
			State         string  `json:"state"`
			ErrorCategory *string `json:"error_category"`
		} `json:"sources"`
	}
	if err := json.Unmarshal(bundle, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Artifacts) != 0 || len(decoded.Sources) != 4 {
		t.Fatalf("artifacts/sources = %d/%d, want 0/4", len(decoded.Artifacts), len(decoded.Sources))
	}
	for _, source := range decoded.Sources {
		if source.State == "PRESENT" || source.ErrorCategory == nil {
			t.Fatalf("unexpected terminal source = %#v", source)
		}
	}
	info, err := os.Stat(filepath.Join(config.StateRoot, id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle mode = %o, want 600", info.Mode().Perm())
	}
}

func TestMSP065EvidenceStoreLockFailsBeforeTransportDial(t *testing.T) {
	config := enabledSynchronizedEvidenceConfig(t)
	first, err := openSynchronizedEvidenceRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	dialed := false
	gatewayConfig := ebusgateway.DefaultConfig()
	gatewayConfig.EvidenceRecorderConfig = config
	gatewayConfig.TransportConfig.Network = "tcp"
	gatewayConfig.TransportConfig.Address = "127.0.0.1:9999"
	gatewayConfig.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		dialed = true
		return nil, errors.New("unexpected dial")
	}
	if err := run(context.Background(), gatewayConfig); !errors.Is(err, syncevidence.ErrStoreLocked) {
		t.Fatalf("run() error = %v, want %v", err, syncevidence.ErrStoreLocked)
	}
	if dialed {
		t.Fatal("transport dialed before evidence store lock failure")
	}
}

func TestIssue764DuplicateEvidenceStoreOwnersFailBeforeOpenOrDial(t *testing.T) {
	config := enabledSynchronizedEvidenceConfig(t)
	gatewayConfig := ebusgateway.DefaultConfig()
	gatewayConfig.EvidenceRecorderConfig = config
	gatewayConfig.EvidenceOneShotEnabled = true
	dialed := false
	gatewayConfig.TransportConfig.Network = "tcp"
	gatewayConfig.TransportConfig.Address = "127.0.0.1:9999"
	gatewayConfig.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		dialed = true
		return nil, errors.New("unexpected dial")
	}

	if err := run(context.Background(), gatewayConfig); !errors.Is(err, ebusgateway.ErrSynchronizedEvidenceOwnership) {
		t.Fatalf("run() error = %v, want %v", err, ebusgateway.ErrSynchronizedEvidenceOwnership)
	}
	if dialed {
		t.Fatal("transport dialed before duplicate ownership was rejected")
	}
	if _, err := os.Stat(config.StateRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate ownership opened store: %v", err)
	}
}

func TestMSP065SynchronizedEvidenceRuntimeRejectsSecondConfigurationAndClosesOnce(t *testing.T) {
	config := enabledSynchronizedEvidenceConfig(t)
	runtime, err := openSynchronizedEvidenceRuntime(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Configure(nil, synchronizedEvidenceTestVersion, newSynchronizedEvidenceClock(), bytes.NewReader(bytes.Repeat([]byte{0x55}, 1<<20))); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Configure(nil, synchronizedEvidenceTestVersion, newSynchronizedEvidenceClock(), bytes.NewReader(bytes.Repeat([]byte{0x66}, 1<<20))); err == nil {
		t.Fatal("second Configure() succeeded")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestIssue764GatewayOneShotRuntimeIsFixedReadOnlyAndSerialized(t *testing.T) {
	eebusRuntime := &msp06GatewayRuntime{}
	adapter := &eebusRuntimeAdapter{runtime: eebusRuntime}
	runtime, err := newSynchronizedEvidenceOneShotRuntime(
		true,
		eebusMCPProvider(adapter),
		eebusMCPCommandRouter(adapter),
		gatewayBuildInfo{ReleaseVersion: "test", BuildID: "test-build"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil || runtime.root != synchronizedEvidenceOneShotRoot || runtime.reader == nil {
		t.Fatalf("one-shot runtime = %#v", runtime)
	}
	if _, ok := any(runtime).(mcp.SynchronizedEvidenceCapture); !ok {
		t.Fatal("one-shot runtime does not implement the private MCP owner seam")
	}

	var stateMu sync.Mutex
	active, maximum, calls := 0, 0, 0
	runtime.execute = func(_ context.Context, options syncevidence.OneShotExecutionOptions) syncevidence.OneShotReceiptV1 {
		if options.Root != synchronizedEvidenceOneShotRoot || options.Reader == nil ||
			options.ClockFactory == nil || options.BuildIdentity == nil {
			t.Fatalf("execution options = %#v", options)
		}
		stateMu.Lock()
		active++
		calls++
		if active > maximum {
			maximum = active
		}
		stateMu.Unlock()
		time.Sleep(20 * time.Millisecond)
		stateMu.Lock()
		active--
		stateMu.Unlock()
		return syncevidence.OneShotReceiptV1{Category: syncevidence.OneShotExisting}
	}
	var wait sync.WaitGroup
	wait.Add(2)
	for range 2 {
		go func() {
			defer wait.Done()
			if receipt := runtime.CaptureSynchronizedEvidence(context.Background()); receipt.Category != syncevidence.OneShotExisting {
				t.Errorf("receipt = %#v", receipt)
			}
		}()
	}
	wait.Wait()
	if calls != 2 || maximum != 1 {
		t.Fatalf("calls/maximum concurrency = %d/%d", calls, maximum)
	}

	if absent, err := newSynchronizedEvidenceOneShotRuntime(true, nil, nil, gatewayBuildInfo{ReleaseVersion: "test", BuildID: "test-build"}); err != nil || absent != nil {
		t.Fatalf("disabled one-shot runtime = %#v, %v", absent, err)
	}
	if absent, err := newSynchronizedEvidenceOneShotRuntime(
		false,
		eebusMCPProvider(adapter),
		eebusMCPCommandRouter(adapter),
		gatewayBuildInfo{ReleaseVersion: "test", BuildID: "test-build"},
	); err != nil || absent != nil {
		t.Fatalf("explicitly disabled one-shot runtime = %#v, %v", absent, err)
	}
}

// This AST check is retained because exact-once registration and ordering are
// process-bootstrap invariants that cannot be exercised without constructing
// the unrelated eBUS transport lifecycle.
func TestIssue764GatewayRegistersPrivateCaptureExactlyOnceBeforeMCPMount(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var registration token.Pos
	var mount token.Pos
	count := 0
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if selector.Sel.Name == "RegisterSynchronizedEvidenceCapture" {
			count++
			registration = call.Pos()
		}
		if selector.Sel.Name == "Handle" && len(call.Args) > 0 {
			if path, ok := call.Args[0].(*ast.SelectorExpr); ok && path.Sel.Name == "MCPPath" {
				mount = call.Pos()
			}
		}
		return true
	})
	if count != 1 || !registration.IsValid() || !mount.IsValid() || registration >= mount {
		t.Fatalf("private registration count/positions = %d/%d/%d", count, registration, mount)
	}
}
