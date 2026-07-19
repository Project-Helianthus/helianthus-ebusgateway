package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
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
