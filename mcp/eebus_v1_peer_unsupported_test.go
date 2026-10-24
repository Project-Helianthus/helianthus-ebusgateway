//go:build !linux

package mcp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestIssue743UnsupportedPlatformOperatorStartupFailsClosed(t *testing.T) {
	server, _ := issue743Server(t)
	socketPath := EEBusV1OperatorSocketPath(filepath.Join(t.TempDir(), "eebus"))
	closer, err := server.StartEEBusV1OperatorEndpoint(context.Background(), socketPath)
	if closer != nil {
		_ = closer.Close()
		t.Fatal("unsupported platform returned an operator endpoint")
	}
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unsupported platform startup error = %v", err)
	}
}
