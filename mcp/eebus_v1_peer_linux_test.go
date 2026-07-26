//go:build linux

package mcp

import (
	"context"
	"path/filepath"
	"testing"
)

func TestIssue743LinuxPlatformPeerCredentialResolverAdmitsSameEffectiveUID(t *testing.T) {
	server, _ := issue743Server(t)
	socketPath := filepath.Join(t.TempDir(), "operator.sock")
	closer, err := server.eebusV1ServeOperator(
		context.Background(),
		socketPath,
		eebusV1PlatformPeerUIDResolver(),
	)
	if err != nil {
		t.Fatalf("start endpoint with Linux SO_PEERCRED resolver: %v", err)
	}
	t.Cleanup(func() { _ = closer.Close() })

	result := issue743CallUnix(t, socketPath, msp06RuntimeStatusTool, map[string]any{})
	issue743Meta(t, result, "raw", "eebus.raw.read")
}
