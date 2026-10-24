package mcp

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// M71: the operator socket is derived from the eeBUS state root and lives in a
// sibling directory, never inside the root the eeBUS store owns.
func TestM71OperatorSocketPathIsASiblingOfTheStateRoot(t *testing.T) {
	for _, test := range []struct {
		stateRoot string
		root      string
		want      string
	}{
		{stateRoot: "/data/eebus", root: "/data/eebus", want: "/data/eebus-operator/operator-mcp.sock"},
		{stateRoot: "/data/eebus/", root: "/data/eebus", want: "/data/eebus-operator/operator-mcp.sock"},
		{stateRoot: "/var/lib/helianthus/eebus", root: "/var/lib/helianthus/eebus", want: "/var/lib/helianthus/eebus-operator/operator-mcp.sock"},
		{stateRoot: "/srv/eebus-operator", root: "/srv/eebus-operator", want: "/srv/eebus-operator-operator/operator-mcp.sock"},
		// Without a valid state root the add-on root applies.
		{stateRoot: "", root: "/data/eebus", want: "/data/eebus-operator/operator-mcp.sock"},
		{stateRoot: "relative/eebus", root: "/data/eebus", want: "/data/eebus-operator/operator-mcp.sock"},
		{stateRoot: "/", root: "/data/eebus", want: "/data/eebus-operator/operator-mcp.sock"},
	} {
		got := EEBusV1OperatorSocketPath(test.stateRoot)
		if got != test.want {
			t.Errorf("EEBusV1OperatorSocketPath(%q) = %q; want %q", test.stateRoot, got, test.want)
		}
		if strings.HasPrefix(got, test.root+string(filepath.Separator)) {
			t.Errorf("EEBusV1OperatorSocketPath(%q) = %q lies inside the state root", test.stateRoot, got)
		}
		if directory := filepath.Dir(got); filepath.Dir(directory) != filepath.Dir(test.root) || directory == test.root {
			t.Errorf("EEBusV1OperatorSocketPath(%q) directory %q is not a sibling of %q", test.stateRoot, directory, test.root)
		}
		if filepath.Base(got) != EEBusV1OperatorSocketName {
			t.Errorf("EEBusV1OperatorSocketPath(%q) file name = %q; want %q", test.stateRoot, filepath.Base(got), EEBusV1OperatorSocketName)
		}
	}
}

// M71: the operator socket path is what clients dial, so it is bounded by the
// sockaddr_un path capacity of the platform minus the terminating NUL.
func TestM71OperatorSocketPathLimitIsThePlatformAddressCapacity(t *testing.T) {
	switch runtime.GOOS {
	case "linux":
		if EEBusV1OperatorSocketPathLimit != 107 {
			t.Fatalf("limit = %d; want 107 (sun_path is 108 bytes)", EEBusV1OperatorSocketPathLimit)
		}
	case "darwin":
		if EEBusV1OperatorSocketPathLimit != 103 {
			t.Fatalf("limit = %d; want 103 (sun_path is 104 bytes)", EEBusV1OperatorSocketPathLimit)
		}
	}
	atLimit := "/" + strings.Repeat("a", EEBusV1OperatorSocketPathLimit-1)
	if len(atLimit) != EEBusV1OperatorSocketPathLimit {
		t.Fatalf("fixture length = %d; want %d", len(atLimit), EEBusV1OperatorSocketPathLimit)
	}
	if err := ValidateEEBusV1OperatorSocketPath(atLimit); err != nil {
		t.Fatalf("a path of exactly the limit was rejected: %v", err)
	}
	err := ValidateEEBusV1OperatorSocketPath(atLimit + "a")
	if err == nil {
		t.Fatal("a path one byte over the limit was accepted")
	}
	if strings.Contains(err.Error(), "aaaa") {
		t.Fatalf("error carries the path: %v", err)
	}
}

// The endpoint refuses an over-long path before it binds anything: on Linux
// the listener would otherwise bind through its short descriptor alias and
// leave a socket that no client can dial.
func TestM71OperatorEndpointRejectsAnOverlongPathBeforeBinding(t *testing.T) {
	server, _ := issue743Server(t)
	root := t.TempDir()
	socketPath := filepath.Join(root, strings.Repeat("d", EEBusV1OperatorSocketPathLimit), EEBusV1OperatorSocketName)
	resolve := func(net.Conn) (int, error) { return os.Geteuid(), nil }
	closer, err := server.eebusV1ServeOperator(context.Background(), socketPath, resolve)
	if closer != nil {
		_ = closer.Close()
		t.Fatal("an over-long operator socket path produced an endpoint")
	}
	if err == nil || !strings.Contains(err.Error(), "AF_UNIX limit") {
		t.Fatalf("over-long path error = %v; want the AF_UNIX limit", err)
	}
	if entries, readErr := os.ReadDir(root); readErr != nil || len(entries) != 0 {
		t.Fatalf("root entries = %v (%v); the rejection must precede any filesystem change", entries, readErr)
	}
}
