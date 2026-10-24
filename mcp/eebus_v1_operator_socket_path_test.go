package mcp

import (
	"path/filepath"
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
