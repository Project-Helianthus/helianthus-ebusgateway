package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/matrix"
)

func TestCollectExpectedFailuresFromCSVAndFile(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "expected.json")
	if err := os.WriteFile(filePath, []byte("{\"T08\":\"udp backend known limitation\",\" t09 \":\"\"}\n"), 0o644); err != nil {
		t.Fatalf("write expected.json: %v", err)
	}

	values, err := collectExpectedFailures("t07", filePath, "default reason")
	if err != nil {
		t.Fatalf("collectExpectedFailures error = %v", err)
	}
	if len(values) != 3 {
		t.Fatalf("len(values) = %d; want 3", len(values))
	}
	if values["T07"] != "default reason" {
		t.Fatalf("T07 reason = %q; want default reason", values["T07"])
	}
	if values["T08"] != "udp backend known limitation" {
		t.Fatalf("T08 reason = %q", values["T08"])
	}
	if values["T09"] != "default reason" {
		t.Fatalf("T09 reason = %q; want default reason", values["T09"])
	}
}

func TestCollectExpectedFailuresEmptyReturnsNil(t *testing.T) {
	t.Parallel()

	values, err := collectExpectedFailures("", "", "")
	if err != nil {
		t.Fatalf("collectExpectedFailures error = %v", err)
	}
	if values != nil {
		t.Fatalf("values should be nil, got %#v", values)
	}
}

func TestRequireExplicitTarget(t *testing.T) {
	t.Parallel()

	if err := requireExplicitTarget(matrix.SuiteAdapterMuxV2, "ENS", false); err == nil {
		t.Fatalf("--transports without --target must be rejected")
	}
	if err := requireExplicitTarget(matrix.SuiteAdapterMuxV2, "ENS", true); err != nil {
		t.Fatalf("explicit target rejected: %v", err)
	}
	if err := requireExplicitTarget(matrix.SuiteAdapterMuxV2, "", false); err != nil {
		t.Fatalf("an unfiltered dry run needs no explicit target: %v", err)
	}
	if err := requireExplicitTarget(matrix.SuiteFullMatrix, "", false); err != nil {
		t.Fatalf("full88 default rejected: %v", err)
	}
}

func TestPromptAdapterActionRequiresTypedCaseID(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	confirm := promptAdapterAction(strings.NewReader("UDP-DS01\nyes\n"), &out)
	if !confirm("UDP-DS01", "switch the adapter protocol") {
		t.Fatalf("typed case ID not accepted")
	}
	if confirm("TCP-DS01", "switch the adapter protocol") {
		t.Fatalf("a different answer was accepted")
	}
	if confirm("TCP-DS01", "switch the adapter protocol") {
		t.Fatalf("end of input was accepted")
	}
	if !strings.Contains(out.String(), "Type UDP-DS01 to confirm") {
		t.Fatalf("prompt = %q", out.String())
	}
}
