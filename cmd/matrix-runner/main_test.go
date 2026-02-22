package main

import (
	"os"
	"path/filepath"
	"testing"
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
