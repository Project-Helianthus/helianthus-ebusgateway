//go:build darwin || linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadPrivateJSONRequiresClosedOwnerOnlyRegularInput(t *testing.T) {
	type document struct {
		Value string `json:"value"`
	}
	directory := t.TempDir()
	valid := filepath.Join(directory, "valid.json")
	if err := os.WriteFile(valid, []byte(`{"value":"ok"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var result document
	if err := readPrivateJSON(valid, &result); err != nil || result.Value != "ok" {
		t.Fatalf("valid input = %#v, %v", result, err)
	}

	unknown := filepath.Join(directory, "unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"value":"ok","extra":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := readPrivateJSON(unknown, &document{}); err == nil {
		t.Fatal("unknown field was accepted")
	}

	open := filepath.Join(directory, "open.json")
	if err := os.WriteFile(open, []byte(`{"value":"ok"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := readPrivateJSON(open, &document{}); err == nil {
		t.Fatal("group/world-readable input was accepted")
	}

	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(valid, link); err != nil {
		t.Fatal(err)
	}
	if err := readPrivateJSON(link, &document{}); err == nil {
		t.Fatal("symlink input was accepted")
	}
}
