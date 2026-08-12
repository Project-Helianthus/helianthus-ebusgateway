package main

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/m8sourcecapture"
)

func TestProcessInstanceIDBindsContainerAndStart(t *testing.T) {
	inputs := []m8sourcecapture.Input{{
		ID:      "container.inspect",
		Payload: []byte(`[{"Id":"container-1","State":{"StartedAt":"2026-08-12T06:00:00Z"}}]`),
	}}
	wantDigest := sha256.Sum256([]byte("container-1\x002026-08-12T06:00:00Z"))
	want := "process-" + hex.EncodeToString(wantDigest[:16])
	got, err := processInstanceID(inputs)
	if err != nil || got != want {
		t.Fatalf("processInstanceID() = %q, %v; want %q", got, err, want)
	}

	inputs[0].Payload = []byte(`[{"Id":"container-1","State":{"StartedAt":""}}]`)
	if _, err := processInstanceID(inputs); err == nil {
		t.Fatal("processInstanceID accepted missing StartedAt")
	}
}

func TestValidateOutputPathsRequiresDisjointSourceAndGeneration(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "staging")
	destination := filepath.Join(root, "capture")
	if err := validateOutputPaths(source, destination); err != nil {
		t.Fatalf("valid capture layout rejected: %v", err)
	}
	for _, test := range []struct {
		source, destination string
	}{
		{source, filepath.Join(source, "capture")},
		{destination, destination},
		{"relative", destination},
	} {
		if err := validateOutputPaths(test.source, test.destination); err == nil {
			t.Fatalf("unsafe layout accepted: %#v", test)
		}
	}
}
