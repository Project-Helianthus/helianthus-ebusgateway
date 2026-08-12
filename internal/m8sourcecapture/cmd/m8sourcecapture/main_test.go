package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
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

func TestWriteManifestRequiresNewPrivateRegularFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "capture.manifest.json")
	if err := writeManifest(path, []byte(`{"contract":"test"}`)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("manifest mode = %v, want regular 0600", info.Mode())
	}
	if err := writeManifest(path, []byte(`{}`)); err == nil {
		t.Fatal("writeManifest overwrote existing output")
	}
}

func TestValidateOutputPathsRequiresSiblingManifestAndDisjointSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "staging")
	destination := filepath.Join(root, "capture")
	manifest := destination + ".manifest.json"
	if err := validateOutputPaths(source, destination, manifest); err != nil {
		t.Fatalf("valid capture layout rejected: %v", err)
	}
	for _, test := range []struct {
		source, destination, manifest string
	}{
		{source, destination, filepath.Join(root, "other.json")},
		{source, filepath.Join(source, "capture"), filepath.Join(source, "capture.manifest.json")},
		{destination, destination, manifest},
	} {
		if err := validateOutputPaths(test.source, test.destination, test.manifest); err == nil {
			t.Fatalf("unsafe layout accepted: %#v", test)
		}
	}
}
