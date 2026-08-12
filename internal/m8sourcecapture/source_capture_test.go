package m8sourcecapture

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func validMetadata() Metadata {
	return Metadata{
		Phase:                PhasePreRestart,
		WindowID:             "before-window-1",
		AuthScopeHash:        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		ProcessInstanceID:    "process-0123456789abcdef0123456789abcdef",
		CaptureStartOffsetNS: 10,
		CaptureEndOffsetNS:   20,
		CapturedAt:           time.Date(2026, 8, 12, 10, 11, 12, 123456789, time.UTC),
	}
}

func validInputs() []Input {
	inputs := make([]Input, len(inputDefinitions))
	for index, definition := range inputDefinitions {
		inputs[index] = Input{ID: definition.ID, Payload: []byte(`{"source":"` + definition.ID + `"}`)}
	}
	inputs[len(inputs)-1].Payload = []byte("2026-08-12T10:11:12.123456789Z\n")
	return inputs
}

func TestPublish_ProducesCanonicalManifestAndPrivateRoot(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "before")
	inputs := validInputs()

	manifest, err := Publish(destination, validMetadata(), inputs)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if !bytes.Contains(manifest, []byte(`"contract":"helianthus.platform.multi-runtime-source-capture-manifest.v1"`)) {
		t.Fatalf("manifest missing contract: %s", manifest)
	}
	if bytes.Contains(manifest, []byte("\n")) {
		t.Fatalf("manifest is not compact canonical JSON: %q", manifest)
	}
	var decoded struct {
		Inputs []struct {
			ID           string `json:"input_id"`
			AuthBoundary string `json:"auth_boundary"`
		} `json:"inputs"`
	}
	if err := json.Unmarshal(manifest, &decoded); err != nil {
		t.Fatalf("manifest is invalid JSON: %v", err)
	}
	if len(decoded.Inputs) != len(inputDefinitions) || decoded.Inputs[0].ID != "tools.list" || decoded.Inputs[15].ID != "capture.timestamp" {
		t.Fatalf("manifest inputs = %#v, want ordered contract inputs", decoded.Inputs)
	}
	for index, definition := range inputDefinitions {
		if decoded.Inputs[index].ID != definition.ID || decoded.Inputs[index].AuthBoundary != definition.AuthBoundary {
			t.Fatalf("manifest input %d = %#v, want %q/%q", index, decoded.Inputs[index], definition.ID, definition.AuthBoundary)
		}
	}

	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("root mode = %o, want 0700", info.Mode().Perm())
	}
	entries, err := os.ReadDir(destination)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(inputDefinitions) {
		t.Fatalf("file count = %d, want %d", len(entries), len(inputDefinitions))
	}
	byName := make(map[string]os.DirEntry, len(entries))
	for _, entry := range entries {
		byName[entry.Name()] = entry
	}
	for _, definition := range inputDefinitions {
		entry, ok := byName[definition.Filename]
		if !ok || entry.Type()&os.ModeSymlink != 0 {
			t.Fatalf("missing regular source file %q", definition.Filename)
		}
		fileInfo, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if !fileInfo.Mode().IsRegular() || fileInfo.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode = %v, want regular 0600", entry.Name(), fileInfo.Mode())
		}
	}
}

func TestPublish_RejectsWrongOrderSecretAndUnsafeDestination(t *testing.T) {
	metadata := validMetadata()
	inputs := validInputs()
	inputs[0], inputs[1] = inputs[1], inputs[0]
	if _, err := Publish(filepath.Join(t.TempDir(), "wrong-order"), metadata, inputs); err == nil {
		t.Fatal("Publish accepted reordered inputs")
	}

	for _, payload := range [][]byte{
		[]byte("Authorization: Bearer secret-value"),
		[]byte(`{"token":"secret-value"}`),
	} {
		inputs = validInputs()
		inputs[0].Payload = payload
		if _, err := Publish(filepath.Join(t.TempDir(), "secret"), metadata, inputs); err == nil {
			t.Fatalf("Publish accepted secret material %q", payload)
		}
	}

	parent := t.TempDir()
	if _, err := Publish(parent+"/../escape", metadata, validInputs()); err == nil {
		t.Fatal("Publish accepted path traversal destination")
	}
}

func TestPublish_RejectsEmptyOversizedAndMismatchedTimestamp(t *testing.T) {
	metadata := validMetadata()
	tests := []struct {
		name   string
		mutate func([]Input)
	}{
		{
			name: "empty",
			mutate: func(inputs []Input) {
				inputs[3].Payload = nil
			},
		},
		{
			name: "oversized",
			mutate: func(inputs []Input) {
				inputs[3].Payload = make([]byte, maxInputBytes+1)
			},
		},
		{
			name: "timestamp mismatch",
			mutate: func(inputs []Input) {
				inputs[len(inputs)-1].Payload = []byte("2026-08-12T10:11:13Z\n")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "capture")
			inputs := validInputs()
			test.mutate(inputs)
			if _, err := Publish(destination, metadata, inputs); err == nil {
				t.Fatal("Publish accepted invalid input")
			}
			if _, err := os.Lstat(destination); !os.IsNotExist(err) {
				t.Fatalf("invalid capture published a root: %v", err)
			}
		})
	}
}

func TestReadInputsRequiresExactSymlinkFreeRoot(t *testing.T) {
	root := t.TempDir()
	inputs := validInputs()
	for index, definition := range inputDefinitions {
		if err := os.WriteFile(filepath.Join(root, definition.Filename), inputs[index].Payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	read, err := ReadInputs(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(read) != len(inputs) || read[0].ID != "tools.list" || !bytes.Equal(read[15].Payload, inputs[15].Payload) {
		t.Fatalf("ReadInputs = %#v", read)
	}

	if err := os.WriteFile(filepath.Join(root, "extra.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadInputs(root); err == nil {
		t.Fatal("ReadInputs accepted an extra file")
	}

	linked := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(root, linked); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadInputs(linked); err == nil {
		t.Fatal("ReadInputs accepted a symlinked root")
	}
}
