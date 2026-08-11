//go:build darwin || linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWritePrivateOutputsRequiresEmptyOwnerOnlyDirectory(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "output")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	outputs := []privateOutput{{name: "private-campaign.json", content: []byte("{}\n")}}
	if err := writePrivateOutputs(directory, outputs); err != nil {
		t.Fatalf("writePrivateOutputs: %v", err)
	}
	path := filepath.Join(directory, outputs[0].name)
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode = %s", info.Mode())
	}
	if err := writePrivateOutputs(directory, outputs); err == nil {
		t.Fatal("non-empty output directory was accepted")
	}
}

func TestWritePrivateOutputsRejectsSymlinkedPathComponent(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedDirectory := filepath.Join(root, "linked")
	if err := os.Symlink(realDirectory, linkedDirectory); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateOutputs(linkedDirectory, []privateOutput{{name: "campaign.json", content: []byte("{}")}}); err == nil {
		t.Fatal("symlinked output directory was accepted")
	}
}
