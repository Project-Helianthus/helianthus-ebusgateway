package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateOutputDirectoryAndFilesFailClosed(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "private")
	if err := ensurePrivateOutputDirectory(output); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(output)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("private directory mode = %v, err = %v", info.Mode(), err)
	}

	artifact := filepath.Join(output, "artifact.json")
	if err := writeNewPrivateFile(artifact, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	info, err = os.Lstat(artifact)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("private artifact mode = %v, err = %v", info.Mode(), err)
	}
	if err := writeNewPrivateFile(artifact, []byte("replacement")); err == nil {
		t.Fatal("private artifact was overwritten")
	}

	public := filepath.Join(root, "public")
	if err := os.Mkdir(public, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateOutputDirectory(public); err == nil {
		t.Fatal("public output directory was accepted")
	}
	symlink := filepath.Join(root, "symlink")
	if err := os.Symlink(output, symlink); err != nil {
		t.Fatal(err)
	}
	if err := ensurePrivateOutputDirectory(symlink); err == nil {
		t.Fatal("symlink output directory was accepted")
	}
}
