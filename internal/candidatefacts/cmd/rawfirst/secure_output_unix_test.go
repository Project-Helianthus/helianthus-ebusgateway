//go:build darwin || linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateOutputRootRejectsUnsafeDirectories(t *testing.T) {
	root := t.TempDir()
	public := filepath.Join(root, "public")
	if err := os.Mkdir(public, 0o755); err != nil {
		t.Fatal(err)
	}
	if opened, err := openPrivateOutputRoot(public); err == nil {
		_ = opened.close()
		t.Fatal("public output directory was accepted")
	}

	nonempty := filepath.Join(root, "nonempty")
	if err := os.Mkdir(nonempty, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nonempty, "existing"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if opened, err := openPrivateOutputRoot(nonempty); err == nil {
		_ = opened.close()
		t.Fatal("nonempty output directory was accepted")
	}

	private := filepath.Join(root, "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(root, "symlink")
	if err := os.Symlink(private, symlink); err != nil {
		t.Fatal(err)
	}
	if opened, err := openPrivateOutputRoot(symlink); err == nil {
		_ = opened.close()
		t.Fatal("symlink output directory was accepted")
	}
	ancestor := filepath.Join(root, "ancestor")
	if err := os.Symlink(root, ancestor); err != nil {
		t.Fatal(err)
	}
	if opened, err := openPrivateOutputRoot(filepath.Join(ancestor, "private")); err == nil {
		_ = opened.close()
		t.Fatal("symlink ancestor was accepted")
	}
}

func TestPrivateOutputRootStaysAnchoredAcrossDirectorySwap(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(parent, "private")
	moved := filepath.Join(parent, "moved")
	if err := os.Mkdir(original, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := openPrivateOutputRoot(original)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.close() }()

	if err := os.Rename(original, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(original, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := root.writeNewFile("artifact.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(moved, "artifact.json")); err != nil {
		t.Fatalf("anchored output missing from original directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(original, "artifact.json")); !os.IsNotExist(err) {
		t.Fatalf("replacement directory received output: %v", err)
	}
	info, err := os.Stat(filepath.Join(moved, "artifact.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("private artifact mode = %v", info.Mode())
	}
	if err := root.writeNewFile("artifact.json", []byte("replacement")); err == nil {
		t.Fatal("private artifact was overwritten")
	}
}
