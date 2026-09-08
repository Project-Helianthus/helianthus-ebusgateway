package adversarialpackaging

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompiledPublisherRunsOutsideCheckoutWithoutPython(t *testing.T) {
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	imports := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, "./internal/adversarial")
	imports.Dir = repo
	out, err := imports.CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v: %s", err, out)
	}
	if strings.Contains(string(out), "os/exec") {
		t.Fatal("runtime package imports os/exec")
	}

	for _, trimpath := range []bool{false, true} {
		name := "normal"
		if trimpath {
			name = "trimpath"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			clone := exec.Command("git", "clone", "--quiet", "--no-hardlinks", repo, source)
			if out, err := clone.CombinedOutput(); err != nil {
				t.Fatalf("clone: %v: %s", err, out)
			}
			checkout := exec.Command("git", "checkout", "--quiet", "--detach", "HEAD")
			checkout.Dir = source
			if out, err := checkout.CombinedOutput(); err != nil {
				t.Fatalf("checkout: %v: %s", err, out)
			}
			binary := filepath.Join(root, "adversarial.test")
			args := []string{"test", "-c", "-buildvcs=true", "-o", binary}
			if trimpath {
				args = append(args, "-trimpath")
			}
			args = append(args, "./internal/adversarial")
			build := exec.Command("go", args...)
			build.Dir = source
			if out, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build: %v: %s", err, out)
			}
			headCommand := exec.Command("git", "rev-parse", "HEAD")
			headCommand.Dir = source
			headRaw, err := headCommand.Output()
			if err != nil {
				t.Fatal(err)
			}
			expectedRevision := strings.TrimSpace(string(headRaw))
			binaryRaw, err := os.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			expectedDigest := fmt.Sprintf("%x", sha256.Sum256(binaryRaw))

			run := filepath.Join(root, "run")
			if err := os.Mkdir(run, 0o755); err != nil {
				t.Fatal(err)
			}
			input, err := os.ReadFile(filepath.Join(source, "internal", "adversarial", "fixtures", "v1", "inputs", "offline-all-pass.json"))
			if err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(run, "input.json")
			outputPath := filepath.Join(run, "output.json")
			if err := os.WriteFile(inputPath, input, 0o644); err != nil {
				t.Fatal(err)
			}
			emptyPath := filepath.Join(root, "empty-path")
			if err := os.Mkdir(emptyPath, 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-test.run=^TestCompiledPublisherHelper$", "--", inputPath, outputPath)
			cmd.Dir = run
			cmd.Env = append(os.Environ(), "PATH="+emptyPath)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("outside-checkout run: %v: %s", err, out)
			}
			written, err := os.ReadFile(outputPath)
			if err != nil || len(written) == 0 {
				t.Fatalf("written=%d err=%v", len(written), err)
			}
			var report struct {
				Provenance struct {
					Subject struct {
						Commit string `json:"commit"`
					} `json:"subject"`
					Producer struct {
						Commit      string `json:"commit"`
						BuildSHA256 string `json:"build_sha256"`
					} `json:"producer"`
				} `json:"provenance"`
			}
			if err := json.Unmarshal(written, &report); err != nil {
				t.Fatal(err)
			}
			if report.Provenance.Subject.Commit != expectedRevision || report.Provenance.Producer.Commit != expectedRevision || report.Provenance.Producer.BuildSHA256 != expectedDigest {
				t.Fatalf("provenance = %#v", report.Provenance)
			}
		})
	}
}
