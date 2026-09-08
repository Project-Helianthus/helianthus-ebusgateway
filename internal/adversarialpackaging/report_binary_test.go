package adversarialpackaging

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
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

	cases := []struct {
		name     string
		trimpath bool
		buildVCS bool
	}{
		{name: "normal", buildVCS: true},
		{name: "trimpath", trimpath: true, buildVCS: true},
		{name: "metadata-omitted-control", buildVCS: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			args := []string{"test", "-c", fmt.Sprintf("-buildvcs=%t", tc.buildVCS), "-o", binary}
			if tc.trimpath {
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
			info, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatalf("read build info: %v", err)
			}
			revision, modified, revisionCount, modifiedCount := vcsSettings(info)
			hasVCSSettings := revisionCount != 0 || modifiedCount != 0
			if !tc.buildVCS && hasVCSSettings {
				t.Fatalf("-buildvcs=false emitted VCS settings: revision=%q modified=%q", revision, modified)
			}
			if hasVCSSettings && (revisionCount != 1 || modifiedCount != 1 || revision != expectedRevision || modified != "false") {
				t.Fatalf("unexpected VCS settings: revision=%q count=%d modified=%q count=%d", revision, revisionCount, modified, modifiedCount)
			}

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
			output, runErr := cmd.CombinedOutput()
			if !hasVCSSettings {
				t.Logf("%s omitted VCS settings; requiring fail-closed publication", info.GoVersion)
				if runErr == nil || !strings.Contains(string(output), "producer_build_info") {
					t.Fatalf("metadata-free binary result: err=%v output=%s", runErr, output)
				}
				if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
					t.Fatalf("metadata-free binary created destination: %v", err)
				}
				return
			}
			t.Logf("%s emitted clean VCS settings; requiring authenticated publication", info.GoVersion)
			if runErr != nil {
				t.Fatalf("outside-checkout run: %v: %s", runErr, output)
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

func vcsSettings(info *debug.BuildInfo) (revision, modified string, revisionCount, modifiedCount int) {
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
			revisionCount++
		case "vcs.modified":
			modified = setting.Value
			modifiedCount++
		}
	}
	return revision, modified, revisionCount, modifiedCount
}
