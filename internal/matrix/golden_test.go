package matrix

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// goldenUpdateEnv regenerates the dry-run goldens when set to 1. The goldens
// were produced by the runner before the adaptermux-v2 suite existed, so a
// diff here means full88 or passive output changed.
const goldenUpdateEnv = "MATRIX_UPDATE_GOLDEN"

func TestDryRunGoldenFull88AndPassiveUnchanged(t *testing.T) {
	t.Parallel()

	for _, suite := range []string{"", SuitePassive} {
		suite := suite
		name := suite
		if name == "" {
			name = "default"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := dryRunTree(t, suite)
			goldenName := "full88-dryrun.golden"
			if suite == SuitePassive {
				goldenName = "passive-dryrun.golden"
			}
			goldenPath := filepath.Join("testdata", goldenName)
			if os.Getenv(goldenUpdateEnv) == "1" {
				if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
			}
			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden %s: %v", goldenPath, err)
			}
			if got != string(want) {
				t.Fatalf("dry-run output for suite %q differs from %s", name, goldenPath)
			}
		})
	}
}

// dryRunTree runs a dry run with a fixed clock and fixed synthetic commands
// and returns every written file, sorted by path, with the output directory
// replaced by a placeholder.
func dryRunTree(t *testing.T, suite string) string {
	t.Helper()

	outputDir := filepath.Join(t.TempDir(), "results")
	runner, err := NewRunner(RunnerOptions{
		OutputDir:    outputDir,
		Target:       "local",
		Suite:        suite,
		StartGateway: "start-gateway",
		StopGateway:  "stop-gateway",
		StartProxy:   "start-proxy",
		StopProxy:    "stop-proxy",
		StartEbusd:   "start-ebusd",
		StopEbusd:    "stop-ebusd",
		SmokeCommand: "smoke",
	})
	if err != nil {
		t.Fatalf("NewRunner error = %v", err)
	}
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	runner.nowUTC = func() time.Time { return fixed }
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run error = %v", err)
	}

	paths := make([]string, 0, 512)
	if err := filepath.WalkDir(outputDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatalf("walk output: %v", err)
	}
	sort.Strings(paths)

	prefix := filepath.ToSlash(outputDir)
	var builder strings.Builder
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		rel, err := filepath.Rel(outputDir, path)
		if err != nil {
			t.Fatalf("rel %s: %v", path, err)
		}
		builder.WriteString("== " + filepath.ToSlash(rel) + "\n")
		builder.WriteString(strings.ReplaceAll(string(data), prefix, "$OUT"))
	}
	return builder.String()
}
