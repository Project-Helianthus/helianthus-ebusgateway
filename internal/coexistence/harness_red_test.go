package coexistence

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type harnessResult struct {
	stdout   []byte
	stderr   []byte
	exitCode int
}

func TestMSP08ProductionHarnessConformance(t *testing.T) {
	harness := buildProductionHarness(t)
	positive := filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json")

	verification := runHarness(t, harness, nil, verifierArgs(t, "verify", positive)...)
	assertHarnessSuccess(t, verification)
	if string(verification.stdout) != "ok\n" {
		t.Fatalf("positive verify stdout = %q", verification.stdout)
	}

	wantReport := readFile(t, filepath.Join(packageDir(t), "testdata/canonical/positive/report.json"))
	for _, environment := range [][]string{nil, append(os.Environ(), "TZ=Pacific/Kiritimati", "LC_ALL=C", "HTTP_PROXY=http://127.0.0.1:1")} {
		result := runHarness(t, harness, environment, verifierArgs(t, "report", positive)...)
		assertHarnessSuccess(t, result)
		if !bytes.Equal(result.stdout, wantReport) {
			t.Fatal("report does not match canonical bytes")
		}
	}

	output := filepath.Join(t.TempDir(), "generated")
	result := runHarness(t, harness, nil, generationArgs(t, output)...)
	assertHarnessSuccess(t, result)
	for _, name := range []string{"evidence.json", "report.json"} {
		got := readFile(t, filepath.Join(output, name))
		want := readFile(t, filepath.Join(packageDir(t), "testdata/canonical/positive", name))
		if !bytes.Equal(got, want) {
			t.Fatalf("generated %s does not match canonical bytes", name)
		}
	}
}

func TestMSP08HarnessNegativeMutationMatrix(t *testing.T) {
	harness := buildProductionHarness(t)
	root := packageDir(t)
	positive := loadObject(t, filepath.Join(root, "testdata/canonical/positive/evidence.json"))
	for _, name := range sortedKeys(negativeCategoriesV1) {
		name := name
		t.Run(name, func(t *testing.T) {
			descriptor := loadObject(t, filepath.Join(root, "testdata/canonical/negative", name))
			evidence := cloneObject(t, positive)
			applyMutation(t, evidence, stringValue(t, descriptor, "mutation"))
			path := filepath.Join(t.TempDir(), name)
			writeJSON(t, path, evidence)
			assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), negativeCategoriesV1[name])
		})
	}
}

func TestMSP08HarnessFailsClosedWithoutPartialReport(t *testing.T) {
	harness := buildProductionHarness(t)
	positivePath := filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json")
	positive := loadObject(t, positivePath)

	for _, test := range []struct {
		name      string
		mutations []string
		category  string
	}{
		{name: "limits before schema", mutations: []string{"UNKNOWN_FIELD", "RESOURCE_LIMIT_EXCEEDED"}, category: "limits.exceeded"},
		{name: "schema before candidate leak", mutations: []string{"CANDIDATE_LEAK_EBUS_MCP", "MISSING_PROVENANCE"}, category: "schema.evidence"},
		{name: "runtime before config", mutations: []string{"CONFIG_HASH_MISMATCH", "RUNTIME_ARTIFACT_MISMATCH"}, category: "provenance.runtime"},
		{name: "clock before ordering", mutations: []string{"DUPLICATE_PROVENANCE", "STALE_CAPTURE"}, category: "provenance.clock"},
		{name: "coverage before payload hash", mutations: []string{"CANONICAL_HASH_MISMATCH", "MISSING_REQUIRED_VIEW"}, category: "view.coverage"},
	} {
		t.Run(test.name, func(t *testing.T) {
			evidence := cloneObject(t, positive)
			for _, mutation := range test.mutations {
				applyMutation(t, evidence, mutation)
			}
			path := filepath.Join(t.TempDir(), "evidence.json")
			writeJSON(t, path, evidence)
			for _, command := range []string{"verify", "report"} {
				result := runHarness(t, harness, nil, verifierArgs(t, command, path)...)
				assertHarnessCategory(t, result, test.category)
				if bytes.HasPrefix(bytes.TrimSpace(result.stdout), []byte("{")) {
					t.Fatalf("%s emitted a partial report", command)
				}
			}
		})
	}

	t.Run("duplicate JSON key", func(t *testing.T) {
		raw := readFile(t, positivePath)
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeRaw(t, path, append([]byte("{\"contract\":\""+evidenceContractV1+"\","), raw[1:]...))
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "json.syntax")
	})
	t.Run("evidence byte ceiling", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeRaw(t, path, bytes.Repeat([]byte(" "), int(limitsV1["max_evidence_bytes"])+1))
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "limits.exceeded")
	})
}

func buildProductionHarness(t *testing.T) string {
	t.Helper()
	output := filepath.Join(t.TempDir(), "msp08harness")
	command := exec.Command("go", "build", "-o", output, productionHarnessPackage)
	command.Dir = repoDir(t)
	if combined, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build MSP-08 harness: %v\n%s", err, combined)
	}
	return output
}

func verifierArgs(t *testing.T, command, evidence string) []string {
	t.Helper()
	return append([]string{command, "--evidence", evidence}, m7AndRegistryArgs(t)...)
}

func m7AndRegistryArgs(t *testing.T) []string {
	t.Helper()
	repo := repoDir(t)
	return []string{
		"--registry", filepath.Join(repo, "internal/coexistence/contracts/multi-runtime-coexistence-registry-v1.json"),
		"--m7-graph", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/graph.json"),
		"--m7-replay", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/positive/replay-result.json"),
		"--m7-registry", filepath.Join(repo, "internal/candidatefacts/contracts/draft-candidate-fact-registry-v1.json"),
		"--m7-source-bundle", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/bundle.json"),
		"--m7-source-replay", filepath.Join(repo, "internal/candidatefacts/testdata/canonical/source/replay-result.json"),
	}
}

func generationArgs(t *testing.T, outputRoot string) []string {
	t.Helper()
	args := append([]string{"generate", "--output-root", outputRoot}, m7AndRegistryArgs(t)...)
	evidence := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	runs := arrayValue(t, evidence, "runs")
	var timestamps, subjects []any
	for _, rawRun := range runs {
		view := asObject(t, arrayValue(t, asObject(t, rawRun, "run"), "protected_views")[0], "view")
		meta := objectValue(t, objectValue(t, view, "payload"), "meta")
		timestamps = append(timestamps, stringValue(t, meta, "captured_at"))
		subjects = append(subjects, stringValue(t, meta, "auth_subject"))
	}
	values := []struct {
		flag, name string
		value      any
	}{
		{"--baseline-runtime", "baseline-runtime.json", objectValue(t, objectValue(t, asObject(t, runs[0], "run"), "provenance"), "runtime")},
		{"--compared-runtime", "compared-runtime.json", objectValue(t, objectValue(t, asObject(t, runs[1], "run"), "provenance"), "runtime")},
		{"--capture-clock", "capture-clock.json", objectValue(t, evidence, "capture_clock")},
		{"--capture-timestamps", "capture-timestamps.json", timestamps},
		{"--masked-subjects", "masked-subjects.json", subjects},
	}
	root := t.TempDir()
	for _, item := range values {
		path := filepath.Join(root, item.name)
		writeJSON(t, path, item.value)
		args = append(args, item.flag, path)
	}
	return args
}

func runHarness(t *testing.T, harness string, environment []string, args ...string) harnessResult {
	t.Helper()
	command := exec.Command(harness, args...)
	command.Dir = repoDir(t)
	if environment != nil {
		command.Env = environment
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatalf("run harness %v: %v", args, err)
		}
		exitCode = exitError.ExitCode()
	}
	return harnessResult{stdout: stdout.Bytes(), stderr: stderr.Bytes(), exitCode: exitCode}
}

func assertHarnessSuccess(t *testing.T, result harnessResult) {
	t.Helper()
	if result.exitCode != 0 || len(result.stderr) != 0 {
		t.Fatalf("harness exit=%d stdout=%q stderr=%q", result.exitCode, result.stdout, result.stderr)
	}
}

func assertHarnessCategory(t *testing.T, result harnessResult, category string) {
	t.Helper()
	if result.exitCode != 1 || string(result.stdout) != category+"\n" || len(result.stderr) != 0 {
		t.Fatalf("harness failure = exit %d stdout=%q stderr=%q; want %s", result.exitCode, result.stdout, result.stderr, category)
	}
}

func writeRaw(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
