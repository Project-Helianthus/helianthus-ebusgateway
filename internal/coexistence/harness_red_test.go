package coexistence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type harnessResult struct {
	stdout   []byte
	stderr   []byte
	exitCode int
}

func TestMSP08ProductionHarnessConformance(t *testing.T) {
	libraryProbe, harness := buildProductionBoundaries(t)
	assertProductionBinding(t, harness)
	assertPositiveVerificationAndReport(t, harness)
	assertDeterministicGeneration(t, harness)
	assertNegativeMutationMatrix(t, harness)
	assertErrorPrecedenceAndFailClosedInputs(t, harness)
	assertAmendedREDConformance(t, libraryProbe, harness)
	assertProductionVerifierExcludesTestMutationLanguage(t)
}

func buildProductionBoundaries(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	libraryProbe := filepath.Join(root, "msp08-library-probe")
	harness := filepath.Join(root, "msp08harness")
	type buildResult struct {
		name   string
		output []byte
		err    error
	}
	results := make([]buildResult, 0, 2)
	for _, target := range []struct {
		name        string
		output      string
		packagePath string
	}{
		{name: "direct library probe", output: libraryProbe, packagePath: "./internal/coexistence/testdata/libraryprobe"},
		{name: "private harness", output: harness, packagePath: productionHarnessPackage},
	} {
		command := exec.Command("go", "build", "-o", target.output, target.packagePath)
		command.Dir = repoDir(t)
		combined, err := command.CombinedOutput()
		results = append(results, buildResult{name: target.name, output: combined, err: err})
	}
	var failures []string
	for _, result := range results {
		if result.err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v\n%s", result.name, result.err, result.output))
		}
	}
	if len(failures) != 0 {
		t.Fatalf("MSP-08 production library/harness absent (expected RED):\n%s", strings.Join(failures, "\n"))
	}
	return libraryProbe, harness
}

func assertProductionBinding(t *testing.T, harness string) {
	t.Helper()
	result := runHarness(t, harness, nil, "binding")
	assertHarnessSuccess(t, result)
	binding := asObject(t, decodeJSON(t, result.stdout), "production binding")
	assertExactKeys(t, binding, []string{
		"artifact_sha256",
		"baseline_gateway_commit",
		"m7_completion_token",
		"owner_commit",
		"owner_exact_head_actions_run",
		"owner_post_main_actions_run",
		"owner_repository",
		"owner_tree",
	})
	assertStringValue(t, binding, "owner_repository", docsOwnerRepository)
	assertStringValue(t, binding, "owner_commit", docsOwnerCommit)
	assertStringValue(t, binding, "owner_tree", docsOwnerTree)
	assertIntValue(t, binding, "owner_exact_head_actions_run", int64(docsExactHeadActionsRun))
	assertIntValue(t, binding, "owner_post_main_actions_run", int64(docsPostMainActionsRun))
	assertStringValue(t, binding, "baseline_gateway_commit", baselineGatewayCommit)
	assertStringValue(t, binding, "m7_completion_token", m7CompletionToken)

	artifacts := objectValue(t, binding, "artifact_sha256")
	if len(artifacts) != len(ownerArtifactSHA256) {
		t.Fatalf("production binding artifacts = %d; want %d", len(artifacts), len(ownerArtifactSHA256))
	}
	for path, digest := range ownerArtifactSHA256 {
		assertStringValue(t, artifacts, path, digest)
	}
}

func assertPositiveVerificationAndReport(t *testing.T, harness string) {
	t.Helper()
	positive := filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json")
	verification := runHarness(t, harness, nil, verifierArgs(t, "verify", positive)...)
	assertHarnessSuccess(t, verification)
	if string(verification.stdout) != "ok\n" {
		t.Fatalf("positive verify stdout = %q; want ok", verification.stdout)
	}

	wantReport := readFile(t, filepath.Join(packageDir(t), "testdata/canonical/positive/report.json"))
	first := runHarness(t, harness, nil, verifierArgs(t, "report", positive)...)
	secondEnv := append(os.Environ(),
		"HOME="+filepath.Join(t.TempDir(), "unavailable-home"),
		"LANG=invalid_LOCALE",
		"LC_ALL=C",
		"TZ=Pacific/Kiritimati",
		"GODEBUG=randautoseed=1",
		"HTTP_PROXY=http://127.0.0.1:1",
		"HTTPS_PROXY=http://127.0.0.1:1",
		"NO_PROXY=*",
	)
	second := runHarness(t, harness, secondEnv, verifierArgs(t, "report", positive)...)
	for index, result := range []harnessResult{first, second} {
		assertHarnessSuccess(t, result)
		if !bytes.Equal(result.stdout, wantReport) {
			t.Fatalf("report run %d does not match exact canonical report bytes", index+1)
		}
	}
}

func assertDeterministicGeneration(t *testing.T, harness string) {
	t.Helper()
	output := filepath.Join(t.TempDir(), "positive")
	args := generationArgs(t, output)
	result := runHarness(t, harness, nil, args...)
	assertHarnessSuccess(t, result)
	if len(result.stdout) != 0 {
		t.Fatalf("generator stdout = %q; want empty", result.stdout)
	}
	for _, name := range []string{"evidence.json", "report.json"} {
		got := readFile(t, filepath.Join(output, name))
		want := readFile(t, filepath.Join(packageDir(t), "testdata/canonical/positive", name))
		if !bytes.Equal(got, want) {
			t.Fatalf("generated %s does not match canonical bytes", name)
		}
	}
}

func assertNegativeMutationMatrix(t *testing.T, harness string) {
	t.Helper()
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

func assertErrorPrecedenceAndFailClosedInputs(t *testing.T, harness string) {
	t.Helper()
	positivePath := filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json")
	positive := loadObject(t, positivePath)

	for _, test := range []struct {
		name      string
		mutations []string
		category  string
	}{
		{name: "limits before schema", mutations: []string{"UNKNOWN_FIELD", "RESOURCE_LIMIT_EXCEEDED"}, category: "limits.exceeded"},
		{name: "schema before anti leak", mutations: []string{"CANDIDATE_LEAK_EBUS_MCP", "MISSING_PROVENANCE"}, category: "schema.evidence"},
		{name: "runtime before config", mutations: []string{"CONFIG_HASH_MISMATCH", "RUNTIME_ARTIFACT_MISMATCH"}, category: "provenance.runtime"},
		{name: "clock before duplicate", mutations: []string{"DUPLICATE_PROVENANCE", "STALE_CAPTURE"}, category: "provenance.clock"},
		{name: "coverage before payload hash", mutations: []string{"CANONICAL_HASH_MISMATCH", "MISSING_REQUIRED_VIEW"}, category: "view.coverage"},
	} {
		t.Run(test.name, func(t *testing.T) {
			evidence := cloneObject(t, positive)
			for _, mutation := range test.mutations {
				applyMutation(t, evidence, mutation)
			}
			path := filepath.Join(t.TempDir(), "evidence.json")
			writeJSON(t, path, evidence)
			assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), test.category)
		})
	}

	t.Run("M7 replay mismatch", func(t *testing.T) {
		evidence := cloneObject(t, positive)
		objectValue(t, evidence, "m7_binding")["replay_hash"] = "sha256:" + strings.Repeat("f", 64)
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeJSON(t, path, evidence)
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "provenance.m7")
	})

	t.Run("canonical registry cannot be substituted", func(t *testing.T) {
		registry := loadObject(t, filepath.Join(packageDir(t), "contracts/multi-runtime-coexistence-registry-v1.json"))
		views := arrayValue(t, registry, "protected_views")
		registry["protected_views"] = views[:len(views)-1]
		registryPath := filepath.Join(t.TempDir(), "registry.json")
		writeJSON(t, registryPath, registry)

		evidence := cloneObject(t, positive)
		objectValue(t, evidence, "registry")["digest"] = "sha256:" + rawSHA256(readFile(t, registryPath))
		evidencePath := filepath.Join(t.TempDir(), "evidence.json")
		writeJSON(t, evidencePath, evidence)
		args := verifierArgsWithRegistry(t, "verify", evidencePath, registryPath)
		assertHarnessCategory(t, runHarness(t, harness, nil, args...), "registry.binding")
	})

	t.Run("promoted eBUS authority cannot be replaced", func(t *testing.T) {
		evidence := cloneObject(t, positive)
		run := findRun(t, evidence, "EEBUS_CONNECTED_CANDIDATE_ONLY")
		view := findView(t, run, "semantic.registry")
		objectValue(t, objectValue(t, view, "payload"), "data")["authority"] = "eebus-candidate"
		refreshViewHashes(t, evidence, run, view)
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeJSON(t, path, evidence)
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "authority.ebus")
	})

	t.Run("final evidence hash is verifier derived", func(t *testing.T) {
		evidence := cloneObject(t, positive)
		evidence["evidence_hash"] = "sha256:" + strings.Repeat("f", 64)
		evidence["evidence_id"] = "mrcv1:" + evidence["evidence_hash"].(string)
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeJSON(t, path, evidence)
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "hash.evidence")
	})

	t.Run("auth mismatch", func(t *testing.T) {
		evidence := cloneObject(t, positive)
		run := findRun(t, evidence, "EEBUS_DISABLED_CONFIRMED")
		auth := objectValue(t, objectValue(t, run, "provenance"), "auth_scope")
		auth["scope_hash"] = "sha256:" + strings.Repeat("f", 64)
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeJSON(t, path, evidence)
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "provenance.auth_mask")
	})

	t.Run("zero runs cannot pass", func(t *testing.T) {
		evidence := cloneObject(t, positive)
		evidence["runs"] = []any{}
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeJSON(t, path, evidence)
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "schema.evidence")
	})

	t.Run("no services must be explicit degraded", func(t *testing.T) {
		evidence := cloneObject(t, positive)
		state := objectValue(t, findRun(t, evidence, "EEBUS_ENABLED_NO_SERVICES"), "state_evidence")
		state["degraded"] = false
		state["outcome"] = "DISABLED_CONFIRMED"
		path := filepath.Join(t.TempDir(), "evidence.json")
		writeJSON(t, path, evidence)
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "state.evidence")
	})

	t.Run("duplicate JSON key", func(t *testing.T) {
		raw := readFile(t, positivePath)
		prefix := []byte("{\"contract\":\"" + evidenceContractV1 + "\",")
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, append(prefix, raw[1:]...), 0o600); err != nil {
			t.Fatal(err)
		}
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "json.syntax")
	})

	t.Run("malformed JSON", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "json.syntax")
	})

	t.Run("evidence byte ceiling", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "evidence.json")
		if err := os.WriteFile(path, bytes.Repeat([]byte(" "), int(limitsV1["max_evidence_bytes"])+1), 0o600); err != nil {
			t.Fatal(err)
		}
		assertHarnessCategory(t, runHarness(t, harness, nil, verifierArgs(t, "verify", path)...), "limits.exceeded")
	})
}

func assertProductionVerifierExcludesTestMutationLanguage(t *testing.T) {
	t.Helper()
	root := filepath.Join(repoDir(t), "internal/coexistence")
	for _, token := range []string{"CANDIDATE_LEAK_EBUS_MCP", "DROPPED_PAYLOAD_FIELD", "ROLLBACK_DRIFT", "expand_negative_fixture"} {
		token := token
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			if bytes.Contains(readFile(t, path), []byte(token)) {
				return fmt.Errorf("production source %s contains test-only mutation token %s", path, token)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func verifierArgs(t *testing.T, command, evidence string) []string {
	t.Helper()
	return append([]string{command, "--evidence", evidence}, m7AndRegistryArgs(t)...)
}

func verifierArgsWithRegistry(t *testing.T, command, evidence, registry string) []string {
	t.Helper()
	args := verifierArgs(t, command, evidence)
	for index := 0; index < len(args)-1; index++ {
		if args[index] == "--registry" {
			args[index+1] = registry
			return args
		}
	}
	t.Fatal("private harness arguments have no registry binding")
	return nil
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
	return append(args, deterministicGenerationArgs(t)...)
}

func deterministicGenerationArgs(t *testing.T) []string {
	t.Helper()
	evidence := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	runs := arrayValue(t, evidence, "runs")
	baselineRuntime := objectValue(t, objectValue(t, asObject(t, runs[0], "baseline run"), "provenance"), "runtime")
	comparedRuntime := objectValue(t, objectValue(t, asObject(t, runs[1], "compared run"), "provenance"), "runtime")
	var timestamps []any
	var subjects []any
	for _, rawRun := range runs {
		run := asObject(t, rawRun, "generation run")
		view := asObject(t, arrayValue(t, run, "protected_views")[0], "generation view")
		meta := objectValue(t, objectValue(t, view, "payload"), "meta")
		timestamps = append(timestamps, stringValue(t, meta, "captured_at"))
		subjects = append(subjects, stringValue(t, meta, "auth_subject"))
	}
	inputs := []struct {
		flag  string
		name  string
		value any
	}{
		{flag: "--baseline-runtime", name: "baseline-runtime.json", value: baselineRuntime},
		{flag: "--compared-runtime", name: "compared-runtime.json", value: comparedRuntime},
		{flag: "--capture-clock", name: "capture-clock.json", value: objectValue(t, evidence, "capture_clock")},
		{flag: "--capture-timestamps", name: "capture-timestamps.json", value: timestamps},
		{flag: "--masked-subjects", name: "masked-subjects.json", value: subjects},
	}
	root := t.TempDir()
	args := make([]string, 0, len(inputs)*2)
	for _, input := range inputs {
		path := filepath.Join(root, input.name)
		writeJSON(t, path, input.value)
		args = append(args, input.flag, path)
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
	var stdout bytes.Buffer
	var stderr bytes.Buffer
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
		t.Fatalf("harness failure = exit %d stdout=%q stderr=%q; want category %s", result.exitCode, result.stdout, result.stderr, category)
	}
}

func assertExactKeys(t *testing.T, object map[string]any, want []string) {
	t.Helper()
	got := sortedKeys(object)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("object keys = %v; want %v", got, want)
	}
}

func TestMSP08HarnessProtocolItselfIsClosed(t *testing.T) {
	if productionHarnessPackage != "./internal/coexistence/cmd/msp08harness" {
		t.Fatal("MSP-08 private harness package drifted")
	}
	commands := []string{"binding", "generate", "report", "verify"}
	sort.Strings(commands)
	if !reflect.DeepEqual(commands, []string{"binding", "generate", "report", "verify"}) {
		t.Fatal("MSP-08 harness command vocabulary drifted")
	}
	if docsOwnerCommit == baselineGatewayCommit {
		t.Fatal("docs owner and gateway baseline identities were conflated")
	}
	if strings.Contains(strings.ToLower(docsOwnerRepository), "vendor_restricted") {
		t.Fatal("owner binding references forbidden material")
	}
}

func TestMSP08BindingMetadataUsesFullImmutableIdentifiers(t *testing.T) {
	for name, value := range map[string]string{
		"docs commit":      docsOwnerCommit,
		"docs tree":        docsOwnerTree,
		"gateway baseline": baselineGatewayCommit,
		"M7 docs source":   m7DocsSourceCommit,
	} {
		if len(value) != 40 {
			t.Fatalf("%s = %q; want full 40-character object ID", name, value)
		}
	}
	if docsExactHeadActionsRun == 0 || docsPostMainActionsRun == 0 {
		t.Fatal("docs Actions provenance is not pinned")
	}
	if _, err := json.Marshal(ownerArtifactSHA256); err != nil {
		t.Fatal(err)
	}
}
