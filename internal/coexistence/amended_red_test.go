package coexistence

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func assertAmendedREDConformance(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	positive := filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json")
	assertDirectLibraryOwnershipAndParity(t, libraryProbe, harness, positive)
	assertM7InputsAreAuthoritative(t, libraryProbe, harness, positive)
	assertIndependentCapturedRuntimeVariant(t, libraryProbe, harness)
	assertAllResourceCeilings(t, libraryProbe, harness)
	assertClosedJSONAndOrdering(t, libraryProbe, harness)
	assertEveryViewRejectsEEBUSDriftAndLeaks(t, libraryProbe, harness)
	assertV1SurfaceAndPrecedenceClosure(t, libraryProbe, harness)
	assertAllTwentyPrecedenceCombinations(t, libraryProbe, harness)
	assertProductionLibraryUsesCandidateFactsAuthority(t)
	assertHarnessIsStrictlyThin(t)
}

func assertDirectLibraryOwnershipAndParity(t *testing.T, libraryProbe, harness, positive string) {
	t.Helper()
	args := verifierArgs(t, "selftest", positive)
	args = append(args, deterministicGenerationArgs(t)...)
	selftest := runHarness(t, libraryProbe, nil, args...)
	assertHarnessSuccess(t, selftest)
	if string(selftest.stdout) != "ok\n" {
		t.Fatalf("direct library defensive-copy selftest stdout = %q", selftest.stdout)
	}

	for _, command := range []string{"binding", "verify", "report"} {
		var commandArgs []string
		if command == "binding" {
			commandArgs = []string{command}
		} else {
			commandArgs = verifierArgs(t, command, positive)
		}
		assertExactBoundaryParity(t, libraryProbe, harness, nil, commandArgs...)
	}
	assertEmptyEvidenceCannotSelectGeneration(t, libraryProbe, harness)

	libraryOutput := filepath.Join(t.TempDir(), "library")
	harnessOutput := filepath.Join(t.TempDir(), "harness")
	libraryArgs := generationArgs(t, libraryOutput)
	harnessArgs := generationArgs(t, harnessOutput)
	libraryResult := runHarness(t, libraryProbe, nil, libraryArgs...)
	harnessResult := runHarness(t, harness, nil, harnessArgs...)
	assertSameResult(t, libraryResult, harnessResult)
	assertHarnessSuccess(t, libraryResult)
	for _, name := range []string{"evidence.json", "report.json"} {
		if !bytes.Equal(readFile(t, filepath.Join(libraryOutput, name)), readFile(t, filepath.Join(harnessOutput, name))) {
			t.Fatalf("direct library and harness generated %s differ", name)
		}
	}
	assertGenerationInputsAreRequired(t, libraryProbe, harness)
}

func assertEmptyEvidenceCannotSelectGeneration(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	for _, command := range []string{"verify", "report"} {
		args := append([]string{command}, m7AndRegistryArgs(t)...)
		libraryResult := runHarness(t, libraryProbe, nil, args...)
		harnessResult := runHarness(t, harness, nil, args...)
		assertSameResult(t, libraryResult, harnessResult)
		assertHarnessCategory(t, libraryResult, "schema.evidence")
	}
}

func assertGenerationInputsAreRequired(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	for _, test := range []struct {
		flag, category string
	}{
		{flag: "--baseline-runtime", category: "provenance.runtime"},
		{flag: "--compared-runtime", category: "provenance.runtime"},
		{flag: "--capture-clock", category: "provenance.clock"},
		{flag: "--capture-timestamps", category: "provenance.clock"},
		{flag: "--masked-subjects", category: "provenance.auth_mask"},
	} {
		t.Run("generate requires "+strings.TrimPrefix(test.flag, "--"), func(t *testing.T) {
			libraryOutput := filepath.Join(t.TempDir(), "library")
			harnessOutput := filepath.Join(t.TempDir(), "harness")
			libraryArgs := withoutFlagPair(t, generationArgs(t, libraryOutput), test.flag)
			harnessArgs := withoutFlagPair(t, generationArgs(t, harnessOutput), test.flag)
			libraryResult := runHarness(t, libraryProbe, nil, libraryArgs...)
			harnessResult := runHarness(t, harness, nil, harnessArgs...)
			assertSameResult(t, libraryResult, harnessResult)
			assertHarnessCategory(t, libraryResult, test.category)
			for _, root := range []string{libraryOutput, harnessOutput} {
				if entries, err := os.ReadDir(root); err == nil && len(entries) != 0 {
					t.Fatalf("failed generation created partial artifacts in %s", root)
				} else if err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
			}
		})
	}
}

func withoutFlagPair(t *testing.T, args []string, flag string) []string {
	t.Helper()
	for index := 0; index < len(args); index++ {
		if args[index] != flag {
			continue
		}
		if index+1 >= len(args) {
			t.Fatalf("flag %s has no value", flag)
		}
		return append(append([]string(nil), args[:index]...), args[index+2:]...)
	}
	t.Fatalf("generation arguments do not contain %s", flag)
	return nil
}

func assertM7InputsAreAuthoritative(t *testing.T, libraryProbe, harness, positive string) {
	t.Helper()
	for _, flag := range []string{"--m7-graph", "--m7-replay", "--m7-registry", "--m7-source-bundle", "--m7-source-replay"} {
		flag := flag
		t.Run("substitute "+strings.TrimPrefix(flag, "--"), func(t *testing.T) {
			args := verifierArgs(t, "verify", positive)
			for index := range args {
				if args[index] != flag {
					continue
				}
				raw := readFile(t, args[index+1])
				if len(raw) < 2 {
					t.Fatal("M7 authority input unexpectedly empty")
				}
				altered := bytes.Clone(raw)
				altered[len(altered)/2] ^= 1
				path := filepath.Join(t.TempDir(), "substituted.json")
				if err := os.WriteFile(path, altered, 0o600); err != nil {
					t.Fatal(err)
				}
				args[index+1] = path
				assertFailureParity(t, libraryProbe, harness, args, "provenance.m7")
				return
			}
			t.Fatalf("missing verifier flag %s", flag)
		})
	}
}

func assertIndependentCapturedRuntimeVariant(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	evidence := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	evidence["evidence_class"] = "CAPTURED_RUNTIME_EVIDENCE"
	objectValue(t, evidence, "scope")["live_vr940_claim"] = false
	clock := objectValue(t, evidence, "capture_clock")
	clock["clock_id"] = "clock-fedcba9876543210fedcba9876543210"
	clock["wall_anchor_utc"] = "2031-11-12T13:14:15Z"
	clock["monotonic_epoch_id"] = "msp08-independent-synthetic-epoch"
	clock["clock_hash"] = domainDigest(t, clockDomainV1, withoutKeys(t, clock, "clock_hash"))

	for runIndex, rawRun := range arrayValue(t, evidence, "runs") {
		run := asObject(t, rawRun, "captured synthetic run")
		provenance := objectValue(t, run, "provenance")
		provenance["capture_clock_id"] = stringValue(t, clock, "clock_id")
		if runIndex > 0 {
			runtimeIdentity := objectValue(t, provenance, "runtime")
			runtimeIdentity["source_commit"] = strings.Repeat("e", 40)
			runtimeIdentity["artifact_digest"] = "sha256:" + strings.Repeat("f", 64)
			runtimeIdentity["artifact_id"] = "gateway:sha256:" + strings.Repeat("f", 64)
		}
		for _, rawView := range arrayValue(t, run, "protected_views") {
			view := asObject(t, rawView, "captured synthetic view")
			meta := objectValue(t, objectValue(t, view, "payload"), "meta")
			meta["captured_at"] = fmt.Sprintf("2031-11-12T13:14:%02dZ", 16+runIndex)
			meta["auth_subject"] = fmt.Sprintf("synthetic-masked-subject-%d", runIndex+1)
			refreshViewHashes(t, evidence, run, view)
		}
	}
	refreshEvidenceIdentity(t, evidence)
	path := filepath.Join(t.TempDir(), "captured-runtime-synthetic.json")
	writeJSON(t, path, evidence)
	if bytes.Contains(readFile(t, path), []byte("fixture-principal")) {
		t.Fatal("independent captured-runtime fixture retained canonical masked subjects")
	}
	if boolValue(t, objectValue(t, evidence, "scope"), "live_vr940_claim") {
		t.Fatal("synthetic captured-runtime material must not claim live VR940 proof")
	}
	for _, command := range []string{"verify", "report"} {
		args := verifierArgs(t, command, path)
		assertExactBoundaryParity(t, libraryProbe, harness, nil, args...)
		result := runHarness(t, libraryProbe, nil, args...)
		assertHarnessSuccess(t, result)
		if command == "verify" && string(result.stdout) != "ok\n" {
			t.Fatalf("captured-runtime verify stdout = %q", result.stdout)
		}
		if command == "report" {
			report := asObject(t, decodeJSON(t, result.stdout), "captured-runtime report")
			assertStringValue(t, report, "gate", "EEBUS-G18")
			assertStringValue(t, report, "verdict", "PASS")
		}
	}
}

func assertAllResourceCeilings(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	positive := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	tests := []struct {
		name string
		raw  func(*testing.T) []byte
	}{
		{name: "max_evidence_bytes=2097152", raw: func(t *testing.T) []byte { return bytes.Repeat([]byte(" "), int(limitsV1["max_evidence_bytes"])+1) }},
		{name: "max_depth=32", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			var value any = "leaf"
			for index := int64(0); index <= limitsV1["max_depth"]; index++ {
				value = map[string]any{"n": value}
			}
			e["depth_probe"] = value
			return canonicalJSON(t, e)
		}},
		{name: "max_runs=8", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			runs := arrayValue(t, e, "runs")
			for int64(len(runs)) <= limitsV1["max_runs"] {
				runs = append(runs, cloneValue(t, runs[0]))
			}
			e["runs"] = runs
			return canonicalJSON(t, e)
		}},
		{name: "max_views_per_run=16", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			run := asObject(t, arrayValue(t, e, "runs")[0], "run")
			views := arrayValue(t, run, "protected_views")
			for int64(len(views)) <= limitsV1["max_views_per_run"] {
				views = append(views, cloneValue(t, views[0]))
			}
			run["protected_views"] = views
			return canonicalJSON(t, e)
		}},
		{name: "max_inputs_per_run=16", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			provenance := objectValue(t, asObject(t, arrayValue(t, e, "runs")[0], "run"), "provenance")
			inputs := arrayValue(t, provenance, "immutable_inputs")
			for int64(len(inputs)) <= limitsV1["max_inputs_per_run"] {
				inputs = append(inputs, cloneValue(t, inputs[0]))
			}
			provenance["immutable_inputs"] = inputs
			return canonicalJSON(t, e)
		}},
		{name: "max_internal_facts_per_run=64", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			state := objectValue(t, findRun(t, e, "EEBUS_CONNECTED_CANDIDATE_ONLY"), "state_evidence")
			facts := make([]any, limitsV1["max_internal_facts_per_run"]+1)
			for index := range facts {
				facts[index] = map[string]any{
					"candidate_id":            fmt.Sprintf("m7-candidate-synthetic-%04d", index),
					"status":                  "CANDIDATE",
					"terminal_negative_state": nil,
					"visibility_channel":      "CANDIDATE_DEBUG_REPLAY",
				}
			}
			state["facts"] = facts
			return canonicalJSON(t, e)
		}},
		{name: "max_payload_bytes=262144", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			view := findView(t, findRun(t, e, "EEBUS_DISABLED_CONFIRMED"), "debug.ebus")
			large := make(map[string]any, 18000)
			for index := 0; index < 18000; index++ {
				large[fmt.Sprintf("k%05d", index)] = "12345678"
			}
			objectValue(t, objectValue(t, view, "payload"), "data")["bounded_probe"] = large
			return canonicalJSON(t, e)
		}},
		{name: "max_string_bytes=4096", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			e["string_probe"] = strings.Repeat("x", int(limitsV1["max_string_bytes"])+1)
			return canonicalJSON(t, e)
		}},
		{name: "max_total_members=65536", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			members := make(map[string]any, 64000)
			for index := 0; index < 64000; index++ {
				members[fmt.Sprintf("m%05d", index)] = 0
			}
			e["member_probe"] = members
			return canonicalJSON(t, e)
		}},
		{name: "max_total_list_items=32768", raw: func(t *testing.T) []byte {
			e := cloneObject(t, positive)
			items := make([]any, limitsV1["max_total_list_items"]+1)
			for index := range items {
				items[index] = 0
			}
			e["list_probe"] = items
			return canonicalJSON(t, e)
		}},
	}
	if len(tests) != len(limitsV1) {
		t.Fatalf("resource tests = %d; want all %d exact ceilings", len(tests), len(limitsV1))
	}
	for _, test := range tests {
		t.Run(test.name+" bounded before schema and recursive validation", func(t *testing.T) {
			raw := test.raw(t)
			if test.name != "max_evidence_bytes=2097152" && int64(len(raw)) > limitsV1["max_evidence_bytes"] {
				t.Fatalf("%s probe accidentally exceeds earlier byte ceiling: %d", test.name, len(raw))
			}
			path := filepath.Join(t.TempDir(), "limit.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			assertFailureParity(t, libraryProbe, harness, verifierArgs(t, "verify", path), "limits.exceeded")
		})
	}
}

func assertClosedJSONAndOrdering(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	positivePath := filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json")
	raw := readFile(t, positivePath)
	jsonCases := map[string][]byte{
		"malformed UTF-8": append([]byte{'{', '"', 'x', '"', ':', '"'}, 0xff, '"', '}'),
		"duplicate keys":  append([]byte("{\"contract\":\""+evidenceContractV1+"\","), raw[1:]...),
		"fraction":        bytes.Replace(raw, []byte("\"schema_version\": 1"), []byte("\"schema_version\": 1.5"), 1),
		"negative zero":   bytes.Replace(raw, []byte("\"schema_version\": 1"), []byte("\"schema_version\": -0"), 1),
		"unsafe integer":  bytes.Replace(raw, []byte("\"schema_version\": 1"), []byte("\"schema_version\": 9007199254740992"), 1),
	}
	for name, malformed := range jsonCases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "evidence.json")
			if err := os.WriteFile(path, malformed, 0o600); err != nil {
				t.Fatal(err)
			}
			assertFailureParity(t, libraryProbe, harness, verifierArgs(t, "verify", path), "json.syntax")
		})
	}

	positive := loadObject(t, positivePath)
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, map[string]any)
	}{
		{name: "omitted top-level field", mutate: func(t *testing.T, e map[string]any) { delete(e, "scope") }},
		{name: "added unknown field", mutate: func(t *testing.T, e map[string]any) { e["unknown"] = true }},
		{name: "duplicate run identity", mutate: func(t *testing.T, e map[string]any) {
			runs := arrayValue(t, e, "runs")
			asObject(t, runs[1], "run")["run_id"] = stringValue(t, asObject(t, runs[0], "run"), "run_id")
		}},
		{name: "reordered run identity", mutate: func(t *testing.T, e map[string]any) {
			runs := arrayValue(t, e, "runs")
			runs[1], runs[2] = runs[2], runs[1]
		}},
		{name: "duplicate view identity", mutate: func(t *testing.T, e map[string]any) {
			views := arrayValue(t, asObject(t, arrayValue(t, e, "runs")[0], "run"), "protected_views")
			asObject(t, views[1], "view")["view_id"] = stringValue(t, asObject(t, views[0], "view"), "view_id")
		}},
		{name: "reordered view identity", mutate: func(t *testing.T, e map[string]any) {
			views := arrayValue(t, asObject(t, arrayValue(t, e, "runs")[0], "run"), "protected_views")
			views[0], views[1] = views[1], views[0]
		}},
		{name: "duplicate provenance identity", mutate: func(t *testing.T, e map[string]any) { applyMutation(t, e, "DUPLICATE_PROVENANCE") }},
		{name: "reordered provenance identity", mutate: func(t *testing.T, e map[string]any) {
			inputs := arrayValue(t, objectValue(t, asObject(t, arrayValue(t, e, "runs")[0], "run"), "provenance"), "immutable_inputs")
			inputs[0], inputs[1] = inputs[1], inputs[0]
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := cloneObject(t, positive)
			test.mutate(t, e)
			path := filepath.Join(t.TempDir(), "evidence.json")
			writeJSON(t, path, e)
			category := "ordering.duplicate"
			if strings.Contains(test.name, "omitted") || strings.Contains(test.name, "added") {
				category = "schema.evidence"
			}
			assertFailureParity(t, libraryProbe, harness, verifierArgs(t, "verify", path), category)
		})
	}
}

func assertEveryViewRejectsEEBUSDriftAndLeaks(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	positive := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	for _, viewID := range protectedViewsV1 {
		viewID := viewID
		for _, variant := range []struct {
			name, key, value, category string
		}{
			{name: "neutral eeBUS substitution", key: "eebus_observation", value: "neutral-synthetic-substitution", category: "drift.consumer"},
			{name: "candidate leak", key: "candidate_status", value: "CANDIDATE", category: "anti_leak.candidate"},
			{name: "conflict leak", key: "conflict_status", value: "WITHHELD/CONFLICT", category: "anti_leak.candidate"},
		} {
			variant := variant
			t.Run(viewID+"/"+variant.name, func(t *testing.T) {
				e := cloneObject(t, positive)
				run := findRun(t, e, "EEBUS_CONNECTED_CANDIDATE_ONLY")
				view := findView(t, run, viewID)
				objectValue(t, objectValue(t, view, "payload"), "data")[variant.key] = variant.value
				refreshViewHashes(t, e, run, view)
				path := filepath.Join(t.TempDir(), "evidence.json")
				writeJSON(t, path, e)
				assertFailureParity(t, libraryProbe, harness, verifierArgs(t, "verify", path), variant.category)
			})
		}
	}
}

func assertV1SurfaceAndPrecedenceClosure(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	positive := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	for _, test := range []struct {
		name     string
		viewID   string
		mutation func(map[string]any)
		category string
	}{
		{name: "direct eebus contract V2", viewID: "mcp.eebus.v1.contract", mutation: func(data map[string]any) {
			data["version"] = 2
			data["public_v2"] = true
			data["namespace"] = "eebus.v2"
		}, category: "gate.scope"},
		{name: "direct GraphQL stable surface", viewID: "graphql.schema", mutation: func(data map[string]any) {
			data["query_fields"] = append(data["query_fields"].([]any), "eebusCandidate")
		}, category: "gate.scope"},
		{name: "direct semantic stable surface", viewID: "semantic.registry", mutation: func(data map[string]any) { data["eebus_v2"] = map[string]any{"stable": true} }, category: "gate.scope"},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := cloneObject(t, positive)
			run := findRun(t, e, "EEBUS_DISABLED_CONFIRMED")
			view := findView(t, run, test.viewID)
			test.mutation(objectValue(t, objectValue(t, view, "payload"), "data"))
			refreshViewHashes(t, e, run, view)
			path := filepath.Join(t.TempDir(), "evidence.json")
			writeJSON(t, path, e)
			assertFailureParity(t, libraryProbe, harness, verifierArgs(t, "verify", path), test.category)
		})
	}

	covered := map[string]bool{}
	for _, category := range negativeCategoriesV1 {
		covered[category] = true
	}
	for _, category := range []string{"json.syntax", "registry.binding", "hash.evidence", "authority.ebus"} {
		covered[category] = true
	}
	for _, category := range validationPrecedenceV1 {
		if !covered[category] {
			t.Fatalf("amended RED does not exercise precedence category %s", category)
		}
	}
	for index := 0; index < len(validationPrecedenceV1)-1; index++ {
		if validationPrecedenceV1[index] == validationPrecedenceV1[index+1] {
			t.Fatalf("duplicate precedence category at %d", index)
		}
	}

	for _, test := range []struct {
		name      string
		mutations []string
		category  string
	}{
		{name: "schema plus anti-leak has no partial report", mutations: []string{"CANDIDATE_LEAK_EBUS_MCP", "MISSING_PROVENANCE"}, category: "schema.evidence"},
		{name: "M7 plus runtime has no partial report", mutations: []string{"M7_GRAPH_MISMATCH", "RUNTIME_ARTIFACT_MISMATCH"}, category: "provenance.m7"},
		{name: "runtime plus config has no partial report", mutations: []string{"CONFIG_HASH_MISMATCH", "RUNTIME_ARTIFACT_MISMATCH"}, category: "provenance.runtime"},
		{name: "auth plus clock has no partial report", mutations: []string{"CLOCK_MISMATCH", "MASK_SCOPE_MISMATCH"}, category: "provenance.auth_mask"},
		{name: "clock plus ordering has no partial report", mutations: []string{"DUPLICATE_PROVENANCE", "STALE_CAPTURE"}, category: "provenance.clock"},
		{name: "coverage plus hash has no partial report", mutations: []string{"CANONICAL_HASH_MISMATCH", "MISSING_REQUIRED_VIEW"}, category: "view.coverage"},
		{name: "anti-leak plus authority has no partial report", mutations: []string{"CANDIDATE_LEAK_EBUS_MCP", "ROLLBACK_DRIFT"}, category: "anti_leak.candidate"},
		{name: "gate plus drift has no partial report", mutations: []string{"DROPPED_PAYLOAD_FIELD", "G19_CLAIM"}, category: "gate.scope"},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := cloneObject(t, positive)
			for _, mutation := range test.mutations {
				applyMutation(t, e, mutation)
			}
			path := filepath.Join(t.TempDir(), "evidence.json")
			writeJSON(t, path, e)
			assertFailureParity(t, libraryProbe, harness, verifierArgs(t, "verify", path), test.category)
		})
	}
}

func assertAllTwentyPrecedenceCombinations(t *testing.T, libraryProbe, harness string) {
	t.Helper()
	type fault struct {
		category string
		mutate   func(*testing.T, map[string]any)
		args     func(*testing.T, []string) []string
		raw      func(*testing.T, map[string]any) []byte
	}
	faults := []fault{
		{category: "json.syntax", raw: func(t *testing.T, evidence map[string]any) []byte {
			return append(canonicalJSON(t, evidence), '{')
		}},
		{category: "limits.exceeded", mutate: func(t *testing.T, evidence map[string]any) {
			items := make([]any, limitsV1["max_total_list_items"]+1)
			for index := range items {
				items[index] = 0
			}
			evidence["precedence_limit_probe"] = items
		}},
		{category: "schema.evidence", mutate: func(t *testing.T, evidence map[string]any) { evidence["precedence_unknown"] = true }},
		{category: "registry.binding", args: func(t *testing.T, args []string) []string {
			registry := loadObject(t, filepath.Join(packageDir(t), "contracts/multi-runtime-coexistence-registry-v1.json"))
			registry["contract"] = "helianthus.platform.substituted-registry.v1"
			path := filepath.Join(t.TempDir(), "registry.json")
			writeJSON(t, path, registry)
			for index := range args {
				if args[index] == "--registry" {
					args[index+1] = path
					return args
				}
			}
			t.Fatal("registry argument absent")
			return nil
		}},
		{category: "provenance.m7", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "M7_GRAPH_MISMATCH") }},
		{category: "provenance.runtime", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "RUNTIME_ARTIFACT_MISMATCH") }},
		{category: "provenance.config", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "CONFIG_HASH_MISMATCH") }},
		{category: "provenance.auth_mask", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "MASK_SCOPE_MISMATCH") }},
		{category: "provenance.clock", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "STALE_CAPTURE") }},
		{category: "ordering.duplicate", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "DUPLICATE_PROVENANCE") }},
		{category: "state.evidence", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "NO_SERVICES_EMPTY_SUCCESS") }},
		{category: "view.coverage", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "MISSING_REQUIRED_VIEW") }},
		{category: "canonicalization.invalid", mutate: func(t *testing.T, evidence map[string]any) {
			applyMutation(t, evidence, "TIMESTAMP_EXCLUSION_MISMATCH")
		}},
		{category: "hash.payload", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "CANONICAL_HASH_MISMATCH") }},
		{category: "anti_leak.candidate", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "CANDIDATE_LEAK_EBUS_MCP") }},
		{category: "authority.ebus", mutate: func(t *testing.T, evidence map[string]any) {
			run := findRun(t, evidence, "EEBUS_CONNECTED_CANDIDATE_ONLY")
			view := findView(t, run, "semantic.registry")
			objectValue(t, objectValue(t, view, "payload"), "data")["authority"] = "eebus-candidate"
			refreshViewHashes(t, evidence, run, view)
		}},
		{category: "gate.scope", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "G17_CLAIM") }},
		{category: "drift.consumer", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "DROPPED_PAYLOAD_FIELD") }},
		{category: "rollback.drift", mutate: func(t *testing.T, evidence map[string]any) { applyMutation(t, evidence, "ROLLBACK_DRIFT") }},
		{category: "hash.evidence"},
	}
	gotOrder := make([]string, len(faults))
	for index, item := range faults {
		gotOrder[index] = item.category
	}
	if !reflect.DeepEqual(gotOrder, validationPrecedenceV1) {
		t.Fatalf("precedence fault order = %v; want %v", gotOrder, validationPrecedenceV1)
	}
	positive := loadObject(t, filepath.Join(packageDir(t), "testdata/canonical/positive/evidence.json"))
	for index, item := range faults {
		item := item
		t.Run(fmt.Sprintf("%02d_%s_before_final_hash_no_partial_report", index+1, item.category), func(t *testing.T) {
			evidence := cloneObject(t, positive)
			if item.mutate != nil {
				item.mutate(t, evidence)
			}
			evidence["evidence_hash"] = "sha256:" + strings.Repeat("f", 64)
			evidence["evidence_id"] = "mrcv1:" + stringValue(t, evidence, "evidence_hash")
			raw := canonicalJSON(t, evidence)
			if item.raw != nil {
				raw = item.raw(t, evidence)
			}
			path := filepath.Join(t.TempDir(), "evidence.json")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
			args := verifierArgs(t, "verify", path)
			if item.args != nil {
				args = item.args(t, args)
			}
			assertFailureParity(t, libraryProbe, harness, args, item.category)
		})
	}
}

func assertProductionLibraryUsesCandidateFactsAuthority(t *testing.T) {
	t.Helper()
	root := packageDir(t)
	fset := token.NewFileSet()
	found := map[string]bool{"Verify": false, "Replay": false}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, filepath.Join(root, entry.Name()), nil, parser.SkipObjectResolution)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		for _, imported := range file.Imports {
			if strings.Trim(imported.Path.Value, "\"") != "github.com/Project-Helianthus/helianthus-ebusgateway/internal/candidatefacts" {
				continue
			}
			alias := "candidatefacts"
			if imported.Name != nil {
				alias = imported.Name.Name
			}
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if ok && identifier.Name == alias {
					if _, required := found[selector.Sel.Name]; required {
						found[selector.Sel.Name] = true
					}
				}
				return true
			})
		}
	}
	for name, present := range found {
		if !present {
			t.Fatalf("production coexistence library must delegate M7 authority to candidatefacts.%s", name)
		}
	}
}

func assertHarnessIsStrictlyThin(t *testing.T) {
	t.Helper()
	root := filepath.Join(packageDir(t), "cmd/msp08harness")
	var files []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && filepath.Ext(path) == ".go" && !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("private MSP-08 harness production source absent")
	}
	allowedImports := map[string]bool{
		"bytes": true, "encoding/json": true, "errors": true, "flag": true, "fmt": true, "io": true, "os": true, "path/filepath": true, "strings": true,
		"github.com/Project-Helianthus/helianthus-ebusgateway/internal/coexistence": true,
	}
	requiredCalls := map[string]bool{"Binding": false, "Verify": false, "Report": false, "Generate": false}
	boundedRead := false
	fset := token.NewFileSet()
	for _, path := range files {
		raw := readFile(t, path)
		for _, forbidden := range []string{"net.", "http.", "time.Now", "candidatefacts", "syncevidence", "cmd/gateway", "graphql", "portal", "transport", "listener", "socket"} {
			if bytes.Contains(bytes.ToLower(raw), bytes.ToLower([]byte(forbidden))) {
				t.Fatalf("thin harness %s contains forbidden wiring/validation token %q", path, forbidden)
			}
		}
		for _, duplicatedValidationToken := range append(append([]string(nil), validationPrecedenceV1...), rawPayloadDomainV1, shapeDomainV1, canonicalPayloadDomainV1, evidenceContractV1) {
			if bytes.Contains(raw, []byte(duplicatedValidationToken)) {
				t.Fatalf("thin harness %s duplicates library validation token %q", path, duplicatedValidationToken)
			}
		}
		file, err := parser.ParseFile(fset, path, raw, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			name := strings.Trim(imported.Path.Value, "\"")
			if !allowedImports[name] {
				t.Fatalf("thin harness imports %q", name)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "LimitReader" {
				boundedRead = true
			}
			if identifier, ok := selector.X.(*ast.Ident); ok && identifier.Name == "coexistence" {
				if _, required := requiredCalls[selector.Sel.Name]; required {
					requiredCalls[selector.Sel.Name] = true
				}
			}
			return true
		})
	}
	if !boundedRead {
		t.Fatal("thin harness local file reads must use io.LimitReader before allocation")
	}
	for name, called := range requiredCalls {
		if !called {
			t.Fatalf("thin harness must call coexistence.%s directly", name)
		}
	}
}

func assertFailureParity(t *testing.T, libraryProbe, harness string, verifyArgs []string, category string) {
	t.Helper()
	for _, command := range []string{"verify", "report"} {
		args := append([]string(nil), verifyArgs...)
		args[0] = command
		libraryResult := runHarness(t, libraryProbe, nil, args...)
		harnessResult := runHarness(t, harness, nil, args...)
		assertSameResult(t, libraryResult, harnessResult)
		assertHarnessCategory(t, libraryResult, category)
		if bytes.HasPrefix(bytes.TrimSpace(libraryResult.stdout), []byte("{")) {
			t.Fatalf("%s emitted a partial report for %s", command, category)
		}
	}
}

func assertExactBoundaryParity(t *testing.T, libraryProbe, harness string, environment []string, args ...string) {
	t.Helper()
	assertSameResult(t, runHarness(t, libraryProbe, environment, args...), runHarness(t, harness, environment, args...))
}

func assertSameResult(t *testing.T, left, right harnessResult) {
	t.Helper()
	if left.exitCode != right.exitCode || !bytes.Equal(left.stdout, right.stdout) || !bytes.Equal(left.stderr, right.stderr) {
		t.Fatalf("direct-library/subprocess parity mismatch: library=(exit=%d stdout=%q stderr=%q), harness=(exit=%d stdout=%q stderr=%q)", left.exitCode, left.stdout, left.stderr, right.exitCode, right.stdout, right.stderr)
	}
}

func refreshEvidenceIdentity(t *testing.T, evidence map[string]any) {
	t.Helper()
	digest := domainDigest(t, evidenceDomainV1, withoutKeys(t, evidence, "evidence_id", "evidence_hash"))
	evidence["evidence_hash"] = digest
	evidence["evidence_id"] = "mrcv1:" + digest
}

func TestMSP08AmendedREDSurfaceIsCompleteAndTestsOnly(t *testing.T) {
	if len(validationPrecedenceV1) != 20 || len(limitsV1) != 10 || len(protectedViewsV1) != 11 || len(negativeCategoriesV1) != 22 || len(scenarioOrderV1) != 6 {
		t.Fatal("MSP-08 amended RED cardinalities drifted")
	}
	if !reflect.DeepEqual(scenarioOrderV1, []string{
		"EEBUS_DISABLED_BASELINE", "EEBUS_DISABLED_CONFIRMED", "EEBUS_ENABLED_NO_SERVICES", "EEBUS_CONNECTED_CANDIDATE_ONLY", "EEBUS_CONFLICTED_WITHHELD", "EEBUS_DISABLED_ROLLBACK",
	}) {
		t.Fatal("MSP-08 six-state sequence drifted")
	}
	wantViews := append([]string(nil), protectedViewsV1...)
	sort.Strings(wantViews)
	if len(wantViews) != 11 || wantViews[0] == wantViews[len(wantViews)-1] {
		t.Fatal("MSP-08 protected-view set is not closed")
	}
	if strings.Contains(strings.ToLower(string(readFile(t, filepath.Join(packageDir(t), "testdata/libraryprobe/main.go")))), "vr940 claim true") {
		t.Fatal("library probe claims live VR940 proof")
	}
	probe := string(readFile(t, filepath.Join(packageDir(t), "testdata/libraryprobe/main.go")))
	for _, required := range []string{"coexistence.GenerateInputsV1", "assertExactAPI", "Evidence must not be representable", "has the wrong input boundary"} {
		if !strings.Contains(probe, required) {
			t.Fatalf("direct-library API probe is missing %q", required)
		}
	}
	if strings.Contains(probe, "coexistence.Generate(readInputs(") || strings.Contains(probe, "coexistence.Generate(coexistence.InputsV1") {
		t.Fatal("Generate still accepts verification InputsV1 or uses empty Evidence as a mode switch")
	}
}
