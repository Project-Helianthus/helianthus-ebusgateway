package coexistence

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const (
	productionHarnessPackage  = "./internal/coexistence/cmd/msp08harness"
	evidenceContractV1        = "helianthus.platform.multi-runtime-coexistence-evidence.v1"
	reportContractV1          = "helianthus.platform.multi-runtime-coexistence-report.v1"
	registryContractV1        = "helianthus.platform.multi-runtime-coexistence-registry.v1"
	negativeFixtureContractV1 = "helianthus.platform.multi-runtime-coexistence-negative-fixture.v1"
	rawPayloadDomainV1        = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-RAW-PAYLOAD:V1"
	shapeDomainV1             = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-PAYLOAD-SHAPE:V1"
	canonicalPayloadDomainV1  = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-CANONICAL-PAYLOAD:V1"
	normalizationDomainV1     = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-NORMALIZATION:V1"
	clockDomainV1             = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-CLOCK:V1"
	buildDomainV1             = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-BUILD:V1"
	configDomainV1            = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-CONFIG:V1"
	authDomainV1              = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-AUTH:V1"
	evidenceDomainV1          = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-EVIDENCE:V1"
	reportDomainV1            = "HELIANTHUS:MULTI-RUNTIME-COEXISTENCE-REPORT:V1"
)

var scenarioOrderV1 = []string{
	"EEBUS_DISABLED_BASELINE",
	"EEBUS_DISABLED_CONFIRMED",
	"EEBUS_ENABLED_NO_SERVICES",
	"EEBUS_CONNECTED_CANDIDATE_ONLY",
	"EEBUS_CONFLICTED_WITHHELD",
	"EEBUS_DISABLED_ROLLBACK",
}

var capturedScenarioOrderV1 = []string{
	"EEBUS_CONNECTED_BASELINE",
	"EEBUS_CONNECTED_RAW_WITHHELD",
	"EEBUS_RESTART_PERSISTED",
	"EEBUS_CONNECTED_ROLLBACK",
}

var protectedViewsV1 = []string{
	"mcp.ebus.v1.responses",
	"mcp.tool.inventory",
	"graphql.schema",
	"graphql.ebus.values",
	"ha.graphql.values",
	"ha.identity",
	"debug.ebus",
	"portal.ebus.bootstrap",
	"command.routing",
	"semantic.registry",
	"mcp.eebus.v1.contract",
}

var validationPrecedenceV1 = []string{
	"json.syntax",
	"limits.exceeded",
	"schema.evidence",
	"registry.binding",
	"provenance.m7",
	"provenance.runtime",
	"provenance.config",
	"provenance.auth_mask",
	"provenance.clock",
	"ordering.duplicate",
	"state.evidence",
	"view.coverage",
	"canonicalization.invalid",
	"hash.payload",
	"anti_leak.candidate",
	"redaction.public",
	"authority.ebus",
	"gate.scope",
	"drift.consumer",
	"rollback.drift",
	"hash.evidence",
}

var limitsV1 = map[string]int64{
	"max_evidence_bytes":         2_097_152,
	"max_depth":                  32,
	"max_runs":                   8,
	"max_views_per_run":          16,
	"max_inputs_per_run":         27,
	"max_internal_facts_per_run": 64,
	"max_payload_bytes":          262_144,
	"max_string_bytes":           4_096,
	"max_total_members":          65_536,
	"max_total_list_items":       32_768,
}

var negativeCategoriesV1 = map[string]string{
	"candidate-leak-ebus-mcp.json":      "anti_leak.candidate",
	"canonical-hash-mismatch.json":      "hash.payload",
	"clock-mismatch.json":               "provenance.clock",
	"config-hash-mismatch.json":         "provenance.config",
	"conflict-leak-graphql.json":        "anti_leak.candidate",
	"dropped-payload-field.json":        "drift.consumer",
	"duplicate-provenance.json":         "ordering.duplicate",
	"g17-claim.json":                    "gate.scope",
	"g19-claim.json":                    "gate.scope",
	"input-hash-mismatch.json":          "provenance.runtime",
	"m7-graph-mismatch.json":            "provenance.m7",
	"mask-scope-mismatch.json":          "provenance.auth_mask",
	"missing-provenance.json":           "schema.evidence",
	"missing-required-view.json":        "view.coverage",
	"no-services-empty-success.json":    "state.evidence",
	"public-v2-surface.json":            "gate.scope",
	"resource-limit-exceeded.json":      "limits.exceeded",
	"rollback-drift.json":               "rollback.drift",
	"runtime-artifact-mismatch.json":    "provenance.runtime",
	"stale-capture.json":                "provenance.clock",
	"timestamp-exclusion-mismatch.json": "canonicalization.invalid",
	"unknown-field.json":                "schema.evidence",
}

func TestMSP08PinsExecutableV1ArtifactsAndM7Inputs(t *testing.T) {
	root := packageDir(t)
	want := map[string]string{
		"contracts/multi-runtime-coexistence-evidence-v1.schema.json": evidenceSchemaSHA256,
		"contracts/multi-runtime-coexistence-report-v1.schema.json":   reportSchemaSHA256,
		"contracts/multi-runtime-coexistence-registry-v1.json":        registrySHA256,
		"contracts/draft-candidate-fact-public-status-v1.schema.json": statusSchemaSHA256,
		"testdata/canonical/positive/evidence.json":                   positiveEvidenceSHA,
		"testdata/canonical/positive/report.json":                     positiveReportSHA,
		"testdata/canonical/positive/live-public-status.json":         liveStatusSHA256,
	}
	for path, digest := range want {
		assertFileSHA256(t, filepath.Join(root, path), digest)
	}

	m7 := map[string]string{
		"internal/candidatefacts/testdata/canonical/positive/graph.json":                         "b5c5d79e540a1691ee60c6db3e9405a92d9d544d871c74b26800fe449a318b0e",
		"internal/candidatefacts/testdata/canonical/positive/replay-result.json":                 "8280f6278ffe8598dfd767bb5bf9e60dce3c145b4612174b7c5a32fbff282f5c",
		"internal/candidatefacts/contracts/draft-candidate-fact-registry-v1.json":                "e6895b8d7406b58ed97599d8da7e9bd3b252e6e7ca3b0578ec6385bfe6dfe1c0",
		"internal/candidatefacts/testdata/canonical/source/bundle.json":                          "e6db2862f9001148deb6f40e286ee5f1eef2907812685a9b48128ddbfca5ce5a",
		"internal/candidatefacts/testdata/canonical/source/replay-result.json":                   "3061c507677f1f41861c20096ff7581ccb6e35c2e01bf66a568e2277df285539",
		"internal/candidatefacts/testdata/canonical/positive/source-terminal-graph.json":         "2fd356d9d3262281bcf830154d8507bbb237f3f0d091b737365e3812cdeaafb3",
		"internal/candidatefacts/testdata/canonical/positive/source-terminal-replay-result.json": "ba478963171fc238e76ee19036119e0f2543d98f73c35c124abef4028b2fae22",
		"internal/candidatefacts/testdata/canonical/source/source-terminal-bundle.json":          "b7269e608dfc004f6737fd80b34a1c6018483942632e0c3c8bfd33cadbaa8134",
		"internal/candidatefacts/testdata/canonical/source/source-terminal-replay-result.json":   "82ed1c66222348746abd9f4291e10b968314363160ba4cfc568044390aced9d2",
	}
	for path, digest := range m7 {
		assertFileSHA256(t, filepath.Join(repoDir(t), path), digest)
	}
}

func TestMSP08RegistryClosesProfilesViewsPrecedenceAndBounds(t *testing.T) {
	registry := loadObject(t, filepath.Join(packageDir(t), "contracts/multi-runtime-coexistence-registry-v1.json"))
	assertStringValue(t, registry, "contract", registryContractV1)
	assertIntValue(t, registry, "version", 1)
	assertStringValue(t, registry, "gate", "EEBUS-G18")
	assertStringSlice(t, registry["excluded_gates"], []string{"EEBUS-G17", "EEBUS-G19"})
	profiles := objectValue(t, registry, "scenario_profiles")
	assertStringSlice(t, profiles["SYNTHETIC_OFFLINE_FIXTURE"], scenarioOrderV1)
	assertStringSlice(t, profiles["CAPTURED_RUNTIME_EVIDENCE"], capturedScenarioOrderV1)
	assertStringSlice(t, registry["protected_views"], protectedViewsV1)
	assertStringSlice(t, registry["validation_precedence"], validationPrecedenceV1)
	if got := intMap(t, objectValue(t, registry, "limits")); !reflect.DeepEqual(got, limitsV1) {
		t.Fatalf("registry limits = %#v; want %#v", got, limitsV1)
	}

	synthetic := objectValue(t, registry, "m7_synthetic_predecessor")
	assertStringValue(t, synthetic, "source_commit", coexBaselineGatewayCommit)
	assertStringValue(t, synthetic, "docs_source_commit", coexSyntheticDocsCommit)
	live := objectValue(t, registry, "m7_live_predecessor")
	assertStringValue(t, live, "source_commit", coexLiveGatewayCommit)
	assertStringValue(t, live, "docs_source_commit", coexLiveDocsCommit)
	assertStringValue(t, live, "binding_mode", "VALIDATED_INPUTS_AND_REGENERATED_REPLAY")

	rules := arrayValue(t, registry, "view_rules")
	if len(rules) != len(protectedViewsV1) {
		t.Fatalf("view rules = %d; want %d", len(rules), len(protectedViewsV1))
	}
	for index, raw := range rules {
		rule := asObject(t, raw, fmt.Sprintf("view_rules[%d]", index))
		assertStringValue(t, rule, "view_id", protectedViewsV1[index])
		assertStringSlice(t, rule["timestamp_pointers"], []string{"/meta/captured_at"})
		assertStringSlice(t, rule["mask_pointers"], []string{"/meta/auth_subject"})
		if capturePath := stringValue(t, rule, "capture_path"); !strings.HasPrefix(capturePath, "artifacts/protected/") {
			t.Fatalf("capture path %q is outside artifacts/protected", capturePath)
		}
	}
}

func TestMSP08SchemasAreClosedV1Only(t *testing.T) {
	root := packageDir(t)
	for _, test := range []struct {
		path     string
		contract string
		required []string
	}{
		{
			path:     "contracts/multi-runtime-coexistence-evidence-v1.schema.json",
			contract: "https://docs.helianthus.local/schemas/multi-runtime-coexistence-evidence-v1.schema.json",
			required: []string{"contract", "schema_version", "fixture_id", "evidence_class", "export_tier", "evidence_id", "evidence_hash", "registry", "scope", "m7_binding", "m7_live_status", "capture_clock", "normalization", "limits", "runs"},
		},
		{
			path:     "contracts/multi-runtime-coexistence-report-v1.schema.json",
			contract: "https://docs.helianthus.local/schemas/multi-runtime-coexistence-report-v1.schema.json",
			required: []string{"contract", "schema_version", "fixture_id", "evidence_class", "export_tier", "report_id", "report_hash", "evidence_id", "evidence_hash", "gate", "verdict", "m7_binding", "baseline", "scenarios", "acceptance_matrix", "rollback"},
		},
	} {
		schema := loadObject(t, filepath.Join(root, test.path))
		assertStringValue(t, schema, "$id", test.contract)
		if additional, ok := schema["additionalProperties"].(bool); !ok || additional {
			t.Fatalf("%s must reject unknown top-level fields", test.path)
		}
		assertStringSlice(t, schema["required"], test.required)
		if properties := objectValue(t, schema, "properties"); len(properties) != len(test.required) {
			t.Fatalf("%s has %d properties; want %d", test.path, len(properties), len(test.required))
		}
	}
}

func TestMSP08NegativeMutationInventoryIsExact(t *testing.T) {
	root := filepath.Join(packageDir(t), "testdata/canonical/negative")
	files, err := filepath.Glob(filepath.Join(root, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(files))
	for _, path := range files {
		got = append(got, filepath.Base(path))
	}
	sort.Strings(got)
	if want := sortedKeys(negativeCategoriesV1); !reflect.DeepEqual(got, want) {
		t.Fatalf("negative fixture inventory = %v; want %v", got, want)
	}
	for _, name := range got {
		descriptor := loadObject(t, filepath.Join(root, name))
		assertStringValue(t, descriptor, "contract", negativeFixtureContractV1)
		if fixtureID := stringValue(t, descriptor, "fixture_id"); !strings.HasPrefix(fixtureID, "MSP08-G18-SYNTHETIC-NEG-") {
			t.Fatalf("%s fixture id = %q", name, fixtureID)
		}
		if stringValue(t, descriptor, "mutation") == "" {
			t.Fatalf("%s has empty mutation", name)
		}
	}
}

func TestMSP08ProductionSurfaceExcludesObsoleteAuthorizationMachinery(t *testing.T) {
	// Source inspection is intentional here: forbidden exported/process authority
	// cannot be proven by one accepted runtime artifact.
	forbidden := []string{
		"completion_" + "token",
		"hosted_" + "run",
		"owner_" + "tree",
		"prose_" + "hash",
	}
	root := packageDir(t)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") || strings.Contains(path, string(filepath.Separator)+"testdata"+string(filepath.Separator)) {
			return nil
		}
		raw := bytes.ToLower(readFile(t, path))
		for _, token := range forbidden {
			if bytes.Contains(raw, []byte(token)) {
				return fmt.Errorf("production source %s retains obsolete authority token %q", path, token)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
