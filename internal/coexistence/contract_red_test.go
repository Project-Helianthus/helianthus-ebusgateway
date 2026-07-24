package coexistence

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const (
	docsOwnerRepository       = "Project-Helianthus/helianthus-docs-ebus"
	docsOwnerCommit           = "fa335b6f66c97f5f82519ae71f3078687d919800"
	docsOwnerTree             = "6afa212c29b9270edea031b78de39411505c27b6"
	docsExactHeadActionsRun   = uint64(29707590892)
	docsPostMainActionsRun    = uint64(29707652181)
	baselineGatewayCommit     = "ff511b035b85aef6123fb0853bb3d2f3af6fc01e"
	m7CompletionToken         = "MSP-07@ff511b035b85aef6123fb0853bb3d2f3af6fc01e"
	m7DocsSourceCommit        = "ea88fef23ecb154b08f70e7f94b36e1738ed08bf"
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

var ownerArtifactSHA256 = map[string]string{
	"docs/platform/multi-runtime-coexistence-no-drift-v1.md":                                    "92af0448093e5690dbb4a77cde63e76d8af43d2640f50c2b90400d3d66541050",
	"docs/platform/schemas/multi-runtime-coexistence-evidence-v1.schema.json":                   "b0c891ea44e073b9bab150f2142584db356ed5e0569d70112156355617235b5d",
	"docs/platform/schemas/multi-runtime-coexistence-report-v1.schema.json":                     "7769c91654319c208576d45ed5c9f10aaa133c6591f37a5ed1adeda0e8c4c25c",
	"docs/platform/schemas/multi-runtime-coexistence-registry-v1.json":                          "82cf854d335da31dcda65ff45a024cbb4a1ce515965cb8165122c0b4ef7d8505",
	"docs/platform/fixtures/coexistence-no-drift/v1/positive/evidence.json":                     "7fe0eced765b2599ee6341985d60edf86936485c75c56cb8c51892775f8d352b",
	"docs/platform/fixtures/coexistence-no-drift/v1/positive/report.json":                       "687662756a2c833548bbc15285c895442c70346bf3832bf21c3a8309a9da0007",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/candidate-leak-ebus-mcp.json":      "a19f30e188b30ef6eea49aad3330609aa0cbb834c308e747736a8806a417bf6d",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/canonical-hash-mismatch.json":      "baf75894c6d16db5c17aafbe760c96c7eda71a4619f91d9b4e4166c9c8e4398b",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/clock-mismatch.json":               "db0ee4072a7c6c6b3538e1464c6cc48a51265239835df4a2a63fd1194949ae90",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/config-hash-mismatch.json":         "aa6715ea27b7d4de5ed666f072ece1a9846fdd27fc63f9da2df9b1e0b536fdc3",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/conflict-leak-graphql.json":        "e06efffde2acce942ae134417a6a0ffa6863a1017a445d6614c90ad13c6ef27a",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/dropped-payload-field.json":        "d34dca1c986861d8722a4d00a7723838af66c1f512f9516d35e28549eda9ddb4",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/duplicate-provenance.json":         "cd20491ce774f2790300990c887d4ffba135c54fee4da3a3467d1651cca7c5d2",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/g17-claim.json":                    "9ad7f8479b061f0bc29b153027e4efcf053b8146de0b15b0a2b5bfa4e78cfed4",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/g19-claim.json":                    "48b8dd8bd160b3825d5d418692fa3d32a1745094f6a030bac9e178632e139a39",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/input-hash-mismatch.json":          "a3823ed0b9749d40d277ef7ef08d69f5d1bb12bd3f5a33a8cbda425cc4132d48",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/m7-graph-mismatch.json":            "9c89c4a4bebbd40575f076641e3c9b7ec970016b6de18fd800ef7eec974d988b",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/mask-scope-mismatch.json":          "a0f7ca01d41f3275ef0e88ea7ba0bf2b6edc033bd7a56c309c3fbc678d49e682",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/missing-provenance.json":           "749d2c27538322d5c6e9436950af7e6844c691af8c21248fb0fee52a03b9f9f5",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/missing-required-view.json":        "42af95c2deb90389400c028002773d77dab6e3d79e580142521a622ff9eb5a0b",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/no-services-empty-success.json":    "bdf0aec91f569189a2a7da4dd68c8e696cde2844a02ebe2c290700b373023252",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/public-v2-surface.json":            "ee5f5fde625fb7e4bb36ec7fa0ab9b55523c67e90d1b9ce9cac2974c3b0a4712",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/resource-limit-exceeded.json":      "28439a65dd7591a53d0c285e34be6e55a82ab975106c34c0d9584bf31fdcff1e",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/rollback-drift.json":               "c7043773a7c6997389f65b571f8d4a5d7b8cac1cc7bf0b449e6d2450ea066711",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/runtime-artifact-mismatch.json":    "770ae20b17c3f757476163ba513d11374191bc7f063828ae95697ff470632ffb",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/stale-capture.json":                "34517b862196a3fac24eaf852c116e064a2ff0019c2cdd02802339db90f42b77",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/timestamp-exclusion-mismatch.json": "66047ff619808f40e3ecc57baef9bb876a94c02f91deecd08b0b46d7f82a2def",
	"docs/platform/fixtures/coexistence-no-drift/v1/negative/unknown-field.json":                "11b90b388871a336e61b18ac01bfbba773ec7d91e41e3faaa4fd442b23332ed0",
	"scripts/validate_multi_runtime_coexistence.py":                                             "3579ba4429d46ed9eaa90f8e3c599bff11c60a32dbe032105040747b5bbf3faf",
	"scripts/generate_multi_runtime_coexistence_fixture.py":                                     "60c039e81c4a8b274c2cfa284cb16545379fa3a526acf932dd74ead53e400ab2",
}

var embeddedOwnerArtifacts = map[string]string{
	"docs/platform/schemas/multi-runtime-coexistence-evidence-v1.schema.json": "contracts/multi-runtime-coexistence-evidence-v1.schema.json",
	"docs/platform/schemas/multi-runtime-coexistence-report-v1.schema.json":   "contracts/multi-runtime-coexistence-report-v1.schema.json",
	"docs/platform/schemas/multi-runtime-coexistence-registry-v1.json":        "contracts/multi-runtime-coexistence-registry-v1.json",
	"docs/platform/fixtures/coexistence-no-drift/v1/positive/evidence.json":   "testdata/canonical/positive/evidence.json",
	"docs/platform/fixtures/coexistence-no-drift/v1/positive/report.json":     "testdata/canonical/positive/report.json",
}

var m7InputSHA256 = map[string]string{
	"internal/candidatefacts/testdata/canonical/positive/graph.json":          "b5c5d79e540a1691ee60c6db3e9405a92d9d544d871c74b26800fe449a318b0e",
	"internal/candidatefacts/testdata/canonical/positive/replay-result.json":  "8280f6278ffe8598dfd767bb5bf9e60dce3c145b4612174b7c5a32fbff282f5c",
	"internal/candidatefacts/contracts/draft-candidate-fact-registry-v1.json": "e6895b8d7406b58ed97599d8da7e9bd3b252e6e7ca3b0578ec6385bfe6dfe1c0",
	"internal/candidatefacts/testdata/canonical/source/bundle.json":           "e6db2862f9001148deb6f40e286ee5f1eef2907812685a9b48128ddbfca5ce5a",
	"internal/candidatefacts/testdata/canonical/source/replay-result.json":    "3061c507677f1f41861c20096ff7581ccb6e35c2e01bf66a568e2277df285539",
}

var scenarioOrderV1 = []string{
	"EEBUS_DISABLED_BASELINE",
	"EEBUS_DISABLED_CONFIRMED",
	"EEBUS_ENABLED_NO_SERVICES",
	"EEBUS_CONNECTED_CANDIDATE_ONLY",
	"EEBUS_CONFLICTED_WITHHELD",
	"EEBUS_DISABLED_ROLLBACK",
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
	"authority.ebus",
	"gate.scope",
	"drift.consumer",
	"rollback.drift",
	"hash.evidence",
}

var limitsV1 = map[string]int64{
	"max_evidence_bytes":         2097152,
	"max_depth":                  32,
	"max_runs":                   8,
	"max_views_per_run":          16,
	"max_inputs_per_run":         16,
	"max_internal_facts_per_run": 64,
	"max_payload_bytes":          262144,
	"max_string_bytes":           4096,
	"max_total_members":          65536,
	"max_total_list_items":       32768,
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

func TestMSP08PinnedOwnerArtifactsAndM7Inputs(t *testing.T) {
	if len(ownerArtifactSHA256) != 30 {
		t.Fatalf("owner artifact inventory has %d entries; want 30", len(ownerArtifactSHA256))
	}

	root := packageDir(t)
	for ownerPath, localPath := range embeddedOwnerArtifacts {
		assertFileSHA256(t, filepath.Join(root, localPath), ownerArtifactSHA256[ownerPath])
	}
	for name := range negativeCategoriesV1 {
		ownerPath := "docs/platform/fixtures/coexistence-no-drift/v1/negative/" + name
		assertFileSHA256(t, filepath.Join(root, "testdata/canonical/negative", name), ownerArtifactSHA256[ownerPath])
	}

	files, err := filepath.Glob(filepath.Join(root, "testdata/canonical/negative/*.json"))
	if err != nil {
		t.Fatal(err)
	}
	gotNames := make([]string, 0, len(files))
	for _, path := range files {
		gotNames = append(gotNames, filepath.Base(path))
	}
	wantNames := sortedKeys(negativeCategoriesV1)
	sort.Strings(gotNames)
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("negative fixture inventory = %v; want %v", gotNames, wantNames)
	}

	repo := repoDir(t)
	for path, digest := range m7InputSHA256 {
		assertFileSHA256(t, filepath.Join(repo, path), digest)
	}
}

func TestMSP08RegistryClosesG18VocabularyPrecedenceAndBounds(t *testing.T) {
	registry := loadObject(t, filepath.Join(packageDir(t), "contracts/multi-runtime-coexistence-registry-v1.json"))
	assertStringValue(t, registry, "contract", registryContractV1)
	assertIntValue(t, registry, "version", 1)
	assertStringValue(t, registry, "gate", "EEBUS-G18")
	assertStringValue(t, registry, "m7_completion_token", m7CompletionToken)
	assertStringValue(t, registry, "m7_docs_source_commit", m7DocsSourceCommit)
	assertStringSlice(t, registry["excluded_gates"], []string{"EEBUS-G17", "EEBUS-G19"})
	assertStringSlice(t, registry["scenario_order"], scenarioOrderV1)
	assertStringSlice(t, registry["protected_views"], protectedViewsV1)
	assertStringSlice(t, registry["validation_precedence"], validationPrecedenceV1)

	evidenceContract := objectValue(t, registry, "evidence_contract")
	assertStringValue(t, evidenceContract, "contract", evidenceContractV1)
	assertStringValue(t, evidenceContract, "schema_path", "docs/platform/schemas/multi-runtime-coexistence-evidence-v1.schema.json")
	reportContract := objectValue(t, registry, "report_contract")
	assertStringValue(t, reportContract, "contract", reportContractV1)
	assertStringValue(t, reportContract, "schema_path", "docs/platform/schemas/multi-runtime-coexistence-report-v1.schema.json")

	m7 := objectValue(t, registry, "m7_binding")
	assertStringValue(t, m7, "graph_contract", "helianthus.platform.draft-candidate-fact-graph.v1")
	assertStringValue(t, m7, "graph_id", "dcfgv1:sha256:00f2b3c48959605d311d0d3895ec924b475d8fa25ee4e236d32d6facbd32c4ac")
	assertStringValue(t, m7, "graph_hash", "sha256:00f2b3c48959605d311d0d3895ec924b475d8fa25ee4e236d32d6facbd32c4ac")
	assertStringValue(t, m7, "replay_contract", "helianthus.platform.draft-candidate-fact-replay.v1")
	assertStringValue(t, m7, "replay_id", "dcfrv1:sha256:0d3d6c1b4d23e1a8dfe6137fd7956f2c0c3fa51009c1ebb9129807c9fd49850b")
	assertStringValue(t, m7, "replay_hash", "sha256:0d3d6c1b4d23e1a8dfe6137fd7956f2c0c3fa51009c1ebb9129807c9fd49850b")

	gotLimits := intMap(t, objectValue(t, registry, "limits"))
	if !reflect.DeepEqual(gotLimits, limitsV1) {
		t.Fatalf("registry limits = %#v; want %#v", gotLimits, limitsV1)
	}

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
			required: []string{"contract", "schema_version", "fixture_id", "evidence_class", "evidence_id", "evidence_hash", "registry", "scope", "m7_binding", "capture_clock", "normalization", "limits", "runs"},
		},
		{
			path:     "contracts/multi-runtime-coexistence-report-v1.schema.json",
			contract: "https://docs.helianthus.local/schemas/multi-runtime-coexistence-report-v1.schema.json",
			required: []string{"contract", "schema_version", "fixture_id", "report_id", "report_hash", "evidence_id", "evidence_hash", "gate", "verdict", "m7_binding", "baseline", "scenarios", "acceptance_matrix", "rollback"},
		},
	} {
		schema := loadObject(t, filepath.Join(root, test.path))
		assertStringValue(t, schema, "$id", test.contract)
		if additional, ok := schema["additionalProperties"].(bool); !ok || additional {
			t.Fatalf("%s must reject unknown top-level fields", test.path)
		}
		assertStringSlice(t, schema["required"], test.required)
		properties := objectValue(t, schema, "properties")
		if len(properties) != len(test.required) {
			t.Fatalf("%s has %d properties; want closed %d-field root", test.path, len(properties), len(test.required))
		}
	}
}

func TestMSP08NegativeMutationInventoryIsExact(t *testing.T) {
	root := filepath.Join(packageDir(t), "testdata/canonical/negative")
	names := sortedKeys(negativeCategoriesV1)
	for _, name := range names {
		descriptor := loadObject(t, filepath.Join(root, name))
		assertStringValue(t, descriptor, "contract", negativeFixtureContractV1)
		fixtureID := stringValue(t, descriptor, "fixture_id")
		if !strings.HasPrefix(fixtureID, "MSP08-G18-SYNTHETIC-NEG-") {
			t.Fatalf("%s fixture id = %q", name, fixtureID)
		}
		mutation := stringValue(t, descriptor, "mutation")
		if mutation == "" {
			t.Fatalf("%s has empty mutation", name)
		}
	}

	wantMutations := map[string]string{
		"candidate-leak-ebus-mcp.json":      "CANDIDATE_LEAK_EBUS_MCP",
		"canonical-hash-mismatch.json":      "CANONICAL_HASH_MISMATCH",
		"clock-mismatch.json":               "CLOCK_MISMATCH",
		"config-hash-mismatch.json":         "CONFIG_HASH_MISMATCH",
		"conflict-leak-graphql.json":        "CONFLICT_LEAK_GRAPHQL",
		"dropped-payload-field.json":        "DROPPED_PAYLOAD_FIELD",
		"duplicate-provenance.json":         "DUPLICATE_PROVENANCE",
		"g17-claim.json":                    "G17_CLAIM",
		"g19-claim.json":                    "G19_CLAIM",
		"input-hash-mismatch.json":          "INPUT_HASH_MISMATCH",
		"m7-graph-mismatch.json":            "M7_GRAPH_MISMATCH",
		"mask-scope-mismatch.json":          "MASK_SCOPE_MISMATCH",
		"missing-provenance.json":           "MISSING_PROVENANCE",
		"missing-required-view.json":        "MISSING_REQUIRED_VIEW",
		"no-services-empty-success.json":    "NO_SERVICES_EMPTY_SUCCESS",
		"public-v2-surface.json":            "PUBLIC_V2_SURFACE",
		"resource-limit-exceeded.json":      "RESOURCE_LIMIT_EXCEEDED",
		"rollback-drift.json":               "ROLLBACK_DRIFT",
		"runtime-artifact-mismatch.json":    "RUNTIME_ARTIFACT_MISMATCH",
		"stale-capture.json":                "STALE_CAPTURE",
		"timestamp-exclusion-mismatch.json": "TIMESTAMP_EXCLUSION_MISMATCH",
		"unknown-field.json":                "UNKNOWN_FIELD",
	}
	for name, want := range wantMutations {
		descriptor := loadObject(t, filepath.Join(root, name))
		assertStringValue(t, descriptor, "mutation", want)
	}
}
