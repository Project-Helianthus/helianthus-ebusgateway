package coexistence

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

// Canonical contract and positive-fixture copies are pinned to the owner tree.
//
//go:embed contracts/*.json testdata/canonical/positive/*.json
var artifactFiles embed.FS

const (
	registrySHA256       = "82cf854d335da31dcda65ff45a024cbb4a1ce515965cb8165122c0b4ef7d8505"
	evidenceSchemaSHA256 = "b0c891ea44e073b9bab150f2142584db356ed5e0569d70112156355617235b5d"
	reportSchemaSHA256   = "7769c91654319c208576d45ed5c9f10aaa133c6591f37a5ed1adeda0e8c4c25c"
	positiveEvidenceSHA  = "f1b2b0c9033ae985343ffd67748273f8ec5389f853e34fab90a218767fe74e26"
	positiveReportSHA    = "7b5c964ab4794be28c5d87c09d36813dc2dddc564ba54ba6a81de51559a0a4bd"
)

var coexOwnerArtifactSHA256 = map[string]string{
	"docs/platform/multi-runtime-coexistence-no-drift-v1.md":                                    "92af0448093e5690dbb4a77cde63e76d8af43d2640f50c2b90400d3d66541050",
	"docs/platform/schemas/multi-runtime-coexistence-evidence-v1.schema.json":                   evidenceSchemaSHA256,
	"docs/platform/schemas/multi-runtime-coexistence-report-v1.schema.json":                     reportSchemaSHA256,
	"docs/platform/schemas/multi-runtime-coexistence-registry-v1.json":                          registrySHA256,
	"docs/platform/fixtures/coexistence-no-drift/v1/positive/evidence.json":                     positiveEvidenceSHA,
	"docs/platform/fixtures/coexistence-no-drift/v1/positive/report.json":                       positiveReportSHA,
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

func Binding() BindingV1 {
	artifacts := make(map[string]string, len(coexOwnerArtifactSHA256))
	for path, digest := range coexOwnerArtifactSHA256 {
		artifacts[path] = digest
	}
	return BindingV1{
		OwnerRepository:          ownerRepository,
		OwnerCommit:              ownerCommit,
		OwnerTree:                ownerTree,
		OwnerExactHeadActionsRun: ownerExactHeadActionsRun,
		OwnerPostMainActionsRun:  ownerPostMainActionsRun,
		BaselineGatewayCommit:    coexBaselineGatewayCommit,
		M7CompletionToken:        coexM7CompletionToken,
		ArtifactSHA256:           artifacts,
	}
}

func readPinnedArtifact(name, expected string) []byte {
	raw, err := artifactFiles.ReadFile(name)
	if err != nil {
		panic("coexistence: missing pinned artifact: " + name)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != expected {
		panic("coexistence: pinned artifact digest mismatch: " + name)
	}
	return append([]byte(nil), raw...)
}
