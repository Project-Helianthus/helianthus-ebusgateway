package candidatefacts

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

// The embedded files are byte-for-byte copies from the canonical docs merge.
//
//go:embed contracts/*.json testdata/canonical/positive/*.json testdata/canonical/source/*.json
var artifactFiles embed.FS

func PinnedContractV1() ContractBindingV1 {
	return ContractBindingV1{
		OwnerRepository:      "Project-Helianthus/helianthus-docs-ebus",
		OwnerCommit:          "ea88fef23ecb154b08f70e7f94b36e1738ed08bf",
		GraphSchemaPath:      "docs/platform/schemas/draft-candidate-fact-graph-v1.schema.json",
		GraphSchemaSHA256:    "ab5f65a527633c3c5f60119cf0071a6ffdd3ee66d6e210f78bf17a03e438069a",
		ReplaySchemaPath:     "docs/platform/schemas/draft-candidate-fact-replay-v1.schema.json",
		ReplaySchemaSHA256:   "d691cfc970e99faae7f5a1f4e30ed0cb6a4c3cc6e11959dffba5f82a8fc6d232",
		RegistryPath:         "docs/platform/schemas/draft-candidate-fact-registry-v1.json",
		RegistrySHA256:       "e6895b8d7406b58ed97599d8da7e9bd3b252e6e7ca3b0578ec6385bfe6dfe1c0",
		SourceContract:       "helianthus.platform.synchronized-evidence-bundle.v1",
		SourceSchemaVersion:  1,
		SourceOwnerCommit:    "4d7747730be023acb251b20a22d796545a9f3688",
		SourceSchemaSHA256:   "ed574071fdb11e10d5696c62e873a38c6c6dde64c6069bf616476cea8e8bf737",
		SourceRegistrySHA256: "a91b2106076c3ef0f70578e9fc1c85925dd085af323c5889f809b5b2ef1a2488",
	}
}

func pinnedArtifactsV1() artifactsV1 {
	return artifactsV1{
		GraphSchema:    readPinned("contracts/draft-candidate-fact-graph-v1.schema.json", PinnedContractV1().GraphSchemaSHA256),
		ReplaySchema:   readPinned("contracts/draft-candidate-fact-replay-v1.schema.json", PinnedContractV1().ReplaySchemaSHA256),
		Registry:       readPinned("contracts/draft-candidate-fact-registry-v1.json", PinnedContractV1().RegistrySHA256),
		PositiveGraph:  readPinned("testdata/canonical/positive/graph.json", "b5c5d79e540a1691ee60c6db3e9405a92d9d544d871c74b26800fe449a318b0e"),
		PositiveReplay: readPinned("testdata/canonical/positive/replay-result.json", "8280f6278ffe8598dfd767bb5bf9e60dce3c145b4612174b7c5a32fbff282f5c"),
		SourceBundle:   readPinned("testdata/canonical/source/bundle.json", "e6db2862f9001148deb6f40e286ee5f1eef2907812685a9b48128ddbfca5ce5a"),
		SourceReplay:   readPinned("testdata/canonical/source/replay-result.json", "3061c507677f1f41861c20096ff7581ccb6e35c2e01bf66a568e2277df285539"),
	}
}

func readPinned(name, expected string) []byte {
	raw := readArtifact(name)
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != expected {
		panic("candidatefacts: pinned artifact digest mismatch: " + name)
	}
	return raw
}

func readArtifact(name string) []byte {
	raw, err := artifactFiles.ReadFile(name)
	if err != nil {
		panic("candidatefacts: missing pinned artifact: " + name)
	}
	return append([]byte(nil), raw...)
}
