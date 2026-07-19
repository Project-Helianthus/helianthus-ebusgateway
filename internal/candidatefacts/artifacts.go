package candidatefacts

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

// The embedded files are byte-for-byte copies from the canonical docs merge.
//
//go:embed contracts/*.json testdata/canonical/positive/*.json testdata/canonical/source/*.json testdata/canonical/negative/*.json
var artifactFiles embed.FS

var negativeFixtureDigestsV1 = map[string]string{
	"anti-leak-stable-surface.json":     "7dc7e071ae3ad4c1a7d191af889d378297e2387121991a5c79854df2a4ccb3d7",
	"comparator-parameter-invalid.json": "54ca728b2bb6bc49a10d5591b53b615208d3e46d429b307787741cf14be30f49",
	"evidence-ref-not-in-bundle.json":   "e41acc15ea033069c3b38d9eb6e8215e44e7e3467ac7bf208d338438151683cf",
	"forged-artifact-id.json":           "9546d98e46cdb8e666c134744cbb92498ef83a35746e6a558ea9d995df40e736",
	"forged-b524-opcode.json":           "44618dbe22a14045375d267279b6d6930c8ca82def9da7e7b9fb2311fb911f9d",
	"forged-eebus-entity-feature.json":  "46309c0bc58b7a39f3fd7a7d5e59810076f14fe04cbc3c471c334d413905721e",
	"forged-source-id.json":             "8066d3ad8c509b86785e37a94c5155db18d7f0922efe581c0804a2478a61f140",
	"graph-hash-mismatch.json":          "11272a453a211a178e7773600d3eaba5928f63413ffff08e268266b6be038c67",
	"incomplete-b524-identity.json":     "d574c5bb84fd893621ccc1909da248cdfad2feb997259a42aa8d4f9e0ed22d8e",
	"invalid-eebus-feature-path.json":   "51ff7033e184196d3a94f0260fc4132740d13e76521477a2a6539eda7da5ea0b",
	"limit-exceeded.json":               "17dd0dbdae52f190a1b8ea60631a7441c787656910a0bfc4c379abee0254ea9d",
	"ordering-invalid.json":             "515a3535d6c509601a510bf2c534a1cf8a6627c6ca6088cfdeb0f456cda9825c",
	"registry-mismatch.json":            "c3799d07f7d11c190fac4a450065987e1b5c2f410d5efc942ee1fc6cd3fecc37",
	"terminal-state-not-withheld.json":  "4acfad48a847b4a767adbcbfae06219e080ffb51349514f80e1983594006a260",
	"unknown-field.json":                "455fc749eca249b0502a81b36a4b135fb362df52942a70839015c0c8356edecb",
	"wrong-source-bundle.json":          "f01f496e451dcd43823ca11776dac378312af7b1336da28f6f56875cdfb5452a",
	"wrong-source-replay.json":          "69db280970154ef0f3ddd52002b4054ccee1475072e8f975ce79e65cff6f17bb",
}

func PinnedContractV1() ContractBindingV1 {
	return ContractBindingV1{
		OwnerRepository:      "Project-Helianthus/helianthus-docs-ebus",
		OwnerCommit:          "4f7ed144f5d75c4123d90580bf2575d139303bb5",
		GraphSchemaPath:      "docs/platform/schemas/draft-candidate-fact-graph-v1.schema.json",
		GraphSchemaSHA256:    "5a8c18119d9dc92bcfd516cab487e045e1df3bd95acb0195a3af2c9f2f73ab83",
		ReplaySchemaPath:     "docs/platform/schemas/draft-candidate-fact-replay-v1.schema.json",
		ReplaySchemaSHA256:   "d691cfc970e99faae7f5a1f4e30ed0cb6a4c3cc6e11959dffba5f82a8fc6d232",
		RegistryPath:         "docs/platform/schemas/draft-candidate-fact-registry-v1.json",
		RegistrySHA256:       "3126379f5a59d5751954235ea996bdcb63ce09d51d009382982716c46ba4559a",
		SourceContract:       "helianthus.platform.synchronized-evidence-bundle.v1",
		SourceSchemaVersion:  1,
		SourceOwnerCommit:    "4d7747730be023acb251b20a22d796545a9f3688",
		SourceSchemaSHA256:   "ed574071fdb11e10d5696c62e873a38c6c6dde64c6069bf616476cea8e8bf737",
		SourceRegistrySHA256: "a91b2106076c3ef0f70578e9fc1c85925dd085af323c5889f809b5b2ef1a2488",
	}
}

func PinnedArtifactsV1() ArtifactsV1 {
	negative := make(map[string][]byte, len(negativeFixtureDigestsV1))
	for name, digest := range negativeFixtureDigestsV1 {
		negative[name] = readPinned("testdata/canonical/negative/"+name, digest)
	}
	return ArtifactsV1{
		GraphSchema:    readPinned("contracts/draft-candidate-fact-graph-v1.schema.json", PinnedContractV1().GraphSchemaSHA256),
		ReplaySchema:   readPinned("contracts/draft-candidate-fact-replay-v1.schema.json", PinnedContractV1().ReplaySchemaSHA256),
		Registry:       readPinned("contracts/draft-candidate-fact-registry-v1.json", PinnedContractV1().RegistrySHA256),
		PositiveGraph:  readPinned("testdata/canonical/positive/graph.json", "5d6d62b6ac7767efaae9cc6080fae3df629c091dbc886cdbeff863605259f0b4"),
		PositiveReplay: readPinned("testdata/canonical/positive/replay-result.json", "fb7724abcf4dca7d1dea5a092846d2e6777b2f2c9c19f92c060ea1e0f7b8f530"),
		SourceBundle:   readPinned("testdata/canonical/source/bundle.json", "e6db2862f9001148deb6f40e286ee5f1eef2907812685a9b48128ddbfca5ce5a"),
		SourceReplay:   readPinned("testdata/canonical/source/replay-result.json", "3061c507677f1f41861c20096ff7581ccb6e35c2e01bf66a568e2277df285539"),
		NegativeGraphs: negative,
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
