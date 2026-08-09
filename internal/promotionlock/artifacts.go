package promotionlock

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

// Executable contract copies are byte-identical to canonical docs commit
// 58f683ca3faa752fd3bb4c5b37a42f35ab3cf249.
//
//go:embed contracts/*.json testdata/canonical/positive/*.json
var contractFiles embed.FS

const (
	canonicalDocsCommitV1     = "58f683ca3faa752fd3bb4c5b37a42f35ab3cf249"
	promotionRegistrySHA256V1 = "ad33736c00aa2c3ecaac981606d25c064088c80cb72ca5389b83c5d9df40f6a3"
)

var artifactSHA256V1 = map[string]string{
	"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json":                                  "ee206ea23d595169d7dec2dd305250a1fd7320a630f89b3b9826b5098e3e1f74",
	"docs/platform/schemas/leaf-promotion-captured-assessment-v1.schema.json":                      "dc2ef02d81d5791ed363f1b18b87874400ab195fcc5463217bef3d165ca19731",
	"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json":                              "f0da41bc87618bebc2a44b2192e7c7f3b41f75e94108d87e92122a16f5e19a54",
	"docs/platform/schemas/leaf-promotion-registry-v1.json":                                        promotionRegistrySHA256V1,
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json":                       "3b12e3b6f625f6efb28fced19d679ab73b974fc4369e0dba9f61f1a2d104ec64",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json":                        "a4e5deb1027e337e917304addfa1aebaaf8f04659d7de38b36083c78525d1a04",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/captured-runtime-zero-profile.json": "9b3f2643cb46e45b9b7c890f1ecca27b29942cd91419afb96b30c82912f68cc7",
}

var embeddedPathV1 = map[string]string{
	"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json":                                  "contracts/leaf-promotion-dossier-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-captured-assessment-v1.schema.json":                      "contracts/leaf-promotion-captured-assessment-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json":                              "contracts/leaf-promotion-lock-result-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-registry-v1.json":                                        "contracts/leaf-promotion-registry-v1.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json":                       "testdata/canonical/positive/dossier.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json":                        "testdata/canonical/positive/result.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/captured-runtime-zero-profile.json": "testdata/canonical/positive/captured-runtime-zero-profile.json",
}

func ContractArtifactV1(canonicalPath string) ([]byte, bool) {
	name, ok := embeddedPathV1[canonicalPath]
	if !ok {
		return nil, false
	}
	raw, err := contractFiles.ReadFile(name)
	if err != nil {
		panic("promotionlock: missing embedded contract artifact: " + name)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != artifactSHA256V1[canonicalPath] {
		panic("promotionlock: embedded contract digest mismatch: " + canonicalPath)
	}
	return append([]byte(nil), raw...), true
}

func mustContractArtifactV1(canonicalPath string) []byte {
	raw, ok := ContractArtifactV1(canonicalPath)
	if !ok {
		panic("promotionlock: unregistered contract artifact: " + canonicalPath)
	}
	return raw
}
