package promotionlock

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

// Executable contract copies are byte-identical to canonical docs commit
// bc3f4f949634bfcbe660984e13a97b11bd135eaf.
//
//go:embed contracts/*.json testdata/canonical/positive/*.json
var contractFiles embed.FS

const (
	canonicalDocsCommitV1     = "bc3f4f949634bfcbe660984e13a97b11bd135eaf"
	promotionRegistrySHA256V1 = "33448958278b5974d47aabb2c247927677f2650d2f9bd59355e93067b872bde8"
)

var artifactSHA256V1 = map[string]string{
	"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json":                                  "ee206ea23d595169d7dec2dd305250a1fd7320a630f89b3b9826b5098e3e1f74",
	"docs/platform/schemas/leaf-promotion-captured-assessment-v1.schema.json":                      "ba0968915197788ce9fad014ed5a5ab1534f79a9c14a373915aa5ff8c4a42e10",
	"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json":                              "5f9107b65e5bb7ca838465df0985ffabdc6278deb651449871cec757e877e5ce",
	"docs/platform/schemas/leaf-promotion-registry-v1.json":                                        promotionRegistrySHA256V1,
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json":                       "499a42df0c91acfd73c06051190cc17f54f964de4bc4dde37127e2ddb9713ddf",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json":                        "8085052a9dbe0f34279e992ec56b095f28fb826aad742e76183f8ee1c6c00bd1",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/captured-runtime-zero-profile.json": "7e23ae7fca8ab141b5bb5cd989e5f268eaea6ee11bad95b149b2af31768bb8cd",
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
