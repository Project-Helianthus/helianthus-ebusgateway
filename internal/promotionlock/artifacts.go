package promotionlock

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

const (
	ownerRepository          = "Project-Helianthus/helianthus-docs-ebus"
	ownerCommit              = "e8614eed91b424b81c414c3cfad596b7c1e8402f"
	ownerTree                = "24794312f89defcbed5cb9654e8539f37c1aa1df"
	ownerExactHeadActionsRun = uint64(30135202717)
	ownerPostMainActionsRun  = uint64(30135494435)
)

// These are executable contract copies, not substantive codebase docs.
//
//go:embed contracts/*.json testdata/canonical/positive/*.json
var contractFiles embed.FS

var ownerArtifactSHA256 = map[string]string{
	"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json":            "665baa6356b86d279169ffa3c3667e7c0f5ee4a68bb0972eda360e32cf0fcd24",
	"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json":        "8fb2caf77ac46cbcfe6074eee25183c21fd19b512dc19f1c38ea308767959de3",
	"docs/platform/schemas/leaf-promotion-registry-v1.json":                  "89d2abad7c981d95a2cb6077ee383404ef13be04a3f1f34f79b8bf177a90792e",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json": "665898298a37032623e8269280ca3116ecd41b66e1414f627c9d587fc7702cff",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json":  "4bd17234c77bcacecb5b016db2eeadb17c10c096b1579a378767115b991c26ec",
}

var embeddedPath = map[string]string{
	"docs/platform/schemas/leaf-promotion-dossier-v1.schema.json":            "contracts/leaf-promotion-dossier-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-lock-result-v1.schema.json":        "contracts/leaf-promotion-lock-result-v1.schema.json",
	"docs/platform/schemas/leaf-promotion-registry-v1.json":                  "contracts/leaf-promotion-registry-v1.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/dossier.json": "testdata/canonical/positive/dossier.json",
	"docs/platform/fixtures/leaf-promotion-dossier/v1/positive/result.json":  "testdata/canonical/positive/result.json",
}

func PinnedContractV1() ContractBindingV1 {
	artifacts := make(map[string]string, len(ownerArtifactSHA256))
	for path, digest := range ownerArtifactSHA256 {
		artifacts[path] = digest
	}
	return ContractBindingV1{
		OwnerRepository:          ownerRepository,
		OwnerCommit:              ownerCommit,
		OwnerTree:                ownerTree,
		OwnerExactHeadActionsRun: ownerExactHeadActionsRun,
		OwnerPostMainActionsRun:  ownerPostMainActionsRun,
		ArtifactSHA256:           artifacts,
	}
}

func ContractArtifactV1(ownerPath string) ([]byte, bool) {
	name, ok := embeddedPath[ownerPath]
	if !ok {
		return nil, false
	}
	raw, err := contractFiles.ReadFile(name)
	if err != nil {
		panic("promotionlock: missing embedded contract artifact: " + name)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != ownerArtifactSHA256[ownerPath] {
		panic("promotionlock: embedded contract digest mismatch: " + ownerPath)
	}
	return append([]byte(nil), raw...), true
}
