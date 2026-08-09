package coexistence

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
)

// These are executable contract copies used by the internal verifier.
//
//go:embed contracts/*.json testdata/canonical/positive/*.json
var artifactFiles embed.FS

const (
	registrySHA256       = "8fab50c488cf99a5f6c29cb8cddc41df9728b5c5edde99e3c1e58d13c9f8407b"
	evidenceSchemaSHA256 = "f95e1a0b4e27c6139b843d1476b427e56efc804b8ca080c883fc8ae3f4cff104"
	reportSchemaSHA256   = "74035251d5ca175014df6f32ea3ded12a6075b78c3ab82b1f8cd8ab2deb7da6e"
	statusSchemaSHA256   = "279acdfdf218c69ebf5ed53567040ca9084e950e53cc9b4f21f30e19e795752b"
	positiveEvidenceSHA  = "32049994cb8a89cd6c11f7852979e6d9b400c75fd106bde52ba28734cc9fd8f2"
	positiveReportSHA    = "24c875c9bb43ad3819da20403435d6d8a4533451154407df089302db45dbcc0c"
	liveStatusSHA256     = "63ecafd94d507cedadc2cba4bb9ac108488d1020d3b43b0ad034409460826428"
)

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
