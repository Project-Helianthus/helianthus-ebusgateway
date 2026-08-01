package syncevidence

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"path"
)

// contractFiles are copied byte-for-byte from helianthus-docs-ebus
// 11d2f2c18218495577b630b5970b7fe2b2fd72e8 and the eeBUS schema commit named
// by its registry entry. The registry drives runtime authority bindings.
//
//go:embed contracts/*.json
var contractFiles embed.FS

const (
	registryContractV1 = "helianthus.platform.source-schema-registry.v1"
	registryVersionV1  = uint64(1)
)

type embeddedRegistry struct {
	Contract string                  `json:"contract"`
	Version  uint64                  `json:"version"`
	Entries  []embeddedRegistryEntry `json:"entries"`
}

type embeddedRegistryEntry struct {
	SourceKind          SourceKind `json:"source_kind"`
	SourceContract      string     `json:"source_contract"`
	SourceSchemaVersion uint64     `json:"source_schema_version"`
	OwnerRepository     string     `json:"owner_repository"`
	OwnerPath           string     `json:"owner_path"`
	OwnerCommit         string     `json:"owner_commit"`
	SchemaSHA256        string     `json:"schema_sha256"`
	EmbeddedSchema      *string    `json:"embedded_schema"`
}

type sourceAuthorityKey struct {
	kind     SourceKind
	contract string
	version  uint64
}

func mustLoadSourceAuthorities() map[sourceAuthorityKey]sourceAuthority {
	raw := mustReadContract("synchronized-evidence-source-registry-v1.json")
	if digestHex(raw) != "bfc105545bcbe96b528928ac5893c2273057d18bae4dc7d1f81fb6291f94045c" {
		panic("syncevidence: pinned source registry digest mismatch")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var registry embeddedRegistry
	if err := decoder.Decode(&registry); err != nil {
		panic("syncevidence: invalid embedded source registry")
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		panic("syncevidence: trailing embedded source registry data")
	}
	if registry.Contract != registryContractV1 || registry.Version != registryVersionV1 || len(registry.Entries) != 6 {
		panic("syncevidence: unsupported embedded source registry")
	}
	result := make(map[sourceAuthorityKey]sourceAuthority, len(registry.Entries))
	for _, entry := range registry.Entries {
		key := sourceAuthorityKey{
			kind:     entry.SourceKind,
			contract: entry.SourceContract,
			version:  entry.SourceSchemaVersion,
		}
		if _, duplicate := result[key]; duplicate || !validRegistryEntry(entry) {
			panic("syncevidence: invalid embedded source registry entry")
		}
		schemaName := "helianthus.eebus.mcp.v1.schema.json"
		if entry.EmbeddedSchema != nil {
			schemaName = path.Base(*entry.EmbeddedSchema)
		}
		if digestHex(mustReadContract(schemaName)) != entry.SchemaSHA256 {
			panic("syncevidence: embedded source schema digest mismatch")
		}
		result[key] = sourceAuthority{
			kind:            entry.SourceKind,
			contract:        entry.SourceContract,
			version:         entry.SourceSchemaVersion,
			ownerRepository: entry.OwnerRepository,
			ownerPath:       entry.OwnerPath,
			ownerCommit:     entry.OwnerCommit,
			schemaSHA256:    entry.SchemaSHA256,
		}
	}
	for _, kind := range []SourceKind{SourceEBusB509, SourceEBusB524, SourceEBusB555, SourceEEBus, SourceCloudApp} {
		key, ok := defaultSourceAuthorityKey(kind)
		if !ok {
			panic("syncevidence: invalid default source authority")
		}
		if _, ok := result[key]; !ok {
			panic("syncevidence: incomplete embedded source registry")
		}
	}
	if _, ok := result[sourceAuthorityKey{kind: SourceEEBus, contract: M625EEBusContractV1, version: 1}]; !ok {
		panic("syncevidence: incomplete M6.25 source registry")
	}
	return result
}

func defaultSourceAuthorityKey(kind SourceKind) (sourceAuthorityKey, bool) {
	var contract string
	switch kind {
	case SourceEBusB509:
		contract = "helianthus.ebus.b509.evidence.v1"
	case SourceEBusB524:
		contract = "helianthus.ebus.b524.evidence.v1"
	case SourceEBusB555:
		contract = "helianthus.ebus.b555.evidence.v1"
	case SourceEEBus:
		contract = HistoricalEEBusContractV1
	case SourceCloudApp:
		contract = "helianthus.cloud-app.precaptured.evidence.v1"
	default:
		return sourceAuthorityKey{}, false
	}
	return sourceAuthorityKey{kind: kind, contract: contract, version: 1}, true
}

func registeredSourceAuthority(source RegisteredSource) (sourceAuthority, bool) {
	key, ok := defaultSourceAuthorityKey(source.SourceKind)
	if !ok {
		return sourceAuthority{}, false
	}
	if source.SourceContract != "" || source.SourceVersion != 0 {
		if source.SourceContract == "" || source.SourceVersion == 0 {
			return sourceAuthority{}, false
		}
		key = sourceAuthorityKey{
			kind:     source.SourceKind,
			contract: source.SourceContract,
			version:  source.SourceVersion,
		}
	}
	authority, ok := sourceAuthorities[key]
	return authority, ok
}

func boundSourceAuthority(kind SourceKind, contract string, version uint64) (sourceAuthority, bool) {
	authority, ok := sourceAuthorities[sourceAuthorityKey{
		kind:     kind,
		contract: contract,
		version:  version,
	}]
	return authority, ok
}

func validRegistryEntry(entry embeddedRegistryEntry) bool {
	_, validKind := runtimeForSource(entry.SourceKind)
	return validKind && entry.SourceContract != "" && entry.SourceSchemaVersion == 1 &&
		validRepository(entry.OwnerRepository) && validRepositoryPath(entry.OwnerPath) &&
		gitCommitPattern.MatchString(entry.OwnerCommit) && len(entry.SchemaSHA256) == 64
}

func mustReadContract(name string) []byte {
	raw, err := contractFiles.ReadFile("contracts/" + name)
	if err != nil {
		panic("syncevidence: missing embedded contract")
	}
	return raw
}

func digestHex(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func mustSchemaRequired(fileName, definition, expectedDigest string) []string {
	raw := mustReadContract(fileName)
	if digestHex(raw) != expectedDigest {
		panic("syncevidence: pinned platform schema digest mismatch")
	}
	var document struct {
		Definitions map[string]json.RawMessage `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		panic("syncevidence: invalid embedded platform schema")
	}
	rawDefinition, ok := document.Definitions[definition]
	if !ok {
		panic("syncevidence: missing embedded platform definition")
	}
	var schema struct {
		AdditionalProperties *bool                      `json:"additionalProperties"`
		Required             []string                   `json:"required"`
		Properties           map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(rawDefinition, &schema); err != nil || schema.AdditionalProperties == nil || *schema.AdditionalProperties || len(schema.Required) == 0 || len(schema.Required) != len(schema.Properties) {
		panic("syncevidence: platform definition is not closed")
	}
	seen := make(map[string]struct{}, len(schema.Required))
	for _, field := range schema.Required {
		if _, exists := schema.Properties[field]; !exists {
			panic("syncevidence: required platform field is undefined")
		}
		if _, duplicate := seen[field]; duplicate {
			panic("syncevidence: duplicate required platform field")
		}
		seen[field] = struct{}{}
	}
	return append([]string(nil), schema.Required...)
}
