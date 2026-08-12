// Package m8sourcecapture creates bounded private M8 source-capture roots.
package m8sourcecapture

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	Contract          = "helianthus.platform.multi-runtime-source-capture-manifest.v1"
	ProjectionPolicy  = "M8_PROTECTED_VIEWS_SINGLE_WINDOW_V1"
	MetadataContract  = "helianthus.platform.m8-source-capture-metadata.v1"
	SourceDirectory   = "source"
	ManifestFilename  = "source-capture-manifest.json"
	MetadataFilename  = "capture-metadata.json"
	maxInputBytes     = 2_097_152
	maxTotalBytes     = 16_777_216
	maxMetadataLength = 256
)

var (
	errInvalidCapture = errors.New("m8sourcecapture: invalid source capture")
	errSecretMaterial = errors.New("m8sourcecapture: secret material is not allowed")

	tokenPattern  = regexp.MustCompile(`^[ -~]+$`)
	hashPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	secretPattern = regexp.MustCompile(`(?i)-----BEGIN (?:[A-Z0-9]+(?: [A-Z0-9]+)* )?PRIVATE KEY-----|\bauthorization\s*:\s*(?:bearer|basic)\s+\S+|\bbearer\s+[a-z0-9._~+/=-]+|\b(?:set-cookie|cookie)\s*:\s*\S+|\b(?:access[_-]?keys?|api[_-]?keys?|credentials?|passwords?|secrets?|session[_-]?cookies?|tokens?)\s*(?::|=|\bis\b)\s*\S+|(?:^|[^a-z0-9])(?:[a-z0-9]+[_-])+(?:cookie|credential|password|secret|token)\s*[:=]\s*\S+|(?:^|[^a-z0-9])private[_-]key\s*[:=]\s*\S+|"(?:access[_-]?keys?|api[_-]?keys?|credential|password|secret|token)"[[:space:]]*:[[:space:]]*"[^\"]+"`)
)

// Phase identifies the position of one capture window around a restart.
type Phase string

const (
	PhasePreRestart  Phase = "PRE_RESTART"
	PhasePostRestart Phase = "POST_RESTART"
)

// inputDefinition fixes an input's identity, authorization boundary, and file name.
type inputDefinition struct {
	ID           string
	AuthBoundary string
	Filename     string
}

// inputDefinitions is the contract-mandated order of the complete source set.
var inputDefinitions = [...]inputDefinition{
	{"tools.list", "READ_ONLY_TEST_MCP", "tools-list.json"},
	{"ebus.devices", "PUBLIC_LOOPBACK_MCP", "ebus-devices.json"},
	{"ebus.semantic", "PUBLIC_LOOPBACK_MCP", "ebus-semantic.json"},
	{"ebus.debug", "READ_ONLY_TEST_MCP", "ebus-debug.json"},
	{"eebus.runtime", "OWNER_UNIX_MCP", "eebus-runtime.json"},
	{"eebus.services", "OWNER_UNIX_MCP", "eebus-services.json"},
	{"eebus.sessions", "OWNER_UNIX_MCP", "eebus-sessions.json"},
	{"eebus.pairing", "OWNER_UNIX_MCP", "eebus-pairing.json"},
	{"eebus.topology", "OWNER_UNIX_MCP", "eebus-topology.json"},
	{"graphql.schema", "PUBLIC_LOOPBACK_GRAPHQL", "graphql-schema.json"},
	{"graphql.values", "PUBLIC_LOOPBACK_GRAPHQL", "graphql-values.json"},
	{"portal.bootstrap", "PUBLIC_LOOPBACK_HTTP", "portal-bootstrap.json"},
	{"command.routing", "LOCAL_RUNTIME_OBSERVATION", "command-routing.json"},
	{"semantic.registry", "LOCAL_RUNTIME_OBSERVATION", "semantic-registry.json"},
	{"container.inspect", "LOCAL_RUNTIME_ADMIN", "container-inspect.json"},
	{"capture.timestamp", "LOCAL_CAPTURE_CLOCK", "captured-at.txt"},
}

// Input is one raw source payload. Inputs must match InputDefinitions exactly.
type Input struct {
	ID      string
	Payload []byte
}

// Metadata binds a capture manifest to exactly one authenticated window.
type Metadata struct {
	Phase                Phase
	WindowID             string
	AuthScopeHash        string
	ProcessInstanceID    string
	ClockID              string
	MonotonicEpochID     string
	WallAnchorUTC        time.Time
	CaptureStartOffsetNS int64
	CaptureEndOffsetNS   int64
	CapturedAt           time.Time
}

// PublishGeneration atomically publishes one private generation containing
// the exact sixteen-file source root, its manifest, and capture-clock metadata.
func PublishGeneration(destination string, metadata Metadata, inputs []Input) ([]byte, error) {
	manifest, err := buildManifest(metadata, inputs)
	if err != nil {
		return nil, err
	}
	parent, finalName, err := publicationTarget(destination)
	if err != nil {
		return nil, err
	}
	defer func() { _ = parent.close() }()
	metadataBytes, err := buildCaptureMetadata(metadata)
	if err != nil {
		return nil, err
	}
	if err := publishPreparedGenerationAt(parent, finalName, manifest, metadataBytes, inputs); err != nil {
		return nil, err
	}
	return manifest, nil
}

// ReadInputs reads exactly the fixed sixteen regular files from an absolute,
// symlink-free source root. It returns inputs in the contract-mandated order.
func ReadInputs(root string) ([]Input, error) {
	directory, err := openSecureDirectory(root, true)
	if err != nil {
		return nil, err
	}
	defer func() { _ = directory.close() }()
	return readInputsFromDirectory(directory)
}

func readInputsFromDirectory(directory *secureDirectory) ([]Input, error) {
	entries, err := directory.entries()
	if err != nil || len(entries) != len(inputDefinitions) {
		return nil, fmt.Errorf("%w: source root contents", errInvalidCapture)
	}
	expected := make(map[string]inputDefinition, len(inputDefinitions))
	for _, definition := range inputDefinitions {
		expected[definition.Filename] = definition
	}
	for _, entry := range entries {
		if _, ok := expected[entry.Name()]; !ok || entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%w: source root contents", errInvalidCapture)
		}
	}
	inputs := make([]Input, 0, len(inputDefinitions))
	for _, definition := range inputDefinitions {
		payload, err := directory.readRegular(definition.Filename, maxInputBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: read input %s", err, definition.ID)
		}
		inputs = append(inputs, Input{ID: definition.ID, Payload: payload})
	}
	return inputs, nil
}

func buildManifest(metadata Metadata, inputs []Input) ([]byte, error) {
	if err := validateMetadata(metadata); err != nil {
		return nil, err
	}
	if len(inputs) != len(inputDefinitions) {
		return nil, fmt.Errorf("%w: expected exactly %d inputs", errInvalidCapture, len(inputDefinitions))
	}
	total := 0
	manifestInputs := make([]map[string]any, 0, len(inputs))
	for index, definition := range inputDefinitions {
		input := inputs[index]
		if input.ID != definition.ID || len(input.Payload) == 0 || len(input.Payload) > maxInputBytes {
			return nil, fmt.Errorf("%w: invalid input %d", errInvalidCapture, index)
		}
		if containsSecretMaterial(definition, input.Payload) {
			return nil, fmt.Errorf("%w: input %s", errSecretMaterial, input.ID)
		}
		total += len(input.Payload)
		if total > maxTotalBytes {
			return nil, fmt.Errorf("%w: aggregate input limit", errInvalidCapture)
		}
		digest := sha256.Sum256(input.Payload)
		manifestInputs = append(manifestInputs, map[string]any{
			"auth_boundary": definition.AuthBoundary,
			"byte_length":   len(input.Payload),
			"digest":        "sha256:" + hex.EncodeToString(digest[:]),
			"input_id":      definition.ID,
		})
	}
	if capturedAt := strings.TrimSpace(string(inputs[len(inputs)-1].Payload)); capturedAt != metadata.CapturedAt.UTC().Format(time.RFC3339Nano) {
		return nil, fmt.Errorf("%w: capture timestamp input", errInvalidCapture)
	}
	manifest := map[string]any{
		"auth_scope_hash":         metadata.AuthScopeHash,
		"capture_end_offset_ns":   metadata.CaptureEndOffsetNS,
		"capture_start_offset_ns": metadata.CaptureStartOffsetNS,
		"captured_at":             metadata.CapturedAt.UTC().Format(time.RFC3339Nano),
		"contract":                Contract,
		"inputs":                  manifestInputs,
		"phase":                   string(metadata.Phase),
		"process_instance_id":     metadata.ProcessInstanceID,
		"projection_policy":       ProjectionPolicy,
		"schema_version":          1,
		"window_id":               metadata.WindowID,
		"window_scope":            "SINGLE_WINDOW_ONLY",
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(manifest); err != nil {
		return nil, fmt.Errorf("%w: canonical manifest: %v", errInvalidCapture, err)
	}
	encoded := bytes.TrimSuffix(output.Bytes(), []byte{'\n'})
	return encoded, nil
}

func validateMetadata(metadata Metadata) error {
	if metadata.Phase != PhasePreRestart && metadata.Phase != PhasePostRestart {
		return fmt.Errorf("%w: phase", errInvalidCapture)
	}
	for _, value := range []string{metadata.WindowID, metadata.ProcessInstanceID, metadata.ClockID, metadata.MonotonicEpochID} {
		if len(value) == 0 || len(value) > maxMetadataLength || !tokenPattern.MatchString(value) {
			return fmt.Errorf("%w: metadata token", errInvalidCapture)
		}
	}
	if !hashPattern.MatchString(metadata.AuthScopeHash) || metadata.CaptureStartOffsetNS < 0 ||
		metadata.CaptureEndOffsetNS < metadata.CaptureStartOffsetNS || metadata.CapturedAt.IsZero() || metadata.WallAnchorUTC.IsZero() ||
		!metadata.CapturedAt.Equal(metadata.WallAnchorUTC.Add(time.Duration(metadata.CaptureStartOffsetNS))) {
		return fmt.Errorf("%w: metadata binding", errInvalidCapture)
	}
	return nil
}

func buildCaptureMetadata(metadata Metadata) ([]byte, error) {
	if err := validateMetadata(metadata); err != nil {
		return nil, err
	}
	return canonicalJSON(map[string]any{
		"capture_end_offset_ns":   metadata.CaptureEndOffsetNS,
		"capture_start_offset_ns": metadata.CaptureStartOffsetNS,
		"captured_at":             metadata.CapturedAt.UTC().Format(time.RFC3339Nano),
		"clock_id":                metadata.ClockID,
		"contract":                MetadataContract,
		"monotonic_epoch_id":      metadata.MonotonicEpochID,
		"phase":                   string(metadata.Phase),
		"process_instance_id":     metadata.ProcessInstanceID,
		"wall_anchor_utc":         metadata.WallAnchorUTC.UTC().Format(time.RFC3339Nano),
		"window_id":               metadata.WindowID,
	})
}

func canonicalJSON(value any) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("%w: canonical JSON: %v", errInvalidCapture, err)
	}
	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}

func containsSecretMaterial(definition inputDefinition, payload []byte) bool {
	if secretPattern.Match(payload) {
		return true
	}
	if definition.ID == "capture.timestamp" {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if decoder.Decode(&value) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return true
	}
	return containsSecretKey(value)
}

func containsSecretKey(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.Map(func(char rune) rune {
				if char >= 'A' && char <= 'Z' {
					return char + ('a' - 'A')
				}
				if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
					return char
				}
				return -1
			}, key)
			for _, secret := range []string{"authorization", "password", "passwords", "secret", "secrets", "token", "tokens", "cookie", "cookies", "privatekey", "accesskey", "accesskeys", "apikey", "apikeys", "credential", "credentials", "sessioncookie", "setcookie"} {
				if normalized == secret || strings.HasSuffix(normalized, secret) {
					return true
				}
			}
			if containsSecretKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsSecretKey(child) {
				return true
			}
		}
	}
	return false
}

func publicationTarget(destination string) (*secureDirectory, string, error) {
	if destination == "" || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return nil, "", fmt.Errorf("%w: unsafe destination", errInvalidCapture)
	}
	parent, name := filepath.Dir(destination), filepath.Base(destination)
	if !safeChildName(name) {
		return nil, "", fmt.Errorf("%w: unsafe destination", errInvalidCapture)
	}
	directory, err := openSecureDirectory(parent, false)
	if err != nil {
		return nil, "", fmt.Errorf("%w: destination parent", errInvalidCapture)
	}
	return directory, name, nil
}

func publishPreparedGenerationAt(parent *secureDirectory, finalName string, manifest, metadata []byte, inputs []Input) error {
	if parent == nil || len(inputs) != len(inputDefinitions) || parent.absent(finalName) != nil {
		return fmt.Errorf("%w: destination", errInvalidCapture)
	}
	temporaryName, temporary, err := parent.temporaryDirectory()
	if err != nil {
		return err
	}
	publishedName := ""
	defer func() {
		_ = temporary.close()
		if publishedName == "" {
			_ = parent.removeTree(temporaryName)
		}
	}()
	source, err := temporary.childDirectory(SourceDirectory, true)
	if err != nil {
		return err
	}
	for index, definition := range inputDefinitions {
		if err := source.writeRegular(definition.Filename, inputs[index].Payload); err != nil {
			_ = source.close()
			return err
		}
	}
	if err := source.sync(); err != nil {
		_ = source.close()
		return err
	}
	if err := source.close(); err != nil {
		return fmt.Errorf("%w: close source directory", errInvalidCapture)
	}
	if err := temporary.writeRegular(ManifestFilename, manifest); err != nil {
		return err
	}
	if err := temporary.writeRegular(MetadataFilename, metadata); err != nil {
		return err
	}
	if err := temporary.sync(); err != nil {
		return err
	}
	if err := parent.renameNoReplace(temporaryName, finalName); err != nil {
		return err
	}
	publishedName = finalName
	if err := parent.sync(); err != nil {
		_ = parent.removeTree(finalName)
		_ = parent.sync()
		return err
	}
	return nil
}
