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
	if _, err := os.Lstat(destination); err == nil || !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: destination already exists or cannot be inspected", errInvalidCapture)
	}

	temporary, err := os.MkdirTemp(parent, ".m8sourcecapture-")
	if err != nil {
		return nil, fmt.Errorf("%w: create temporary root: %v", errInvalidCapture, err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(temporary)
		}
	}()
	if err := os.Chmod(temporary, 0o700); err != nil {
		return nil, fmt.Errorf("%w: secure temporary root: %v", errInvalidCapture, err)
	}
	sourceRoot := filepath.Join(temporary, SourceDirectory)
	if err := os.Mkdir(sourceRoot, 0o700); err != nil {
		return nil, fmt.Errorf("%w: create source root: %v", errInvalidCapture, err)
	}
	for index, definition := range inputDefinitions {
		path := filepath.Join(sourceRoot, definition.Filename)
		if err := writePrivateFile(path, inputs[index].Payload); err != nil {
			return nil, err
		}
	}
	metadataBytes, err := buildCaptureMetadata(metadata)
	if err != nil {
		return nil, err
	}
	if err := writePrivateFile(filepath.Join(temporary, ManifestFilename), manifest); err != nil {
		return nil, err
	}
	if err := writePrivateFile(filepath.Join(temporary, MetadataFilename), metadataBytes); err != nil {
		return nil, err
	}
	if err := syncDirectory(sourceRoot); err != nil {
		return nil, err
	}
	if err := syncDirectory(temporary); err != nil {
		return nil, err
	}
	if err := os.Rename(temporary, filepath.Join(parent, finalName)); err != nil {
		return nil, fmt.Errorf("%w: atomically publish root: %v", errInvalidCapture, err)
	}
	if err := syncDirectory(parent); err != nil {
		_ = os.RemoveAll(destination)
		_ = syncDirectory(parent)
		return nil, err
	}
	cleanup = false
	return manifest, nil
}

// ReadInputs reads exactly the fixed sixteen regular files from an absolute,
// symlink-free source root. It returns inputs in the contract-mandated order.
func ReadInputs(root string) ([]Input, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("%w: unsafe source root", errInvalidCapture)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: unsafe source root", errInvalidCapture)
	}
	entries, err := os.ReadDir(root)
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
		path := filepath.Join(root, definition.Filename)
		before, err := os.Lstat(path)
		if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() < 1 || before.Size() > maxInputBytes {
			return nil, fmt.Errorf("%w: unsafe input %s", errInvalidCapture, definition.ID)
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("%w: open input %s", errInvalidCapture, definition.ID)
		}
		after, statErr := file.Stat()
		payload, readErr := io.ReadAll(io.LimitReader(file, maxInputBytes+1))
		closeErr := file.Close()
		if statErr != nil || !os.SameFile(before, after) || readErr != nil || closeErr != nil || len(payload) < 1 || len(payload) > maxInputBytes {
			return nil, fmt.Errorf("%w: read input %s", errInvalidCapture, definition.ID)
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

func publicationTarget(destination string) (string, string, error) {
	if destination == "" || !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return "", "", fmt.Errorf("%w: unsafe destination", errInvalidCapture)
	}
	parent, name := filepath.Dir(destination), filepath.Base(destination)
	if name == "." || name == string(filepath.Separator) || strings.Contains(name, string(filepath.Separator)) {
		return "", "", fmt.Errorf("%w: unsafe destination", errInvalidCapture)
	}
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("%w: destination parent", errInvalidCapture)
	}
	return parent, name, nil
}

func writePrivateFile(path string, payload []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("%w: create source file: %v", errInvalidCapture, err)
	}
	written, writeErr := file.Write(payload)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || written != len(payload) || syncErr != nil || closeErr != nil {
		return fmt.Errorf("%w: write source file", errInvalidCapture)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("%w: secure source file: %v", errInvalidCapture, err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: open directory for sync", errInvalidCapture)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("%w: sync directory", errInvalidCapture)
	}
	return nil
}
