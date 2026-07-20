package promotionlock

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	maxManifestBytesV1 = 1 << 20
	maxSafeIntegerV1   = int64(9_007_199_254_740_991)
)

var digestPatternV1 = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func Verify(raw []byte, inputs InputsV1) error {
	manifest, err := decodeManifestV1(raw)
	if err != nil {
		return err
	}
	if err := validateManifestShape(manifest); err != nil {
		return err
	}
	sources, err := validateSources(inputs)
	if err != nil {
		return err
	}
	if manifest.StableSurfaceChanges {
		return fail("anti_leak.stable_surface")
	}
	if manifest.M9ConsumerGate != "BLOCKED_ZERO_PROMOTED_LEAVES" {
		return fail("consumer.block")
	}
	if manifest.PromotionState != "LOCKED_ZERO_PROMOTION" || manifest.Verdict != "VALID_ZERO_PROMOTION" ||
		manifest.Counts.Dossiers != 0 || manifest.Counts.Promoted != 0 ||
		len(manifest.PromotedPaths) != 0 || len(manifest.LockedDossierIDs) != 0 {
		return fail("promotion.forbidden")
	}
	for _, assessment := range manifest.Assessments {
		if assessment.Decision != "WITHHELD" || assessment.Visibility != "RAW_DEBUG_ONLY" ||
			assessment.DossierState != "NOT_CREATED" {
			return fail("promotion.forbidden")
		}
	}
	expected, err := buildManifest(sources)
	if err != nil {
		return err
	}
	gotView := manifest
	wantView := expected
	gotView.ManifestID, gotView.ManifestHash = "", ""
	wantView.ManifestID, wantView.ManifestHash = "", ""
	if !reflect.DeepEqual(gotView, wantView) {
		return fail("manifest.mismatch")
	}
	if manifest.ManifestHash != hashManifest(manifest) || manifest.ManifestID != "lplmv1:"+manifest.ManifestHash {
		return fail("hash.manifest")
	}
	return nil
}

func decodeManifestV1(raw []byte) (ManifestV1, error) {
	if len(raw) == 0 || len(raw) > maxManifestBytesV1 {
		return ManifestV1{}, fail("json.syntax")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := scanJSONValue(decoder); err != nil {
		return ManifestV1{}, fail("json.syntax")
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return ManifestV1{}, fail("json.syntax")
	}

	var manifest ManifestV1
	typed := json.NewDecoder(bytes.NewReader(raw))
	typed.DisallowUnknownFields()
	if err := typed.Decode(&manifest); err != nil {
		return ManifestV1{}, fail("schema.manifest")
	}
	if err := ensureDecoderEOF(typed); err != nil {
		return ManifestV1{}, fail("json.syntax")
	}
	return manifest, nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	return scanJSONToken(decoder, token)
}

func scanJSONToken(decoder *json.Decoder, token json.Token) error {
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			keys := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return strconv.ErrSyntax
				}
				if _, exists := keys[key]; exists {
					return strconv.ErrSyntax
				}
				keys[key] = struct{}{}
				if err := scanJSONValue(decoder); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return strconv.ErrSyntax
			}
		case '[':
			for decoder.More() {
				if err := scanJSONValue(decoder); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return strconv.ErrSyntax
			}
		default:
			return strconv.ErrSyntax
		}
	case json.Number:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil || parsed > maxSafeIntegerV1 || parsed < -maxSafeIntegerV1 {
			return strconv.ErrSyntax
		}
	case string, bool, nil:
		return nil
	default:
		return strconv.ErrSyntax
	}
	return nil
}

func ensureDecoderEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return strconv.ErrSyntax
	}
	return nil
}

func validateManifestShape(manifest ManifestV1) error {
	if manifest.Contract != ContractV1 || manifest.SchemaVersion != SchemaVersionV1 ||
		!digestPatternV1.MatchString(manifest.ManifestHash) ||
		manifest.ManifestID == "" || manifest.ContractBinding.OwnerRepository == "" ||
		manifest.ContractBinding.OwnerCommit == "" || manifest.ContractBinding.OwnerTree == "" ||
		manifest.SourceBindings.M7GraphID == "" || manifest.SourceBindings.M7ReplayID == "" ||
		manifest.SourceBindings.M8EvidenceID == "" || manifest.SourceBindings.M8ReportID == "" ||
		!digestPatternV1.MatchString(manifest.SourceBindings.M7GraphHash) ||
		!digestPatternV1.MatchString(manifest.SourceBindings.M7ReplayHash) ||
		!digestPatternV1.MatchString(manifest.SourceBindings.M8EvidenceHash) ||
		!digestPatternV1.MatchString(manifest.SourceBindings.M8ReportHash) ||
		manifest.Assessments == nil || manifest.PromotedPaths == nil || manifest.LockedDossierIDs == nil ||
		len(manifest.Assessments) > 64 {
		return fail("schema.manifest")
	}
	paths := make([]string, 0, len(manifest.Assessments))
	ids := make(map[string]struct{}, len(manifest.Assessments))
	for _, assessment := range manifest.Assessments {
		if !validToken(assessment.CandidateID) || !validPath(assessment.SemanticPath) ||
			!digestPatternV1.MatchString(assessment.CandidateHash) ||
			!validToken(assessment.CandidateStatus) || !validToken(assessment.Decision) ||
			!validToken(assessment.Visibility) || !validToken(assessment.DossierState) ||
			!validToken(assessment.ReasonCode) || !validToken(assessment.RetestTrigger) {
			return fail("schema.manifest")
		}
		if assessment.TerminalState != nil && !member(*assessment.TerminalState,
			"NO_SIGNAL", "CLOUD_ONLY", "CONFLICT", "NOT_TESTED") {
			return fail("schema.manifest")
		}
		if _, duplicate := ids[assessment.CandidateID]; duplicate {
			return fail("schema.manifest")
		}
		ids[assessment.CandidateID] = struct{}{}
		paths = append(paths, assessment.SemanticPath)
	}
	if !sort.StringsAreSorted(paths) {
		return fail("schema.manifest")
	}
	return nil
}

func validToken(value string) bool {
	if len(value) == 0 || len(value) > 512 {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char > 0x7e {
			return false
		}
	}
	return true
}

func validPath(value string) bool {
	if len(value) < 2 || len(value) > 512 || !strings.HasPrefix(value, "/") {
		return false
	}
	for _, segment := range strings.Split(value[1:], "/") {
		if segment == "" {
			return false
		}
		for _, char := range segment {
			if char == '_' || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') {
				continue
			}
			return false
		}
	}
	return true
}

func member(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}
