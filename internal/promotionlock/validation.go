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
	maxResultBytesV1    = 1 << 20
	maxSafeIntegerV1    = int64(9_007_199_254_740_991)
	maxDepthV1          = 32
	maxStringBytesV1    = 4_096
	maxTotalMembersV1   = 16_384
	maxTotalListItemsV1 = 8_192
)

var (
	digestPatternV1       = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	commitPatternV1       = regexp.MustCompile(`^[0-9a-f]{40}$`)
	candidateIDPatternV1  = regexp.MustCompile(`^m7-candidate-[0-9]{4}$`)
	graphIDPatternV1      = regexp.MustCompile(`^dcfgv1:sha256:[0-9a-f]{64}$`)
	replayIDPatternV1     = regexp.MustCompile(`^dcfrv1:sha256:[0-9a-f]{64}$`)
	statusIDPatternV1     = regexp.MustCompile(`^dcfpsv1:sha256:[0-9a-f]{64}$`)
	evidenceIDPatternV1   = regexp.MustCompile(`^mrcv1:sha256:[0-9a-f]{64}$`)
	reportIDPatternV1     = regexp.MustCompile(`^mrcrv1:sha256:[0-9a-f]{64}$`)
	negativeZeroPatternV1 = regexp.MustCompile(`(^|[^0-9A-Za-z_])-0([^0-9.]|$)`)
)

func Verify(raw []byte, inputs InputsV1) error {
	raw = bytes.Clone(raw)
	inputs = cloneInputsV1(inputs)
	result, resultObject, err := decodeResultDocumentV1(raw)
	if err != nil {
		return err
	}
	expectedRaw, err := Build(inputs)
	if err != nil {
		return err
	}
	expected, err := decodeResultV1(expectedRaw)
	if err != nil {
		panic("promotionlock: internally generated result is invalid: " + err.Error())
	}
	if err := validateResultShape(result, inputs.Profile); err != nil {
		return err
	}
	if result.Profile == ProfileCapturedZeroPromotionV1 {
		if assessmentsOutOfOrder(result.Assessments, expected.Assessments) {
			return fail("assessment.ordering")
		}
		if !reflect.DeepEqual(result.Assessments, expected.Assessments) ||
			!reflect.DeepEqual(result.SourceBindings, expected.SourceBindings) {
			return fail("assessment.derivation")
		}
		if result.DossierCount != 0 || result.Counts.Promoted != 0 || result.Counts.Withheld != result.Counts.Total {
			return fail("promotion.forbidden")
		}
	}
	if result.M9ConsumerGate != M9BlockedZeroPromotedLeavesV1 {
		return fail("consumer.block")
	}
	if result.Profile == ProfileCapturedZeroPromotionV1 && leaksPrivateIdentityV1(raw) {
		return fail("redaction.public")
	}
	gotView := result
	wantView := expected
	gotView.ResultHash = ""
	wantView.ResultHash = ""
	if !reflect.DeepEqual(gotView, wantView) {
		return fail("captured.result")
	}
	if result.ResultHash != hashObjectWithoutFieldV1(resultHashDomainV1, resultObject, "result_hash") {
		return fail("hash.result")
	}
	return nil
}

func decodeResultV1(raw []byte) (ResultV1, error) {
	result, _, err := decodeResultDocumentV1(raw)
	return result, err
}

func decodeResultDocumentV1(raw []byte) (ResultV1, map[string]any, error) {
	if len(raw) == 0 {
		return ResultV1{}, nil, fail("json.syntax")
	}
	if len(raw) > maxResultBytesV1 {
		return ResultV1{}, nil, fail("limits.exceeded")
	}
	if negativeZeroPatternV1.Match(raw) {
		return ResultV1{}, nil, fail("json.syntax")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	limits := jsonLimitsV1{}
	if err := scanJSONValue(decoder, 0, &limits); err != nil {
		if err == errLimitsExceededV1 {
			return ResultV1{}, nil, fail("limits.exceeded")
		}
		return ResultV1{}, nil, fail("json.syntax")
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return ResultV1{}, nil, fail("json.syntax")
	}

	var object map[string]any
	objectDecoder := json.NewDecoder(bytes.NewReader(raw))
	objectDecoder.UseNumber()
	if err := objectDecoder.Decode(&object); err != nil || object == nil {
		return ResultV1{}, nil, fail("json.syntax")
	}
	if err := validateExactResultDocumentV1(object); err != nil {
		return ResultV1{}, nil, err
	}

	var result ResultV1
	typed := json.NewDecoder(bytes.NewReader(raw))
	typed.DisallowUnknownFields()
	if err := typed.Decode(&result); err != nil {
		return ResultV1{}, nil, fail("captured.result")
	}
	if err := ensureDecoderEOF(typed); err != nil {
		return ResultV1{}, nil, fail("json.syntax")
	}
	return result, object, nil
}

func validateExactResultDocumentV1(object map[string]any) error {
	common := []string{
		"contract", "schema_version", "profile", "export_tier", "replay_tool", "replay_version", "counts",
		"dossier_count", "m9_consumer_gate", "verdict", "result_hash",
	}
	profile, ok := object["profile"].(string)
	if !ok {
		return fail("captured.result")
	}
	var topLevel []string
	switch profile {
	case ProfileSyntheticConformanceV1:
		topLevel = append(append([]string(nil), common...), "dossier_id", "dossier_hash", "leaves")
	case ProfileCapturedZeroPromotionV1:
		topLevel = append(append([]string(nil), common...), "source_bindings", "assessments")
	default:
		return fail("captured.result")
	}
	if !hasExactKeysV1(object, topLevel...) {
		return fail("captured.result")
	}
	if _, err := exactObjectV1(object["counts"], "total", "promoted", "withheld"); err != nil {
		return err
	}
	if profile == ProfileSyntheticConformanceV1 {
		return validateExactObjectListV1(object["leaves"],
			"leaf_id", "semantic_path", "decision", "terminal_state", "visibility")
	}
	if _, err := exactObjectV1(object["source_bindings"],
		"m7_gateway_source_commit", "m7_docs_source_commit", "m7_graph_id", "m7_graph_hash",
		"m7_replay_id", "m7_replay_hash", "m7_status_projection_id", "m7_status_projection_hash",
		"m8_gateway_source_commit", "m8_docs_source_commit", "m8_evidence_id", "m8_evidence_hash",
		"m8_report_id", "m8_report_hash", "coexistence_verdict"); err != nil {
		return err
	}
	assessments, ok := object["assessments"].([]any)
	if !ok {
		return fail("captured.result")
	}
	for _, rawAssessment := range assessments {
		assessment, err := exactObjectV1(rawAssessment,
			"candidate_id", "fact_hash", "source_status", "terminal_state", "decision", "withholding_reasons", "retest_trigger")
		if err != nil {
			return err
		}
		if _, err := exactObjectV1(assessment["retest_trigger"],
			"trigger", "required_source_kinds", "minimum_new_samples"); err != nil {
			return err
		}
	}
	return nil
}

func validateExactObjectListV1(value any, keys ...string) error {
	items, ok := value.([]any)
	if !ok {
		return fail("captured.result")
	}
	for _, item := range items {
		if _, err := exactObjectV1(item, keys...); err != nil {
			return err
		}
	}
	return nil
}

func exactObjectV1(value any, keys ...string) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok || !hasExactKeysV1(object, keys...) {
		return nil, fail("captured.result")
	}
	return object, nil
}

func hasExactKeysV1(object map[string]any, keys ...string) bool {
	if len(object) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := object[key]; !ok {
			return false
		}
	}
	return true
}

type sentinelErrorV1 string

func (err sentinelErrorV1) Error() string { return string(err) }

const errLimitsExceededV1 = sentinelErrorV1("limits")

type jsonLimitsV1 struct {
	members int
	items   int
}

func scanJSONValue(decoder *json.Decoder, depth int, limits *jsonLimitsV1) error {
	if depth > maxDepthV1 {
		return errLimitsExceededV1
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	return scanJSONToken(decoder, token, depth, limits)
}

func scanJSONToken(decoder *json.Decoder, token json.Token, depth int, limits *jsonLimitsV1) error {
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
				if !ok || len([]byte(key)) > maxStringBytesV1 {
					if ok {
						return errLimitsExceededV1
					}
					return strconv.ErrSyntax
				}
				if _, exists := keys[key]; exists {
					return strconv.ErrSyntax
				}
				keys[key] = struct{}{}
				limits.members++
				if limits.members > maxTotalMembersV1 {
					return errLimitsExceededV1
				}
				if err := scanJSONValue(decoder, depth+1, limits); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return strconv.ErrSyntax
			}
		case '[':
			for decoder.More() {
				limits.items++
				if limits.items > maxTotalListItemsV1 {
					return errLimitsExceededV1
				}
				if err := scanJSONValue(decoder, depth+1, limits); err != nil {
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
	case string:
		if len([]byte(value)) > maxStringBytesV1 {
			return errLimitsExceededV1
		}
	case bool, nil:
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

func validateResultShape(result ResultV1, inputProfile string) error {
	if result.Contract != ContractV1 || result.SchemaVersion != SchemaVersionV1 || result.Profile != inputProfile ||
		result.ExportTier != ExportTierPublicRedactedV1 || result.ReplayTool != replayToolV1 ||
		result.ReplayVersion != SchemaVersionV1 || !digestPatternV1.MatchString(result.ResultHash) ||
		result.Counts.Total < 1 || result.Counts.Total > 64 || result.Counts.Promoted > result.Counts.Total ||
		result.Counts.Withheld > result.Counts.Total || result.Counts.Promoted+result.Counts.Withheld != result.Counts.Total ||
		result.DossierCount > 64 || !member(result.Verdict, VerdictValidZeroPromotionV1, "VALID_PROMOTION_LOCK") {
		return fail("captured.result")
	}
	switch result.Profile {
	case ProfileSyntheticConformanceV1:
		if result.DossierID == "" || !digestPatternV1.MatchString(result.DossierHash) || result.SourceBindings != nil ||
			result.Assessments != nil || result.DossierCount != 1 || len(result.Leaves) < 1 || len(result.Leaves) > 64 {
			return fail("captured.result")
		}
		for _, leaf := range result.Leaves {
			if !validToken(leaf.LeafID) || !validPath(leaf.SemanticPath) || !member(leaf.Decision, "PROMOTED", "WITHHELD") ||
				!member(leaf.Visibility, "RAW_DEBUG_ONLY", "LOCKED_NOT_EXPOSED") || !validTerminal(leaf.TerminalState) {
				return fail("captured.result")
			}
		}
	case ProfileCapturedZeroPromotionV1:
		if result.DossierID != "" || result.DossierHash != "" || result.SourceBindings == nil || result.Leaves != nil ||
			len(result.Assessments) < 1 || len(result.Assessments) > 64 {
			return fail("captured.result")
		}
		if err := validateSourceBindings(*result.SourceBindings); err != nil {
			return err
		}
		for _, assessment := range result.Assessments {
			if !candidateIDPatternV1.MatchString(assessment.CandidateID) || !digestPatternV1.MatchString(assessment.FactHash) ||
				!member(assessment.SourceStatus, "RAW_ONLY", "WITHHELD") || !validTerminal(assessment.TerminalState) ||
				assessment.Decision != "WITHHELD" || !validReasons(assessment.WithholdingReasons) ||
				!validRetest(assessment.RetestTrigger) {
				return fail("captured.result")
			}
		}
	default:
		return fail("captured.result")
	}
	return nil
}

func validateSourceBindings(source PublicSourceBindingsV1) error {
	if !commitPatternV1.MatchString(source.M7GatewaySourceCommit) || !commitPatternV1.MatchString(source.M7DocsSourceCommit) ||
		!graphIDPatternV1.MatchString(source.M7GraphID) || !digestPatternV1.MatchString(source.M7GraphHash) ||
		!replayIDPatternV1.MatchString(source.M7ReplayID) || !digestPatternV1.MatchString(source.M7ReplayHash) ||
		!statusIDPatternV1.MatchString(source.M7StatusProjectionID) || !digestPatternV1.MatchString(source.M7StatusProjectionHash) ||
		!commitPatternV1.MatchString(source.M8GatewaySourceCommit) || !commitPatternV1.MatchString(source.M8DocsSourceCommit) ||
		!evidenceIDPatternV1.MatchString(source.M8EvidenceID) || !digestPatternV1.MatchString(source.M8EvidenceHash) ||
		!reportIDPatternV1.MatchString(source.M8ReportID) || !digestPatternV1.MatchString(source.M8ReportHash) ||
		source.CoexistenceVerdict != "PASS" {
		return fail("captured.result")
	}
	return nil
}

func assessmentsOutOfOrder(got, want []PublicAssessmentV1) bool {
	gotIDs := make([]string, len(got))
	wantIDs := make([]string, len(want))
	for index := range got {
		gotIDs[index] = got[index].CandidateID
	}
	for index := range want {
		wantIDs[index] = want[index].CandidateID
	}
	if !uniqueStrings(gotIDs) {
		return true
	}
	if reflect.DeepEqual(gotIDs, wantIDs) {
		return false
	}
	sort.Strings(gotIDs)
	sort.Strings(wantIDs)
	return reflect.DeepEqual(gotIDs, wantIDs)
}

func validReasons(reasons []string) bool {
	if len(reasons) < 1 || len(reasons) > 8 || !uniqueStrings(reasons) {
		return false
	}
	positions := make(map[string]int, len(withholdingReasonOrderV1))
	for index, reason := range withholdingReasonOrderV1 {
		positions[reason] = index
	}
	last := -1
	for _, reason := range reasons {
		position, ok := positions[reason]
		if !ok || position <= last {
			return false
		}
		last = position
	}
	return true
}

func validRetest(retest RetestV1) bool {
	if !member(retest.Trigger, "NEW_SYNCHRONIZED_BUNDLE", "SOURCE_RECOVERED", "IDENTITY_CONFIRMED", "COMPARATOR_REVISED") ||
		len(retest.RequiredSourceKinds) < 1 || len(retest.RequiredSourceKinds) > 3 || !uniqueStrings(retest.RequiredSourceKinds) ||
		retest.MinimumNewSamples < 1 || retest.MinimumNewSamples > 1024 {
		return false
	}
	for _, kind := range retest.RequiredSourceKinds {
		if !member(kind, "EBUS", "EEBUS", "CLOUD_APP") {
			return false
		}
	}
	return true
}

func validTerminal(value *string) bool {
	return value == nil || member(*value, "NO_SIGNAL", "CLOUD_ONLY", "CONFLICT", "NOT_TESTED")
}

func validToken(value string) bool {
	if len(value) < 1 || len(value) > 256 {
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
			if char == '_' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
				continue
			}
			return false
		}
	}
	return true
}

func uniqueStrings(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func leaksPrivateIdentityV1(raw []byte) bool {
	lower := strings.ToLower(string(raw))
	for _, forbidden := range []string{
		`"semantic_path"`, `"proposed_path"`, `"source_address"`, `"target_address"`,
		`"entity"`, `"service"`, `"feature"`, `"selector"`, `"ski"`, `"ship_id"`,
		`"candidate_` + `ref"`, `"private_key"`, `"trust_store"`, `"secret"`, `"token"`,
	} {
		if strings.Contains(lower, forbidden) {
			return true
		}
	}
	return false
}

func member(value string, options ...string) bool {
	for _, option := range options {
		if value == option {
			return true
		}
	}
	return false
}
