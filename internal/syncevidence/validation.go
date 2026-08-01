package syncevidence

import (
	"encoding/json"
	"net"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	digestPattern        = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	gitCommitPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	versionPattern       = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+\+git\.[0-9a-f]{40}$`)
	bundleIDPattern      = regexp.MustCompile(`^sebv1:sha256:[0-9a-f]{64}$`)
	artifactIDPattern    = regexp.MustCompile(`^seav1:sha256:[0-9a-f]{64}$`)
	markerIDPattern      = regexp.MustCompile(`^marker-[0-9a-f]{32}$`)
	runtimeIDPattern     = regexp.MustCompile(`^runtime-[0-9a-f]{32}$`)
	sourceIDPattern      = regexp.MustCompile(`^(ebus|eebus|cloud)-[0-9a-f]{32}$`)
	targetIDPattern      = regexp.MustCompile(`^target-[0-9a-f]{32}$`)
	remaskIDPattern      = regexp.MustCompile(`^remask-[0-9a-f]{32}$`)
	remaskedValuePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	operationVersionExpr = regexp.MustCompile(`^git:[0-9a-f]{40}$`)
	timestampPattern     = regexp.MustCompile(`^[0-9]{4}-(0[1-9]|1[0-2])-([0-2][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,9})?Z$`)
	timeIdentityPattern  = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$`)
	repositoryPattern    = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	repositoryPathExpr   = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*(/[A-Za-z0-9_-][A-Za-z0-9._-]*)*$`)
	macPattern           = regexp.MustCompile(`(?i)(^|[^0-9a-f])([0-9a-f]{2}:){5}[0-9a-f]{2}([^0-9a-f]|$)`)
	ipv4Pattern          = regexp.MustCompile(`(^|[^0-9])((?:[0-9]{1,3}\.){3}[0-9]{1,3})([^0-9]|$)`)
	hostPathPattern      = regexp.MustCompile(`(?i)(^|[[:space:]"'=])/(Users|home|private|tmp|var|etc|opt|proc|sys|dev)(/|$)`)
	windowsPathPattern   = regexp.MustCompile(`(?i)(^|[[:space:]"'=])[a-z]:\\`)
	prohibitedText       = regexp.MustCompile(`(?i)(password|secret|private[ _-]?key|access[ _-]?token|refresh[ _-]?token|serial[ _-]?number|ship[ _-]?id|account[ _-]?id|vendor_restricted|raw packet|packet capture|\bpcap\b|wire transcript|protocol payload dump|native object dump|command line|process detail|environment value|credential)`)
)

type sourceAuthority struct {
	kind            SourceKind
	contract        string
	version         uint64
	ownerRepository string
	ownerPath       string
	ownerCommit     string
	schemaSHA256    string
}

var sourceAuthorities = mustLoadSourceAuthorities()

func ValidateLimitsV1(limits CaptureLimitsV1) error {
	hard := DefaultLimitsV1()
	values := [][2]uint64{
		{limits.MaxSources, hard.MaxSources},
		{limits.MaxItemsPerSource, hard.MaxItemsPerSource},
		{limits.MaxArtifactBytes, hard.MaxArtifactBytes},
		{limits.MaxBundleBytes, hard.MaxBundleBytes},
		{limits.MaxDepth, hard.MaxDepth},
		{limits.MaxStringBytes, hard.MaxStringBytes},
		{limits.MaxCaptureDurationNS, hard.MaxCaptureDurationNS},
		{limits.MaxSourceDurationNS, hard.MaxSourceDurationNS},
	}
	for _, value := range values {
		if value[0] == 0 || value[0] > value[1] || value[0] > MaxSafeIntegerV1 {
			return ErrLimitsExceeded
		}
	}
	if limits.MaxArtifactBytes > limits.MaxBundleBytes || limits.MaxSourceDurationNS > limits.MaxCaptureDurationNS {
		return ErrLimitsExceeded
	}
	return nil
}

func validRepository(value string) bool {
	return len(value) <= 256 && repositoryPattern.MatchString(value)
}

func validRepositoryPath(value string) bool {
	if len(value) == 0 || len(value) > 1024 || !repositoryPathExpr.MatchString(value) {
		return false
	}
	lower := strings.ToLower(value)
	return !strings.Contains(lower, "%2e") && !strings.Contains(lower, "%2f") && !strings.Contains(lower, "%5c")
}

func validASCIIToken(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for index := range value {
		if value[index] < 0x20 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func validateTimestamp(value time.Time) bool {
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 {
		return false
	}
	_, offset := value.Zone()
	return offset == 0
}

func canonicalTimestamp(value string) bool {
	match := timestampPattern.FindStringSubmatch(value)
	if match == nil {
		return false
	}
	fraction := match[len(match)-1]
	if fraction != "" && strings.HasSuffix(fraction, "0") {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && validateTimestamp(parsed) && parsed.UTC().Format(time.RFC3339Nano) == value
}

func validateEvidenceRef(ref EvidenceRefV1) error {
	if !digestPattern.MatchString(ref.Digest) {
		return contractError("schema.bundle")
	}
	switch ref.Kind {
	case EvidenceKindContent:
		if ref.DigestAlgorithm != DigestAlgorithmContentBytes || ref.Repository != nil || ref.Commit != nil || ref.Path != nil {
			return contractError("schema.bundle")
		}
	case EvidenceKindGitBlob:
		if ref.DigestAlgorithm != DigestAlgorithmGitBlobV1 || ref.Repository == nil || ref.Commit == nil || ref.Path == nil {
			return contractError("schema.bundle")
		}
		if !validRepository(*ref.Repository) || !gitCommitPattern.MatchString(*ref.Commit) || !validRepositoryPath(*ref.Path) {
			return contractError("schema.bundle")
		}
	default:
		return contractError("schema.bundle")
	}
	return nil
}

func evidenceRefKey(ref EvidenceRefV1) string {
	parts := []string{string(ref.Kind), string(ref.DigestAlgorithm), ref.Digest, "", "", ""}
	if ref.Repository != nil {
		parts[3] = "1" + *ref.Repository
	}
	if ref.Commit != nil {
		parts[4] = "1" + *ref.Commit
	}
	if ref.Path != nil {
		parts[5] = "1" + *ref.Path
	}
	return strings.Join(parts, "\x00")
}

func normalizeEvidenceRefs(refs []EvidenceRefV1) ([]EvidenceRefV1, error) {
	result := append([]EvidenceRefV1(nil), refs...)
	for _, ref := range result {
		if err := validateEvidenceRef(ref); err != nil {
			return nil, err
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return evidenceRefKey(result[left]) < evidenceRefKey(result[right])
	})
	for index := 1; index < len(result); index++ {
		if evidenceRefKey(result[index-1]) == evidenceRefKey(result[index]) {
			return nil, contractError("ordering.invalid")
		}
	}
	return result, nil
}

func normalizePermissions(permissions []string) ([]string, error) {
	result := append([]string(nil), permissions...)
	for _, permission := range result {
		if !validASCIIToken(permission, 128) {
			return nil, contractError("schema.bundle")
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index-1] == result[index] {
			return nil, contractError("ordering.invalid")
		}
	}
	if len(result) == 0 {
		return nil, contractError("schema.bundle")
	}
	return result, nil
}

func runtimeForSource(kind SourceKind) (RuntimeKind, bool) {
	switch kind {
	case SourceEBusB509, SourceEBusB524, SourceEBusB555:
		return RuntimeEBus, true
	case SourceEEBus:
		return RuntimeEEBus, true
	case SourceCloudApp:
		return RuntimeCloudApp, true
	default:
		return "", false
	}
}

func validateIdentity(identity *EBusSourceIdentityV1, kind SourceKind) error {
	if identity == nil || !targetIDPattern.MatchString(identity.TargetPseudonym) || !validASCIIToken(identity.UnitScaleSource, 128) {
		return contractError("schema.bundle")
	}
	switch kind {
	case SourceEBusB509:
		if identity.Family != EBusFamilyB509 || !validASCIIToken(identity.TargetProduct, 128) ||
			!validASCIIToken(identity.RegisterFamily, 128) ||
			(identity.EvidenceRole != "AUTHORITATIVE" && identity.EvidenceRole != "MIRROR" && identity.EvidenceRole != "FALLBACK") {
			return contractError("schema.bundle")
		}
	case SourceEBusB524:
		if identity.Family != EBusFamilyB524 || !validASCIIToken(identity.GroupMeaning, 128) ||
			!validASCIIToken(identity.InstanceGate, 128) ||
			(identity.RegisterCategory != "STATE" && identity.RegisterCategory != "CONFIG" && identity.RegisterCategory != "PARAMS") {
			return contractError("schema.bundle")
		}
	case SourceEBusB555:
		validDay := map[string]bool{"MONDAY": true, "TUESDAY": true, "WEDNESDAY": true, "THURSDAY": true, "FRIDAY": true, "SATURDAY": true, "SUNDAY": true}
		if identity.Family != EBusFamilyB555 || !validASCIIToken(identity.DeviceFamily, 128) ||
			!validASCIIToken(identity.ScheduleProgram, 128) || !validDay[identity.DayOfWeek] ||
			!timeIdentityPattern.MatchString(identity.TimeIdentity) || !validASCIIToken(identity.OperationModeContext, 128) {
			return contractError("schema.bundle")
		}
	default:
		return contractError("schema.bundle")
	}
	return nil
}

func validateInternalCapture(capture sourceCapture, phase Phase, limits CaptureLimitsV1) error {
	hasEvidence := len(capture.normalizedEvidence) > 0
	if !validPhase(phase) || capture.runtimeKind == "" || capture.sourceKind == "" ||
		capture.sourceContract == "" || capture.sourceSchemaVersion != 1 ||
		!operationVersionExpr.MatchString(capture.operationVersion) || !validASCIIToken(capture.operationScope, 128) ||
		len(capture.evidenceRefs) == 0 {
		return contractError("binding.registry")
	}
	if _, err := normalizeEvidenceRefs(capture.evidenceRefs); err != nil {
		return err
	}
	switch capture.state {
	case StatePresent:
		if capture.errorCategory != "" || !hasEvidence || !validateTimestamp(capture.sourceObservedAt) {
			return contractError("schema.bundle")
		}
	case StateWithheld:
		if hasEvidence || !capture.sourceObservedAt.IsZero() || !oneOfError(capture.errorCategory, ErrorPolicyWithheld, ErrorAuthorizationDenied, ErrorRedactionFailed, ErrorExactIdentityMissing) {
			return contractError("schema.bundle")
		}
	case StateNotTested:
		if hasEvidence || !capture.sourceObservedAt.IsZero() || !oneOfError(capture.errorCategory, ErrorNotSelected, ErrorBudgetExhausted, ErrorExactIdentityMissing) {
			return contractError("schema.bundle")
		}
	case StateUnavailable:
		if hasEvidence || !capture.sourceObservedAt.IsZero() || !oneOfError(capture.errorCategory, ErrorBackendUnavailable, ErrorTimeout, ErrorAcquisitionFailed) {
			return contractError("schema.bundle")
		}
	default:
		return contractError("schema.bundle")
	}
	if uint64(len(capture.normalizedEvidence)) > limits.MaxArtifactBytes {
		return contractError("limits.exceeded")
	}
	return nil
}

func oneOfError(value ErrorCategory, allowed ...ErrorCategory) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func validateRegisteredSource(source RegisteredSource) error {
	authority, exists := registeredSourceAuthority(source)
	runtimeKind, validKind := runtimeForSource(source.SourceKind)
	if !exists || !validKind || authority.kind != source.SourceKind || !validPhase(source.Phase) ||
		!operationVersionExpr.MatchString(source.OperationVersion) || source.OperationScope != expectedOperationScope(authority) {
		return ErrInvalidArgument
	}
	if source.RuntimeInstance != "" && source.SourceID != "" && source.RuntimeInstance != source.SourceID {
		return ErrInvalidArgument
	}
	if !validASCIIToken(runtimeInstanceKey(source), 128) {
		return ErrInvalidArgument
	}
	if source.Admission.Selection != SelectionIncluded && source.Admission.Selection != SelectionExcluded {
		return ErrInvalidArgument
	}
	if source.Admission.Policy != PolicyAllowed && source.Admission.Policy != PolicyWithheld {
		return ErrInvalidArgument
	}
	if source.Admission.Backend != BackendUnknown && source.Admission.Backend != BackendUnreachable {
		return ErrInvalidArgument
	}
	if _, err := normalizePermissions(source.Admission.EffectivePermissions); err != nil {
		return ErrInvalidArgument
	}
	if len(source.Admission.RequiredPermissions) == 0 {
		return ErrInvalidArgument
	}
	for _, permission := range source.Admission.RequiredPermissions {
		if !validASCIIToken(permission, 128) {
			return ErrInvalidArgument
		}
	}
	if runtimeKind == RuntimeEBus {
		if source.EBusReader == nil || source.EEBusReader != nil || source.EEBusM625Reader != nil || source.PrecapturedCloud != nil {
			return ErrInvalidArgument
		}
		if source.EBusIdentity != nil {
			identity := *source.EBusIdentity
			if identity.TargetPseudonym != "" {
				return ErrInvalidArgument
			}
			identity.TargetPseudonym = "target-00000000000000000000000000000000"
			if validateIdentity(&identity, source.SourceKind) != nil {
				return ErrInvalidArgument
			}
		}
		if len(source.EvidenceRefs) == 0 {
			return ErrInvalidArgument
		}
	} else if source.EBusIdentity != nil {
		return ErrInvalidArgument
	} else if runtimeKind == RuntimeEEBus {
		historical := authority.contract == HistoricalEEBusContractV1 && source.EEBusReader != nil && source.EEBusM625Reader == nil
		m625 := authority.contract == M625EEBusContractV1 && source.EEBusReader == nil && source.EEBusM625Reader != nil
		if (!historical && !m625) || source.EBusReader != nil || source.PrecapturedCloud != nil || len(source.EvidenceRefs) == 0 {
			return ErrInvalidArgument
		}
	} else {
		if source.PrecapturedCloud == nil || source.EBusReader != nil || source.EEBusReader != nil || source.EEBusM625Reader != nil || len(source.EvidenceRefs) != 0 {
			return ErrInvalidArgument
		}
		if err := validateEvidenceRef(source.PrecapturedCloud.EvidenceRef); err != nil {
			return ErrInvalidArgument
		}
	}
	if runtimeKind != RuntimeCloudApp {
		if _, err := normalizeEvidenceRefs(source.EvidenceRefs); err != nil {
			return ErrInvalidArgument
		}
	}
	return nil
}

func expectedOperationScope(authority sourceAuthority) string {
	switch authority.kind {
	case SourceEBusB509:
		return "ebus-b509"
	case SourceEBusB524:
		return "ebus-b524"
	case SourceEBusB555:
		return "ebus-b555"
	case SourceEEBus:
		if authority.contract == M625EEBusContractV1 {
			return "feature-data"
		}
		return "services"
	case SourceCloudApp:
		return "cloud-app"
	default:
		return ""
	}
}

func expectedSourceOperation(kind RuntimeKind, contract string) (string, SnapshotMode) {
	switch kind {
	case RuntimeEBus:
		return "ebus.v1.snapshot.capture", SnapshotFrozen
	case RuntimeEEBus:
		if contract == M625EEBusContractV1 {
			return "eebus.v1.features.data.get", SnapshotLiveRead
		}
		return "eebus.v1.services.list", SnapshotLiveRead
	case RuntimeCloudApp:
		return "cloud.precaptured.import", SnapshotPrecaptured
	default:
		return "", ""
	}
}

func validPhase(phase Phase) bool {
	return phase == PhasePre || phase == PhaseAction || phase == PhasePost
}

func validatePrivacy(value any) error {
	sensitiveKeys := map[string]bool{
		"password": true, "secret": true, "private_key": true, "access_token": true,
		"refresh_token": true, "serial_number": true, "ship_id": true, "ski": true,
		"account_id": true, "ip_address": true, "mac_address": true, "hostname": true,
		"interface_name": true, "host_path": true, "username": true, "endpoint": true,
		"certificate": true, "certificate_material": true, "credential": true,
		"device_id": true, "stable_device_id": true, "ski_digest": true,
		"command_line": true, "process_detail": true, "environment_value": true,
	}
	var visit func(any) error
	visit = func(current any) error {
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				if sensitiveKeys[strings.ToLower(key)] {
					return contractError("privacy.prohibited")
				}
				if err := visit(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := visit(child); err != nil {
					return err
				}
			}
		case string:
			if containsIPAddress(typed) || macPattern.MatchString(typed) || strings.Contains(typed, "://") ||
				hostPathPattern.MatchString(typed) || windowsPathPattern.MatchString(typed) || prohibitedText.MatchString(typed) ||
				strings.Contains(typed, "-----BEGIN CERTIFICATE-----") || strings.Contains(typed, "-----BEGIN PRIVATE KEY-----") {
				return contractError("privacy.prohibited")
			}
		}
		return nil
	}
	return visit(value)
}

func containsIPAddress(value string) bool {
	for _, match := range ipv4Pattern.FindAllStringSubmatch(value, -1) {
		if len(match) > 2 && net.ParseIP(match[2]) != nil {
			return true
		}
	}
	for _, token := range strings.FieldsFunc(value, func(char rune) bool {
		return (char < '0' || char > '9') &&
			(char < 'a' || char > 'f') &&
			(char < 'A' || char > 'F') &&
			char != ':' && char != '.'
	}) {
		if strings.Contains(token, ":") && net.ParseIP(token) != nil {
			return true
		}
	}
	return false
}

func validateWholeBundlePrivacy(bundle SynchronizedEvidenceBundleV1, limits CaptureLimitsV1) error {
	raw, err := json.Marshal(bundle)
	if err != nil {
		return contractError("schema.bundle")
	}
	value, _, err := parseJSON(raw, limits, true)
	if err != nil {
		return err
	}
	return validatePrivacy(value)
}

func validateTimestampLexemes(value any) error {
	timestampFields := map[string]bool{
		"captured_at": true, "marker_captured_at": true, "wall_anchor": true,
		"observed_at": true, "acquisition_started_at": true, "acquisition_ended_at": true,
		"source_observed_at": true, "recorder_ingested_at": true, "data_timestamp": true,
		"expires_at": true, "since": true,
	}
	var visit func(any) error
	visit = func(current any) error {
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				if timestampFields[key] && child != nil {
					text, ok := child.(string)
					if !ok || !canonicalTimestamp(text) {
						return contractError("schema.bundle")
					}
				}
				if err := visit(child); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := visit(child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(value)
}

func exactKeys(object map[string]any, expected ...string) bool {
	if len(object) != len(expected) {
		return false
	}
	for _, key := range expected {
		if _, exists := object[key]; !exists {
			return false
		}
	}
	return true
}
