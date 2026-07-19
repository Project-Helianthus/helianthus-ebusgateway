package syncevidence

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	rootKeys     = mustSchemaRequired("synchronized-evidence-bundle-v1.schema.json", "SynchronizedEvidenceBundleV1", "ed574071fdb11e10d5696c62e873a38c6c6dde64c6069bf616476cea8e8bf737")
	sourceKeys   = mustSchemaRequired("synchronized-evidence-bundle-v1.schema.json", "SourceRecordV1", "ed574071fdb11e10d5696c62e873a38c6c6dde64c6069bf616476cea8e8bf737")
	artifactKeys = mustSchemaRequired("synchronized-evidence-bundle-v1.schema.json", "SourceArtifactV1", "ed574071fdb11e10d5696c62e873a38c6c6dde64c6069bf616476cea8e8bf737")
	bindingKeys  = mustSchemaRequired("synchronized-evidence-bundle-v1.schema.json", "SourceBindingV1", "ed574071fdb11e10d5696c62e873a38c6c6dde64c6069bf616476cea8e8bf737")
)

func Replay(bundleBytes []byte) ([]byte, error) {
	bundle, err := verifyBundle(bundleBytes)
	if err != nil {
		return nil, err
	}
	result := replayResult(bundle)
	return canonicalMarshal(result, bundle.Limits, true)
}

func verifyBundle(bundleBytes []byte) (SynchronizedEvidenceBundleV1, error) {
	hard := DefaultLimitsV1()
	value, _, err := parseJSON(bundleBytes, hard, true)
	if err != nil {
		return SynchronizedEvidenceBundleV1{}, err
	}
	root, ok := value.(map[string]any)
	if !ok || !exactKeys(root, rootKeys...) {
		return SynchronizedEvidenceBundleV1{}, contractError("schema.bundle")
	}
	if err := validateClosedBundleShape(root); err != nil {
		return SynchronizedEvidenceBundleV1{}, err
	}
	if err := validateTimestampLexemes(root); err != nil {
		return SynchronizedEvidenceBundleV1{}, err
	}
	if err := validatePrivacy(root); err != nil {
		return SynchronizedEvidenceBundleV1{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(bundleBytes))
	decoder.DisallowUnknownFields()
	var bundle SynchronizedEvidenceBundleV1
	if err := decoder.Decode(&bundle); err != nil {
		return SynchronizedEvidenceBundleV1{}, contractError("schema.bundle")
	}
	if err := validateBundle(&bundle); err != nil {
		return SynchronizedEvidenceBundleV1{}, err
	}
	if uint64(len(bundleBytes)) > bundle.Limits.MaxBundleBytes {
		return SynchronizedEvidenceBundleV1{}, contractError("limits.exceeded")
	}
	if err := verifyGenericHashes(root); err != nil {
		return SynchronizedEvidenceBundleV1{}, err
	}
	return bundle, nil
}

func verifyGenericHashes(root map[string]any) error {
	artifacts, ok := root["artifacts"].([]any)
	if !ok {
		return contractError("schema.bundle")
	}
	for _, raw := range artifacts {
		artifact, ok := raw.(map[string]any)
		if !ok {
			return contractError("schema.bundle")
		}
		view := cloneJSONObject(artifact)
		delete(view, "artifact_id")
		delete(view, "redacted_hash")
		canonical, err := canonicalJSONValue(view)
		if err != nil {
			return err
		}
		hexdigest := domainDigest(artifactHashDomain, canonical)
		if artifact["artifact_id"] != "seav1:sha256:"+hexdigest || artifact["redacted_hash"] != "sha256:"+hexdigest {
			return contractError("hash.artifact")
		}
	}
	view := cloneJSONObject(root)
	delete(view, "bundle_id")
	delete(view, "bundle_hash")
	canonical, err := canonicalJSONValue(view)
	if err != nil {
		return err
	}
	hexdigest := domainDigest(bundleHashDomain, canonical)
	if root["bundle_id"] != "sebv1:sha256:"+hexdigest || root["bundle_hash"] != "sha256:"+hexdigest {
		return contractError("hash.bundle")
	}
	return nil
}

func cloneJSONObject(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, child := range value {
		result[key] = child
	}
	return result
}

func validateClosedBundleShape(root map[string]any) error {
	window, ok := root["capture_window"].(map[string]any)
	if !ok || !validWindowShape(window) {
		return contractError("schema.bundle")
	}
	clock, ok := root["clock"].(map[string]any)
	if !ok || !exactKeys(clock, "clock_id", "wall_anchor", "monotonic_anchor_ns", "captured_offset_ns", "resolution_ns", "maximum_skew_ns", "observations") {
		return contractError("schema.bundle")
	}
	observations, ok := clock["observations"].([]any)
	if !ok {
		return contractError("schema.bundle")
	}
	for _, raw := range observations {
		row, ok := raw.(map[string]any)
		if !ok || !exactKeys(row, "observed_at", "offset_ns", "uncertainty_ns") {
			return contractError("schema.bundle")
		}
	}
	if scope, ok := root["scope"].(map[string]any); !ok || !exactKeys(scope, "purpose", "source_kinds", "phases") {
		return contractError("schema.bundle")
	}
	if auth, ok := root["auth_scope"].(map[string]any); !ok || !exactKeys(auth, "authority", "permissions") {
		return contractError("schema.bundle")
	}
	if limits, ok := root["limits"].(map[string]any); !ok || !exactKeys(limits, "max_sources", "max_items_per_source", "max_artifact_bytes", "max_bundle_bytes", "max_depth", "max_string_bytes", "max_capture_duration_ns", "max_source_duration_ns") {
		return contractError("schema.bundle")
	}
	if err := validateRefShapes(root["evidence_refs"]); err != nil {
		return err
	}
	sources, ok := root["sources"].([]any)
	if !ok || len(sources) == 0 {
		return contractError("schema.bundle")
	}
	for _, raw := range sources {
		source, ok := raw.(map[string]any)
		if !ok || !exactKeys(source, sourceKeys...) || !validBindingShape(source["source_binding"]) ||
			!validWindowShapeMap(source["capture_window"]) || !validClockShapeMap(source["clock"]) ||
			!validScopeShapeMap(source["scope"]) || !validAuthShapeMap(source["auth_scope"]) {
			return contractError("schema.bundle")
		}
		if err := validateRefShapes(source["evidence_refs"]); err != nil {
			return err
		}
		if err := validIdentityShape(source["ebus_identity"]); err != nil {
			return err
		}
	}
	artifacts, ok := root["artifacts"].([]any)
	if !ok {
		return contractError("schema.bundle")
	}
	for _, raw := range artifacts {
		artifact, ok := raw.(map[string]any)
		if !ok || !exactKeys(artifact, artifactKeys...) || !validBindingShape(artifact["source_binding"]) ||
			!validWindowShapeMap(artifact["capture_window"]) || !validClockShapeMap(artifact["clock"]) ||
			!validScopeShapeMap(artifact["scope"]) || !validAuthShapeMap(artifact["auth_scope"]) {
			return contractError("schema.bundle")
		}
		if err := validateRefShapes(artifact["evidence_refs"]); err != nil {
			return err
		}
		if err := validIdentityShape(artifact["ebus_identity"]); err != nil {
			return err
		}
		remasking, ok := artifact["remasking"].(map[string]any)
		if !ok || !exactKeys(remasking, "method", "scope_id", "entries") {
			return contractError("schema.bundle")
		}
		entries, ok := remasking["entries"].([]any)
		if !ok {
			return contractError("schema.bundle")
		}
		for _, rawEntry := range entries {
			entry, ok := rawEntry.(map[string]any)
			if !ok || !exactKeys(entry, "path", "pseudonym") {
				return contractError("schema.bundle")
			}
		}
	}
	return nil
}

func validWindowShape(window map[string]any) bool {
	if !exactKeys(window, "pre", "action", "post") {
		return false
	}
	pre, preOK := window["pre"].(map[string]any)
	action, actionOK := window["action"].(map[string]any)
	post, postOK := window["post"].(map[string]any)
	return preOK && actionOK && postOK && exactKeys(pre, "start_offset_ns", "end_offset_ns") &&
		exactKeys(action, "start_offset_ns", "marker_offset_ns", "marker_captured_at", "marker_id", "evidence_ref", "end_offset_ns") &&
		exactKeys(post, "start_offset_ns", "end_offset_ns") && validRefShape(action["evidence_ref"])
}

func validWindowShapeMap(value any) bool {
	window, ok := value.(map[string]any)
	return ok && validWindowShape(window)
}

func validClockShapeMap(value any) bool {
	clock, ok := value.(map[string]any)
	if !ok || !exactKeys(clock, "clock_id", "wall_anchor", "monotonic_anchor_ns", "captured_offset_ns", "resolution_ns", "maximum_skew_ns", "observations") {
		return false
	}
	rows, ok := clock["observations"].([]any)
	if !ok {
		return false
	}
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok || !exactKeys(row, "observed_at", "offset_ns", "uncertainty_ns") {
			return false
		}
	}
	return true
}

func validScopeShapeMap(value any) bool {
	scope, ok := value.(map[string]any)
	return ok && exactKeys(scope, "purpose", "source_kinds", "phases")
}

func validAuthShapeMap(value any) bool {
	auth, ok := value.(map[string]any)
	return ok && exactKeys(auth, "authority", "permissions")
}

func validBindingShape(value any) bool {
	binding, ok := value.(map[string]any)
	if !ok || !exactKeys(binding, bindingKeys...) || !validWindowShapeMap(binding["capture_window"]) || !validAuthShapeMap(binding["auth_scope"]) {
		return false
	}
	request, requestOK := binding["request_scope"].(map[string]any)
	snapshot, snapshotOK := binding["snapshot_scope"].(map[string]any)
	return requestOK && snapshotOK && exactKeys(request, "phase", "source_kind", "operation_scope") &&
		exactKeys(snapshot, "mode", "selector") && validIdentityShape(binding["ebus_identity"]) == nil
}

func validIdentityShape(value any) error {
	if value == nil {
		return nil
	}
	identity, ok := value.(map[string]any)
	if !ok {
		return contractError("schema.bundle")
	}
	switch identity["family"] {
	case "B509":
		if !exactKeys(identity, "family", "target_pseudonym", "target_address", "target_product", "register_family", "register_id", "unit_scale_source", "evidence_role") {
			return contractError("schema.bundle")
		}
	case "B524":
		if !exactKeys(identity, "family", "target_pseudonym", "opcode", "GG", "II", "RR", "target_address", "source_address", "group_meaning", "instance_gate", "register_category", "unit_scale_source") {
			return contractError("schema.bundle")
		}
	case "B555":
		if !exactKeys(identity, "family", "target_pseudonym", "device_family", "schedule_program", "slot_index", "day_of_week", "time_identity", "operation_mode_context", "unit_scale_source") {
			return contractError("schema.bundle")
		}
	default:
		return contractError("schema.bundle")
	}
	return nil
}

func validateRefShapes(value any) error {
	refs, ok := value.([]any)
	if !ok || len(refs) == 0 {
		return contractError("schema.bundle")
	}
	for _, ref := range refs {
		if !validRefShape(ref) {
			return contractError("schema.bundle")
		}
	}
	return nil
}

func validRefShape(value any) bool {
	ref, ok := value.(map[string]any)
	return ok && exactKeys(ref, "kind", "digest_algorithm", "digest", "repository", "commit", "path")
}

func validateBundle(bundle *SynchronizedEvidenceBundleV1) error {
	if bundle.Contract != BundleContractV1 || bundle.SchemaVersion != 1 || !bundleIDPattern.MatchString(bundle.BundleID) ||
		!digestPattern.MatchString(bundle.BundleHash) || !versionPattern.MatchString(bundle.RecorderVersion) ||
		!versionPattern.MatchString(bundle.ReplayVersion) || bundle.MaskTier != "redacted" || !validateTimestamp(bundle.CapturedAt) {
		return contractError("schema.bundle")
	}
	if err := ValidateLimitsV1(bundle.Limits); err != nil {
		return contractError("limits.exceeded")
	}
	if uint64(len(bundle.Sources)) > bundle.Limits.MaxSources || len(bundle.Sources) == 0 {
		return contractError("limits.exceeded")
	}
	if uint64(len(bundle.Artifacts)) > bundle.Limits.MaxSources*3 {
		return contractError("limits.exceeded")
	}
	if err := validateWindow(bundle.CaptureWindow); err != nil {
		return err
	}
	if err := validateClock(bundle.Clock, bundle.CapturedAt); err != nil {
		return err
	}
	if !wallMatchesClock(bundle.CaptureWindow.Action.MarkerCapturedAt, bundle.CaptureWindow.Action.MarkerOffsetNS, bundle.Clock) {
		return contractError("clock.skew")
	}
	if bundle.CaptureWindow.Post.EndOffsetNS > bundle.Clock.CapturedOffsetNS || bundle.Clock.CapturedOffsetNS > bundle.Limits.MaxCaptureDurationNS {
		return contractError("clock.skew")
	}
	if err := validateScope(bundle.Scope, bundle.Sources); err != nil {
		return err
	}
	rootPermissions, err := validateAuth(bundle.AuthScope, nil)
	if err != nil {
		return err
	}
	refs, err := normalizeEvidenceRefs(bundle.EvidenceRefs)
	if err != nil || !reflect.DeepEqual(refs, bundle.EvidenceRefs) {
		return contractError("ordering.invalid")
	}
	rootRefSet := make(map[string]bool, len(refs))
	for _, ref := range refs {
		rootRefSet[evidenceRefKey(ref)] = true
	}
	if err := validateSources(bundle, rootPermissions, rootRefSet); err != nil {
		return err
	}
	expectedRefs := map[string]bool{evidenceRefKey(bundle.CaptureWindow.Action.EvidenceRef): true}
	for _, source := range bundle.Sources {
		for _, ref := range source.EvidenceRefs {
			expectedRefs[evidenceRefKey(ref)] = true
		}
	}
	if !reflect.DeepEqual(expectedRefs, rootRefSet) {
		return contractError("binding.registry")
	}
	if err := validateArtifacts(bundle, rootPermissions, rootRefSet); err != nil {
		return err
	}
	hexdigest, err := bundleDigestV1(*bundle, bundle.Limits)
	if err != nil {
		return err
	}
	if bundle.BundleID != "sebv1:sha256:"+hexdigest || bundle.BundleHash != "sha256:"+hexdigest {
		return contractError("hash.bundle")
	}
	return nil
}

func validateWindow(window CaptureWindowV1) error {
	if window.Pre.StartOffsetNS >= window.Pre.EndOffsetNS || window.Pre.EndOffsetNS != window.Action.StartOffsetNS ||
		window.Action.StartOffsetNS >= window.Action.EndOffsetNS || window.Action.MarkerOffsetNS < window.Action.StartOffsetNS ||
		window.Action.MarkerOffsetNS > window.Action.EndOffsetNS || window.Action.EndOffsetNS != window.Post.StartOffsetNS ||
		window.Post.StartOffsetNS >= window.Post.EndOffsetNS || !markerIDPattern.MatchString(window.Action.MarkerID) ||
		!validateTimestamp(window.Action.MarkerCapturedAt) {
		return contractError("schema.bundle")
	}
	return validateEvidenceRef(window.Action.EvidenceRef)
}

func validateClock(clock CaptureClockV1, capturedAt time.Time) error {
	if clock.ClockID != "capture-clock-1" || clock.MonotonicAnchorNS != 0 || clock.ResolutionNS == 0 ||
		clock.ResolutionNS > MaxSafeIntegerV1 || clock.MaximumSkewNS > MaximumClockSkew || len(clock.Observations) < 2 || !validateTimestamp(clock.WallAnchor) {
		return contractError("clock.skew")
	}
	if clock.Observations[0].OffsetNS != 0 || !clock.Observations[0].ObservedAt.Equal(clock.WallAnchor) {
		return contractError("clock.skew")
	}
	maximumSkew := uint64(0)
	for index, observation := range clock.Observations {
		if !validateTimestamp(observation.ObservedAt) || observation.OffsetNS > MaxSafeIntegerV1 || observation.UncertaintyNS > MaxSafeIntegerV1 {
			return contractError("clock.skew")
		}
		if index > 0 && observation.OffsetNS <= clock.Observations[index-1].OffsetNS {
			return contractError("clock.skew")
		}
		wallDelta := observation.ObservedAt.Sub(clock.WallAnchor).Nanoseconds()
		if wallDelta < 0 {
			return contractError("clock.skew")
		}
		difference := absoluteDifference(uint64(wallDelta), observation.OffsetNS)
		if difference > MaxSafeIntegerV1-observation.UncertaintyNS {
			return contractError("clock.skew")
		}
		skew := difference + observation.UncertaintyNS
		if skew > maximumSkew {
			maximumSkew = skew
		}
	}
	if maximumSkew != clock.MaximumSkewNS || clock.Observations[len(clock.Observations)-1].OffsetNS < clock.CapturedOffsetNS ||
		!wallMatchesClock(capturedAt, clock.CapturedOffsetNS, clock) {
		return contractError("clock.skew")
	}
	return nil
}

func wallMatchesClock(wall time.Time, offset uint64, clock CaptureClockV1) bool {
	if !validateTimestamp(wall) {
		return false
	}
	delta := wall.Sub(clock.WallAnchor).Nanoseconds()
	return delta >= 0 && absoluteDifference(uint64(delta), offset) <= clock.MaximumSkewNS
}

func validateScope(scope CaptureScopeV1, sources []SourceRecordV1) error {
	if scope.Purpose != "SYNCHRONIZED_EVIDENCE_ONLY" || !reflect.DeepEqual(scope.Phases, []Phase{PhasePre, PhaseAction, PhasePost}) || len(scope.SourceKinds) == 0 {
		return contractError("schema.bundle")
	}
	seen := make(map[RuntimeKind]bool)
	for _, source := range sources {
		seen[source.SourceKind] = true
	}
	expected := make([]RuntimeKind, 0, len(seen))
	for _, kind := range []RuntimeKind{RuntimeEBus, RuntimeEEBus, RuntimeCloudApp} {
		if seen[kind] {
			expected = append(expected, kind)
		}
	}
	if !reflect.DeepEqual(scope.SourceKinds, expected) {
		return contractError("ordering.invalid")
	}
	return nil
}

func validateAuth(scope AuthScopeV1, parent map[string]bool) (map[string]bool, error) {
	if scope.Authority != "effective" {
		return nil, contractError("schema.bundle")
	}
	normalized, err := normalizePermissions(scope.Permissions)
	if err != nil || !reflect.DeepEqual(normalized, scope.Permissions) {
		return nil, contractError("ordering.invalid")
	}
	result := make(map[string]bool, len(scope.Permissions))
	for _, permission := range scope.Permissions {
		if parent != nil && !parent[permission] {
			return nil, contractError("binding.registry")
		}
		result[permission] = true
	}
	return result, nil
}

func validateSources(bundle *SynchronizedEvidenceBundleV1, rootPermissions map[string]bool, rootRefs map[string]bool) error {
	ordered := append([]SourceRecordV1(nil), bundle.Sources...)
	sortSourceRecords(ordered)
	if !reflect.DeepEqual(ordered, bundle.Sources) {
		return contractError("ordering.invalid")
	}
	keys := make(map[string]bool)
	bindingToID := make(map[string]string)
	idToBinding := make(map[string]string)
	for index := range bundle.Sources {
		source := &bundle.Sources[index]
		key := string(source.Phase) + "\x00" + string(source.SourceKind) + "\x00" + source.SourceID
		if keys[key] || !sourceIDPattern.MatchString(source.SourceID) || source.Contract != bundle.Contract || source.SchemaVersion != 1 ||
			source.MaskTier != bundle.MaskTier || source.RecorderVersion != bundle.RecorderVersion || source.ReplayVersion != bundle.ReplayVersion ||
			!reflect.DeepEqual(source.CaptureWindow, bundle.CaptureWindow) || !reflect.DeepEqual(source.Clock, bundle.Clock) ||
			!reflect.DeepEqual(source.Scope, bundle.Scope) || source.MaximumSkewNS != bundle.Clock.MaximumSkewNS {
			return contractError("binding.duplicate")
		}
		expectedPrefix := strings.ToLower(string(source.SourceKind)) + "-"
		if source.SourceKind == RuntimeCloudApp {
			expectedPrefix = "cloud-"
		}
		if !strings.HasPrefix(source.SourceID, expectedPrefix) {
			return contractError("schema.bundle")
		}
		keys[key] = true
		permissions, err := validateAuth(source.AuthScope, rootPermissions)
		if err != nil {
			return err
		}
		if err := validateBinding(source.SourceBinding, source, permissions); err != nil {
			return err
		}
		if err := validateSourceState(source, bundle.CaptureWindow, bundle.Limits, bundle.Clock); err != nil {
			return err
		}
		refs, err := normalizeEvidenceRefs(source.EvidenceRefs)
		if err != nil || !reflect.DeepEqual(refs, source.EvidenceRefs) {
			return contractError("ordering.invalid")
		}
		for _, ref := range refs {
			if !rootRefs[evidenceRefKey(ref)] {
				return contractError("binding.registry")
			}
		}
		if !sort.StringsAreSorted(source.ArtifactIDs) || hasDuplicateStrings(source.ArtifactIDs) {
			return contractError("ordering.invalid")
		}
		bindingBytes, err := canonicalMarshal(source.SourceBinding, bundle.Limits, true)
		if err != nil {
			return err
		}
		bindingKey := string(bindingBytes)
		if prior, exists := bindingToID[bindingKey]; exists && prior != source.SourceID {
			return contractError("binding.duplicate")
		}
		if prior, exists := idToBinding[source.SourceID]; exists && prior != bindingKey {
			return contractError("binding.duplicate")
		}
		bindingToID[bindingKey] = source.SourceID
		idToBinding[source.SourceID] = bindingKey
	}
	return nil
}

func validateBinding(binding SourceBindingV1, source *SourceRecordV1, permissions map[string]bool) error {
	authority, exists := sourceAuthorities[binding.SourceKind]
	expectedRuntime, validKind := runtimeForSource(binding.SourceKind)
	if !exists || !validKind || binding.RuntimeKind != expectedRuntime || binding.RuntimeKind != source.SourceKind ||
		!runtimeIDPattern.MatchString(binding.RuntimePseudonym) || !operationVersionExpr.MatchString(binding.OperationVersion) ||
		!validASCIIToken(binding.OperationID, 128) || !validASCIIToken(binding.RequestScope.OperationScope, 128) ||
		binding.RequestScope.Phase != source.Phase || binding.RequestScope.SourceKind != source.SourceKind ||
		binding.SnapshotScope.Selector != binding.RequestScope.OperationScope || binding.MaskTier != "redacted" ||
		binding.SourceContract != source.SourceContract || binding.SourceSchemaVersion != source.SourceSchemaVersion ||
		binding.SourceContract != authority.contract || binding.SourceSchemaVersion != authority.version ||
		binding.OwnerRepository != authority.ownerRepository || binding.OwnerPath != authority.ownerPath ||
		binding.OwnerCommit != authority.ownerCommit || binding.SchemaSHA256 != authority.schemaSHA256 ||
		!reflect.DeepEqual(binding.CaptureWindow, source.CaptureWindow) || !reflect.DeepEqual(binding.AuthScope, source.AuthScope) ||
		!reflect.DeepEqual(binding.EBusIdentity, source.EBusIdentity) {
		return contractError("binding.registry")
	}
	expectedOperation, expectedMode := expectedSourceOperation(source.SourceKind)
	if binding.OperationID != expectedOperation || binding.SnapshotScope.Mode != expectedMode {
		return contractError("binding.registry")
	}
	if _, err := validateAuth(binding.AuthScope, permissions); err != nil {
		return err
	}
	if source.SourceKind == RuntimeEBus {
		if source.State == StatePresent {
			return validateIdentity(source.EBusIdentity, binding.SourceKind)
		}
		if source.EBusIdentity != nil {
			return validateIdentity(source.EBusIdentity, binding.SourceKind)
		}
	} else if source.EBusIdentity != nil {
		return contractError("schema.bundle")
	}
	return nil
}

func validateSourceState(source *SourceRecordV1, window CaptureWindowV1, limits CaptureLimitsV1, clock CaptureClockV1) error {
	timingCount := 0
	for _, present := range []bool{source.AcquisitionStartedAt != nil, source.AcquisitionEndedAt != nil, source.AcquisitionStartOffsetNS != nil, source.AcquisitionEndOffsetNS != nil, source.MeasuredLatencyNS != nil} {
		if present {
			timingCount++
		}
	}
	validCategory := func(values ...ErrorCategory) bool {
		if source.ErrorCategory == nil {
			return false
		}
		return oneOfError(*source.ErrorCategory, values...)
	}
	switch source.State {
	case StatePresent:
		if source.ErrorCategory != nil || len(source.ArtifactIDs) == 0 || timingCount != 5 {
			return contractError("schema.bundle")
		}
	case StateWithheld:
		if len(source.ArtifactIDs) != 0 || (timingCount != 0 && timingCount != 5) || !validCategory(ErrorPolicyWithheld, ErrorAuthorizationDenied, ErrorRedactionFailed, ErrorExactIdentityMissing) {
			return contractError("schema.bundle")
		}
	case StateNotTested:
		if len(source.ArtifactIDs) != 0 || timingCount != 0 || !validCategory(ErrorNotSelected, ErrorBudgetExhausted, ErrorExactIdentityMissing) {
			return contractError("schema.bundle")
		}
	case StateUnavailable:
		if len(source.ArtifactIDs) != 0 || timingCount != 5 || !validCategory(ErrorBackendUnavailable, ErrorTimeout, ErrorAcquisitionFailed) {
			return contractError("schema.bundle")
		}
	default:
		return contractError("schema.bundle")
	}
	if timingCount == 5 {
		if *source.AcquisitionEndOffsetNS < *source.AcquisitionStartOffsetNS ||
			*source.MeasuredLatencyNS != *source.AcquisitionEndOffsetNS-*source.AcquisitionStartOffsetNS ||
			*source.MeasuredLatencyNS > limits.MaxSourceDurationNS ||
			!wallMatchesClock(*source.AcquisitionStartedAt, *source.AcquisitionStartOffsetNS, clock) ||
			!wallMatchesClock(*source.AcquisitionEndedAt, *source.AcquisitionEndOffsetNS, clock) {
			return contractError("clock.skew")
		}
		segment := phaseSegment(window, source.Phase)
		if *source.AcquisitionStartOffsetNS < segment.StartOffsetNS || *source.AcquisitionEndOffsetNS > segment.EndOffsetNS {
			return contractError("schema.bundle")
		}
	}
	return nil
}

func phaseSegment(window CaptureWindowV1, phase Phase) WindowSegmentV1 {
	switch phase {
	case PhasePre:
		return window.Pre
	case PhaseAction:
		return WindowSegmentV1{StartOffsetNS: window.Action.StartOffsetNS, EndOffsetNS: window.Action.EndOffsetNS}
	case PhasePost:
		return window.Post
	default:
		return WindowSegmentV1{}
	}
}

func validateArtifacts(bundle *SynchronizedEvidenceBundleV1, rootPermissions map[string]bool, rootRefs map[string]bool) error {
	ordered := append([]SourceArtifactV1(nil), bundle.Artifacts...)
	sortArtifacts(ordered)
	if len(ordered) != len(bundle.Artifacts) || (len(ordered) > 0 && !reflect.DeepEqual(ordered, bundle.Artifacts)) {
		return contractError("ordering.invalid")
	}
	bySource := make(map[string]*SourceRecordV1, len(bundle.Sources))
	for index := range bundle.Sources {
		bySource[bundle.Sources[index].SourceID] = &bundle.Sources[index]
	}
	seenArtifacts := make(map[string]bool)
	referenced := make(map[string]bool)
	remaskScope := ""
	remaskAssignments := make(map[string]string)
	for index := range bundle.Artifacts {
		artifact := &bundle.Artifacts[index]
		source := bySource[artifact.SourceID]
		if source == nil || source.State != StatePresent || seenArtifacts[artifact.ArtifactID] || !artifactIDPattern.MatchString(artifact.ArtifactID) ||
			artifact.Contract != bundle.Contract || artifact.SchemaVersion != 1 || !reflect.DeepEqual(artifact.SourceBinding, source.SourceBinding) ||
			artifact.SourceKind != source.SourceKind || artifact.Phase != source.Phase || artifact.SourceContract != source.SourceContract ||
			artifact.SourceSchemaVersion != source.SourceSchemaVersion || !reflect.DeepEqual(artifact.EBusIdentity, source.EBusIdentity) ||
			!reflect.DeepEqual(artifact.CaptureWindow, bundle.CaptureWindow) || !reflect.DeepEqual(artifact.Clock, bundle.Clock) ||
			!reflect.DeepEqual(artifact.Scope, bundle.Scope) || artifact.MaskTier != bundle.MaskTier ||
			artifact.RecorderVersion != bundle.RecorderVersion || artifact.ReplayVersion != bundle.ReplayVersion ||
			!validateTimestamp(artifact.SourceObservedAt) || !wallMatchesClock(artifact.RecorderIngestedAt, artifact.RecorderIngestedOffsetNS, bundle.Clock) {
			return contractError("binding.registry")
		}
		segment := phaseSegment(bundle.CaptureWindow, artifact.Phase)
		if artifact.RecorderIngestedOffsetNS < segment.StartOffsetNS || artifact.RecorderIngestedOffsetNS > segment.EndOffsetNS {
			return contractError("schema.bundle")
		}
		seenArtifacts[artifact.ArtifactID] = true
		if !containsString(source.ArtifactIDs, artifact.ArtifactID) || referenced[artifact.ArtifactID] {
			return contractError("binding.duplicate")
		}
		referenced[artifact.ArtifactID] = true
		if _, err := validateAuth(artifact.AuthScope, rootPermissions); err != nil || !reflect.DeepEqual(artifact.AuthScope, source.AuthScope) {
			return contractError("binding.registry")
		}
		refs, err := normalizeEvidenceRefs(artifact.EvidenceRefs)
		if err != nil || !reflect.DeepEqual(refs, artifact.EvidenceRefs) {
			return contractError("ordering.invalid")
		}
		for _, ref := range refs {
			if !rootRefs[evidenceRefKey(ref)] {
				return contractError("binding.registry")
			}
			if !containsEvidenceRef(source.EvidenceRefs, ref) {
				return contractError("binding.registry")
			}
		}
		value, stats, err := parseJSON(artifact.NormalizedEvidence, bundle.Limits, false)
		if err != nil {
			return err
		}
		object, ok := value.(map[string]any)
		canonicalEvidence, canonicalErr := canonicalMarshal(value, bundle.Limits, false)
		if !ok || canonicalErr != nil || uint64(len(canonicalEvidence)) != artifact.ByteCount || stats.arrayItems > bundle.Limits.MaxItemsPerSource {
			return contractError("limits.exceeded")
		}
		capture := sourceCapture{sourceKind: artifact.SourceBinding.SourceKind, sourceContract: artifact.SourceContract, sourceSchemaVersion: artifact.SourceSchemaVersion, operationID: artifact.SourceBinding.OperationID, operationScope: artifact.SourceBinding.RequestScope.OperationScope, sourceObservedAt: artifact.SourceObservedAt, ebusIdentity: artifact.EBusIdentity}
		if err := validateSourcePayload(object, capture); err != nil {
			return err
		}
		if sourceItemCount(object, capture.sourceKind) != artifact.ItemCount || artifact.ItemCount > bundle.Limits.MaxItemsPerSource {
			return contractError("limits.exceeded")
		}
		if err := validatePrivacy(value); err != nil {
			return err
		}
		if err := validateRemasking(artifact, value, bundle, &remaskScope, remaskAssignments); err != nil {
			return err
		}
		hexdigest, err := artifactDigest(*artifact, bundle.Limits)
		if err != nil {
			return err
		}
		if artifact.ArtifactID != "seav1:sha256:"+hexdigest || artifact.RedactedHash != "sha256:"+hexdigest {
			return contractError("hash.artifact")
		}
	}
	for _, source := range bundle.Sources {
		for _, artifactID := range source.ArtifactIDs {
			if !referenced[artifactID] {
				return contractError("binding.duplicate")
			}
		}
	}
	return nil
}

func validateRemasking(artifact *SourceArtifactV1, value any, bundle *SynchronizedEvidenceBundleV1, bundleScope *string, assignments map[string]string) error {
	if artifact.Remasking.Method != "PER_BUNDLE_CSPRNG" || !remaskIDPattern.MatchString(artifact.Remasking.ScopeID) {
		return contractError("privacy.remask")
	}
	if *bundleScope == "" {
		*bundleScope = artifact.Remasking.ScopeID
	} else if artifact.Remasking.ScopeID != *bundleScope {
		return contractError("privacy.remask")
	}
	last := ""
	paths := make(map[string]string)
	for _, entry := range artifact.Remasking.Entries {
		if entry.Path == "" || !remaskedValuePattern.MatchString(entry.Pseudonym) || (last != "" && (entry.Path < last || entry.Path == last)) {
			return contractError("privacy.remask")
		}
		resolved, ok := resolveJSONPointer(value, entry.Path)
		if !ok || resolved != entry.Pseudonym {
			return contractError("privacy.remask")
		}
		if prior, duplicate := assignments[entry.Pseudonym]; duplicate && prior != artifact.ArtifactID+"\x00"+entry.Path {
			return contractError("privacy.remask")
		}
		assignments[entry.Pseudonym] = artifact.ArtifactID + "\x00" + entry.Path
		paths[entry.Path] = entry.Pseudonym
		last = entry.Path
	}
	required := make(map[string]string)
	collectRemaskedPaths(value, artifact.SourceBinding.SourceKind, "", required)
	if !reflect.DeepEqual(required, paths) {
		return contractError("privacy.remask")
	}
	canonical, err := canonicalMarshal(value, bundle.Limits, false)
	if err != nil {
		return err
	}
	for _, forbidden := range []string{artifact.SourceID, artifact.SourceBinding.RuntimePseudonym, bundle.CaptureWindow.Action.MarkerID} {
		if forbidden != "" && bytes.Contains(canonical, []byte(forbidden)) {
			return contractError("privacy.remask")
		}
	}
	if artifact.EBusIdentity != nil && bytes.Contains(canonical, []byte(artifact.EBusIdentity.TargetPseudonym)) {
		// The eBUS source schema deliberately repeats this per-bundle target pseudonym.
		return nil
	}
	return nil
}

func collectRemaskedPaths(value any, kind SourceKind, pointer string, result map[string]string) {
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			path := pointer + "/" + strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
			if text, ok := child.(string); ok && ((kind == SourceEEBus && isEEBusIdentityDigest(current, key)) || (kind == SourceCloudApp && key == "subject_pseudonym")) {
				result[path] = text
			} else {
				collectRemaskedPaths(child, kind, path, result)
			}
		}
	case []any:
		for index, child := range current {
			collectRemaskedPaths(child, kind, pointer+"/"+strconv.Itoa(index), result)
		}
	}
}

func resolveJSONPointer(value any, pointer string) (string, bool) {
	if pointer == "" || pointer[0] != '/' {
		return "", false
	}
	current := value
	for _, encoded := range strings.Split(pointer[1:], "/") {
		token := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
		switch node := current.(type) {
		case map[string]any:
			var exists bool
			current, exists = node[token]
			if !exists {
				return "", false
			}
		case []any:
			index := 0
			if token == "" {
				return "", false
			}
			for _, char := range token {
				if char < '0' || char > '9' {
					return "", false
				}
				index = index*10 + int(char-'0')
			}
			if index >= len(node) {
				return "", false
			}
			current = node[index]
		default:
			return "", false
		}
	}
	text, ok := current.(string)
	return text, ok
}

func replayResult(bundle SynchronizedEvidenceBundleV1) ReplayResultV1 {
	result := ReplayResultV1{
		Contract: ReplayContractV1, SchemaVersion: 1, BundleID: bundle.BundleID,
		RawNormalizedEvidence: make([]RawNormalizedEvidenceRowV1, 0, len(bundle.Artifacts)),
		CapturedTimestamps:    []CapturedTimestampRowV1{{EventKind: "ACTION_MARKER", RecorderObservedAt: bundle.CaptureWindow.Action.MarkerCapturedAt, RecorderOffsetNS: bundle.CaptureWindow.Action.MarkerOffsetNS}},
		TerminalStates:        make([]TerminalStateRowV1, 0, len(bundle.Sources)),
		RedactedHashes:        make([]RedactedHashRowV1, 0, len(bundle.Artifacts)+1),
		FutureCandidateInputs: make([]FutureCandidateInputRowV1, 0, len(bundle.Artifacts)),
	}
	for _, source := range bundle.Sources {
		if source.AcquisitionStartedAt != nil {
			sourceID := source.SourceID
			result.CapturedTimestamps = append(result.CapturedTimestamps,
				CapturedTimestampRowV1{EventKind: "SOURCE_ACQUISITION_START", SourceID: &sourceID, RecorderObservedAt: *source.AcquisitionStartedAt, RecorderOffsetNS: *source.AcquisitionStartOffsetNS},
				CapturedTimestampRowV1{EventKind: "SOURCE_ACQUISITION_END", SourceID: &sourceID, RecorderObservedAt: *source.AcquisitionEndedAt, RecorderOffsetNS: *source.AcquisitionEndOffsetNS},
			)
		}
		result.TerminalStates = append(result.TerminalStates, TerminalStateRowV1{SourceID: source.SourceID, Phase: source.Phase, State: source.State, ErrorCategory: source.ErrorCategory})
	}
	for _, artifact := range bundle.Artifacts {
		artifactID, sourceID, observedAt := artifact.ArtifactID, artifact.SourceID, artifact.SourceObservedAt
		result.RawNormalizedEvidence = append(result.RawNormalizedEvidence, RawNormalizedEvidenceRowV1{ArtifactID: artifact.ArtifactID, SourceBinding: artifact.SourceBinding, SourceObservedAt: artifact.SourceObservedAt, NormalizedEvidence: artifact.NormalizedEvidence})
		result.CapturedTimestamps = append(result.CapturedTimestamps, CapturedTimestampRowV1{EventKind: "ARTIFACT_INGESTION", SourceID: &sourceID, ArtifactID: &artifactID, SourceObservedAt: &observedAt, RecorderObservedAt: artifact.RecorderIngestedAt, RecorderOffsetNS: artifact.RecorderIngestedOffsetNS})
		result.RedactedHashes = append(result.RedactedHashes, RedactedHashRowV1{Kind: "ARTIFACT", ArtifactID: &artifactID, Digest: artifact.RedactedHash})
		result.FutureCandidateInputs = append(result.FutureCandidateInputs, FutureCandidateInputRowV1{ArtifactID: artifact.ArtifactID, SourceID: artifact.SourceID, SourceBinding: artifact.SourceBinding, EvidenceRefs: artifact.EvidenceRefs, RedactedHash: artifact.RedactedHash})
	}
	result.RedactedHashes = append(result.RedactedHashes, RedactedHashRowV1{Kind: "BUNDLE", Digest: bundle.BundleHash})
	return result
}

func hasDuplicateStrings(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsEvidenceRef(values []EvidenceRefV1, wanted EvidenceRefV1) bool {
	wantedKey := evidenceRefKey(wanted)
	for _, value := range values {
		if evidenceRefKey(value) == wantedKey {
			return true
		}
	}
	return false
}
