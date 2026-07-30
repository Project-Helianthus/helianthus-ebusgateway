package syncevidence

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errAcquisitionPanic = errors.New("source acquisition panic")

type Recorder struct {
	mu              sync.Mutex
	clock           Clock
	newTimer        func(time.Duration) StoppableTimer
	entropy         io.Reader
	limits          CaptureLimitsV1
	sources         []RegisteredSource
	recorderVersion string
	replayVersion   string
	pendingSource   <-chan acquisitionOutcome
}

type capturedSource struct {
	registered  RegisteredSource
	result      sourceCapture
	authority   sourceAuthority
	permissions []string
	start       ClockObservationV1
	end         ClockObservationV1
	hasTiming   bool
}

type sourceCapture struct {
	runtimeKind         RuntimeKind
	sourceKind          SourceKind
	sourceContract      string
	sourceSchemaVersion uint64
	operationID         string
	operationVersion    string
	operationScope      string
	snapshotMode        SnapshotMode
	state               SourceState
	errorCategory       ErrorCategory
	sourceObservedAt    time.Time
	ebusIdentity        *EBusSourceIdentityV1
	evidenceRefs        []EvidenceRefV1
	normalizedEvidence  json.RawMessage
}

type acquisitionOutcome struct {
	evidence AcquiredEvidence
	err      error
}

type systemTimer struct {
	timer *time.Timer
}

func (timer *systemTimer) C() <-chan time.Time { return timer.timer.C }
func (timer *systemTimer) Stop() bool          { return timer.timer.Stop() }

type captureTimeline struct {
	clock        Clock
	baseOffset   uint64
	observations []ClockObservationV1
}

func NewRecorder(options RecorderOptions) (*Recorder, error) {
	if options.Clock == nil || len(options.Sources) == 0 {
		return nil, ErrInvalidArgument
	}
	if err := ValidateLimitsV1(options.Limits); err != nil {
		return nil, err
	}
	if uint64(len(options.Sources)) > options.Limits.MaxSources {
		return nil, ErrLimitsExceeded
	}
	for _, source := range options.Sources {
		if err := validateRegisteredSource(source); err != nil {
			return nil, err
		}
	}
	if !versionPattern.MatchString(options.RecorderVersion) || !versionPattern.MatchString(options.ReplayVersion) {
		return nil, ErrInvalidArgument
	}
	entropy := options.Entropy
	if entropy == nil {
		entropy = rand.Reader
	}
	newTimer := options.NewTimer
	if newTimer == nil {
		newTimer = func(duration time.Duration) StoppableTimer {
			return &systemTimer{timer: time.NewTimer(duration)}
		}
	}
	return &Recorder{
		clock: options.Clock, newTimer: newTimer, entropy: entropy, limits: options.Limits,
		sources: CanonicalSourceOrder(options.Sources), recorderVersion: options.RecorderVersion,
		replayVersion: options.ReplayVersion,
	}, nil
}

func CanonicalSourceOrder(sources []RegisteredSource) []RegisteredSource {
	result := append([]RegisteredSource(nil), sources...)
	sort.SliceStable(result, func(left, right int) bool {
		leftKey := registeredSourceKey(result[left])
		rightKey := registeredSourceKey(result[right])
		return leftKey < rightKey
	})
	return result
}

func registeredSourceKey(source RegisteredSource) string {
	runtimeKind, _ := runtimeForSource(source.SourceKind)
	return string(rune('0'+phaseRank(source.Phase))) + "\x00" +
		string(rune('0'+runtimeRank(runtimeKind))) + "\x00" +
		string(source.SourceKind) + "\x00" + runtimeInstanceKey(source)
}

func phaseRank(phase Phase) int {
	switch phase {
	case PhasePre:
		return 0
	case PhaseAction:
		return 1
	case PhasePost:
		return 2
	default:
		return 9
	}
}

func runtimeRank(kind RuntimeKind) int {
	switch kind {
	case RuntimeEBus:
		return 0
	case RuntimeEEBus:
		return 1
	case RuntimeCloudApp:
		return 2
	default:
		return 9
	}
}

func (timeline *captureTimeline) observe() (ClockObservationV1, error) {
	raw := timeline.clock.Observe()
	if !validateTimestamp(raw.Wall) || raw.OffsetNS > MaxSafeIntegerV1 || raw.UncertaintyNS > MaxSafeIntegerV1 {
		return ClockObservationV1{}, contractError("clock.skew")
	}
	if len(timeline.observations) == 0 {
		timeline.baseOffset = raw.OffsetNS
	}
	if raw.OffsetNS < timeline.baseOffset {
		return ClockObservationV1{}, contractError("clock.skew")
	}
	observation := ClockObservationV1{ObservedAt: raw.Wall.UTC(), OffsetNS: raw.OffsetNS - timeline.baseOffset, UncertaintyNS: raw.UncertaintyNS}
	if len(timeline.observations) > 0 && observation.OffsetNS <= timeline.observations[len(timeline.observations)-1].OffsetNS {
		return ClockObservationV1{}, contractError("clock.skew")
	}
	timeline.observations = append(timeline.observations, observation)
	return observation, nil
}

func (recorder *Recorder) Capture(ctx context.Context, marker ActionMarker) ([]byte, error) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()

	if recorder.pendingSource != nil {
		select {
		case <-recorder.pendingSource:
			recorder.pendingSource = nil
		default:
			return nil, ErrCapturePending
		}
	}
	if ctx == nil {
		return nil, ErrInvalidArgument
	}
	if err := validateEvidenceRef(marker.EvidenceRef); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, contractError("capture.cancelled")
	}
	totalTimer := recorder.newTimer(time.Duration(recorder.limits.MaxCaptureDurationNS))
	if totalTimer == nil || totalTimer.C() == nil {
		return nil, ErrInvalidArgument
	}
	defer totalTimer.Stop()
	totalExpired := false
	checkTotalDeadline := func() bool {
		if totalExpired {
			return true
		}
		select {
		case <-totalTimer.C():
			totalExpired = true
		default:
		}
		return totalExpired
	}
	timeline := &captureTimeline{clock: recorder.clock}
	anchor, err := timeline.observe()
	if err != nil {
		return nil, err
	}
	if anchor.OffsetNS != 0 {
		return nil, contractError("clock.skew")
	}
	usedIDs := make(map[string]struct{})
	markerID, err := recorder.mintHexID("marker", usedIDs)
	if err != nil {
		return nil, err
	}

	captured := make([]capturedSource, 0, len(recorder.sources))
	capturePhase := func(phase Phase) error {
		for _, registered := range recorder.sources {
			if registered.Phase != phase {
				continue
			}
			if err := ctx.Err(); err != nil {
				return contractError("capture.cancelled")
			}
			base, authority, permissions, terminal, err := recorder.sourcePrecedence(registered)
			if err != nil {
				return err
			}
			if checkTotalDeadline() || recorder.hasPendingSource() {
				base.state = StateNotTested
				base.errorCategory = ErrorBudgetExhausted
				terminal = true
			}
			if terminal {
				if err := validateInternalCapture(base, phase, recorder.limits); err != nil {
					return err
				}
				captured = append(captured, capturedSource{registered: registered, result: base, authority: authority, permissions: permissions})
				continue
			}
			start, err := timeline.observe()
			if err != nil {
				return err
			}
			result := base
			if base.errorCategory != ErrorBackendUnavailable {
				var hitTotal bool
				var rootErr error
				result, hitTotal, rootErr = recorder.captureSource(ctx, registered, SourceRequest{
					Phase: phase, Limits: recorder.limits,
					OperationID: base.operationID, OperationScope: base.operationScope, MaskTier: "redacted",
					AuthScope: AuthScopeV1{Authority: "effective", Permissions: append([]string(nil), permissions...)},
				}, totalTimer.C())
				if rootErr != nil {
					return rootErr
				}
				if hitTotal {
					totalExpired = true
				}
			}
			base.state = result.state
			base.errorCategory = result.errorCategory
			base.sourceObservedAt = result.sourceObservedAt
			base.normalizedEvidence = result.normalizedEvidence
			if base.state == StatePresent && (!validateTimestamp(base.sourceObservedAt) || uint64(len(base.normalizedEvidence)) > recorder.limits.MaxArtifactBytes) {
				base.state = StateWithheld
				base.errorCategory = ErrorRedactionFailed
				base.sourceObservedAt = time.Time{}
				base.normalizedEvidence = nil
			}
			end, err := timeline.observe()
			if err != nil {
				return err
			}
			if end.OffsetNS-start.OffsetNS > recorder.limits.MaxSourceDurationNS {
				base.state = StateUnavailable
				base.errorCategory = ErrorTimeout
				base.normalizedEvidence = nil
				base.sourceObservedAt = time.Time{}
			}
			if err := validateInternalCapture(base, phase, recorder.limits); err != nil {
				return err
			}
			captured = append(captured, capturedSource{registered: registered, result: base, authority: authority, permissions: permissions, start: start, end: end, hasTiming: true})
		}
		return nil
	}

	if err := capturePhase(PhasePre); err != nil {
		return nil, err
	}
	preBoundary, err := timeline.observe()
	if err != nil {
		return nil, err
	}
	markerObservation, err := timeline.observe()
	if err != nil {
		return nil, err
	}
	if err := capturePhase(PhaseAction); err != nil {
		return nil, err
	}
	actionBoundary, err := timeline.observe()
	if err != nil {
		return nil, err
	}
	if err := capturePhase(PhasePost); err != nil {
		return nil, err
	}
	finalObservation, err := timeline.observe()
	if err != nil {
		return nil, err
	}
	if finalObservation.OffsetNS > recorder.limits.MaxCaptureDurationNS {
		return nil, ErrLimitsExceeded
	}

	window := CaptureWindowV1{
		Pre: WindowSegmentV1{StartOffsetNS: anchor.OffsetNS, EndOffsetNS: preBoundary.OffsetNS},
		Action: ActionWindowV1{
			StartOffsetNS: preBoundary.OffsetNS, MarkerOffsetNS: markerObservation.OffsetNS,
			MarkerCapturedAt: markerObservation.ObservedAt, MarkerID: markerID,
			EvidenceRef: marker.EvidenceRef, EndOffsetNS: actionBoundary.OffsetNS,
		},
		Post: WindowSegmentV1{StartOffsetNS: actionBoundary.OffsetNS, EndOffsetNS: finalObservation.OffsetNS},
	}
	clock, err := finalizeClock(timeline.observations, finalObservation)
	if err != nil {
		return nil, err
	}
	scope := captureScope(captured)
	rootPermissions := permissionUnion(captured)
	if len(rootPermissions) == 0 {
		return nil, contractError("schema.bundle")
	}

	runtimeIDs := make(map[string]string)
	targetIDs := make(map[string]string)
	remaskedValues := make(map[string]struct{})
	remaskScopeID := ""
	for _, current := range captured {
		if current.result.state == StatePresent {
			remaskScopeID, err = recorder.mintHexID("remask", usedIDs)
			if err != nil {
				return nil, err
			}
			break
		}
	}
	records := make([]SourceRecordV1, 0, len(captured))
	artifacts := make([]SourceArtifactV1, 0, len(captured))
	for _, current := range captured {
		instanceKey := string(current.result.runtimeKind) + "\x00" + runtimeInstanceKey(current.registered)
		runtimeID := runtimeIDs[instanceKey]
		if runtimeID == "" {
			runtimeID, err = recorder.mintHexID("runtime", usedIDs)
			if err != nil {
				return nil, err
			}
			runtimeIDs[instanceKey] = runtimeID
		}
		sourcePrefix := strings.ToLower(string(current.result.runtimeKind))
		if current.result.runtimeKind == RuntimeCloudApp {
			sourcePrefix = "cloud"
		}
		sourceID, err := recorder.mintHexID(sourcePrefix, usedIDs)
		if err != nil {
			return nil, err
		}
		identity, err := recorder.remaskIdentity(current.result.ebusIdentity, current.result.sourceKind, targetIDs, usedIDs)
		if err != nil {
			return nil, err
		}
		auth := AuthScopeV1{Authority: "effective", Permissions: append([]string(nil), current.permissions...)}
		binding := SourceBindingV1{
			RuntimeKind: current.result.runtimeKind, RuntimePseudonym: runtimeID,
			OperationID: current.result.operationID, OperationVersion: current.result.operationVersion,
			RequestScope:  RequestScopeV1{Phase: current.registered.Phase, SourceKind: current.result.runtimeKind, OperationScope: current.result.operationScope},
			SnapshotScope: SnapshotScopeV1{Mode: current.result.snapshotMode, Selector: current.result.operationScope},
			SourceKind:    current.result.sourceKind, SourceContract: current.result.sourceContract,
			SourceSchemaVersion: current.result.sourceSchemaVersion,
			OwnerRepository:     current.authority.ownerRepository, OwnerPath: current.authority.ownerPath,
			OwnerCommit: current.authority.ownerCommit, SchemaSHA256: current.authority.schemaSHA256,
			CaptureWindow: window, MaskTier: "redacted", AuthScope: auth, EBusIdentity: identity,
		}
		refs, err := normalizeEvidenceRefs(current.result.evidenceRefs)
		if err != nil {
			return nil, err
		}
		record := SourceRecordV1{
			Contract: BundleContractV1, SchemaVersion: 1, SourceID: sourceID,
			SourceKind: current.result.runtimeKind, Phase: current.registered.Phase,
			State: current.result.state, SourceContract: current.result.sourceContract,
			SourceSchemaVersion: current.result.sourceSchemaVersion, SourceBinding: binding,
			CaptureWindow: window, Clock: clock, Scope: scope, MaskTier: "redacted", AuthScope: auth,
			EvidenceRefs: refs, RecorderVersion: recorder.recorderVersion, ReplayVersion: recorder.replayVersion,
			MaximumSkewNS: clock.MaximumSkewNS, EBusIdentity: identity, ArtifactIDs: []string{},
		}
		if current.result.errorCategory != "" {
			category := current.result.errorCategory
			record.ErrorCategory = &category
		}
		if current.hasTiming {
			startAt, endAt := current.start.ObservedAt, current.end.ObservedAt
			startOffset, endOffset := current.start.OffsetNS, current.end.OffsetNS
			latency := endOffset - startOffset
			record.AcquisitionStartedAt, record.AcquisitionEndedAt = &startAt, &endAt
			record.AcquisitionStartOffsetNS, record.AcquisitionEndOffsetNS = &startOffset, &endOffset
			record.MeasuredLatencyNS = &latency
		}
		if current.result.state == StatePresent {
			normalized, remasking, itemCount, err := recorder.prepareEvidence(current.result, identity, remaskScopeID, remaskedValues)
			if err != nil {
				if err.Error() == "entropy.unavailable" {
					return nil, err
				}
				record.State = StateWithheld
				category := ErrorRedactionFailed
				record.ErrorCategory = &category
				record.ArtifactIDs = []string{}
				records = append(records, record)
				continue
			}
			artifact := SourceArtifactV1{
				Contract: BundleContractV1, SchemaVersion: 1, SourceID: sourceID,
				SourceKind: current.result.runtimeKind, Phase: current.registered.Phase,
				SourceContract: current.result.sourceContract, SourceSchemaVersion: current.result.sourceSchemaVersion,
				SourceBinding: binding, EBusIdentity: identity, SourceObservedAt: current.result.sourceObservedAt.UTC(),
				RecorderIngestedAt: current.end.ObservedAt, RecorderIngestedOffsetNS: current.end.OffsetNS,
				CaptureWindow: window, Clock: clock, Scope: scope, MaskTier: "redacted", AuthScope: auth,
				EvidenceRefs: refs, RecorderVersion: recorder.recorderVersion, ReplayVersion: recorder.replayVersion,
				Remasking: remasking, ItemCount: itemCount, ByteCount: uint64(len(normalized)),
				NormalizedEvidence: normalized,
			}
			hexdigest, err := artifactDigest(artifact, recorder.limits)
			if err != nil {
				return nil, err
			}
			artifact.ArtifactID = "seav1:sha256:" + hexdigest
			artifact.RedactedHash = "sha256:" + hexdigest
			record.ArtifactIDs = []string{artifact.ArtifactID}
			artifacts = append(artifacts, artifact)
		}
		records = append(records, record)
	}

	sortSourceRecords(records)
	sortArtifacts(artifacts)
	evidenceRefs, err := rootEvidenceRefs(marker.EvidenceRef, records)
	if err != nil {
		return nil, err
	}
	bundle := SynchronizedEvidenceBundleV1{
		Contract: BundleContractV1, SchemaVersion: 1, CapturedAt: finalObservation.ObservedAt,
		CaptureWindow: window, Clock: clock, Scope: scope, MaskTier: "redacted",
		AuthScope: AuthScopeV1{Authority: "effective", Permissions: rootPermissions}, Limits: recorder.limits,
		EvidenceRefs: evidenceRefs, Sources: records, Artifacts: artifacts,
		RecorderVersion: recorder.recorderVersion, ReplayVersion: recorder.replayVersion,
	}
	if err := validateWholeBundlePrivacy(bundle, recorder.limits); err != nil {
		return nil, err
	}
	bundleDigest, err := bundleDigestV1(bundle, recorder.limits)
	if err != nil {
		return nil, err
	}
	bundle.BundleID = "sebv1:sha256:" + bundleDigest
	bundle.BundleHash = "sha256:" + bundleDigest
	encoded, err := canonicalMarshal(bundle, recorder.limits, true)
	if err != nil {
		return nil, err
	}
	if uint64(len(encoded)) > recorder.limits.MaxBundleBytes {
		return nil, ErrLimitsExceeded
	}
	return encoded, nil
}

func (recorder *Recorder) captureSource(ctx context.Context, registered RegisteredSource, request SourceRequest, total <-chan time.Time) (sourceCapture, bool, error) {
	timer := recorder.newTimer(time.Duration(recorder.limits.MaxSourceDurationNS))
	if timer == nil || timer.C() == nil {
		return sourceCapture{}, false, ErrInvalidArgument
	}
	defer timer.Stop()
	sourceCtx, cancel := context.WithCancel(ctx)
	results := make(chan acquisitionOutcome, 1)
	go func() {
		outcome := acquisitionOutcome{}
		defer func() {
			if recover() != nil {
				outcome = acquisitionOutcome{err: errAcquisitionPanic}
			}
			results <- outcome
		}()
		switch registered.SourceKind {
		case SourceEBusB509, SourceEBusB524, SourceEBusB555:
			outcome.evidence, outcome.err = registered.EBusReader.ReadSnapshot(sourceCtx, request)
		case SourceEEBus:
			authority, ok := registeredSourceAuthority(registered)
			switch {
			case !ok:
				outcome.err = ErrContractViolation
			case authority.contract == M625EEBusContractV1:
				outcome.evidence, outcome.err = registered.EEBusM625Reader.ReadFeatureData(sourceCtx, request)
			default:
				outcome.evidence, outcome.err = registered.EEBusReader.ListServices(sourceCtx, request)
			}
		case SourceCloudApp:
			outcome.evidence = AcquiredEvidence{SourceObservedAt: registered.PrecapturedCloud.SourceObservedAt, NormalizedEvidence: append(json.RawMessage(nil), registered.PrecapturedCloud.NormalizedEvidence...)}
		default:
			outcome.err = ErrContractViolation
		}
	}()

	select {
	case outcome := <-results:
		cancel()
		return acquisitionTerminal(outcome), false, nil
	case <-ctx.Done():
		cancel()
		recorder.pendingSource = results
		return sourceCapture{}, false, contractError("capture.cancelled")
	case <-total:
		cancel()
		recorder.pendingSource = results
		return sourceCapture{state: StateUnavailable, errorCategory: ErrorTimeout}, true, nil
	case <-timer.C():
		cancel()
		recorder.pendingSource = results
		return sourceCapture{state: StateUnavailable, errorCategory: ErrorTimeout}, false, nil
	}
}

func (recorder *Recorder) hasPendingSource() bool {
	if recorder.pendingSource == nil {
		return false
	}
	select {
	case <-recorder.pendingSource:
		recorder.pendingSource = nil
		return false
	default:
		return true
	}
}

func (recorder *Recorder) sourcePrecedence(registered RegisteredSource) (sourceCapture, sourceAuthority, []string, bool, error) {
	authority, exists := registeredSourceAuthority(registered)
	if !exists {
		return sourceCapture{}, sourceAuthority{}, nil, false, ErrInvalidArgument
	}
	runtimeKind, _ := runtimeForSource(registered.SourceKind)
	permissions, err := normalizePermissions(registered.Admission.EffectivePermissions)
	if err != nil {
		return sourceCapture{}, sourceAuthority{}, nil, false, err
	}
	operationID, snapshotMode := expectedSourceOperation(runtimeKind, authority.contract)
	refs := append([]EvidenceRefV1(nil), registered.EvidenceRefs...)
	if registered.SourceKind == SourceCloudApp {
		refs = []EvidenceRefV1{registered.PrecapturedCloud.EvidenceRef}
	}
	base := sourceCapture{
		runtimeKind:         runtimeKind,
		sourceKind:          registered.SourceKind,
		sourceContract:      authority.contract,
		sourceSchemaVersion: authority.version,
		operationID:         operationID,
		operationVersion:    registered.OperationVersion,
		operationScope:      registered.OperationScope,
		snapshotMode:        snapshotMode,
		ebusIdentity:        cloneIdentity(registered.EBusIdentity),
		evidenceRefs:        refs,
	}
	switch {
	case registered.Admission.Selection == SelectionExcluded:
		base.state, base.errorCategory = StateNotTested, ErrorNotSelected
		return base, authority, permissions, true, nil
	case registered.Admission.Policy == PolicyWithheld:
		base.state, base.errorCategory = StateWithheld, ErrorPolicyWithheld
		return base, authority, permissions, true, nil
	case !permissionsContain(permissions, registered.Admission.RequiredPermissions):
		base.state, base.errorCategory = StateWithheld, ErrorAuthorizationDenied
		return base, authority, permissions, true, nil
	case runtimeKind == RuntimeEBus && registered.EBusIdentity == nil:
		base.state, base.errorCategory = StateNotTested, ErrorExactIdentityMissing
		return base, authority, permissions, true, nil
	case registered.Admission.Backend == BackendUnreachable:
		base.state, base.errorCategory = StateUnavailable, ErrorBackendUnavailable
	}
	return base, authority, permissions, false, nil
}

func acquisitionTerminal(outcome acquisitionOutcome) sourceCapture {
	if outcome.err == nil {
		if outcome.evidence.SourceObservedAt.IsZero() || len(outcome.evidence.NormalizedEvidence) == 0 {
			return sourceCapture{state: StateWithheld, errorCategory: ErrorRedactionFailed}
		}
		return sourceCapture{
			state:              StatePresent,
			sourceObservedAt:   outcome.evidence.SourceObservedAt,
			normalizedEvidence: append(json.RawMessage(nil), outcome.evidence.NormalizedEvidence...),
		}
	}
	if errors.Is(outcome.err, ErrBackendUnavailable) {
		return sourceCapture{state: StateUnavailable, errorCategory: ErrorBackendUnavailable}
	}
	if errors.Is(outcome.err, context.DeadlineExceeded) {
		return sourceCapture{state: StateUnavailable, errorCategory: ErrorTimeout}
	}
	return sourceCapture{state: StateUnavailable, errorCategory: ErrorAcquisitionFailed}
}

func permissionsContain(effective, required []string) bool {
	available := make(map[string]struct{}, len(effective))
	for _, permission := range effective {
		available[permission] = struct{}{}
	}
	for _, permission := range required {
		if _, ok := available[permission]; !ok {
			return false
		}
	}
	return true
}

func runtimeInstanceKey(source RegisteredSource) string {
	if source.RuntimeInstance != "" {
		return source.RuntimeInstance
	}
	return source.SourceID
}

func cloneIdentity(identity *EBusSourceIdentityV1) *EBusSourceIdentityV1 {
	if identity == nil {
		return nil
	}
	copyIdentity := *identity
	return &copyIdentity
}

func finalizeClock(observations []ClockObservationV1, final ClockObservationV1) (CaptureClockV1, error) {
	if len(observations) < 2 {
		return CaptureClockV1{}, contractError("clock.skew")
	}
	anchor := observations[0]
	resolution := uint64(0)
	maximumSkew := uint64(0)
	for index, observation := range observations {
		wallDelta := observation.ObservedAt.Sub(anchor.ObservedAt).Nanoseconds()
		if wallDelta < 0 {
			return CaptureClockV1{}, contractError("clock.skew")
		}
		difference := absoluteDifference(uint64(wallDelta), observation.OffsetNS)
		if difference > MaxSafeIntegerV1-observation.UncertaintyNS {
			return CaptureClockV1{}, contractError("clock.skew")
		}
		skew := difference + observation.UncertaintyNS
		if skew > maximumSkew {
			maximumSkew = skew
		}
		if index > 0 {
			delta := observation.OffsetNS - observations[index-1].OffsetNS
			if resolution == 0 || delta < resolution {
				resolution = delta
			}
		}
	}
	if resolution == 0 || maximumSkew > MaximumClockSkew {
		return CaptureClockV1{}, contractError("clock.skew")
	}
	return CaptureClockV1{
		ClockID: "capture-clock-1", WallAnchor: anchor.ObservedAt, MonotonicAnchorNS: 0,
		CapturedOffsetNS: final.OffsetNS, ResolutionNS: resolution, MaximumSkewNS: maximumSkew,
		Observations: append([]ClockObservationV1(nil), observations...),
	}, nil
}

func absoluteDifference(left, right uint64) uint64 {
	if left >= right {
		return left - right
	}
	return right - left
}

func captureScope(captured []capturedSource) CaptureScopeV1 {
	seen := make(map[RuntimeKind]bool)
	for _, current := range captured {
		seen[current.result.runtimeKind] = true
	}
	kinds := make([]RuntimeKind, 0, len(seen))
	for _, kind := range []RuntimeKind{RuntimeEBus, RuntimeEEBus, RuntimeCloudApp} {
		if seen[kind] {
			kinds = append(kinds, kind)
		}
	}
	return CaptureScopeV1{Purpose: "SYNCHRONIZED_EVIDENCE_ONLY", SourceKinds: kinds, Phases: []Phase{PhasePre, PhaseAction, PhasePost}}
}

func permissionUnion(captured []capturedSource) []string {
	seen := make(map[string]struct{})
	for _, current := range captured {
		for _, permission := range current.permissions {
			seen[permission] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for permission := range seen {
		result = append(result, permission)
	}
	sort.Strings(result)
	return result
}

func (recorder *Recorder) mintHexID(prefix string, used map[string]struct{}) (string, error) {
	raw := make([]byte, 16)
	if _, err := io.ReadFull(recorder.entropy, raw); err != nil {
		return "", contractError("entropy.unavailable")
	}
	candidate := prefix + "-" + hex.EncodeToString(raw)
	if _, exists := used[candidate]; exists {
		for counter := 1; counter < 256; counter++ {
			copyCandidate := append([]byte(nil), raw...)
			copyCandidate[len(copyCandidate)-1] ^= byte(counter)
			candidate = prefix + "-" + hex.EncodeToString(copyCandidate)
			if _, duplicate := used[candidate]; !duplicate {
				break
			}
		}
		if _, duplicate := used[candidate]; duplicate {
			return "", contractError("entropy.unavailable")
		}
	}
	used[candidate] = struct{}{}
	return candidate, nil
}

func (recorder *Recorder) mintUniqueRemask(used map[string]struct{}) (string, error) {
	raw := make([]byte, 32)
	if _, err := io.ReadFull(recorder.entropy, raw); err != nil {
		return "", contractError("entropy.unavailable")
	}
	for counter := 0; counter < 256; counter++ {
		candidateBytes := append([]byte(nil), raw...)
		candidateBytes[len(candidateBytes)-1] ^= byte(counter)
		candidate := base64.RawURLEncoding.EncodeToString(candidateBytes)
		if _, exists := used[candidate]; exists {
			continue
		}
		used[candidate] = struct{}{}
		return candidate, nil
	}
	return "", contractError("entropy.unavailable")
}

func (recorder *Recorder) remaskIdentity(identity *EBusSourceIdentityV1, kind SourceKind, targetIDs map[string]string, used map[string]struct{}) (*EBusSourceIdentityV1, error) {
	if identity == nil {
		return nil, nil
	}
	copyIdentity := *identity
	copyIdentity.TargetPseudonym = ""
	keyBytes, err := json.Marshal(copyIdentity)
	if err != nil {
		return nil, contractError("schema.bundle")
	}
	key := string(kind) + "\x00" + string(keyBytes)
	pseudonym := targetIDs[key]
	if pseudonym == "" {
		pseudonym, err = recorder.mintHexID("target", used)
		if err != nil {
			return nil, err
		}
		targetIDs[key] = pseudonym
	}
	copyIdentity.TargetPseudonym = pseudonym
	if err := validateIdentity(&copyIdentity, kind); err != nil {
		return nil, err
	}
	return &copyIdentity, nil
}

func (recorder *Recorder) prepareEvidence(capture sourceCapture, identity *EBusSourceIdentityV1, scopeID string, usedValues map[string]struct{}) (json.RawMessage, RemaskingV1, uint64, error) {
	value, _, err := parseJSON(capture.normalizedEvidence, recorder.limits, false)
	if err != nil {
		return nil, RemaskingV1{}, 0, err
	}
	if err := validatePrivacy(value); err != nil {
		return nil, RemaskingV1{}, 0, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, RemaskingV1{}, 0, contractError("schema.bundle")
	}
	if err := validateSourcePayload(object, capture); err != nil {
		return nil, RemaskingV1{}, 0, err
	}
	entries := make([]RemaskedPseudonymV1, 0)
	if capture.sourceKind == SourceEEBus && capture.sourceContract == M625EEBusContractV1 {
		if err := recorder.remaskM625Payload(object, usedValues); err != nil {
			return nil, RemaskingV1{}, 0, err
		}
		if err := sortM625Payload(object); err != nil {
			return nil, RemaskingV1{}, 0, err
		}
		requirements, err := m625RemaskingRequirements(object)
		if err != nil {
			return nil, RemaskingV1{}, 0, err
		}
		for path, requirement := range requirements {
			entries = append(entries, RemaskedPseudonymV1{Path: path, Pseudonym: requirement.pseudonym})
		}
	} else if err := recorder.remaskPayload(object, capture.sourceKind, identity, "", &entries, usedValues); err != nil {
		return nil, RemaskingV1{}, 0, err
	}
	if capture.sourceKind == SourceEEBus && capture.sourceContract == HistoricalEEBusContractV1 {
		sortEEBusPayload(object)
		if err := recomputeEEBusDataHash(object); err != nil {
			return nil, RemaskingV1{}, 0, err
		}
		entries = entries[:0]
		remasked := make(map[string]string)
		collectRemaskedPaths(object, SourceEEBus, "", remasked)
		for path, pseudonym := range remasked {
			entries = append(entries, RemaskedPseudonymV1{Path: path, Pseudonym: pseudonym})
		}
	}
	if err := validateSourcePayload(object, capture); err != nil {
		return nil, RemaskingV1{}, 0, err
	}
	sort.Slice(entries, func(left, right int) bool {
		if entries[left].Path == entries[right].Path {
			return entries[left].Pseudonym < entries[right].Pseudonym
		}
		return entries[left].Path < entries[right].Path
	})
	var encoded bytes.Buffer
	if err := appendCanonical(&encoded, object); err != nil {
		return nil, RemaskingV1{}, 0, contractError("schema.bundle")
	}
	if uint64(encoded.Len()) > recorder.limits.MaxArtifactBytes {
		return nil, RemaskingV1{}, 0, ErrLimitsExceeded
	}
	itemCount := sourceItemCount(object, capture.sourceKind)
	if itemCount > recorder.limits.MaxItemsPerSource {
		return nil, RemaskingV1{}, 0, ErrLimitsExceeded
	}
	return encoded.Bytes(), RemaskingV1{Method: "PER_BUNDLE_CSPRNG", ScopeID: scopeID, Entries: entries}, itemCount, nil
}

func validateSourcePayload(object map[string]any, capture sourceCapture) error {
	switch capture.sourceKind {
	case SourceEBusB509, SourceEBusB524, SourceEBusB555:
		if !exactKeys(object, "contract", "schema_version", "source_observed_at", "identity", "observations") ||
			object["contract"] != capture.sourceContract || object["schema_version"] != json.Number("1") {
			return contractError("schema.bundle")
		}
		observedAt, ok := object["source_observed_at"].(string)
		if !ok || !sameTimestamp(observedAt, capture.sourceObservedAt) {
			return contractError("schema.bundle")
		}
		observations, ok := object["observations"].([]any)
		if !ok || len(observations) == 0 {
			return contractError("schema.bundle")
		}
		identityMap, ok := object["identity"].(map[string]any)
		if !ok || validIdentityShape(identityMap) != nil {
			return contractError("schema.bundle")
		}
		identityBytes, err := json.Marshal(identityMap)
		if err != nil {
			return contractError("schema.bundle")
		}
		var payloadIdentity EBusSourceIdentityV1
		if err := json.Unmarshal(identityBytes, &payloadIdentity); err != nil || validateIdentity(&payloadIdentity, capture.sourceKind) != nil || capture.ebusIdentity == nil {
			return contractError("schema.bundle")
		}
		expectedIdentity := *capture.ebusIdentity
		payloadIdentity.TargetPseudonym = ""
		expectedIdentity.TargetPseudonym = ""
		if !reflect.DeepEqual(payloadIdentity, expectedIdentity) {
			return contractError("schema.bundle")
		}
		for _, rawObservation := range observations {
			observation, ok := rawObservation.(map[string]any)
			if !ok || !validEBusObservation(observation, capture.sourceKind) {
				return contractError("schema.bundle")
			}
		}
	case SourceCloudApp:
		if !exactKeys(object, "contract", "schema_version", "source_observed_at", "subject_pseudonym", "observation_type", "value", "unit") ||
			object["contract"] != capture.sourceContract || object["schema_version"] != json.Number("1") {
			return contractError("schema.bundle")
		}
		observedAt, ok := object["source_observed_at"].(string)
		if !ok || !sameTimestamp(observedAt, capture.sourceObservedAt) {
			return contractError("schema.bundle")
		}
		observationType, typeOK := object["observation_type"].(string)
		value, valueOK := object["value"].(string)
		unit, unitOK := object["unit"].(string)
		subject, subjectOK := object["subject_pseudonym"].(string)
		if !subjectOK || !remaskedValuePattern.MatchString(subject) || !typeOK || !valueOK || !unitOK || !oneOfString(observationType, "ROOM_TEMPERATURE", "TARGET_TEMPERATURE", "OPERATING_MODE", "DHW_STATE") ||
			!validASCIIToken(value, 128) || !validASCIIToken(unit, 32) {
			return contractError("schema.bundle")
		}
	case SourceEEBus:
		if capture.sourceContract == M625EEBusContractV1 {
			return validateM625Payload(object, capture)
		}
		return validateEEBusServicesPayload(object, capture)
	default:
		return contractError("binding.registry")
	}
	return nil
}

func validateEEBusServicesPayload(object map[string]any, capture sourceCapture) error {
	if !exactKeys(object, "meta", "data", "error") || object["error"] != nil {
		return contractError("schema.bundle")
	}
	meta, metaOK := object["meta"].(map[string]any)
	data, dataOK := object["data"].(map[string]any)
	if !metaOK || !dataOK || !exactKeys(meta, "contract", "tool", "scope", "mask_tier", "auth_scope", "mode", "data_timestamp", "data_hash", "runtime") ||
		!exactKeys(data, "services") || meta["tool"] != capture.operationID || meta["scope"] != capture.operationScope ||
		meta["mask_tier"] != "redacted" || meta["auth_scope"] != "eebus.raw.read" || meta["mode"] != "evidence" || !digestString(meta["data_hash"]) {
		return contractError("schema.bundle")
	}
	contract, contractOK := meta["contract"].(map[string]any)
	if !contractOK || !exactKeys(contract, "name", "major", "minor") || contract["name"] != capture.sourceContract ||
		contract["major"] != json.Number("1") || contract["minor"] != json.Number("0") {
		return contractError("schema.bundle")
	}
	if !validateEEBusRuntimeMeta(meta["runtime"]) {
		return contractError("schema.bundle")
	}
	observedAt, ok := meta["data_timestamp"].(string)
	if !ok || !sameTimestamp(observedAt, capture.sourceObservedAt) {
		return contractError("schema.bundle")
	}
	services, ok := data["services"].([]any)
	if !ok || !validateEEBusServices(services) {
		return contractError("schema.bundle")
	}
	expectedHash, err := eebusEnvelopeDataHash(object)
	if err != nil {
		return err
	}
	if meta["data_hash"] != expectedHash {
		return contractError("hash.artifact")
	}
	return nil
}

func validateEEBusRuntimeMeta(value any) bool {
	runtimeMeta, ok := value.(map[string]any)
	if !ok || (!exactKeys(runtimeMeta, "state") && !exactKeys(runtimeMeta, "state", "degradation")) {
		return false
	}
	state, ok := runtimeMeta["state"].(string)
	if !ok || !oneOfString(state, "stopped", "starting", "ready", "degraded", "shutdown") {
		return false
	}
	if raw, exists := runtimeMeta["degradation"]; exists {
		degradation, ok := raw.(map[string]any)
		if !ok || !exactKeys(degradation, "reason", "since") {
			return false
		}
		reason, reasonOK := degradation["reason"].(string)
		since, sinceOK := degradation["since"].(string)
		if !reasonOK || !sinceOK || !oneOfString(reason, "missing-discovery", "denied-trust", "remote-disconnect", "certificate-unavailable", "no-visible-services", "no-data") || !canonicalTimestamp(since) {
			return false
		}
	}
	return true
}

func validateEEBusServices(services []any) bool {
	lastKey := ""
	seen := make(map[string]struct{}, len(services))
	for _, raw := range services {
		service, ok := raw.(map[string]any)
		if !ok || (!exactKeys(service, "id", "kind", "visible", "paired") && !exactKeys(service, "id", "kind", "visible", "paired", "evidence")) {
			return false
		}
		identity, ok := service["id"].(map[string]any)
		kind, kindOK := service["kind"].(string)
		_, visibleOK := service["visible"].(bool)
		_, pairedOK := service["paired"].(bool)
		if !ok || !exactKeys(identity, "kind", "digest") || identity["kind"] != "service" || !remaskedString(identity["digest"]) ||
			!kindOK || !oneOfString(kind, "local", "remote") || !visibleOK || !pairedOK {
			return false
		}
		if evidence, exists := service["evidence"]; exists {
			rows, ok := evidence.([]any)
			if !ok || !validateEEBusEvidence(rows) {
				return false
			}
		}
		key := identity["digest"].(string) + "\x00" + kind
		if lastKey != "" && key <= lastKey {
			return false
		}
		canonical, err := canonicalJSONValue(service)
		if err != nil {
			return false
		}
		encoded := string(canonical)
		if _, duplicate := seen[encoded]; duplicate {
			return false
		}
		seen[encoded] = struct{}{}
		lastKey = key
	}
	return true
}

func validateEEBusEvidence(rows []any) bool {
	type orderKey struct {
		kind, digest, timestamp string
		size                    uint64
	}
	var last orderKey
	hasLast := false
	seen := make(map[string]struct{}, len(rows))
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok || !exactKeys(row, "kind", "digest", "size", "data_timestamp") {
			return false
		}
		kind, kindOK := row["kind"].(string)
		digest, digestOK := row["digest"].(string)
		size, sizeOK := row["size"].(json.Number)
		timestamp, timeOK := row["data_timestamp"].(string)
		if !kindOK || !oneOfString(kind, "identity", "topology", "service", "session", "unknown") || !digestOK || !digestPattern.MatchString(digest) ||
			!sizeOK || !isSafeInteger(string(size)) || !timeOK || !canonicalTimestamp(timestamp) {
			return false
		}
		parsedSize, err := strconv.ParseUint(string(size), 10, 64)
		if err != nil {
			return false
		}
		key := orderKey{kind: kind, digest: digest, size: parsedSize, timestamp: timestamp}
		if hasLast && !lessEEBusEvidence(last, key) {
			return false
		}
		canonical, err := canonicalJSONValue(row)
		if err != nil {
			return false
		}
		encoded := string(canonical)
		if _, duplicate := seen[encoded]; duplicate {
			return false
		}
		seen[encoded] = struct{}{}
		last = key
		hasLast = true
	}
	return true
}

func lessEEBusEvidence(left, right struct {
	kind, digest, timestamp string
	size                    uint64
}) bool {
	if left.kind != right.kind {
		return left.kind < right.kind
	}
	if left.digest != right.digest {
		return left.digest < right.digest
	}
	if left.size != right.size {
		return left.size < right.size
	}
	return left.timestamp < right.timestamp
}

func remaskedString(value any) bool {
	text, ok := value.(string)
	return ok && remaskedValuePattern.MatchString(text)
}

func canonicalJSONValue(value any) ([]byte, error) {
	var encoded bytes.Buffer
	if err := appendCanonical(&encoded, value); err != nil {
		return nil, contractError("schema.bundle")
	}
	return encoded.Bytes(), nil
}

func recomputeEEBusDataHash(object map[string]any) error {
	meta, metaOK := object["meta"].(map[string]any)
	if !metaOK {
		return contractError("schema.bundle")
	}
	hash, err := eebusEnvelopeDataHash(object)
	if err != nil {
		return err
	}
	meta["data_hash"] = hash
	return nil
}

func eebusEnvelopeDataHash(object map[string]any) (string, error) {
	meta, metaOK := object["meta"].(map[string]any)
	runtimeMeta, runtimeOK := meta["runtime"].(map[string]any)
	if !metaOK || !runtimeOK {
		return "", contractError("schema.bundle")
	}
	degradation := any(nil)
	if value, exists := runtimeMeta["degradation"]; exists {
		degradation = value
	}
	view := map[string]any{
		"contract":       meta["contract"],
		"tool":           meta["tool"],
		"scope":          meta["scope"],
		"mask_tier":      meta["mask_tier"],
		"auth_scope":     meta["auth_scope"],
		"mode":           meta["mode"],
		"data_timestamp": meta["data_timestamp"],
		"runtime_state":  runtimeMeta["state"],
		"degradation":    degradation,
		"data":           object["data"],
		"error":          object["error"],
	}
	canonical, err := canonicalJSONValue(view)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func isEEBusIdentityDigest(object map[string]any, key string) bool {
	if key != "digest" || !exactKeys(object, "kind", "digest") {
		return false
	}
	kind, ok := object["kind"].(string)
	return ok && oneOfString(kind, "runtime", "remote", "service", "session", "device", "entity", "feature", "usecase-claim")
}

func sortEEBusPayload(object map[string]any) {
	data, ok := object["data"].(map[string]any)
	if !ok {
		return
	}
	services, ok := data["services"].([]any)
	if !ok {
		return
	}
	for _, raw := range services {
		service, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if evidence, ok := service["evidence"].([]any); ok {
			sort.Slice(evidence, func(left, right int) bool {
				return lessEEBusEvidenceValue(evidence[left], evidence[right])
			})
		}
	}
	sort.Slice(services, func(left, right int) bool { return eebusServiceKey(services[left]) < eebusServiceKey(services[right]) })
}

func eebusServiceKey(value any) string {
	service, _ := value.(map[string]any)
	identity, _ := service["id"].(map[string]any)
	digest, _ := identity["digest"].(string)
	kind, _ := service["kind"].(string)
	return digest + "\x00" + kind
}

func lessEEBusEvidenceValue(left, right any) bool {
	leftRow, _ := left.(map[string]any)
	rightRow, _ := right.(map[string]any)
	leftKind, _ := leftRow["kind"].(string)
	rightKind, _ := rightRow["kind"].(string)
	if leftKind != rightKind {
		return leftKind < rightKind
	}
	leftDigest, _ := leftRow["digest"].(string)
	rightDigest, _ := rightRow["digest"].(string)
	if leftDigest != rightDigest {
		return leftDigest < rightDigest
	}
	leftSize, _ := strconv.ParseUint(string(leftRow["size"].(json.Number)), 10, 64)
	rightSize, _ := strconv.ParseUint(string(rightRow["size"].(json.Number)), 10, 64)
	if leftSize != rightSize {
		return leftSize < rightSize
	}
	leftTimestamp, _ := leftRow["data_timestamp"].(string)
	rightTimestamp, _ := rightRow["data_timestamp"].(string)
	return leftTimestamp < rightTimestamp
}

func validEBusObservation(observation map[string]any, kind SourceKind) bool {
	quality, qualityOK := observation["quality"].(string)
	value, valueOK := observation["value"].(string)
	if !qualityOK || !valueOK || !oneOfString(quality, "OBSERVED", "STALE") || !validASCIIToken(value, 128) {
		return false
	}
	switch kind {
	case SourceEBusB509:
		unit, unitOK := observation["unit"].(string)
		registerID, registerOK := observation["register_id"].(json.Number)
		parsedRegister, parseErr := strconv.ParseUint(string(registerID), 10, 16)
		return exactKeys(observation, "register_id", "value", "unit", "quality") && unitOK && registerOK && parseErr == nil && parsedRegister <= 65535 && validASCIIToken(unit, 128)
	case SourceEBusB524:
		unit, unitOK := observation["unit"].(string)
		return exactKeys(observation, "value", "unit", "quality") && unitOK && validASCIIToken(unit, 128)
	case SourceEBusB555:
		return exactKeys(observation, "value", "quality")
	default:
		return false
	}
}

func oneOfString(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func digestString(value any) bool {
	text, ok := value.(string)
	return ok && digestPattern.MatchString(text)
}

func sameTimestamp(encoded string, expected time.Time) bool {
	if !canonicalTimestamp(encoded) {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, encoded)
	if err != nil || !validateTimestamp(parsed) {
		return false
	}
	return parsed.Equal(expected)
}

func (recorder *Recorder) remaskPayload(value any, kind SourceKind, identity *EBusSourceIdentityV1, pointer string, entries *[]RemaskedPseudonymV1, usedValues map[string]struct{}) error {
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			path := pointer + "/" + strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
			shouldRemask := (kind == SourceEEBus && isEEBusIdentityDigest(current, key)) || (kind == SourceCloudApp && key == "subject_pseudonym")
			if shouldRemask {
				pseudonym, err := recorder.mintUniqueRemask(usedValues)
				if err != nil {
					return err
				}
				current[key] = pseudonym
				*entries = append(*entries, RemaskedPseudonymV1{Path: path, Pseudonym: pseudonym})
				continue
			}
			if kind == SourceEBusB509 || kind == SourceEBusB524 || kind == SourceEBusB555 {
				if key == "target_pseudonym" && identity != nil {
					current[key] = identity.TargetPseudonym
					continue
				}
			}
			if err := recorder.remaskPayload(child, kind, identity, path, entries, usedValues); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range current {
			path := pointer + "/" + strconv.Itoa(index)
			if err := recorder.remaskPayload(child, kind, identity, path, entries, usedValues); err != nil {
				return err
			}
		}
	}
	return nil
}

func (recorder *Recorder) remaskM625Payload(object map[string]any, usedValues map[string]struct{}) error {
	assignments := make(map[string]string)
	assign := func(identity string) (string, error) {
		if pseudonym := assignments[identity]; pseudonym != "" {
			return pseudonym, nil
		}
		pseudonym, err := recorder.mintUniqueRemask(usedValues)
		if err != nil {
			return "", err
		}
		assignments[identity] = pseudonym
		return pseudonym, nil
	}

	services := object["services"].([]any)
	servicePseudonyms := make(map[string]string, len(services))
	for index, raw := range services {
		service := raw.(string)
		pseudonym, err := assign("SERVICE\x00" + service)
		if err != nil {
			return err
		}
		servicePseudonyms[service] = pseudonym
		services[index] = pseudonym
	}
	paths := object["feature_paths"].([]any)
	for _, raw := range paths {
		path := raw.(map[string]any)
		service := path["service"].(string)
		entity := path["entity"].(string)
		feature := path["feature"].(string)
		serviceIdentity := "SERVICE\x00" + service
		entityIdentity := serviceIdentity + "\x00ENTITY\x00" + entity
		featureIdentity := entityIdentity + "\x00FEATURE\x00" + feature
		servicePseudonym := servicePseudonyms[service]
		entityPseudonym, err := assign(entityIdentity)
		if err != nil {
			return err
		}
		featurePseudonym, err := assign(featureIdentity)
		if err != nil {
			return err
		}
		path["service"] = servicePseudonym
		path["entity"] = entityPseudonym
		path["feature"] = featurePseudonym
		segments := path["feature_path"].([]any)
		for index, rawSegment := range segments {
			segment := rawSegment.(map[string]any)
			switch index {
			case 0:
				segment["selector"] = servicePseudonym
			case 1:
				segment["selector"] = entityPseudonym
			case 2:
				segment["selector"] = featurePseudonym
			default:
				selector := segment["selector"].(string)
				featureIdentity += "\x00FIELD\x00" + selector
				pseudonym, err := assign(featureIdentity)
				if err != nil {
					return err
				}
				segment["selector"] = pseudonym
			}
		}
	}
	for _, raw := range object["observations"].([]any) {
		observation := raw.(map[string]any)
		ref := observation["observation_ref"].(string)
		pseudonym, err := assign("OBSERVATION\x00" + ref)
		if err != nil {
			return err
		}
		observation["observation_ref"] = "obs-" + pseudonym
	}
	return nil
}

func sourceItemCount(object map[string]any, kind SourceKind) uint64 {
	switch kind {
	case SourceEBusB509, SourceEBusB524, SourceEBusB555:
		if rows, ok := object["observations"].([]any); ok {
			return uint64(len(rows))
		}
	case SourceCloudApp:
		return 1
	case SourceEEBus:
		if rows, ok := object["observations"].([]any); ok {
			return uint64(len(rows))
		}
		if data, ok := object["data"].(map[string]any); ok {
			if rows, ok := data["services"].([]any); ok {
				return uint64(len(rows))
			}
		}
	}
	return 0
}

func sortSourceRecords(records []SourceRecordV1) {
	sort.Slice(records, func(left, right int) bool {
		if phaseRank(records[left].Phase) != phaseRank(records[right].Phase) {
			return phaseRank(records[left].Phase) < phaseRank(records[right].Phase)
		}
		if runtimeRank(records[left].SourceKind) != runtimeRank(records[right].SourceKind) {
			return runtimeRank(records[left].SourceKind) < runtimeRank(records[right].SourceKind)
		}
		return records[left].SourceID < records[right].SourceID
	})
}

func sortArtifacts(artifacts []SourceArtifactV1) {
	sort.Slice(artifacts, func(left, right int) bool {
		if phaseRank(artifacts[left].Phase) != phaseRank(artifacts[right].Phase) {
			return phaseRank(artifacts[left].Phase) < phaseRank(artifacts[right].Phase)
		}
		if runtimeRank(artifacts[left].SourceKind) != runtimeRank(artifacts[right].SourceKind) {
			return runtimeRank(artifacts[left].SourceKind) < runtimeRank(artifacts[right].SourceKind)
		}
		if artifacts[left].SourceID != artifacts[right].SourceID {
			return artifacts[left].SourceID < artifacts[right].SourceID
		}
		return artifacts[left].ArtifactID < artifacts[right].ArtifactID
	})
}

func rootEvidenceRefs(marker EvidenceRefV1, records []SourceRecordV1) ([]EvidenceRefV1, error) {
	byKey := map[string]EvidenceRefV1{evidenceRefKey(marker): marker}
	for _, record := range records {
		for _, ref := range record.EvidenceRefs {
			byKey[evidenceRefKey(ref)] = ref
		}
	}
	refs := make([]EvidenceRefV1, 0, len(byKey))
	for _, ref := range byKey {
		refs = append(refs, ref)
	}
	return normalizeEvidenceRefs(refs)
}

func artifactDigest(artifact SourceArtifactV1, limits CaptureLimitsV1) (string, error) {
	raw, err := json.Marshal(artifact)
	if err != nil {
		return "", contractError("hash.artifact")
	}
	value, _, err := parseJSON(raw, limits, true)
	if err != nil {
		return "", err
	}
	object := value.(map[string]any)
	delete(object, "artifact_id")
	delete(object, "redacted_hash")
	var canonical bytes.Buffer
	if err := appendCanonical(&canonical, object); err != nil {
		return "", contractError("hash.artifact")
	}
	return domainDigest(artifactHashDomain, canonical.Bytes()), nil
}

func bundleDigestV1(bundle SynchronizedEvidenceBundleV1, limits CaptureLimitsV1) (string, error) {
	raw, err := json.Marshal(bundle)
	if err != nil {
		return "", contractError("hash.bundle")
	}
	value, _, err := parseJSON(raw, limits, true)
	if err != nil {
		return "", err
	}
	object := value.(map[string]any)
	delete(object, "bundle_id")
	delete(object, "bundle_hash")
	var canonical bytes.Buffer
	if err := appendCanonical(&canonical, object); err != nil {
		return "", contractError("hash.bundle")
	}
	return domainDigest(bundleHashDomain, canonical.Bytes()), nil
}
