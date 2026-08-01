package syncevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"reflect"
	"time"
)

const (
	oneShotStoreQuota     = int64(1 << 28)
	oneShotStoreRetention = 7 * 24 * time.Hour
)

var oneShotStoreLockProof = bytes.Repeat([]byte{0x4c}, 32)

type OneShotReceiptCategory string

const (
	OneShotPublished         OneShotReceiptCategory = "PUBLISHED"
	OneShotExisting          OneShotReceiptCategory = "EXISTING"
	OneShotInvalidRequest    OneShotReceiptCategory = "INVALID_REQUEST"
	OneShotPermissionDenied  OneShotReceiptCategory = "PERMISSION_DENIED"
	OneShotConflict          OneShotReceiptCategory = "CONFLICT"
	OneShotAcquisitionFailed OneShotReceiptCategory = "ACQUISITION_FAILED"
	OneShotReplayMismatch    OneShotReceiptCategory = "REPLAY_MISMATCH"
	OneShotPublishFailed     OneShotReceiptCategory = "PUBLISH_FAILED"
	OneShotInternal          OneShotReceiptCategory = "INTERNAL"
)

type OneShotReceiptV1 struct {
	Category OneShotReceiptCategory `json:"category"`
}

type OneShotBuildIdentity struct {
	RecorderVersion  string
	ReplayVersion    string
	OperationVersion string
}

type OneShotExecutionOptions struct {
	Root          string
	Reader        EEBusM625Reader
	ClockFactory  func() Clock
	Entropy       io.Reader
	BuildIdentity func() (OneShotBuildIdentity, error)
	replay        func([]byte) ([]byte, error)
}

type oneShotUnavailableEBusReader struct{}

func (oneShotUnavailableEBusReader) ReadSnapshot(context.Context, SourceRequest) (AcquiredEvidence, error) {
	return AcquiredEvidence{}, ErrBackendUnavailable
}

func (options OneShotExecutionOptions) sourceTuple() SourceTupleV1 {
	return SourceTupleV1{SourceKind: SourceEEBus, Contract: M625EEBusContractV1, Version: 1}
}

func ExecuteOneShot(ctx context.Context, options OneShotExecutionOptions) OneShotReceiptV1 {
	if ctx == nil {
		return oneShotReceipt(OneShotInternal)
	}
	parent, err := openOneShotRequestDirectory(options.Root)
	if err != nil {
		return oneShotReceipt(OneShotInvalidRequest)
	}
	defer func() {
		_ = parent.Close()
	}()
	request, err := loadOneShotRequestFromDirectory(parent, nil)
	if err != nil {
		return oneShotReceipt(OneShotInvalidRequest)
	}

	store, err := openOneShotFileStore(parent, FileStoreConfig{
		Root:       filepath.Join(options.Root, "store"),
		QuotaBytes: oneShotStoreQuota,
		Retention:  oneShotStoreRetention,
		Entropy:    options.Entropy,
		LockProof:  oneShotStoreLockProof,
	})
	if err != nil {
		return oneShotReceipt(OneShotPublishFailed)
	}
	defer func() {
		_ = store.Close()
	}()

	retained, err := store.lookupOneShot(request.ActionEvidenceRef, options.sourceTuple())
	if err != nil {
		return oneShotReceipt(OneShotInternal)
	}
	switch retained.Status {
	case OneShotLookupExisting:
		return oneShotReceipt(OneShotExisting)
	case OneShotLookupConflict:
		return oneShotReceipt(OneShotConflict)
	case OneShotLookupNone:
	default:
		return oneShotReceipt(OneShotInternal)
	}

	if options.Reader == nil || options.ClockFactory == nil || options.BuildIdentity == nil {
		return oneShotReceipt(OneShotInternal)
	}
	limits := oneShotCaptureLimits()
	reservation, err := store.ReserveCapture(int64(limits.MaxBundleBytes))
	if err != nil {
		return oneShotReceipt(OneShotPublishFailed)
	}
	defer reservation.Release()

	build, err := options.BuildIdentity()
	if err != nil {
		return oneShotReceipt(OneShotInternal)
	}
	clock := options.ClockFactory()
	if clock == nil {
		return oneShotReceipt(OneShotInternal)
	}
	cloudObservedAt, err := oneShotCloudObservedAt(request.CloudAppAction.NormalizedEvidence)
	if err != nil {
		return oneShotReceipt(OneShotInvalidRequest)
	}
	recorder, err := NewRecorder(RecorderOptions{
		Clock: clock, Entropy: options.Entropy, Limits: limits,
		Sources:         oneShotSources(request, options.Reader, build.OperationVersion, cloudObservedAt),
		RecorderVersion: build.RecorderVersion, ReplayVersion: build.ReplayVersion,
	})
	if err != nil {
		return oneShotReceipt(OneShotInternal)
	}
	bundleBytes, err := recorder.Capture(ctx, ActionMarker{EvidenceRef: request.ActionEvidenceRef})
	if err != nil {
		return oneShotReceipt(OneShotAcquisitionFailed)
	}
	bundle, err := verifyBundle(bundleBytes)
	if err != nil || validateOneShotBundle(bundle, options.sourceTuple()) != nil {
		return oneShotReceipt(OneShotAcquisitionFailed)
	}

	replay := options.replay
	if replay == nil {
		replay = Replay
	}
	firstReplay, firstErr := replay(bundleBytes)
	secondReplay, secondErr := replay(bundleBytes)
	if firstErr != nil || secondErr != nil || !bytes.Equal(firstReplay, secondReplay) {
		return oneShotReceipt(OneShotReplayMismatch)
	}
	if _, err := reservation.PublishVerified(bundleBytes, firstReplay); err != nil {
		return oneShotReceipt(OneShotPublishFailed)
	}
	return oneShotReceipt(OneShotPublished)
}

func oneShotCaptureLimits() CaptureLimitsV1 {
	limits := DefaultLimitsV1()
	limits.MaxSources = 5
	return limits
}

func oneShotReceipt(category OneShotReceiptCategory) OneShotReceiptV1 {
	return OneShotReceiptV1{Category: category}
}

func oneShotCloudObservedAt(raw json.RawMessage) (time.Time, error) {
	var payload struct {
		SourceObservedAt string `json:"source_observed_at"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || !canonicalTimestamp(payload.SourceObservedAt) {
		return time.Time{}, ErrInvalidArgument
	}
	observedAt, err := time.Parse(time.RFC3339Nano, payload.SourceObservedAt)
	if err != nil {
		return time.Time{}, ErrInvalidArgument
	}
	return observedAt, nil
}

func oneShotSources(
	request OneShotRequestV1,
	reader EEBusM625Reader,
	operationVersion string,
	cloudObservedAt time.Time,
) []RegisteredSource {
	return []RegisteredSource{
		oneShotTerminalEBusSource(
			SourceEBusB509,
			PhasePre,
			"ebus-b509",
			"dbe91a10a208613183f890849c634f8d13661194aad937a03ae2a4143070bf2d",
			operationVersion,
			oneShotB509Identity(),
		),
		{
			Phase: PhasePre, SourceKind: SourceEEBus,
			SourceContract: M625EEBusContractV1, SourceVersion: 1,
			RuntimeInstance: "eebus-runtime", OperationVersion: operationVersion,
			OperationScope: "feature-data", Admission: oneShotAdmission("eebus.raw.read", BackendUnknown),
			EvidenceRefs: []EvidenceRefV1{
				oneShotContentRef("0a2885d01d6703389541e246db59bcd845a332e7ed296abca2d49b4f8de31811"),
			},
			EEBusM625Reader: reader,
		},
		oneShotTerminalEBusSource(
			SourceEBusB524,
			PhaseAction,
			"ebus-b524",
			"1002e09890801c9032548af407b13b58d889217dfc83e58bfe2a28df6bc33b78",
			operationVersion,
			oneShotB524Identity(),
		),
		{
			Phase: PhaseAction, SourceKind: SourceCloudApp,
			RuntimeInstance: "cloud-app-precaptured", OperationVersion: operationVersion,
			OperationScope: "cloud-app", Admission: oneShotAdmission("cloud.read", BackendUnknown),
			cloudBound: true,
			PrecapturedCloud: &PrecapturedCloudInput{
				SourceObservedAt: cloudObservedAt,
				NormalizedEvidence: append(
					json.RawMessage(nil),
					request.CloudAppAction.NormalizedEvidence...,
				),
				EvidenceRef: request.CloudAppAction.EvidenceRef,
			},
		},
		oneShotTerminalEBusSource(
			SourceEBusB555,
			PhasePost,
			"ebus-b555",
			"a3237e344f5c3582ceaf1ca947eabf200bede8fe9388ec85cf647331f026c72d",
			operationVersion,
			oneShotB555Identity(),
		),
	}
}

func oneShotTerminalEBusSource(
	kind SourceKind,
	phase Phase,
	scope string,
	schemaDigest string,
	operationVersion string,
	identity *EBusSourceIdentityV1,
) RegisteredSource {
	return RegisteredSource{
		Phase: phase, SourceKind: kind, RuntimeInstance: "ebus-runtime",
		OperationVersion: operationVersion, OperationScope: scope,
		Admission:    oneShotAdmission("ebus.read", BackendUnreachable),
		EvidenceRefs: []EvidenceRefV1{oneShotContentRef(schemaDigest)},
		EBusIdentity: identity,
		EBusReader:   oneShotUnavailableEBusReader{},
	}
}

func oneShotAdmission(permission string, backend BackendDecision) SourceAdmission {
	return SourceAdmission{
		Selection: SelectionIncluded, Policy: PolicyAllowed, Backend: backend,
		EffectivePermissions: []string{permission}, RequiredPermissions: []string{permission},
	}
}

func oneShotContentRef(digest string) EvidenceRefV1 {
	return EvidenceRefV1{
		Kind: EvidenceKindContent, DigestAlgorithm: DigestAlgorithmContentBytes,
		Digest: "sha256:" + digest,
	}
}

func oneShotB509Identity() *EBusSourceIdentityV1 {
	return &EBusSourceIdentityV1{
		Family: EBusFamilyB509, TargetAddress: 0x08, TargetProduct: "BAI00",
		RegisterFamily: "system", RegisterID: 512, UnitScaleSource: "gateway-catalog-v1",
		EvidenceRole: "AUTHORITATIVE",
	}
}

func oneShotB524Identity() *EBusSourceIdentityV1 {
	return &EBusSourceIdentityV1{
		Family: EBusFamilyB524, TargetAddress: 0x15, SourceAddress: 0xf7,
		Opcode: 2, GG: 3, II: 0, RR: 28, GroupMeaning: "zones",
		InstanceGate: "index-not-ff", RegisterCategory: "STATE",
		UnitScaleSource: "vrc-explorer-v1",
	}
}

func oneShotB555Identity() *EBusSourceIdentityV1 {
	return &EBusSourceIdentityV1{
		Family: EBusFamilyB555, DeviceFamily: "VRC",
		ScheduleProgram: "heating-program-1", SlotIndex: 0, DayOfWeek: "MONDAY",
		TimeIdentity: "06:00:00", OperationModeContext: "AUTO",
		UnitScaleSource: "source-native",
	}
}

func validateOneShotBundle(bundle SynchronizedEvidenceBundleV1, tuple SourceTupleV1) error {
	if len(bundle.Sources) != 5 {
		return ErrContractViolation
	}
	expected := map[SourceKind]struct {
		phase    Phase
		contract string
		state    SourceState
		identity *EBusSourceIdentityV1
	}{
		SourceEBusB509: {PhasePre, "helianthus.ebus.b509.evidence.v1", StateUnavailable, oneShotB509Identity()},
		SourceEEBus:    {PhasePre, tuple.Contract, StatePresent, nil},
		SourceEBusB524: {PhaseAction, "helianthus.ebus.b524.evidence.v1", StateUnavailable, oneShotB524Identity()},
		SourceCloudApp: {PhaseAction, "helianthus.cloud-app.precaptured.evidence.v1", StatePresent, nil},
		SourceEBusB555: {PhasePost, "helianthus.ebus.b555.evidence.v1", StateUnavailable, oneShotB555Identity()},
	}
	seen := make(map[SourceKind]bool, len(expected))
	for _, source := range bundle.Sources {
		kind := source.SourceBinding.SourceKind
		want, ok := expected[kind]
		if !ok || seen[kind] || source.Phase != want.phase ||
			source.SourceContract != want.contract || source.State != want.state {
			return ErrContractViolation
		}
		if kind == tuple.SourceKind && source.SourceSchemaVersion != tuple.Version {
			return ErrContractViolation
		}
		if want.identity != nil {
			got := cloneIdentity(source.EBusIdentity)
			if got == nil {
				return ErrContractViolation
			}
			got.TargetPseudonym = ""
			if !reflect.DeepEqual(got, want.identity) {
				return ErrContractViolation
			}
		}
		seen[kind] = true
	}
	if len(seen) != len(expected) {
		return ErrContractViolation
	}
	return nil
}

var _ EBusSnapshotReader = oneShotUnavailableEBusReader{}
