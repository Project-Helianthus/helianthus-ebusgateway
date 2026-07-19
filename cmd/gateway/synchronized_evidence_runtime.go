package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/syncevidence"
)

const synchronizedEvidenceSourceVersion = "git:520b6439441cb6e8ef9ff291bde28f4efa4db254"

type synchronizedEvidenceRuntime struct {
	store     *syncevidence.FileStore
	recorder  *syncevidence.Recorder
	config    ebusgateway.EvidenceRecorderConfig
	closeOnce sync.Once
	closeErr  error
}

type synchronizedEvidenceClock struct {
	mu     sync.Mutex
	anchor time.Time
	last   uint64
}

func newSynchronizedEvidenceClock() *synchronizedEvidenceClock {
	return &synchronizedEvidenceClock{anchor: time.Now()}
}

func (clock *synchronizedEvidenceClock) Observe() syncevidence.ClockObservation {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	now := time.Now()
	offset := uint64(now.Sub(clock.anchor))
	if offset <= clock.last && clock.last > 0 {
		offset = clock.last + 1
	}
	clock.last = offset
	return syncevidence.ClockObservation{Wall: now.UTC(), OffsetNS: offset, UncertaintyNS: uint64(time.Millisecond)}
}

type unavailableEBusEvidenceReader struct{}

func (unavailableEBusEvidenceReader) ReadSnapshot(context.Context, syncevidence.SourceRequest) (syncevidence.AcquiredEvidence, error) {
	return syncevidence.AcquiredEvidence{}, syncevidence.ErrBackendUnavailable
}

type unavailableEEBusEvidenceReader struct{}

func (unavailableEEBusEvidenceReader) ListServices(context.Context, syncevidence.SourceRequest) (syncevidence.AcquiredEvidence, error) {
	return syncevidence.AcquiredEvidence{}, syncevidence.ErrBackendUnavailable
}

type gatewayEEBusEvidenceReader struct {
	capture      eebusEvidenceCapture
	pseudonymKey []byte
}

type eebusEvidenceCapture func([]byte) (json.RawMessage, time.Time, error)

func (reader *gatewayEEBusEvidenceReader) ListServices(context.Context, syncevidence.SourceRequest) (syncevidence.AcquiredEvidence, error) {
	payload, observedAt, err := reader.capture(reader.pseudonymKey)
	if err != nil {
		return syncevidence.AcquiredEvidence{}, err
	}
	return syncevidence.AcquiredEvidence{SourceObservedAt: observedAt, NormalizedEvidence: payload}, nil
}

func openSynchronizedEvidenceRuntime(config ebusgateway.EvidenceRecorderConfig) (*synchronizedEvidenceRuntime, error) {
	if err := ebusgateway.ValidateEvidenceRecorderConfig(config); err != nil {
		return nil, err
	}
	if !config.Enabled {
		return nil, nil
	}
	store, err := syncevidence.OpenFileStore(syncevidence.FileStoreConfig{
		Root: config.StateRoot, QuotaBytes: config.QuotaBytes, Retention: config.Retention,
	})
	if err != nil {
		return nil, fmt.Errorf("open synchronized evidence store: %w", err)
	}
	return &synchronizedEvidenceRuntime{store: store, config: config}, nil
}

func (runtime *synchronizedEvidenceRuntime) Configure(captureEEBus eebusEvidenceCapture, version string, clock syncevidence.Clock, entropy io.Reader) error {
	if runtime == nil {
		return nil
	}
	if runtime.recorder != nil || runtime.store == nil || clock == nil || entropy == nil {
		return errors.New("invalid synchronized evidence runtime configuration")
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(entropy, key); err != nil {
		return errors.New("initialize synchronized evidence pseudonym key")
	}
	eebusReader := syncevidence.EEBusServicesReader(unavailableEEBusEvidenceReader{})
	eebusBackend := syncevidence.BackendUnreachable
	if captureEEBus != nil {
		eebusReader = &gatewayEEBusEvidenceReader{capture: captureEEBus, pseudonymKey: key}
		eebusBackend = syncevidence.BackendUnknown
	}
	limits := synchronizedEvidenceLimits(runtime.config.Limits)
	sources := []syncevidence.RegisteredSource{
		ebusEvidenceRegistration(syncevidence.SourceEBusB509, syncevidence.PhasePre, "ebus-runtime", "ebus-b509", "dbe91a10a208613183f890849c634f8d13661194aad937a03ae2a4143070bf2d"),
		eebusEvidenceRegistration(syncevidence.SourceEEBus, syncevidence.PhasePre, "eebus-runtime", eebusReader, eebusBackend),
		ebusEvidenceRegistration(syncevidence.SourceEBusB524, syncevidence.PhaseAction, "ebus-runtime", "ebus-b524", "1002e09890801c9032548af407b13b58d889217dfc83e58bfe2a28df6bc33b78"),
		ebusEvidenceRegistration(syncevidence.SourceEBusB555, syncevidence.PhasePost, "ebus-runtime", "ebus-b555", "a3237e344f5c3582ceaf1ca947eabf200bede8fe9388ec85cf647331f026c72d"),
	}
	recorder, err := syncevidence.NewRecorder(syncevidence.RecorderOptions{
		Clock: clock, Entropy: entropy, Limits: limits, Sources: sources,
		RecorderVersion: version, ReplayVersion: version,
	})
	if err != nil {
		return fmt.Errorf("configure synchronized evidence recorder: %w", err)
	}
	runtime.recorder = recorder
	return nil
}

func (runtime *synchronizedEvidenceRuntime) Capture(ctx context.Context, marker syncevidence.ActionMarker) (string, []byte, error) {
	if runtime == nil || runtime.store == nil || runtime.recorder == nil {
		return "", nil, errors.New("synchronized evidence runtime unavailable")
	}
	reservation, err := runtime.store.ReserveCapture(int64(runtime.config.Limits.MaxBundleBytes))
	if err != nil {
		return "", nil, err
	}
	defer reservation.Release()
	bundle, err := runtime.recorder.Capture(ctx, marker)
	if err != nil {
		return "", nil, err
	}
	id, err := reservation.Publish(bundle)
	if err != nil {
		return "", nil, err
	}
	return id, bundle, nil
}

func (runtime *synchronizedEvidenceRuntime) Close() error {
	if runtime == nil {
		return nil
	}
	runtime.closeOnce.Do(func() {
		if runtime.store != nil {
			runtime.closeErr = runtime.store.Close()
		}
	})
	return runtime.closeErr
}

func synchronizedEvidenceLimits(config ebusgateway.EvidenceRecorderLimits) syncevidence.CaptureLimitsV1 {
	return syncevidence.CaptureLimitsV1{
		MaxSources: uint64(config.MaxSources), MaxItemsPerSource: uint64(config.MaxItemsPerSource),
		MaxArtifactBytes: uint64(config.MaxArtifactBytes), MaxBundleBytes: uint64(config.MaxBundleBytes),
		MaxDepth: uint64(config.MaxDepth), MaxStringBytes: uint64(config.MaxStringBytes),
		MaxCaptureDurationNS: uint64(config.MaxCaptureDuration), MaxSourceDurationNS: uint64(config.MaxSourceDuration),
	}
}

func ebusEvidenceRegistration(kind syncevidence.SourceKind, phase syncevidence.Phase, instance, scope, schemaDigest string) syncevidence.RegisteredSource {
	return syncevidence.RegisteredSource{
		Phase: phase, SourceKind: kind, RuntimeInstance: instance,
		OperationVersion: synchronizedEvidenceSourceVersion, OperationScope: scope,
		Admission: syncevidence.SourceAdmission{
			Selection: syncevidence.SelectionIncluded, Policy: syncevidence.PolicyAllowed, Backend: syncevidence.BackendUnknown,
			EffectivePermissions: []string{"ebus.read"}, RequiredPermissions: []string{"ebus.read"},
		},
		EvidenceRefs: []syncevidence.EvidenceRefV1{contentEvidenceRef(schemaDigest)},
		EBusReader:   unavailableEBusEvidenceReader{},
	}
}

func eebusEvidenceRegistration(kind syncevidence.SourceKind, phase syncevidence.Phase, instance string, reader syncevidence.EEBusServicesReader, backend syncevidence.BackendDecision) syncevidence.RegisteredSource {
	return syncevidence.RegisteredSource{
		Phase: phase, SourceKind: kind, RuntimeInstance: instance,
		OperationVersion: synchronizedEvidenceSourceVersion, OperationScope: "services",
		Admission: syncevidence.SourceAdmission{
			Selection: syncevidence.SelectionIncluded, Policy: syncevidence.PolicyAllowed, Backend: backend,
			EffectivePermissions: []string{"eebus.raw.read"}, RequiredPermissions: []string{"eebus.raw.read"},
		},
		EvidenceRefs: []syncevidence.EvidenceRefV1{contentEvidenceRef("7f10fa6860e8ccee1af7f155e03d5ac208b5a6fb30518aa3145122a9a1dc0a1c")},
		EEBusReader:  reader,
	}
}

func contentEvidenceRef(digest string) syncevidence.EvidenceRefV1 {
	return syncevidence.EvidenceRefV1{
		Kind: syncevidence.EvidenceKindContent, DigestAlgorithm: syncevidence.DigestAlgorithmContentBytes,
		Digest: "sha256:" + digest,
	}
}

func synchronizedEvidenceBuildVersion() (string, error) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", errors.New("read build information")
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && len(setting.Value) == 40 && strings.Trim(setting.Value, "0123456789abcdef") == "" {
			return buildVersion + "+git." + setting.Value, nil
		}
	}
	return "", errors.New("full build revision unavailable")
}

var _ syncevidence.EBusSnapshotReader = unavailableEBusEvidenceReader{}
var _ syncevidence.EEBusServicesReader = unavailableEEBusEvidenceReader{}
var _ syncevidence.EEBusServicesReader = (*gatewayEEBusEvidenceReader)(nil)

var synchronizedEvidenceEntropy io.Reader = rand.Reader
