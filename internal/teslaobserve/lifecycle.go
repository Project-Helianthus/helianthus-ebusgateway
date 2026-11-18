package teslaobserve

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	"github.com/Project-Helianthus/helianthus-modbusreg/tesla"
)

// ObservationOperations are the five retained observation operations of the
// pinned read acquisition. The run names them as the allowlist a later,
// separately admitted acquisition would use; it sends none of them.
var ObservationOperations = []tesla.FC100Operation{
	tesla.FC100OperationCommonSystemInfo,
	tesla.FC100OperationWCGetVitals,
	tesla.FC100OperationWCGetLifetimeStats,
	tesla.FC100OperationWCGetSystemInfo,
	tesla.FC100OperationWCGetLoadSharingState,
}

// LifecycleReplayDisposition states what a lifecycle replay result means.
const LifecycleReplayDisposition = "offline comparison through the pinned read-acquisition lifecycle with a control endpoint that has no transport; not activation evidence and no send authority"

var errControlNoTransport = errors.New("tesla observation control endpoint has no transport")

// LifecycleReplay records one offline pass of an observation through the
// pinned read lifecycle. FramesSent counts calls that reached the control
// endpoint's exchange method; it has no transport, so nothing is transmitted.
type LifecycleReplay struct {
	State          string `json:"lifecycle_state"`
	Ready          bool   `json:"offline_ready"`
	Error          string `json:"error,omitempty"`
	AcquireRefused bool   `json:"acquire_refused"`
	FramesSent     int64  `json:"frames_sent"`
	Disposition    string `json:"disposition"`
}

// controlEndpoint satisfies the lifecycle's endpoint seam with no transport.
type controlEndpoint struct{ exchanges *atomic.Int64 }

func (endpoint controlEndpoint) ExchangePrivateFunction(context.Context, byte, modbus.PrivateFunctionRequest, modbus.PrivateFunctionResponsePolicy, modbus.RTUPrivateFunctionResponseClassifier) ([]modbus.RTUPrivateFunctionResponseADU, modbus.RTUPrivateFunctionEvidence, error) {
	endpoint.exchanges.Add(1)
	return nil, modbus.RTUPrivateFunctionEvidence{}, errControlNoTransport
}

func (controlEndpoint) Recover(context.Context) error { return errControlNoTransport }
func (controlEndpoint) Close() error                  { return nil }

// observationReport hands an observation to the lifecycle only when it is a
// complete receive-side window still current at its owning endpoint. A
// passive observation has no host echo, so the driver refuses after those
// checks and the lifecycle stays unavailable.
type observationReport struct {
	endpoint    *Endpoint
	observation Observation
	// before runs immediately before the currentness check; controls use it
	// to place a fence, close or cancellation ahead of the commit.
	before func()
}

func (report observationReport) DriveTeslaHSCActivation(ctx context.Context, request tesla.HSCActivationRequest) (tesla.HSCActivationReport, error) {
	if request.Endpoint != report.observation.Endpoint || request.Epoch == 0 {
		return tesla.HSCActivationReport{}, ErrNotCurrent
	}
	if report.observation.Termination != TerminationCaptured {
		return tesla.HSCActivationReport{}, errors.New("tesla activation observation is incomplete: " + string(report.observation.Termination))
	}
	for _, segment := range report.observation.Segments {
		if segment.Direction != DirectionReceive {
			return tesla.HSCActivationReport{}, errors.New("tesla activation observation has a non-receive segment")
		}
	}
	if report.before != nil {
		report.before()
	}
	err := report.endpoint.WithCurrent(ctx, report.observation, func() error { return nil })
	if err != nil {
		return tesla.HSCActivationReport{}, err
	}
	return tesla.HSCActivationReport{}, errors.New("passive observation has no host echo")
}

// scriptedActivation is synthetic input for the positive offline control:
// the responder greeting, the host echo of that greeting, and the
// confirmation. The echo is not a write. The owned endpoint is not used,
// so the control endpoint's exchange method is not called.
type scriptedActivation struct{}

func (scriptedActivation) DriveTeslaHSCActivation(ctx context.Context, request tesla.HSCActivationRequest) (tesla.HSCActivationReport, error) {
	if ctx == nil || ctx.Err() != nil || request.Endpoint == "" || request.Epoch == 0 || request.Owned == nil {
		return tesla.HSCActivationReport{}, ErrNotCurrent
	}
	greeting := tesla.HSCActivationGreeting()
	return tesla.HSCActivationReport{
		ClockEpoch: syntheticClockEpoch,
		Events: []tesla.HSCActivationEvent{
			{Kind: tesla.HSCActivationEventGreeting, Bytes: greeting, LastByteOffset: time.Millisecond},
			{Kind: tesla.HSCActivationEventEcho, Bytes: tesla.HSCActivationGreeting(), LastByteOffset: 2 * time.Millisecond, Turnaround: time.Millisecond, WriteDuration: time.Millisecond},
			{Kind: tesla.HSCActivationEventConfirmation, Bytes: tesla.HSCActivationConfirmation(), LastByteOffset: 3 * time.Millisecond},
		},
	}, nil
}

// ReplayLifecycle passes observation through the pinned read lifecycle. now
// is the lifecycle clock; it defaults to time.Now.
func ReplayLifecycle(ctx context.Context, endpoint *Endpoint, observation Observation, now func() time.Time) LifecycleReplay {
	return replayLifecycle(ctx, observationReport{endpoint: endpoint, observation: observation}, observation.Endpoint, now)
}

func replayLifecycle(ctx context.Context, report tesla.HSCActivationDriver, label string, now func() time.Time) LifecycleReplay {
	result := LifecycleReplay{Disposition: LifecycleReplayDisposition}
	var exchanges atomic.Int64
	config, err := controlAcquisitionConfig(label, report, now, &exchanges)
	if err == nil {
		var acquisition *tesla.Gen3ReadAcquisition
		acquisition, err = tesla.NewGen3ReadAcquisition(config)
		if err == nil {
			defer func() { _ = acquisition.Close() }()
			if startErr := acquisition.Start(ctx); startErr != nil {
				result.Error = boundedCause(startErr)
			}
			result.State = string(acquisition.State())
			result.Ready = acquisition.State() == tesla.Gen3ReadLifecycleReady
			if !result.Ready {
				_, acquireErr := acquisition.Acquire(ctx, ObservationOperations[0])
				result.AcquireRefused = acquireErr != nil
			}
		}
	}
	if err != nil {
		result.State, result.Error = "not_constructed", boundedCause(err)
	}
	result.FramesSent = exchanges.Load()
	return result
}

// controlAcquisitionConfig declares the offline control configuration that
// the lifecycle requires before it will observe activation. It is not a
// qualification decision about any device.
func controlAcquisitionConfig(label string, report tesla.HSCActivationDriver, now func() time.Time, exchanges *atomic.Int64) (tesla.Gen3ReadAcquisitionConfig, error) {
	timing, err := modbus.NewRTUTiming(modbus.RTUTimingConfig{
		Baud: 115200, DataBits: 8, Parity: modbus.RTUParityNone, StopBits: 1,
		MaxResponseLatency: 500 * time.Millisecond, MaxQuiescence: 900 * time.Millisecond,
	})
	if err != nil {
		return tesla.Gen3ReadAcquisitionConfig{}, err
	}
	profile, err := tesla.NewHSCProfile(tesla.HSCProfileConfig{
		Enabled: true, Node: 1, PassiveCompatible: true, CompatibilityVersion: tesla.HSCCompatibilityV1,
		WCVitalsOperationVersion:           tesla.HSCWCVitalsCompatibilityV1,
		SystemInfoOperationVersion:         tesla.HSCSystemInfoCompatibilityV1,
		WCLifetimeOperationVersion:         tesla.HSCWCLifetimeCompatibilityV1,
		WCSystemInfoOperationVersion:       tesla.HSCWCSystemInfoCompatibilityV1,
		WCLoadSharingStateOperationVersion: tesla.HSCWCLoadSharingStateCompatibilityV1,
	})
	if err != nil {
		return tesla.Gen3ReadAcquisitionConfig{}, err
	}
	gen3, err := tesla.NewGen3HSCProfile(tesla.Gen3HSCProfileConfig{
		Enabled: true, Version: tesla.HSCFC100OperationCompatibilityV1,
		ActivationCapable: true, PrivateFunctionCapable: true, OperationCapable: true,
	})
	if err != nil {
		return tesla.Gen3ReadAcquisitionConfig{}, err
	}
	windows := make(map[tesla.FC100Operation]tesla.Gen3ReadWindow, len(ObservationOperations))
	for _, operation := range ObservationOperations {
		windows[operation] = tesla.Gen3ReadWindow{Freshness: time.Minute, Retention: time.Hour}
	}
	if now == nil {
		now = time.Now
	}
	return tesla.Gen3ReadAcquisitionConfig{
		Endpoint: label, LocalPath: "offline-control", Timing: timing, ResponseTimeout: time.Second,
		Profile: profile, Gen3: gen3,
		Operations: append([]tesla.FC100Operation(nil), ObservationOperations...), Windows: windows,
		MaxAttempts: 1, AcquisitionDeadline: 10 * time.Second, ActivationTimeout: MaxObservationWindow,
		PollInterval: time.Minute, Activation: report, Now: now,
		Open: func(modbus.RTUProductionConfig) (tesla.Gen3ReadEndpoint, error) {
			return controlEndpoint{exchanges: exchanges}, nil
		},
	}, nil
}
