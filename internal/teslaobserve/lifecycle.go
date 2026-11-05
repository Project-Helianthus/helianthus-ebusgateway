package teslaobserve

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

// ObservationOperations are the five retained observation operations of the
// pinned read acquisition. The run names them as the allowlist a later,
// separately admitted acquisition would use; it sends none of them.
var ObservationOperations = []modbusreg.TeslaFC100Operation{
	modbusreg.TeslaFC100OperationCommonSystemInfo,
	modbusreg.TeslaFC100OperationWCGetVitals,
	modbusreg.TeslaFC100OperationWCGetLifetimeStats,
	modbusreg.TeslaFC100OperationWCGetSystemInfo,
	modbusreg.TeslaFC100OperationWCGetLoadSharingState,
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
// complete receive-side window still current at its owning endpoint.
type observationReport struct {
	endpoint    *Endpoint
	observation Observation
	// before runs immediately before the currentness check; controls use it
	// to place a fence, close or cancellation ahead of the commit.
	before func()
}

func (report observationReport) ObserveTeslaGen3HSCActivation(ctx context.Context, request modbusreg.TeslaGen3HSCActivationRequest) ([]byte, error) {
	if request.Endpoint != report.observation.Endpoint || request.Epoch == 0 {
		return nil, ErrNotCurrent
	}
	if report.observation.Termination != TerminationCaptured {
		return nil, errors.New("tesla activation observation is incomplete: " + string(report.observation.Termination))
	}
	for _, segment := range report.observation.Segments {
		if segment.Direction != DirectionReceive {
			return nil, errors.New("tesla activation observation has a non-receive segment")
		}
	}
	if report.before != nil {
		report.before()
	}
	var window []byte
	err := report.endpoint.WithCurrent(ctx, report.observation, func() error {
		window = report.observation.Window()
		return nil
	})
	return window, err
}

// ReplayLifecycle passes observation through the pinned read lifecycle. now
// is the lifecycle clock; it defaults to time.Now.
func ReplayLifecycle(ctx context.Context, endpoint *Endpoint, observation Observation, now func() time.Time) LifecycleReplay {
	return replayLifecycle(ctx, observationReport{endpoint: endpoint, observation: observation}, observation.Endpoint, now)
}

func replayLifecycle(ctx context.Context, report modbusreg.TeslaGen3HSCActivationWitness, label string, now func() time.Time) LifecycleReplay {
	result := LifecycleReplay{Disposition: LifecycleReplayDisposition}
	var exchanges atomic.Int64
	config, err := controlAcquisitionConfig(label, report, now, &exchanges)
	if err == nil {
		var acquisition *modbusreg.TeslaGen3ReadAcquisition
		acquisition, err = modbusreg.NewTeslaGen3ReadAcquisition(config)
		if err == nil {
			defer func() { _ = acquisition.Close() }()
			if startErr := acquisition.Start(ctx); startErr != nil {
				result.Error = boundedCause(startErr)
			}
			result.State = string(acquisition.State())
			result.Ready = acquisition.State() == modbusreg.TeslaGen3ReadLifecycleReady
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
func controlAcquisitionConfig(label string, report modbusreg.TeslaGen3HSCActivationWitness, now func() time.Time, exchanges *atomic.Int64) (modbusreg.TeslaGen3ReadAcquisitionConfig, error) {
	timing, err := modbus.NewRTUTiming(modbus.RTUTimingConfig{
		Baud: 115200, DataBits: 8, Parity: modbus.RTUParityNone, StopBits: 1,
		MaxResponseLatency: 500 * time.Millisecond, MaxQuiescence: 900 * time.Millisecond,
	})
	if err != nil {
		return modbusreg.TeslaGen3ReadAcquisitionConfig{}, err
	}
	profile, err := modbusreg.NewTeslaHSCProfile(modbusreg.TeslaHSCProfileConfig{
		Enabled: true, Node: 1, PassiveCompatible: true, CompatibilityVersion: modbusreg.TeslaHSCCompatibilityV1,
		WCVitalsOperationVersion:           modbusreg.TeslaHSCWCVitalsCompatibilityV1,
		SystemInfoOperationVersion:         modbusreg.TeslaHSCSystemInfoCompatibilityV1,
		WCLifetimeOperationVersion:         modbusreg.TeslaHSCWCLifetimeCompatibilityV1,
		WCSystemInfoOperationVersion:       modbusreg.TeslaHSCWCSystemInfoCompatibilityV1,
		WCLoadSharingStateOperationVersion: modbusreg.TeslaHSCWCLoadSharingStateCompatibilityV1,
	})
	if err != nil {
		return modbusreg.TeslaGen3ReadAcquisitionConfig{}, err
	}
	gen3, err := modbusreg.NewTeslaGen3HSCProfile(modbusreg.TeslaGen3HSCProfileConfig{
		Enabled: true, Version: modbusreg.TeslaHSCFC100OperationCompatibilityV1,
		ActivationCapable: true, PrivateFunctionCapable: true, OperationCapable: true,
	})
	if err != nil {
		return modbusreg.TeslaGen3ReadAcquisitionConfig{}, err
	}
	windows := make(map[modbusreg.TeslaFC100Operation]modbusreg.TeslaGen3ReadWindow, len(ObservationOperations))
	for _, operation := range ObservationOperations {
		windows[operation] = modbusreg.TeslaGen3ReadWindow{Freshness: time.Minute, Retention: time.Hour}
	}
	if now == nil {
		now = time.Now
	}
	return modbusreg.TeslaGen3ReadAcquisitionConfig{
		Endpoint: label, LocalPath: "offline-control", Timing: timing, ResponseTimeout: time.Second,
		Profile: profile, Gen3: gen3,
		Operations: append([]modbusreg.TeslaFC100Operation(nil), ObservationOperations...), Windows: windows,
		MaxAttempts: 1, AcquisitionDeadline: 10 * time.Second, ActivationTimeout: MaxObservationWindow,
		PollInterval: time.Minute, Activation: report, Now: now,
		Open: func(modbus.RTUProductionConfig) (modbusreg.TeslaGen3ReadEndpoint, error) {
			return controlEndpoint{exchanges: exchanges}, nil
		},
	}, nil
}
