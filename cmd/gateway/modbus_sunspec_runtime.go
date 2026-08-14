package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

const (
	sunSpecLiveSmokeUnitID             = byte(1)
	sunSpecLiveSmokeAuthorizationScope = "smoke:fronius-readonly"
	sunSpecLiveSmokeReadTimeout        = 2 * time.Second
	sunSpecLiveSmokeAttemptTimeout     = 30 * time.Second
	sunSpecLiveSmokeCurrentFlavor      = modbusreg.SunSpecFroniusObservedFlavorV11ID
)

type sunSpecLiveSmokeDecision string

const (
	sunSpecLiveSmokeDecisionGO   sunSpecLiveSmokeDecision = "GO"
	sunSpecLiveSmokeDecisionNoGo sunSpecLiveSmokeDecision = "NO_GO"
	sunSpecLiveSmokeDecisionStop sunSpecLiveSmokeDecision = "STOP"
)

type sunSpecLiveSmokeAttempt struct {
	PollID     uint64
	DeadlineID uint64
	Deadline   time.Duration
}

type sunSpecLiveSmokeSnapshot = modbus.TCPEndpointSnapshot

type sunSpecLiveSmokePollResult struct {
	Snapshot      sunSpecLiveSmokeSnapshot
	Qualification modbusadapter.SunSpecQualificationResult
	Err           error
}

type sunSpecLiveSmokeQualification struct {
	Outcome          modbusadapter.SunSpecQualificationOutcome
	Sample           string
	Capability       string
	Flavor           string
	CapabilityReason modbusreg.SunSpecCapabilityReason
	FlavorReason     modbusreg.SunSpecFroniusFlavorReason
	Err              error
}

type sunSpecLiveSmokeResult struct {
	Decision   sunSpecLiveSmokeDecision
	Outcome    string
	Category   string
	Attempts   int
	Recovered  bool
	Sample     string
	Capability string
	Flavor     string
}

type sunSpecLiveSmokeDriver interface {
	Poll(context.Context, sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error)
	Reconnect(context.Context) error
}

type sunSpecLiveSmokeQualifier interface {
	Qualify(context.Context, sunSpecLiveSmokeAttempt, sunSpecLiveSmokePollResult) sunSpecLiveSmokeQualification
}

type modbusSunSpecLiveSmokeDriver struct {
	adapter  *modbusadapter.Adapter
	producer *modbusadapter.SunSpecProducer
}

func (driver *modbusSunSpecLiveSmokeDriver) Poll(ctx context.Context, attempt sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error) {
	qualification, err := driver.producer.Qualify(ctx, modbusadapter.SunSpecPollIdentity{
		PollGeneration: attempt.PollID, DeadlineIdentity: attempt.DeadlineID,
	})
	result := sunSpecLiveSmokePollResult{
		Snapshot: driver.adapter.Snapshot(), Qualification: qualification, Err: err,
	}
	return result, err
}

func (driver *modbusSunSpecLiveSmokeDriver) Reconnect(ctx context.Context) error {
	return driver.adapter.Reconnect(ctx)
}

type modbusSunSpecLiveSmokeQualifier struct{}

func (modbusSunSpecLiveSmokeQualifier) Qualify(_ context.Context, _ sunSpecLiveSmokeAttempt, poll sunSpecLiveSmokePollResult) sunSpecLiveSmokeQualification {
	if poll.Err != nil {
		return sunSpecLiveSmokeQualification{Err: poll.Err}
	}
	return sunSpecLiveSmokeQualification{
		Outcome:          poll.Qualification.Outcome,
		Sample:           poll.Qualification.SampleID,
		Capability:       poll.Qualification.CapabilityID,
		Flavor:           poll.Qualification.FlavorID,
		CapabilityReason: poll.Qualification.CapabilityReason,
		FlavorReason:     poll.Qualification.FlavorReason,
	}
}

var sunSpecLiveSmokeIdentity atomic.Uint64

func nextSunSpecLiveSmokeIdentity() uint64 {
	for {
		if identity := sunSpecLiveSmokeIdentity.Add(1); identity != 0 {
			return identity
		}
	}
}

func runSunSpecLiveSmoke(
	ctx context.Context,
	attemptTimeout time.Duration,
	driver sunSpecLiveSmokeDriver,
	qualifier sunSpecLiveSmokeQualifier,
	logf func(string, ...any),
) (result sunSpecLiveSmokeResult) {
	result = sunSpecLiveSmokeResult{Decision: sunSpecLiveSmokeDecisionStop, Outcome: "error", Category: "invalid_runtime"}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	defer func() {
		logf(
			"modbus sunspec live smoke decision=%s outcome=%s category=%s attempts=%d recovered=%t",
			result.Decision, result.Outcome, result.Category, result.Attempts, result.Recovered,
		)
	}()
	if ctx == nil || attemptTimeout <= 0 || driver == nil || qualifier == nil {
		return result
	}

	for attemptIndex := 0; attemptIndex < 2; attemptIndex++ {
		attempt := sunSpecLiveSmokeAttempt{
			PollID: nextSunSpecLiveSmokeIdentity(), DeadlineID: nextSunSpecLiveSmokeIdentity(), Deadline: attemptTimeout,
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		poll, err := driver.Poll(attemptCtx, attempt)
		if err == nil {
			err = poll.Err
		}
		result.Attempts++
		if err == nil {
			qualification := qualifier.Qualify(attemptCtx, attempt, poll)
			cancel()
			result.Recovered = attemptIndex == 1
			return mapSunSpecLiveSmokeQualification(qualification, result)
		}
		cancel()

		if attemptIndex != 0 || !poll.Snapshot.ReconnectRequired {
			result.Category = "poll_error"
			return result
		}
		reconnectCtx, reconnectCancel := context.WithTimeout(ctx, attemptTimeout)
		reconnectErr := driver.Reconnect(reconnectCtx)
		reconnectCancel()
		if reconnectErr != nil {
			result.Category = "reconnect_error"
			return result
		}
	}
	return result
}

func mapSunSpecLiveSmokeQualification(qualification sunSpecLiveSmokeQualification, base sunSpecLiveSmokeResult) sunSpecLiveSmokeResult {
	base.Sample, base.Capability, base.Flavor = "", "", ""
	if qualification.Err != nil {
		base.Decision, base.Outcome, base.Category = sunSpecLiveSmokeDecisionStop, "error", "qualification_error"
		return base
	}
	switch qualification.Outcome {
	case modbusadapter.SunSpecQualificationGO:
		if qualification.Sample == "" ||
			qualification.Capability != modbusreg.SunSpecThreePhaseMonitoringCapabilityID ||
			!isSupportedSunSpecLiveSmokeFlavor(qualification.Flavor) ||
			qualification.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
			qualification.FlavorReason != modbusreg.SunSpecFroniusFlavorReasonMatched {
			base.Decision, base.Outcome, base.Category = sunSpecLiveSmokeDecisionStop, "incoherent", "invalid_qualification"
			return base
		}
		base.Decision, base.Outcome, base.Category = sunSpecLiveSmokeDecisionGO, "qualified", "registry_match"
		base.Sample, base.Capability, base.Flavor = qualification.Sample, qualification.Capability, qualification.Flavor
		return base
	case modbusadapter.SunSpecQualificationNoGo:
		base.Decision, base.Outcome, base.Category = sunSpecLiveSmokeDecisionNoGo, "not_qualified", "registry_no_match"
		return base
	case modbusadapter.SunSpecQualificationStop:
		base.Decision, base.Outcome, base.Category = sunSpecLiveSmokeDecisionStop, "incoherent", "registry_stop"
		return base
	default:
		base.Decision, base.Outcome, base.Category = sunSpecLiveSmokeDecisionStop, "incoherent", "invalid_qualification"
		return base
	}
}

func isSupportedSunSpecLiveSmokeFlavor(flavor string) bool {
	return flavor == modbusreg.SunSpecFroniusObservedFlavorID || flavor == sunSpecLiveSmokeCurrentFlavor
}

type sunSpecLiveSmokeWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
	run    func()

	startOnce sync.Once
	closeOnce sync.Once
}

func newSunSpecLiveSmokeWorker(
	parent context.Context,
	attemptTimeout time.Duration,
	driver sunSpecLiveSmokeDriver,
	qualifier sunSpecLiveSmokeQualifier,
	logf func(string, ...any),
) *sunSpecLiveSmokeWorker {
	ctx, cancel := context.WithCancel(parent)
	worker := &sunSpecLiveSmokeWorker{cancel: cancel, done: make(chan struct{})}
	worker.run = func() {
		defer close(worker.done)
		_ = runSunSpecLiveSmoke(ctx, attemptTimeout, driver, qualifier, logf)
	}
	return worker
}

func (worker *sunSpecLiveSmokeWorker) Start() {
	if worker == nil {
		return
	}
	worker.startOnce.Do(func() { go worker.run() })
}

func (worker *sunSpecLiveSmokeWorker) Close() error {
	if worker == nil {
		return nil
	}
	worker.closeOnce.Do(func() {
		worker.cancel()
		worker.Start()
		<-worker.done
	})
	return nil
}

func newGatewaySunSpecLiveSmokeWorker(
	parent context.Context,
	adapter *modbusadapter.Adapter,
	logf func(string, ...any),
) *sunSpecLiveSmokeWorker {
	producer, err := modbusadapter.NewSunSpecProducer(adapter, modbusadapter.SunSpecProducerConfig{
		UnitID:             sunSpecLiveSmokeUnitID,
		AuthorizationScope: sunSpecLiveSmokeAuthorizationScope,
		ReadTimeout:        sunSpecLiveSmokeReadTimeout,
	})
	if err != nil {
		logf("modbus sunspec live smoke decision=STOP outcome=error category=configuration_error attempts=0 recovered=false")
		return nil
	}
	driver := &modbusSunSpecLiveSmokeDriver{adapter: adapter, producer: producer}
	return newSunSpecLiveSmokeWorker(parent, sunSpecLiveSmokeAttemptTimeout, driver, modbusSunSpecLiveSmokeQualifier{}, logf)
}
