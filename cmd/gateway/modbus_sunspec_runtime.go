package main

import (
	"context"
	"strings"
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
	sunSpecLiveSmokeAttemptTimeout     = 10 * time.Second
	sunSpecLiveSmokeCycleInterval      = 15 * time.Second
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
	// FlavorEvaluations carries the registry's per-flavor reasons so a refusal
	// is diagnosable from the decision line.
	FlavorEvaluations []modbusadapter.SunSpecFlavorEvaluation
	Err               error
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
	// CapabilityReason and FlavorReasons are the registry's own refusal or
	// match reasons in the decision-line token form (see
	// formatSunSpecLiveSmokeFlavorReasons). They hold enum names and flavor
	// versions only: no sample, capability or flavor ID, serial or endpoint.
	CapabilityReason string
	FlavorReasons    string
}

type sunSpecLiveSmokeDriver interface {
	Poll(context.Context, sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error)
	Reconnect(context.Context) error
}

type sunSpecLiveSmokeQualifier interface {
	Qualify(context.Context, sunSpecLiveSmokeAttempt, sunSpecLiveSmokePollResult) sunSpecLiveSmokeQualification
}

type sunSpecLiveSmokeAdapter interface {
	Snapshot() modbus.TCPEndpointSnapshot
	Reconnect(context.Context) error
}

type sunSpecLiveSmokeProducer interface {
	Qualify(context.Context, modbusadapter.SunSpecPollIdentity) (modbusadapter.SunSpecQualificationResult, error)
	Refresh(context.Context, modbusadapter.SunSpecPollIdentity) (modbusadapter.SunSpecQualificationResult, error)
}

type modbusSunSpecLiveSmokeDriver struct {
	adapter   sunSpecLiveSmokeAdapter
	producer  sunSpecLiveSmokeProducer
	mu        sync.Mutex
	qualified bool
}

func (driver *modbusSunSpecLiveSmokeDriver) Poll(ctx context.Context, attempt sunSpecLiveSmokeAttempt) (sunSpecLiveSmokePollResult, error) {
	driver.mu.Lock()
	initial := !driver.qualified
	driver.mu.Unlock()
	identity := modbusadapter.SunSpecPollIdentity{
		PollGeneration: attempt.PollID, DeadlineIdentity: attempt.DeadlineID,
	}
	var qualification modbusadapter.SunSpecQualificationResult
	var err error
	if initial {
		qualification, err = driver.producer.Qualify(ctx, identity)
		if err == nil && qualification.Outcome == modbusadapter.SunSpecQualificationGO {
			driver.mu.Lock()
			driver.qualified = true
			driver.mu.Unlock()
		}
	} else {
		qualification, err = driver.producer.Refresh(ctx, identity)
	}
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
		Outcome:           poll.Qualification.Outcome,
		Sample:            poll.Qualification.SampleID,
		Capability:        poll.Qualification.CapabilityID,
		Flavor:            poll.Qualification.FlavorID,
		CapabilityReason:  poll.Qualification.CapabilityReason,
		FlavorReason:      poll.Qualification.FlavorReason,
		FlavorEvaluations: poll.Qualification.FlavorEvaluations,
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
	defer func() { logSunSpecLiveSmokeDecision(logf, result) }()
	if ctx == nil || attemptTimeout <= 0 || driver == nil || qualifier == nil {
		return result
	}

	cycleCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	for attemptIndex := 0; attemptIndex < 2; attemptIndex++ {
		attempt := sunSpecLiveSmokeAttempt{
			PollID: nextSunSpecLiveSmokeIdentity(), DeadlineID: nextSunSpecLiveSmokeIdentity(), Deadline: time.Until(deadlineOrNow(cycleCtx)),
		}
		poll, err := driver.Poll(cycleCtx, attempt)
		if err == nil {
			err = poll.Err
		}
		result.Attempts++
		if err == nil {
			qualification := qualifier.Qualify(cycleCtx, attempt, poll)
			result.Recovered = attemptIndex == 1
			return mapSunSpecLiveSmokeQualification(qualification, result)
		}
		if attemptIndex != 0 || !poll.Snapshot.ReconnectRequired {
			result.Category = "poll_error"
			return result
		}
		reconnectErr := driver.Reconnect(cycleCtx)
		if reconnectErr != nil {
			result.Category = "reconnect_error"
			return result
		}
	}
	return result
}

func deadlineOrNow(ctx context.Context) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Now()
	}
	return deadline
}

// logSunSpecLiveSmokeDecision writes the one decision line of a live-smoke
// cycle. The leading fields and their order are fixed; capability= and flavors=
// follow them. A decision without a qualification prints "-" for both.
func logSunSpecLiveSmokeDecision(logf func(string, ...any), result sunSpecLiveSmokeResult) {
	if logf == nil {
		return
	}
	logf(
		"modbus sunspec live smoke decision=%s outcome=%s category=%s attempts=%d recovered=%t capability=%s flavors=%s",
		result.Decision, result.Outcome, result.Category, result.Attempts, result.Recovered,
		sunSpecLiveSmokeField(result.CapabilityReason), sunSpecLiveSmokeField(result.FlavorReasons),
	)
}

func sunSpecLiveSmokeField(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

// sunSpecLiveSmokeToken reduces a registry name to one whitespace-free word of
// [A-Za-z0-9._-] so a decision line stays a single parseable record.
func sunSpecLiveSmokeToken(value string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, value)
}

// formatSunSpecLiveSmokeFlavorReasons renders every registry flavor decision
// as <flavor version>:<reason>, comma separated in registry order, for example
// "1.0.0:FIRMWARE_MISMATCH,1.1.0:FIRMWARE_MISMATCH,1.2.0:MATCHED". The version
// is the part of the flavor ID after its last '@'.
func formatSunSpecLiveSmokeFlavorReasons(evaluations []modbusadapter.SunSpecFlavorEvaluation) string {
	entries := make([]string, 0, len(evaluations))
	for _, evaluation := range evaluations {
		version := evaluation.FlavorID
		if at := strings.LastIndexByte(version, '@'); at >= 0 {
			version = version[at+1:]
		}
		entries = append(entries,
			sunSpecLiveSmokeField(sunSpecLiveSmokeToken(version))+":"+
				sunSpecLiveSmokeField(sunSpecLiveSmokeToken(string(evaluation.Reason))))
	}
	return strings.Join(entries, ",")
}

func mapSunSpecLiveSmokeQualification(qualification sunSpecLiveSmokeQualification, base sunSpecLiveSmokeResult) sunSpecLiveSmokeResult {
	base.Sample, base.Capability, base.Flavor = "", "", ""
	base.CapabilityReason, base.FlavorReasons = "", ""
	if qualification.Err != nil {
		base.Decision, base.Outcome, base.Category = sunSpecLiveSmokeDecisionStop, "error", "qualification_error"
		return base
	}
	// The registry's reasons accompany every decision, including STOP, so a
	// refusal is diagnosable; they carry no sample, capability or flavor ID.
	base.CapabilityReason = sunSpecLiveSmokeToken(string(qualification.CapabilityReason))
	base.FlavorReasons = formatSunSpecLiveSmokeFlavorReasons(qualification.FlavorEvaluations)
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

// isSupportedSunSpecLiveSmokeFlavor lists every Fronius GEN24 observed flavor
// the registry may select. A registry match on any other ID is incoherent.
func isSupportedSunSpecLiveSmokeFlavor(flavor string) bool {
	switch flavor {
	case modbusreg.SunSpecFroniusObservedFlavorID,
		modbusreg.SunSpecFroniusObservedFlavorV11ID,
		modbusreg.SunSpecFroniusObservedFlavorV12ID:
		return true
	default:
		return false
	}
}

type sunSpecLiveSmokeWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
	run    func()

	startOnce sync.Once
	closeOnce sync.Once
}

type sunSpecLiveSmokeWait func(context.Context, time.Duration) error

func newSunSpecLiveSmokeWorker(
	parent context.Context,
	attemptTimeout time.Duration,
	driver sunSpecLiveSmokeDriver,
	qualifier sunSpecLiveSmokeQualifier,
	logf func(string, ...any),
) *sunSpecLiveSmokeWorker {
	return newSunSpecLiveSmokeWorkerWithWait(parent, attemptTimeout, sunSpecLiveSmokeCycleInterval, driver, qualifier, logf, waitSunSpecLiveSmokeCycle)
}

func newSunSpecLiveSmokeWorkerWithWait(
	parent context.Context,
	attemptTimeout, cycleInterval time.Duration,
	driver sunSpecLiveSmokeDriver,
	qualifier sunSpecLiveSmokeQualifier,
	logf func(string, ...any),
	wait sunSpecLiveSmokeWait,
) *sunSpecLiveSmokeWorker {
	ctx, cancel := context.WithCancel(parent)
	worker := &sunSpecLiveSmokeWorker{cancel: cancel, done: make(chan struct{})}
	worker.run = func() {
		defer close(worker.done)
		for {
			_ = runSunSpecLiveSmoke(ctx, attemptTimeout, driver, qualifier, logf)
			if wait(ctx, cycleInterval) != nil {
				return
			}
		}
	}
	return worker
}

func waitSunSpecLiveSmokeCycle(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
		logSunSpecLiveSmokeDecision(logf, sunSpecLiveSmokeResult{
			Decision: sunSpecLiveSmokeDecisionStop, Outcome: "error", Category: "configuration_error",
		})
		return nil
	}
	driver := &modbusSunSpecLiveSmokeDriver{adapter: adapter, producer: producer}
	return newSunSpecLiveSmokeWorker(parent, sunSpecLiveSmokeAttemptTimeout, driver, modbusSunSpecLiveSmokeQualifier{}, logf)
}
