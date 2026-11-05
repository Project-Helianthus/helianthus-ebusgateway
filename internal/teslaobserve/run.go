package teslaobserve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Attempt cases. The operator performs any reset or power-up separately,
// under its own action-time authorization; the run never does.
const (
	CaseInitial  = "initial"
	CaseRecovery = "recovery"
	CaseIdle     = "idle"
)

const (
	// Schema identifies the local evidence file format.
	Schema = "helianthus.tesla.gen3.activation_observation_run.v1"
	// ModeNoSend is the only mode: nothing is transmitted.
	ModeNoSend = "NO_SEND"
	// Family is published as a family, never as a model.
	Family = "Tesla Wall Connector Gen3 HSC"
	// Disposition is the standing disposition of every evidence file.
	Disposition = "Activation evidence and live qualification remain Unknown. This file is local review input for the activation evidence worksheet; it is not a witness, a qualification decision or send authority."

	MaxRepeats       = 16
	MaxPause         = 10 * time.Minute
	MaxAttempts      = 256
	maxOperatorText  = 200
	defaultLabelText = "unknown"
	// clockEpochPrefix starts the clock epoch of every attempt; the attempt
	// label completes it.
	clockEpochPrefix = "host-monotonic/"
)

var (
	hookContract      = "https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/tesla/activation-observation-hook-v1.md"
	worksheetContract = "https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/tesla/activation-evidence-worksheet.md"
	priorStates       = map[string]bool{"": true, "unknown": true, "fresh_session": true, "already_active": true, "recovery": true, "reset": true, "unavailable": true}
	initiatorStates   = map[string]bool{"": true, "unknown": true, "excluded": true, "present": true}
	authorizations    = map[string]bool{"": true, "unknown": true, "recorded": true}
)

// OperatorRecord is what the operator states about one attempt. It must not
// contain names, credentials, serials, locations or capture times.
type OperatorRecord struct {
	PriorState              string `json:"prior_state"`
	PriorStateBasis         string `json:"prior_state_basis,omitempty"`
	OtherInitiators         string `json:"other_initiators"`
	OtherInitiatorsBasis    string `json:"other_initiators_basis,omitempty"`
	Condition               string `json:"condition,omitempty"`
	ActionTimeAuthorization string `json:"action_time_authorization"`
}

// Attempt is one independent observation window on a fresh port instance.
type Attempt struct {
	Label       string           `json:"label"`
	Case        string           `json:"case"`
	Operator    OperatorRecord   `json:"operator"`
	OpenError   string           `json:"open_error,omitempty"`
	Observation *Observation     `json:"observation,omitempty"`
	Lifecycle   *LifecycleReplay `json:"offline_lifecycle_replay,omitempty"`
}

// Evidence is the local evidence file. Worksheet rows are rebuilt from the
// attempts and controls on every run.
type Evidence struct {
	Schema             string          `json:"schema"`
	Mode               string          `json:"mode"`
	Family             string          `json:"family"`
	Contracts          []string        `json:"contracts"`
	Disposition        string          `json:"disposition"`
	Endpoint           string          `json:"endpoint"`
	FramesSent         int64           `json:"frames_sent"`
	Preconditions      []Row           `json:"preconditions"`
	OrderedEvidence    []EvidenceRow   `json:"ordered_native_evidence"`
	RepeatsAndControls []Row           `json:"repeats_and_controls"`
	Review             []Row           `json:"review"`
	Attempts           []Attempt       `json:"attempts"`
	OfflineControls    []ControlResult `json:"offline_controls"`
}

// RunConfig bounds one invocation. Open returns a fresh receive-only source
// for every attempt; Now is the monotonic endpoint clock.
type RunConfig struct {
	Endpoint       string
	Case           string
	Repeats        int
	Window         time.Duration
	RequestedBytes int
	SegmentCap     int
	SegmentGap     time.Duration
	Pause          time.Duration
	Operator       OperatorRecord
	Open           func() (ByteSource, error)
	Now            func() time.Time
}

// Validate rejects every missing or out-of-range bound before a port opens.
func (config RunConfig) Validate() error {
	switch {
	case !ValidLabel(config.Endpoint):
		return fmt.Errorf("%w: endpoint label must be 1..64 of [a-z0-9._-]", ErrInvalidRequest)
	case config.Case != CaseInitial && config.Case != CaseRecovery && config.Case != CaseIdle:
		return fmt.Errorf("%w: case must be initial, recovery or idle", ErrInvalidRequest)
	case config.Repeats < 1 || config.Repeats > MaxRepeats:
		return fmt.Errorf("%w: repeats must be 1..%d", ErrInvalidRequest, MaxRepeats)
	case config.Window <= 0 || config.Window > MaxObservationWindow:
		return fmt.Errorf("%w: window must be within %s", ErrInvalidRequest, MaxObservationWindow)
	case config.RequestedBytes < 1 || config.RequestedBytes > MaxRequestedBytes:
		return fmt.Errorf("%w: window bytes must be 1..%d", ErrInvalidRequest, MaxRequestedBytes)
	case config.SegmentCap < 1 || config.SegmentCap > MaxSegmentCap:
		return fmt.Errorf("%w: segment cap must be 1..%d", ErrInvalidRequest, MaxSegmentCap)
	case config.SegmentGap <= 0 || config.SegmentGap > config.Window:
		return fmt.Errorf("%w: segment gap must be positive and within the window", ErrInvalidRequest)
	case config.Pause < 0 || config.Pause > MaxPause:
		return fmt.Errorf("%w: pause must be 0..%s", ErrInvalidRequest, MaxPause)
	case config.Open == nil || config.Now == nil:
		return fmt.Errorf("%w: source opener and clock are required", ErrInvalidRequest)
	}
	return config.Operator.validate()
}

func (record OperatorRecord) validate() error {
	if !priorStates[record.PriorState] || !initiatorStates[record.OtherInitiators] || !authorizations[record.ActionTimeAuthorization] {
		return fmt.Errorf("%w: operator state value is not recognized", ErrInvalidRequest)
	}
	for _, text := range []string{record.PriorStateBasis, record.OtherInitiatorsBasis, record.Condition} {
		if len(text) > maxOperatorText {
			return fmt.Errorf("%w: operator text exceeds %d bytes", ErrInvalidRequest, maxOperatorText)
		}
		for _, r := range text {
			if r < 0x20 || r > 0x7e || r == '|' {
				return fmt.Errorf("%w: operator text must be printable ASCII without '|'", ErrInvalidRequest)
			}
		}
	}
	return nil
}

func (record OperatorRecord) normalized() OperatorRecord {
	if record.PriorState == "" {
		record.PriorState = defaultLabelText
	}
	if record.OtherInitiators == "" {
		record.OtherInitiators = defaultLabelText
	}
	if record.ActionTimeAuthorization == "" {
		record.ActionTimeAuthorization = defaultLabelText
	}
	return record
}

// Run performs config.Repeats independent attempts, appends them to prior
// (when given), reruns the offline controls and rebuilds the worksheet rows.
// Each attempt opens a fresh source instance, observes one window, replays
// it offline through the read lifecycle and closes the source.
// ErrAllAttemptsFailed reports that every new attempt of a run failed to open or
// read. The evidence of those attempts is still returned and written.
var ErrAllAttemptsFailed = errors.New("all new observation attempts failed to open or read")

func Run(ctx context.Context, config RunConfig, prior *Evidence) (Evidence, error) {
	if ctx == nil {
		return Evidence{}, ErrInvalidRequest
	}
	if err := config.Validate(); err != nil {
		return Evidence{}, err
	}
	evidence := Evidence{Endpoint: config.Endpoint}
	if prior != nil {
		if err := checkPrior(*prior, config.Endpoint); err != nil {
			return Evidence{}, err
		}
		evidence.Attempts = append(evidence.Attempts, prior.Attempts...)
	}
	if len(evidence.Attempts)+config.Repeats > MaxAttempts {
		return Evidence{}, fmt.Errorf("%w: evidence file would exceed %d attempts", ErrInvalidRequest, MaxAttempts)
	}
	operator := config.Operator.normalized()
	newAttempts, failedAttempts := 0, 0
	for repeat := 0; repeat < config.Repeats; repeat++ {
		if repeat > 0 && config.Pause > 0 {
			timer := time.NewTimer(config.Pause)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			break
		}
		index := len(evidence.Attempts) + 1
		attempt := runAttempt(ctx, config, operator, index)
		evidence.Attempts = append(evidence.Attempts, attempt)
		newAttempts++
		if attempt.OpenError != "" || (attempt.Observation != nil && attempt.Observation.Termination == TerminationReadError) {
			failedAttempts++
		}
	}
	evidence.OfflineControls = RunControls(context.WithoutCancel(ctx))
	finish(&evidence)
	if err := ctx.Err(); err != nil {
		return evidence, err
	}
	if newAttempts > 0 && failedAttempts == newAttempts {
		return evidence, ErrAllAttemptsFailed
	}
	return evidence, nil
}

func runAttempt(ctx context.Context, config RunConfig, operator OperatorRecord, index int) Attempt {
	attempt := Attempt{Label: fmt.Sprintf("a%02d", index), Case: config.Case, Operator: operator}
	source, err := config.Open()
	if err == nil && source == nil {
		err = errors.New("source opener returned no source")
	}
	if err != nil {
		attempt.OpenError = boundedCause(err)
		return attempt
	}
	endpoint, err := NewEndpoint(config.Endpoint, uint64(index), source, config.Now)
	if err != nil {
		_ = source.Close()
		attempt.OpenError = boundedCause(err)
		return attempt
	}
	defer func() { _ = endpoint.Close() }()
	observation, err := endpoint.Observe(ctx, Request{
		ExpectedGeneration: endpoint.Generation(), Deadline: config.Now().Add(config.Window),
		ClockEpoch: clockEpochPrefix + attempt.Label, RequestedBytes: config.RequestedBytes,
		SegmentCap: config.SegmentCap, SegmentGap: config.SegmentGap,
	})
	if err != nil {
		attempt.OpenError = boundedCause(err)
		return attempt
	}
	replay := ReplayLifecycle(context.WithoutCancel(ctx), endpoint, observation, nil)
	attempt.Observation, attempt.Lifecycle = &observation, &replay
	return attempt
}

func checkPrior(prior Evidence, endpoint string) error {
	if prior.Schema != Schema || prior.Mode != ModeNoSend || prior.Endpoint != endpoint {
		return fmt.Errorf("%w: existing evidence file has another schema, mode or endpoint label", ErrInvalidRequest)
	}
	for index, attempt := range prior.Attempts {
		if attempt.Label != fmt.Sprintf("a%02d", index+1) || attempt.Operator.validate() != nil ||
			(attempt.Observation != nil && attempt.Observation.Endpoint != endpoint) {
			return fmt.Errorf("%w: existing evidence file attempt %d is not well formed", ErrInvalidRequest, index+1)
		}
		if attempt.Observation != nil && !attempt.Observation.shapeValid() {
			return fmt.Errorf("%w: existing evidence file attempt %d has malformed segments", ErrInvalidRequest, index+1)
		}
		// Worksheet rows are rebuilt from the persisted metadata, so it must be
		// metadata this observer could have emitted, not merely well shaped.
		if fault := attempt.metadataFault(index + 1); fault != "" {
			return fmt.Errorf("%w: existing evidence file attempt %d has inconsistent observation metadata: %s", ErrInvalidRequest, index+1, fault)
		}
	}
	return nil
}

func finish(evidence *Evidence) {
	evidence.Schema, evidence.Mode, evidence.Family = Schema, ModeNoSend, Family
	evidence.Contracts = []string{hookContract, worksheetContract}
	evidence.Disposition = Disposition
	evidence.FramesSent = 0
	for _, attempt := range evidence.Attempts {
		if attempt.Lifecycle != nil {
			evidence.FramesSent += attempt.Lifecycle.FramesSent
		}
	}
	for _, control := range evidence.OfflineControls {
		evidence.FramesSent += control.FramesSent
	}
	buildWorksheet(evidence)
}

// DecodeEvidence parses an existing evidence file for append.
func DecodeEvidence(data []byte) (Evidence, error) {
	var evidence Evidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		return Evidence{}, fmt.Errorf("%w: existing evidence file is not valid JSON", ErrInvalidRequest)
	}
	return evidence, nil
}

// EncodeEvidence renders the evidence file deterministically.
func EncodeEvidence(evidence Evidence) ([]byte, error) {
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// shapeValid reports whether every segment and overflow byte has a receipt
// offset, so rebuilt worksheet rows can index them safely.
func (observation Observation) shapeValid() bool {
	for _, segment := range observation.Segments {
		if len(segment.Bytes) == 0 || len(segment.Offsets) != len(segment.Bytes) {
			return false
		}
	}
	return len(observation.Coverage.OverflowOffsets) == len(observation.Coverage.Overflow)
}
