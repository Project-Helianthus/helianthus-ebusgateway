package modbusadapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

type integrationJitter struct{}

func (integrationJitter) Next(time.Duration) time.Duration { return 0 }

func integrationConfig(t *testing.T, endpoint string) Config {
	t.Helper()
	clock := modbus.NewRealTCPMonotonicClock()
	source, err := modbus.NewRuntimeAcquisitionSource(modbus.RuntimeAcquisitionConfig{
		Limits: modbus.RuntimeAcquisitionLimits{
			MaxLiveCapabilities:                        16,
			MaxAttempts:                                8,
			MaxMembersPerAttempt:                       8,
			AttemptKeyMaxUTF8Bytes:                     128,
			SourceEvidenceIDMaxUTF8Bytes:               256,
			NormalizationRecordMaxEncodedBytes:         4096,
			NormalizationRequiredStringMaxUTF8Bytes:    256,
			NormalizationExtensionCountMax:             8,
			NormalizationExtensionKeyMaxUTF8Bytes:      128,
			NormalizationExtensionValueMaxEncodedBytes: 1024,
			RetainedDiagnosticCountPerObjectMax:        8,
			RetainedDiagnosticMaxUTF8Bytes:             256,
			CapabilityTombstoneLimit:                   32,
			CapabilityTombstoneMaxEncodedBytes:         128,
		},
		ClaimLifetime: time.Minute,
		Clock:         clock,
	})
	if err != nil {
		t.Fatalf("NewRuntimeAcquisitionSource: %v", err)
	}
	return Config{
		Enabled:     true,
		DialTimeout: time.Second,
		Endpoint: modbus.TCPEndpointConfig{
			Endpoint: endpoint,
			PoolLimits: modbus.EndpointPoolLimits{
				MaxConnections: 1,
				Connection: modbus.ConnectionLimits{
					MaxInFlight:   4,
					MaxTombstones: 8,
				},
			},
			SchedulerLimits: modbus.SchedulerLimits{
				MaxActiveAdmissionKeys:         4,
				ProtectedSlotsPerKey:           1,
				SharedBurstSlots:               4,
				TotalQueued:                    8,
				MaxQueuedPerKey:                4,
				MaxQueuedPerAuthorizationScope: 8,
				MaxCoalescedDependentsPerKey:   4,
				MaxRetryAttempts:               1,
				MaxInFlightRequests:            4,
			},
			Backoff: modbus.BackoffConfig{
				Floor:             time.Millisecond,
				Ceiling:           time.Millisecond,
				MaxAttempts:       1,
				Jitter:            integrationJitter{},
				JitterAlgorithmID: "integration-zero",
				JitterVersion:     "v1",
				JitterEvidence:    "deterministic-test",
			},
			MaxBufferedBytes:         260,
			MaxRequestDeadline:       time.Second,
			MaxResponseDeadline:      time.Second,
			Clock:                    clock,
			RuntimeAcquisitionSource: source,
		},
	}
}

func realFactory(config modbus.TCPEndpointConfig) (Endpoint, error) {
	return modbus.NewTCPEndpoint(config)
}

func realDialer(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func serveResponse(listener net.Listener, words []uint16) <-chan error {
	done := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = connection.Close() }()
		header := make([]byte, 7)
		if _, err := io.ReadFull(connection, header); err != nil {
			done <- err
			return
		}
		length := int(binary.BigEndian.Uint16(header[4:6]))
		if length < 2 {
			done <- fmt.Errorf("invalid request length %d", length)
			return
		}
		body := make([]byte, length-1)
		if _, err := io.ReadFull(connection, body); err != nil {
			done <- err
			return
		}
		response := []byte{
			header[0], header[1], 0, 0,
			0, byte(3 + len(words)*2), header[6],
			body[0], byte(len(words) * 2),
		}
		for _, word := range words {
			response = append(response, byte(word>>8), byte(word))
		}
		_, err = connection.Write(response)
		done <- err
	}()
	return done
}

func TestAdapterDelegatesFairUnitScheduling(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	adapter, err := Start(
		context.Background(),
		integrationConfig(t, "tcp://"+listener.Addr().String()),
		realDialer,
		realFactory,
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = adapter.Close() }()

	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadInputRegisters, 100, 1)
	if err != nil {
		t.Fatalf("NewReadRegistersRequest: %v", err)
	}
	unitOneFirst, err := adapter.EnqueueRead(ReadPlan{
		UnitID: 1, AuthorizationScope: "site", PollGeneration: 1,
		DeadlineIdentity: 1, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 1, Request: request}},
	})
	if err != nil {
		t.Fatalf("enqueue unit one first: %v", err)
	}
	unitOneSecond, err := adapter.EnqueueRead(ReadPlan{
		UnitID: 1, AuthorizationScope: "site", PollGeneration: 1,
		DeadlineIdentity: 2, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 2, Request: request}},
	})
	if err != nil {
		t.Fatalf("enqueue unit one second: %v", err)
	}
	unitTwo, err := adapter.EnqueueRead(ReadPlan{
		UnitID: 2, AuthorizationScope: "site", PollGeneration: 1,
		DeadlineIdentity: 3, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 3, Request: request}},
	})
	if err != nil {
		t.Fatalf("enqueue unit two: %v", err)
	}

	want := []uint64{unitOneFirst.RequestID(), unitTwo.RequestID(), unitOneSecond.RequestID()}
	got := make([]uint64, 0, len(want))
	for range want {
		dispatch, ok := adapter.Dispatch()
		if !ok {
			t.Fatal("fair scheduler returned no dispatch")
		}
		got = append(got, dispatch.RequestID())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatch order = %v; want fair unit rotation %v", got, want)
	}
}

func TestAdapterRejectsSecondOwnerForSameRemote(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	endpoint := "tcp://" + listener.Addr().String()
	first, err := Start(context.Background(), integrationConfig(t, endpoint), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start(first): %v", err)
	}
	defer func() { _ = first.Close() }()
	second, err := Start(context.Background(), integrationConfig(t, endpoint), realDialer, realFactory)
	if err == nil || second != nil {
		t.Fatalf("Start(second owner) = %#v, %v; want nil, error", second, err)
	}
}

func TestAdapterRejectsQueuedWorkFromRetiredGenerationAndCancelsQueuedRead(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = adapter.Close() }()
	endpoint := adapter.endpoint.(*modbus.TCPEndpoint)

	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadInputRegisters, 100, 1)
	if err != nil {
		t.Fatalf("NewReadRegistersRequest: %v", err)
	}
	oldRequest, err := adapter.EnqueueRead(ReadPlan{
		UnitID: 1, AuthorizationScope: "site", PollGeneration: 1,
		DeadlineIdentity: 1, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 1, Request: request}},
	})
	if err != nil {
		t.Fatalf("enqueue old generation: %v", err)
	}
	oldConnection := adapter.connection
	if err := endpoint.CloseConnection(oldConnection); err != nil {
		t.Fatalf("CloseConnection(old): %v", err)
	}
	connection, err := realDialer(context.Background(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial replacement: %v", err)
	}
	newConnection, err := endpoint.OpenConnection(connection)
	if err != nil {
		t.Fatalf("OpenConnection(replacement): %v", err)
	}
	if newConnection.Generation() == oldConnection.Generation() {
		t.Fatalf("replacement reused transport generation %d", newConnection.Generation())
	}
	adapter.connection = newConnection
	if dispatch, ok := adapter.Dispatch(); ok {
		t.Fatalf("retired-generation request %d dispatched as %d", oldRequest.RequestID(), dispatch.RequestID())
	}

	newRequest, err := adapter.EnqueueRead(ReadPlan{
		UnitID: 2, AuthorizationScope: "site", PollGeneration: 2,
		DeadlineIdentity: 2, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 2, Request: request}},
	})
	if err != nil {
		t.Fatalf("enqueue replacement generation: %v", err)
	}
	if err := adapter.Cancel(newRequest); err != nil {
		t.Fatalf("Cancel(queued): %v", err)
	}
	if dispatch, ok := adapter.Dispatch(); ok {
		t.Fatalf("cancelled request dispatched as %d", dispatch.RequestID())
	}
	snapshot := adapter.Snapshot()
	if snapshot.Resources.QueuedRequests != 0 || snapshot.Resources.InFlightRequests != 0 {
		t.Fatalf("cancelled/retired work remained scheduled: %+v", snapshot.Resources)
	}
}

func TestAdapterPreservesCoalescedWireAndLogicalViewProvenance(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	done := serveResponse(listener, []uint16{0x1111, 0x2222, 0x3333})
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = adapter.Close() }()

	first, err := modbus.NewReadRegistersRequest(modbus.FunctionReadInputRegisters, 100, 2)
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	second, err := modbus.NewReadRegistersRequest(modbus.FunctionReadInputRegisters, 101, 2)
	if err != nil {
		t.Fatalf("second request: %v", err)
	}
	_, err = adapter.EnqueueRead(ReadPlan{
		UnitID:             7,
		AuthorizationScope: "readonly:site-a",
		PollGeneration:     41,
		DeadlineIdentity:   12,
		Timeout:            time.Second,
		Reads: []modbus.TCPLogicalRead{
			{LogicalViewID: 1001, Request: first},
			{LogicalViewID: 1002, Request: second},
		},
	})
	if err != nil {
		t.Fatalf("EnqueueRead: %v", err)
	}
	dispatch, ok := adapter.Dispatch()
	if !ok {
		t.Fatal("no dispatch")
	}
	if _, err := adapter.Write(context.Background(), dispatch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	batch, err := adapter.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("fake peer: %v", err)
	}
	if len(batch.Responses) != 1 || len(batch.Views) != 2 {
		t.Fatalf("batch responses/views = %d/%d; want 1/2", len(batch.Responses), len(batch.Views))
	}
	sort.Slice(batch.Views, func(i, j int) bool {
		return batch.Views[i].LogicalViewID() < batch.Views[j].LogicalViewID()
	})
	response := batch.Responses[0]
	if response.WireResponseID() == 0 || response.Outcome() != modbus.WireSuccessfulData {
		t.Fatalf("wire response identity/outcome = %d/%s", response.WireResponseID(), response.Outcome())
	}
	for _, view := range batch.Views {
		provenance := view.Provenance()
		if view.WireResponseID() != response.WireResponseID() ||
			provenance.Wire.Endpoint != config.Endpoint.Endpoint ||
			provenance.Wire.Transport != modbus.TransportTCP ||
			provenance.Wire.TransportGeneration == 0 ||
			provenance.Wire.UnitID != 7 ||
			provenance.AuthorizationScope != "readonly:site-a" ||
			provenance.PollGeneration != 41 ||
			provenance.DeadlineIdentity != 12 {
			t.Fatalf("logical provenance changed: %+v", provenance)
		}
	}
	if got := batch.Views[0].Words(); !reflect.DeepEqual(got, []uint16{0x1111, 0x2222}) {
		t.Fatalf("first view words = %x", got)
	}
	if got := batch.Views[1].Words(); !reflect.DeepEqual(got, []uint16{0x2222, 0x3333}) {
		t.Fatalf("second view words = %x", got)
	}
}

func TestAdapterExecuteReadUsesOwnedEndpoint(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	done := serveResponse(listener, []uint16{0x5375, 0x6e53})
	adapter, err := Start(
		context.Background(),
		integrationConfig(t, "tcp://"+listener.Addr().String()),
		realDialer,
		realFactory,
	)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = adapter.Close() }()
	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 40000, 2)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := adapter.ExecuteRead(context.Background(), ReadPlan{
		UnitID: 1, AuthorizationScope: "mcp:modbus.raw.read", PollGeneration: 7,
		DeadlineIdentity: 8, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 9, Request: request}},
	})
	if err != nil {
		t.Fatalf("ExecuteRead: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("fake peer: %v", err)
	}
	if len(batch.Views) != 1 || !reflect.DeepEqual(batch.Views[0].Words(), []uint16{0x5375, 0x6e53}) {
		t.Fatalf("batch views = %+v", batch.Views)
	}
	provenance := batch.Views[0].Provenance()
	if provenance.AuthorizationScope != "mcp:modbus.raw.read" || provenance.PollGeneration != 7 || provenance.DeadlineIdentity != 8 {
		t.Fatalf("provenance changed: %+v", provenance)
	}
}

type observationCommitter struct {
	mu      sync.Mutex
	state   modbusreg.SampleLedgerState
	restart modbusreg.LedgerRestartState
}

func (committer *observationCommitter) CommitPublication(
	ctx context.Context,
	request modbusreg.PublicationCommitRequest,
) (modbusreg.PublicationCommitDecision, error) {
	if err := ctx.Err(); err != nil {
		return modbusreg.PublicationCommitCancelled, nil
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if committer.state != request.ExpectedState {
		return "", errorsNew("sample state conflict")
	}
	committer.state = request.PublishedState
	committer.restart = request.PublishedRestartState
	return modbusreg.PublicationCommitCommitted, nil
}

func (committer *observationCommitter) CommitTerminalState(
	ctx context.Context,
	request modbusreg.TerminalStateCommitRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	committer.mu.Lock()
	defer committer.mu.Unlock()
	committer.restart = request.TerminalRestartState
	return nil
}

func errorsNew(message string) error { return fmt.Errorf("%s", message) }

func TestAdapterViewsPublishWithoutChangingSampleFacts(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()
	done := serveResponse(listener, []uint16{0x5375})
	config := integrationConfig(t, "tcp://"+listener.Addr().String())
	adapter, err := Start(context.Background(), config, realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = adapter.Close() }()

	request, err := modbus.NewReadRegistersRequest(modbus.FunctionReadHoldingRegisters, 40000, 1)
	if err != nil {
		t.Fatalf("NewReadRegistersRequest: %v", err)
	}
	_, err = adapter.EnqueueRead(ReadPlan{
		UnitID: 1, AuthorizationScope: "readonly:fronius", PollGeneration: 91,
		DeadlineIdentity: 77, Timeout: time.Second,
		Reads: []modbus.TCPLogicalRead{{LogicalViewID: 9001, Request: request}},
	})
	if err != nil {
		t.Fatalf("EnqueueRead: %v", err)
	}
	dispatch, ok := adapter.Dispatch()
	if !ok {
		t.Fatal("no dispatch")
	}
	if _, err := adapter.Write(context.Background(), dispatch); err != nil {
		t.Fatalf("Write: %v", err)
	}
	batch, err := adapter.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("fake peer: %v", err)
	}
	if len(batch.Views) != 1 {
		t.Fatalf("views = %d; want 1", len(batch.Views))
	}

	profile, err := modbusreg.NewSunSpecPhaseOneProfile(modbusreg.SunSpecPhaseOneVersions{
		Profile: modbusreg.CurrentSchemaVersion(),
		Codec:   modbusreg.CurrentCodecContractVersion(),
	})
	if err != nil {
		t.Fatalf("NewSunSpecPhaseOneProfile: %v", err)
	}
	initial, err := modbusreg.EmptySampleLedgerState("gateway-modbus", profile)
	if err != nil {
		t.Fatalf("EmptySampleLedgerState: %v", err)
	}
	ledger, err := modbusreg.NewSampleLedger(initial, 0)
	if err != nil {
		t.Fatalf("NewSampleLedger: %v", err)
	}
	committer := &observationCommitter{state: initial}
	factory, err := modbusreg.NewObservationFactory(profile, ledger, committer)
	if err != nil {
		t.Fatalf("NewObservationFactory: %v", err)
	}
	sourceTime := time.Unix(1_800_000_000, 123).UTC()
	receiptTime := sourceTime.Add(250 * time.Millisecond)
	attempt, err := factory.BeginRuntimeAttempt(modbusreg.RuntimeAttemptRequest{
		Source:     adapter.RuntimeAcquisitionSource(),
		AttemptKey: "fronius-poll-91",
		Identity:   modbusreg.AttemptIdentity{PollGenerationID: 91},
		Observation: modbusreg.RuntimeObservationFacts{
			SourceValidity:          modbusreg.SourceValid,
			SourceTime:              modbusreg.SourceTimeObserved(sourceTime),
			LocalReceiptTime:        receiptTime,
			LocalReceiptTimePresent: true,
		},
		Dependencies: []modbusreg.RuntimeDependencyFacts{{
			SourceTime: modbusreg.SourceTimeUnavailable(),
		}},
	})
	if err != nil {
		t.Fatalf("BeginRuntimeAttempt: %v", err)
	}
	normalization, err := adapter.RuntimeAcquisitionSource().ParseNormalizationRecord([]byte(
		`{"schema_version":1,"source_kind":"runtime","source_evidence_id":"urn:helianthus:evidence:sunspec-phase-one-v1","documentary_notation":"40001","documentary_address":40001,"documentary_address_base":"one_based_register","function_code":3,"logical_table":"holding_registers","normalized_zero_based_pdu_offset":40000,"word_count":1}`,
	))
	if err != nil {
		t.Fatalf("ParseNormalizationRecord: %v", err)
	}
	if err := attempt.Issue(0, batch.Views[0], normalization); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := attempt.Admit(); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if outcome, err := attempt.Claim(0); err != nil || outcome != modbusreg.ClaimSucceeded {
		t.Fatalf("Claim = %q, %v", outcome, err)
	}
	if err := attempt.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	observation, err := attempt.Publish(context.Background())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	encodedObservation, err := json.Marshal(observation)
	if err != nil {
		t.Fatalf("Marshal observation: %v", err)
	}
	if string(encodedObservation) == "{}" || !bytes.Contains(encodedObservation, []byte(`"runtime_normalizations"`)) ||
		!bytes.Contains(encodedObservation, []byte(config.Endpoint.Endpoint)) {
		t.Fatalf("observation JSON omitted exact provenance: %s", encodedObservation)
	}
	if err := adapter.RecordProfileObservation(ProfileObservationRecord{
		Observation:        observation,
		DetectionEvidence:  []string{"detector:standard-only"},
		ActivationEvidence: []string{"activation:runtime"},
	}); err != nil {
		t.Fatalf("RecordProfileObservation: %v", err)
	}
	retained, ok := adapter.ProfileObservation(observation.Spec().ProfileID, observation.SampleID())
	if !ok || retained.Observation.SampleID() != observation.SampleID() ||
		!reflect.DeepEqual(retained.DetectionEvidence, []string{"detector:standard-only"}) {
		t.Fatalf("retained profile observation changed: %+v, ok=%v", retained, ok)
	}
	spec := observation.Spec()
	if spec.PollGenerationID != 91 || spec.SourceValidity != modbusreg.SourceValid ||
		spec.SourceTime.State != modbusreg.SourceTimeObservedState ||
		!spec.SourceTime.Time.Equal(sourceTime) || !spec.LocalReceiptTime.Equal(receiptTime) ||
		spec.Endpoint != config.Endpoint.Endpoint || spec.UnitID != 1 || spec.SampleID == "" {
		t.Fatalf("sample facts changed: %+v", spec)
	}
	replay := observation.Replay()
	if len(replay) != 1 || replay[0].LogicalViewID() != 9001 ||
		replay[0].WireResponseID() != batch.Views[0].WireResponseID() ||
		replay[0].LogicalOffset() != 40000 ||
		!reflect.DeepEqual(replay[0].RawWords(), []uint16{0x5375}) {
		t.Fatalf("dependency provenance changed: %+v", replay)
	}
}
