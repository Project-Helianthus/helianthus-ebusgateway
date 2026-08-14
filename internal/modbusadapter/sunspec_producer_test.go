package modbusadapter

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

func TestSunSpecProducerDiscoversBoundedStandardChainAndPublishesExactObservation(t *testing.T) {
	words := sunSpecWords(1, 65, 101, 50, 102, 50, 103, 50, 0xffff, 0)
	listener, requests := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })

	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{
		PollGeneration: 41, DeadlineIdentity: 91,
	})
	if err != nil {
		t.Fatalf("Qualify(standard chain): %v", err)
	}
	if result.Outcome != SunSpecQualificationSupported || result.ObservationCount != 1 {
		t.Fatalf("qualification result = %#v; want one supported observation", result)
	}
	if got := result.Chain.Models(); !reflect.DeepEqual(sunSpecModelIDs(got), []uint16{1, 101, 102, 103}) {
		t.Fatalf("published SunSpec models = %v; want [1 101 102 103]", sunSpecModelIDs(got))
	}
	assertBoundedFC03Discovery(t, requests(), 1)

	record, ok := adapter.ProfileObservation("sunspec.phase1", result.SampleID)
	if !ok {
		t.Fatalf("registry-owned observation sunspec.phase1/%q was not retained", result.SampleID)
	}
	spec := record.Observation.Spec()
	if spec.SampleID != result.SampleID || spec.PollGenerationID != 41 ||
		spec.Endpoint != "tcp://"+listener.Addr().String() || spec.UnitID != 1 ||
		len(spec.Dependencies) != 1 {
		t.Fatalf("retained observation identity changed: %#v", spec)
	}
	if spec.SourceTime.State != modbusreg.SourceTimeUnavailableState || spec.LocalReceiptTime.IsZero() {
		t.Fatalf("retained observation time facts = source=%#v receipt=%v; want unavailable source time and local receipt", spec.SourceTime, spec.LocalReceiptTime)
	}
	view := spec.Dependencies[0].View.Record()
	if view.LogicalOffset != 40000 || view.LogicalWordCount == 0 ||
		view.PollGeneration != 41 || view.TransportGeneration == 0 ||
		view.ConnectionID == 0 || view.WireResponseID == 0 {
		t.Fatalf("retained source view lost exact sample identity: %#v", view)
	}
	for index := 0; index < reflect.TypeOf(producer).NumMethod(); index++ {
		name := reflect.TypeOf(producer).Method(index).Name
		if name == "Write" || name == "Set" || name == "Control" {
			t.Fatalf("SunSpec producer exposes forbidden write operation %q", name)
		}
	}
}

func TestSunSpecProducerReservesTerminatorInsideReadBudget(t *testing.T) {
	// The payload reaches the bound only when its following two-word header is
	// incorrectly excluded from the dynamic acquisition budget.
	words := sunSpecWords(1, 508, 0xffff, 0)
	listener, requests := serveSunSpecChain(t, words)
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	if _, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 44, DeadlineIdentity: 94}); err != nil {
		t.Fatalf("Qualify(boundary chain): %v", err)
	}
	assertBoundedFC03Discovery(t, requests(), 1)
}

func TestDeferredSunSpecModelRequiresCompleteTerminatedChain(t *testing.T) {
	// A float-family header alone is not sufficient evidence of a structurally
	// complete unsupported profile.
	if completeSunSpecChain([]uint16{0x5375, 0x6e53, 113, 1, 0}) {
		t.Fatal("truncated deferred chain was accepted as structurally complete")
	}
}

func TestSunSpecProducerClassifiesTruncatedDeferredChainAsIncoherentWithoutRetention(t *testing.T) {
	listener, _ := serveSunSpecChain(t, sunSpecWords(113, 1, 0xffff, 0))
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}
	identity := SunSpecPollIdentity{PollGeneration: 45, DeadlineIdentity: 95}
	views, err := producer.acquire(context.Background(), identity)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	result, err := producer.qualifyCapture(context.Background(), identity, views[:len(views)-1])
	if err != nil || result.Outcome != SunSpecQualificationIncoherentCapture {
		t.Fatalf("truncated deferred chain = %#v, %v; want incoherent capture", result, err)
	}
	if _, ok := adapter.ProfileObservation("sunspec.phase1", "gateway-modbus:1"); ok {
		t.Fatal("truncated deferred chain was retained")
	}
}

func TestSunSpecProducerRejectsInvalidModbusUnitID(t *testing.T) {
	listener, _ := serveSunSpecChain(t, sunSpecWords(0xffff, 0))
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	for _, unitID := range []byte{0, 248} {
		if producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{UnitID: unitID, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second}); err == nil || producer != nil {
			t.Fatalf("NewSunSpecProducer(unit %d) = %#v, %v; want rejection", unitID, producer, err)
		}
	}
}

func TestSunSpecProducerFailsClosedForObservedFloatProfileWithoutRetention(t *testing.T) {
	// Exact live-observed Fronius chain: deferred float-family models must not
	// become a partial or guessed phase-one observation.
	listener, _ := serveSunSpecChain(t, sunSpecWords(
		1, 65, 113, 60, 120, 26, 121, 30, 122, 44, 160, 88, 124, 24, 0xffff, 0,
	))
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}

	result, err := producer.Qualify(context.Background(), SunSpecPollIdentity{PollGeneration: 42, DeadlineIdentity: 92})
	if err != nil {
		t.Fatalf("Qualify(live float chain): %v", err)
	}
	if result.Outcome != SunSpecQualificationUnsupportedProfile || result.UnsupportedProfile == "" ||
		result.ObservationCount != 0 || result.SampleID != "" {
		t.Fatalf("float chain result = %#v; want typed fail-closed unsupported profile", result)
	}
	if _, ok := adapter.ProfileObservation("sunspec.phase1", "gateway-modbus:1"); ok {
		t.Fatal("float chain profile observation was retained")
	}
}

func TestSunSpecProducerRejectsMixedTransportGenerationWithoutPublishing(t *testing.T) {
	listener, _ := serveSunSpecChain(t, sunSpecWords(1, 65, 101, 50, 0xffff, 0))
	adapter, err := Start(context.Background(), integrationConfig(t, "tcp://"+listener.Addr().String()), realDialer, realFactory)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	producer, err := NewSunSpecProducer(adapter, SunSpecProducerConfig{
		UnitID: 1, AuthorizationScope: "smoke:fronius-readonly", ReadTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewSunSpecProducer: %v", err)
	}

	views, err := producer.acquire(context.Background(), SunSpecPollIdentity{PollGeneration: 43, DeadlineIdentity: 93})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	capture, _, err := sunSpecCapture(views)
	if err != nil {
		t.Fatalf("sunSpecCapture(valid): %v", err)
	}
	mixed := capture.Views()
	record := mixed[1].Record()
	record.TransportGeneration++
	mixed[1], err = modbusreg.NewLogicalViewSnapshot(record)
	if err != nil {
		t.Fatalf("NewLogicalViewSnapshot(mixed): %v", err)
	}
	if _, _, err := sunSpecCaptureSnapshots(mixed); err == nil {
		t.Fatal("mixed connection/generation capture was accepted")
	}
	if _, ok := adapter.ProfileObservation("sunspec.phase1", "gateway-modbus:1"); ok {
		t.Fatal("mixed capture was retained")
	}
}

type sunSpecReadRequest struct {
	UnitID    byte
	Function  modbus.FunctionCode
	Offset    uint16
	WordCount uint16
}

func serveSunSpecChain(t *testing.T, words []uint16) (net.Listener, func() []sunSpecReadRequest) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var mu sync.Mutex
	var requests []sunSpecReadRequest
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = connection.Close() }()
		for {
			header := make([]byte, 7)
			if _, err := io.ReadFull(connection, header); err != nil {
				return
			}
			length := int(binary.BigEndian.Uint16(header[4:6]))
			body := make([]byte, length-1)
			if _, err := io.ReadFull(connection, body); err != nil || len(body) != 5 {
				return
			}
			request := sunSpecReadRequest{
				UnitID: header[6], Function: modbus.FunctionCode(body[0]),
				Offset: binary.BigEndian.Uint16(body[1:3]), WordCount: binary.BigEndian.Uint16(body[3:5]),
			}
			mu.Lock()
			requests = append(requests, request)
			mu.Unlock()
			start := int(request.Offset) - 40000
			end := start + int(request.WordCount)
			if start < 0 || end > len(words) {
				return
			}
			response := make([]byte, 9+2*int(request.WordCount))
			copy(response[:2], header[:2])
			binary.BigEndian.PutUint16(response[4:6], uint16(3+2*int(request.WordCount)))
			response[6], response[7], response[8] = request.UnitID, byte(request.Function), byte(2*request.WordCount)
			for index, word := range words[start:end] {
				binary.BigEndian.PutUint16(response[9+2*index:], word)
			}
			if _, err := connection.Write(response); err != nil {
				return
			}
		}
	}()
	return listener, func() []sunSpecReadRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]sunSpecReadRequest(nil), requests...)
	}
}

func assertBoundedFC03Discovery(t *testing.T, requests []sunSpecReadRequest, unitID byte) {
	t.Helper()
	if len(requests) == 0 {
		t.Fatal("producer did not issue discovery reads")
	}
	if requests[0].Offset != 40000 || requests[0].WordCount != 1 {
		t.Fatalf("first discovery request = %+v; want exact base view 40000 x 1", requests[0])
	}
	var total uint16
	nextOffset := uint16(40000)
	for _, request := range requests {
		if request.UnitID != unitID || request.Function != modbus.FunctionReadHoldingRegisters ||
			request.Offset != nextOffset || request.WordCount == 0 || request.WordCount > 125 {
			t.Fatalf("discovery request = %+v; want bounded unit-1 FC03 from PDU 40000", request)
		}
		total += request.WordCount
		nextOffset += request.WordCount
	}
	if total > modbusreg.MaxSunSpecPhaseOneChainWords {
		t.Fatalf("discovery read budget = %d; want <= %d words", total, modbusreg.MaxSunSpecPhaseOneChainWords)
	}
}

func sunSpecWords(headers ...uint16) []uint16 {
	words := []uint16{0x5375, 0x6e53}
	for index := 0; index < len(headers); index += 2 {
		words = append(words, headers[index], headers[index+1])
		if headers[index] != 0xffff {
			words = append(words, make([]uint16, headers[index+1])...)
		}
	}
	return words
}

func sunSpecModelIDs(models []modbusreg.SunSpecPhaseOneModel) []uint16 {
	ids := make([]uint16, len(models))
	for index, model := range models {
		ids[index] = model.ID()
	}
	return ids
}
