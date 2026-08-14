package modbusadapter

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

func TestSunSpecProducerQualifiesExactObservedFroniusChainThroughRegistry(t *testing.T) {
	words := observedFroniusFloatWords()
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
	if result.Outcome != SunSpecQualificationGO || result.ObservationCount != 1 || result.SampleID == "" {
		t.Fatalf("qualification result = %#v; want one GO observation", result)
	}
	if result.CapabilityID != modbusreg.SunSpecThreePhaseMonitoringCapabilityID ||
		result.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
		result.FlavorID != modbusreg.SunSpecFroniusObservedFlavorID ||
		result.FlavorReason != modbusreg.SunSpecFroniusFlavorReasonMatched {
		t.Fatalf("registry decisions = %#v; want exact capability and flavor match", result)
	}
	if got := sunSpecWireKeys(result.Chain.Occurrences()); !reflect.DeepEqual(got, []modbusreg.SunSpecWireKey{
		{ModelID: 1, ModelLength: 65}, {ModelID: 113, ModelLength: 60},
		{ModelID: 120, ModelLength: 26}, {ModelID: 121, ModelLength: 30},
		{ModelID: 122, ModelLength: 44}, {ModelID: 160, ModelLength: 88},
		{ModelID: 124, ModelLength: 24},
	}) {
		t.Fatalf("published SunSpec chain = %v; want exact observed Fronius chain", got)
	}
	assertBoundedFC03Discovery(t, requests(), 1)

	views := result.Chain.SourceViews()
	if len(views) == 0 {
		t.Fatal("registry-selected chain lost its source views")
	}
	view := views[0].Record()
	if view.LogicalOffset != 40000 || view.LogicalWordCount == 0 ||
		view.PollGeneration != 41 || view.TransportGeneration == 0 ||
		view.ConnectionID == 0 || view.WireResponseID == 0 {
		t.Fatalf("chain source view lost exact sample identity: %#v", view)
	}
	for index := 0; index < reflect.TypeOf(producer).NumMethod(); index++ {
		name := reflect.TypeOf(producer).Method(index).Name
		if name == "Write" || name == "Set" || name == "Control" {
			t.Fatalf("SunSpec producer exposes forbidden write operation %q", name)
		}
	}
}

func TestSunSpecProducerReturnsNoGoForAdmittedCapabilityWithFlavorMismatch(t *testing.T) {
	listener, _ := serveSunSpecChain(t, admittedFloatChainWords())
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
	if result.Outcome != SunSpecQualificationNoGo ||
		result.CapabilityReason != modbusreg.SunSpecCapabilityReasonAdmitted ||
		result.FlavorReason != modbusreg.SunSpecFroniusFlavorReasonChainMismatch ||
		result.ObservationCount != 0 || result.SampleID != "" {
		t.Fatalf("flavor-mismatch result = %#v; want closed NO_GO without observation", result)
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

	result, err := producer.qualifyMixedGenerationForTest(context.Background(), SunSpecPollIdentity{
		PollGeneration: 43, DeadlineIdentity: 93,
	})
	if err != nil {
		t.Fatalf("qualifyMixedGenerationForTest: %v", err)
	}
	if result.Outcome != SunSpecQualificationStop || result.ObservationCount != 0 || result.SampleID != "" {
		t.Fatalf("mixed generation result = %#v; want STOP without publication", result)
	}
}

func TestSunSpecProducerSourceDelegatesSemanticSelectionToModbusreg(t *testing.T) {
	source, err := os.ReadFile("sunspec_producer.go")
	if err != nil {
		t.Fatalf("ReadFile(sunspec_producer.go): %v", err)
	}
	text := string(source)
	for _, required := range []string{
		"NewStandardSunSpecDecoderRegistry",
		"EvaluateThreePhaseMonitoring",
		"EvaluateFroniusObservedFlavor",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("producer does not delegate through %s", required)
		}
	}
	for _, forbidden := range []string{
		"sunspec.phase1",
		"deferredSunSpecModelInRaw",
		"id >= 111",
		"id >= 120",
		"id >= 200",
		"id >= 700",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("producer retains gateway-owned SunSpec classification %q", forbidden)
		}
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
	if requests[0].Offset != 40000 || requests[0].WordCount != 2 {
		t.Fatalf("first discovery request = %+v; want exact SunSpec signature at 40000 x 2", requests[0])
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
	if total > 1024 {
		t.Fatalf("discovery read budget = %d; want <= 1024 words", total)
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

func observedFroniusFloatWords() []uint16 {
	return sunSpecFixtureWords(
		sunSpecFixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		sunSpecFixtureModel{113, 60, floatInverterPayload()},
		sunSpecFixtureModel{120, 26, make([]uint16, 26)},
		sunSpecFixtureModel{121, 30, make([]uint16, 30)},
		sunSpecFixtureModel{122, 44, make([]uint16, 44)},
		sunSpecFixtureModel{160, 88, mpptPayload(4)},
		sunSpecFixtureModel{124, 24, make([]uint16, 24)},
	)
}

func admittedFloatChainWords() []uint16 {
	return sunSpecFixtureWords(
		sunSpecFixtureModel{1, 65, commonPayload("Fronius", "Symo GEN24 10.0", "1.41.11-1")},
		sunSpecFixtureModel{113, 60, floatInverterPayload()},
	)
}

type sunSpecFixtureModel struct {
	id, length uint16
	payload    []uint16
}

func sunSpecFixtureWords(models ...sunSpecFixtureModel) []uint16 {
	words := []uint16{0x5375, 0x6e53}
	for _, model := range models {
		words = append(words, model.id, model.length)
		words = append(words, model.payload...)
	}
	return append(words, 0xffff, 0)
}

func commonPayload(manufacturer, model, firmware string) []uint16 {
	payload := make([]uint16, 65)
	putSunSpecString(payload[0:16], manufacturer)
	putSunSpecString(payload[16:32], model)
	putSunSpecString(payload[40:48], firmware)
	putSunSpecString(payload[48:64], "synthetic")
	return payload
}

func floatInverterPayload() []uint16 {
	payload := make([]uint16, 60)
	// Model 113's status point is word 48 including the two-word header.
	payload[46] = 4
	return payload
}

func mpptPayload(modules uint16) []uint16 {
	payload := make([]uint16, 88)
	// Model 160's N point is word 8 including the two-word header.
	payload[6] = modules
	return payload
}

func putSunSpecString(words []uint16, value string) {
	data := []byte(value)
	for index := range words {
		var high, low byte
		if 2*index < len(data) {
			high = data[2*index]
		}
		if 2*index+1 < len(data) {
			low = data[2*index+1]
		}
		words[index] = uint16(high)<<8 | uint16(low)
	}
}

func sunSpecWireKeys(occurrences []modbusreg.SunSpecOccurrence) []modbusreg.SunSpecWireKey {
	keys := make([]modbusreg.SunSpecWireKey, len(occurrences))
	for index, occurrence := range occurrences {
		keys[index] = occurrence.WireKey
	}
	return keys
}
